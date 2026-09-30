"""Bronze -> Silver -> Gold pipeline for synthetic hospital patient-transport requests.

Mirrors the layering used in a Microsoft Fabric Lakehouse:
  * Bronze: raw files landed as-is (here: CSV with realistic defects).
  * Silver: typed, de-duplicated, conformed rows; rows failing data-quality rules go to a
            quarantine table with the rule that rejected them.
  * Gold:   star schema (fact + dimensions) and a daily KPI table, shaped for a Power BI
            semantic model in Direct Lake or Import mode.

Storage is local Parquet so the project runs anywhere; see README for the Fabric mapping.

Usage:
    python -m medallion.pipeline --rows 20000 --lakehouse lakehouse
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

log = logging.getLogger("medallion")

FACILITIES = [
    ("F001", "Riverside General", "West", 30),
    ("F002", "Lakeview Medical Center", "Midwest", 25),
    ("F003", "Harbor Community Hospital", "Northeast", 20),
    ("F004", "Summit Regional", "South", 15),
]
PRIORITIES = ["STAT", "Urgent", "Routine"]
TARGET_MINUTES = {"STAT": 10, "Urgent": 20, "Routine": 45}


# --------------------------------------------------------------------------- bronze
def land_bronze(rows: int, out: Path, seed: int = 11) -> Path:
    """Create a raw extract with the defects real source systems produce."""
    rng = np.random.default_rng(seed)
    requested = pd.Timestamp("2025-01-01") + pd.to_timedelta(rng.integers(0, 180 * 24 * 60, rows), unit="min")
    priority = rng.choice(PRIORITIES, rows, p=[0.1, 0.3, 0.6])
    target = np.array([TARGET_MINUTES[p] for p in priority])
    response = np.maximum(1, rng.gamma(2.0, target / 1.6)).round().astype(int)
    df = pd.DataFrame(
        {
            "request_id": [f"TR{i:07d}" for i in range(rows)],
            "facility_code": rng.choice([f[0] for f in FACILITIES], rows, p=[0.35, 0.3, 0.2, 0.15]),
            "priority": priority,
            "requested_at": requested.strftime("%Y-%m-%d %H:%M:%S"),
            "dispatched_at": (requested + pd.to_timedelta(response, unit="min")).strftime("%Y-%m-%d %H:%M:%S"),
            "completed_at": (requested + pd.to_timedelta(response + rng.integers(5, 40, rows), unit="min")).strftime("%Y-%m-%d %H:%M:%S"),
            "transporter_id": [f"T{n:03d}" for n in rng.integers(1, 80, rows)],
        }
    )
    # Inject defects: duplicates, lower-case codes, bad timestamps, cancelled rows, blanks.
    n = max(rows // 100, 1)
    idx = rng.choice(rows, n * 4, replace=False)
    df.loc[idx[:n], "facility_code"] = df.loc[idx[:n], "facility_code"].str.lower() + " "
    df.loc[idx[n:2 * n], "dispatched_at"] = "not recorded"
    df.loc[idx[2 * n:3 * n], "completed_at"] = None
    df.loc[idx[3 * n:], "priority"] = rng.choice(["stat", "ROUTINE", "Unknown"], n)
    df = pd.concat([df, df.sample(n, random_state=seed)], ignore_index=True)  # resent records
    out.mkdir(parents=True, exist_ok=True)
    path = out / "transport_requests.csv"
    df.to_csv(path, index=False)
    log.info("bronze: %d raw rows -> %s", len(df), path)
    return path


# --------------------------------------------------------------------------- silver
@dataclass(frozen=True)
class Rule:
    name: str
    sql_predicate: str  # rows where this is TRUE are rejected


DQ_RULES = [
    Rule("unknown_priority", "priority NOT IN ('STAT', 'Urgent', 'Routine')"),
    Rule("unknown_facility", "facility_code NOT IN (SELECT facility_code FROM facility)"),
    Rule("missing_dispatch_time", "dispatched_at IS NULL"),
    Rule("dispatch_before_request", "dispatched_at < requested_at"),
]


def build_silver(bronze_csv: Path, out: Path) -> dict[str, int]:
    con = duckdb.connect()
    con.register("facility", pd.DataFrame(FACILITIES, columns=["facility_code", "facility_name", "region", "beds_x10"]))
    con.execute(f"""
        CREATE TABLE typed AS
        SELECT
            trim(request_id)                                   AS request_id,
            upper(trim(facility_code))                         AS facility_code,
            CASE upper(trim(priority)) WHEN 'STAT' THEN 'STAT' WHEN 'URGENT' THEN 'Urgent'
                 WHEN 'ROUTINE' THEN 'Routine' ELSE trim(priority) END AS priority,
            try_strptime(requested_at,  '%Y-%m-%d %H:%M:%S')   AS requested_at,
            try_strptime(dispatched_at, '%Y-%m-%d %H:%M:%S')   AS dispatched_at,
            try_strptime(completed_at,  '%Y-%m-%d %H:%M:%S')   AS completed_at,
            trim(transporter_id)                               AS transporter_id
        FROM read_csv('{bronze_csv.as_posix()}', all_varchar = true, header = true)
        QUALIFY row_number() OVER (PARTITION BY trim(request_id) ORDER BY requested_at) = 1
    """)
    reject_case = " ".join(f"WHEN {r.sql_predicate} THEN '{r.name}'" for r in DQ_RULES)
    con.execute(f"CREATE TABLE checked AS SELECT *, CASE {reject_case} END AS rejected_rule FROM typed")
    out.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT * EXCLUDE (rejected_rule) FROM checked WHERE rejected_rule IS NULL) TO '{(out / 'transport_requests.parquet').as_posix()}' (FORMAT parquet)")
    con.execute(f"COPY (SELECT * FROM checked WHERE rejected_rule IS NOT NULL) TO '{(out / 'quarantine.parquet').as_posix()}' (FORMAT parquet)")
    stats = dict(con.execute("""
        SELECT 'raw_distinct', count(*) FROM typed UNION ALL
        SELECT 'accepted', count(*) FROM checked WHERE rejected_rule IS NULL UNION ALL
        SELECT 'quarantined', count(*) FROM checked WHERE rejected_rule IS NOT NULL
    """).fetchall())
    log.info("silver: %s", stats)
    return stats


# --------------------------------------------------------------------------- gold
def build_gold(silver: Path, out: Path) -> None:
    con = duckdb.connect()
    src = (silver / "transport_requests.parquet").as_posix()
    out.mkdir(parents=True, exist_ok=True)
    con.register("facility_src", pd.DataFrame(FACILITIES, columns=["facility_code", "facility_name", "region", "beds_x10"]))
    con.register("target_src", pd.DataFrame(list(TARGET_MINUTES.items()), columns=["priority", "target_response_minutes"]))
    statements = {
        "dim_facility": "SELECT row_number() OVER (ORDER BY facility_code) AS facility_key, facility_code, facility_name, region FROM facility_src",
        "dim_priority": "SELECT row_number() OVER (ORDER BY target_response_minutes) AS priority_key, priority, target_response_minutes FROM target_src",
        "dim_date": f"""SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key, CAST(d AS DATE) AS date,
                           year(d) AS year, month(d) AS month, strftime(d, '%b %Y') AS month_name, dayname(d) AS weekday
                        FROM range((SELECT min(requested_at)::DATE FROM '{src}'), (SELECT max(requested_at)::DATE FROM '{src}') + 1, INTERVAL 1 DAY) t(d)""",
    }
    for name, sql in statements.items():
        con.execute(f"CREATE TABLE {name} AS {sql}")
    con.execute(f"""
        CREATE TABLE fact_transport AS
        SELECT s.request_id,
               CAST(strftime(s.requested_at, '%Y%m%d') AS INTEGER) AS date_key,
               f.facility_key, p.priority_key, s.transporter_id,
               date_diff('minute', s.requested_at, s.dispatched_at)  AS response_minutes,
               date_diff('minute', s.requested_at, s.completed_at)   AS total_minutes,
               date_diff('minute', s.requested_at, s.dispatched_at) <= p.target_response_minutes AS met_target
        FROM '{src}' s
        JOIN dim_facility f USING (facility_code)
        JOIN dim_priority p USING (priority)
    """)
    con.execute("""
        CREATE TABLE kpi_daily_facility AS
        SELECT d.date, f.facility_name, count(*) AS requests,
               round(avg(response_minutes), 1) AS avg_response_minutes,
               round(quantile_cont(response_minutes, 0.9), 1) AS p90_response_minutes,
               round(avg(met_target::INT), 4) AS on_time_rate
        FROM fact_transport t JOIN dim_date d USING (date_key) JOIN dim_facility f USING (facility_key)
        GROUP BY ALL ORDER BY 1, 2
    """)
    for table in ["dim_facility", "dim_priority", "dim_date", "fact_transport", "kpi_daily_facility"]:
        con.execute(f"COPY {table} TO '{(out / f'{table}.parquet').as_posix()}' (FORMAT parquet)")
        log.info("gold: %-20s %7d rows", table, con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])


def run(rows: int, lakehouse: Path) -> dict[str, int]:
    bronze = land_bronze(rows, lakehouse / "bronze")
    stats = build_silver(bronze, lakehouse / "silver")
    build_gold(lakehouse / "silver", lakehouse / "gold")
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=20000)
    parser.add_argument("--lakehouse", type=Path, default=Path("lakehouse"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.rows < 100:
        raise SystemExit("--rows must be at least 100")
    run(args.rows, args.lakehouse)


if __name__ == "__main__":
    main()
