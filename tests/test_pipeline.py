import pandas as pd
import pytest

from medallion import pipeline


@pytest.fixture(scope="module")
def lakehouse(tmp_path_factory):
    root = tmp_path_factory.mktemp("lakehouse")
    stats = pipeline.run(3000, root)
    return root, stats


def read(root, layer, table):
    return pd.read_parquet(root / layer / f"{table}.parquet")


def test_row_counts_reconcile_across_layers(lakehouse):
    root, stats = lakehouse
    assert stats["accepted"] + stats["quarantined"] == stats["raw_distinct"] == 3000
    assert len(read(root, "gold", "fact_transport")) == stats["accepted"]


def test_silver_removes_duplicates_and_normalises_codes(lakehouse):
    root, _ = lakehouse
    silver = read(root, "silver", "transport_requests")
    assert silver["request_id"].is_unique
    assert set(silver["facility_code"]) <= {f[0] for f in pipeline.FACILITIES}
    assert set(silver["priority"]) <= set(pipeline.PRIORITIES)


def test_every_quarantined_row_names_its_rule(lakehouse):
    root, _ = lakehouse
    q = read(root, "silver", "quarantine")
    assert len(q) > 0
    assert set(q["rejected_rule"]) <= {r.name for r in pipeline.DQ_RULES}


def test_gold_fact_has_valid_foreign_keys(lakehouse):
    root, _ = lakehouse
    fact = read(root, "gold", "fact_transport")
    for dim, key in [("dim_facility", "facility_key"), ("dim_priority", "priority_key"), ("dim_date", "date_key")]:
        assert fact[key].isin(read(root, "gold", dim)[key]).all(), dim
    assert (fact["response_minutes"] >= 0).all()


def test_kpi_table_matches_fact(lakehouse):
    root, _ = lakehouse
    kpi = read(root, "gold", "kpi_daily_facility")
    assert kpi["requests"].sum() == len(read(root, "gold", "fact_transport"))
    assert kpi["on_time_rate"].between(0, 1).all()


def test_rejects_tiny_runs(monkeypatch):
    monkeypatch.setattr("sys.argv", ["pipeline", "--rows", "10"])
    with pytest.raises(SystemExit):
        pipeline.main()
