"""Generate a small ledger, run the whole pipeline, and require every planted anomaly to be found."""
import generate_ledger
import run_local


def test_pipeline_finds_every_planted_anomaly(spark, tmp_path):
    raw = str(tmp_path / "raw")
    generate_ledger.generate(raw, scale=0.05, seed=7)
    out = run_local.run(spark, raw)

    for r in out["validation"].collect():
        assert r.missed == 0, f"{r.rule}: {r.missed} planted anomalies missed"
        assert r.false_positives == 0, f"{r.rule}: {r.false_positives} false positives"

    q = out["quarantine_check"].first()
    assert q.missed == 0 and q.wrongly_quarantined == 0
    assert out["bronze_lines"] == out["silver_lines"] + out["quarantined_lines"]
