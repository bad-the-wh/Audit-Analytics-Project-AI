"""Run the whole pipeline locally with Spark (no Databricks needed) and print the validation results.

Usage:
  python data_generator/generate_ledger.py --out data/raw
  python scripts/run_local.py --raw data/raw
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import SparkSession  # noqa: E402

from audit_lakehouse import bronze, dq, gold, jet, validate  # noqa: E402


def run(spark: SparkSession, raw: str):
    lines = bronze.read_raw_csv(spark, f"{raw}/journal_lines.csv")
    accounts = bronze.read_raw_csv(spark, f"{raw}/accounts.csv")
    users = bronze.read_raw_csv(spark, f"{raw}/users.csv")
    calendar = bronze.read_raw_csv(spark, f"{raw}/period_calendar.csv")

    checked = dq.apply_rules(lines, accounts, users).cache()
    silver, quarantine = dq.split_valid_and_quarantine(checked)
    dq_res = dq.dq_results(checked, "local")

    f_lines = gold.fact_journal_lines(silver, accounts, users).cache()
    f_entries = gold.fact_journal_entries(f_lines, users)
    results = jet.run_journal_entry_tests(f_entries, f_lines, gold.dim_periods(calendar)).cache()

    key = spark.read.option("header", True).csv(f"{raw}/answer_key.csv")
    dq_key = spark.read.option("header", True).csv(f"{raw}/dq_answer_key.csv")
    return {
        "bronze_lines": lines.count(),
        "silver_lines": silver.count(),
        "quarantined_lines": quarantine.count(),
        "dq_results": dq_res,
        "results": results,
        "rule_summary": jet.rule_summary(results),
        "validation": validate.score_against_answer_key(results, key),
        "quarantine_check": validate.score_quarantine(quarantine, dq_key),
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    a = p.parse_args()
    spark = (SparkSession.builder.master("local[*]").appName("audit-lakehouse")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    out = run(spark, a.raw)
    print(f"bronze lines: {out['bronze_lines']:,}  silver lines: {out['silver_lines']:,}  "
          f"quarantined lines: {out['quarantined_lines']:,}")
    out["dq_results"].select("rule_code", "checked_rows", "failed_rows", "pass_rate_pct").show(truncate=False)
    out["quarantine_check"].show()
    out["rule_summary"].select("rule", "weight", "flagged_entries", "flagged_pct").show(truncate=False)
    out["validation"].show(truncate=False)
    (out["results"].filter("n_flags > 0").orderBy("risk_score", ascending=False)
     .select("entry_id", "user_id", "source", "total_debit", "risk_score", "flags").show(10, truncate=False))
    spark.stop()
