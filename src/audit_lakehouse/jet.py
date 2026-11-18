"""Journal entry tests (JET): the rules auditors run over 100% of entries to find risky postings.

Each rule adds a boolean flag column named after the risk it detects. A weighted risk score ranks
entries so auditors can start with the riskiest ones.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# flag name -> (weight in the risk score, description shown to auditors)
RULES = {
    "unbalanced_entry": (5, "Total debits differ from total credits"),
    "duplicate_entry": (4, "Same date, description and lines as an earlier entry"),
    "post_close_entry": (4, "Created after the period was closed (backdated)"),
    "below_approval_limit": (3, "Manual entry between 95% and 100% of the user's approval limit"),
    "unusual_user_account": (3, "Department posting to an account it almost never uses"),
    "round_amount": (2, "Total of at least 10,000 and an exact multiple of 1,000"),
    "weekend_posting": (2, "Created by a person on a Saturday or Sunday"),
    "late_night_posting": (2, "Created by a person between 22:00 and 05:00"),
}

RARE_PAIR_MAX_LINES = 5


def rare_department_accounts(fact_lines: DataFrame, max_lines: int = RARE_PAIR_MAX_LINES) -> DataFrame:
    """Department-account pairs used in at most `max_lines` lines over the year."""
    return (
        fact_lines.groupBy("department", "account_id").count()
        .filter(F.col("count") <= max_lines)
        .select("department", "account_id")
    )


def run_journal_entry_tests(entries: DataFrame, fact_lines: DataFrame, periods: DataFrame) -> DataFrame:
    human = F.col("source") != F.lit("SYSTEM")
    hour = F.hour("created_at")

    rare_entries = (
        fact_lines.join(F.broadcast(rare_department_accounts(fact_lines)), ["department", "account_id"])
        .select("entry_id").distinct().withColumn("unusual_user_account", F.lit(True))
    )
    dup_window = Window.partitionBy("posting_date", "description", "line_signature").orderBy("created_at", "entry_id")

    t = (
        entries.join(F.broadcast(periods), "fiscal_period", "left")
        .join(rare_entries, "entry_id", "left")
        .withColumn("unbalanced_entry", F.abs(F.col("total_debit") - F.col("total_credit")) > F.lit(0.005))
        .withColumn("duplicate_entry", F.row_number().over(dup_window) > 1)
        .withColumn("post_close_entry", F.to_date("created_at") > F.col("close_date"))
        .withColumn(
            "below_approval_limit",
            (F.col("source") == "MANUAL") & (F.col("approval_limit") > 0)
            & (F.col("total_debit") >= F.col("approval_limit") * F.lit(0.95))
            & (F.col("total_debit") < F.col("approval_limit")),
        )
        .withColumn("unusual_user_account", F.coalesce("unusual_user_account", F.lit(False)))
        .withColumn("round_amount", (F.col("total_debit") >= 10000) & ((F.col("total_debit") % 1000) == 0))
        .withColumn("weekend_posting", human & F.dayofweek("created_at").isin(1, 7))
        .withColumn("late_night_posting", human & ((hour >= 22) | (hour < 5)))
    )
    for flag in RULES:
        t = t.withColumn(flag, F.coalesce(F.col(flag), F.lit(False)))

    score = sum(F.when(F.col(flag), F.lit(w)).otherwise(F.lit(0)) for flag, (w, _) in RULES.items())
    flags = F.array_compact(F.array(*[F.when(F.col(f), F.lit(f)) for f in RULES]))
    return t.withColumn("risk_score", score).withColumn("flags", flags).withColumn("n_flags", F.size("flags"))


def rule_summary(results: DataFrame) -> DataFrame:
    """Number and share of entries flagged per rule."""
    total = results.count()
    rows = [
        results.agg(
            F.lit(flag).alias("rule"),
            F.lit(desc).alias("description"),
            F.lit(w).alias("weight"),
            F.sum(F.col(flag).cast("int")).alias("flagged_entries"),
        )
        for flag, (w, desc) in RULES.items()
    ]
    out = rows[0]
    for r in rows[1:]:
        out = out.unionByName(r)
    return out.withColumn("flagged_pct", F.round(100 * F.col("flagged_entries") / F.lit(total), 3))
