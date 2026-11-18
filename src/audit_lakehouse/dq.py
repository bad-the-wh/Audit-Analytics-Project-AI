"""Silver layer: typing, data-quality rules and entry-level quarantine.

A journal entry is only usable for audit tests if every one of its lines passes every rule.
If any line fails, the whole entry is quarantined (never silently dropped), with the reasons.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RULES = {
    "DQ01_ACCOUNT_EXISTS": "account_id exists in the chart of accounts",
    "DQ02_USER_EXISTS": "user_id is filled and exists in the user list",
    "DQ03_VALID_POSTING_DATE": "posting_date is a valid yyyy-MM-dd date matching fiscal_period",
    "DQ04_VALID_CREATED_AT": "created_at is a valid timestamp",
    "DQ05_ONE_SIDED_AMOUNT": "exactly one of debit / credit is filled and positive",
    "DQ06_VALID_AMOUNT": "amounts are numeric and not negative",
}


def type_lines(raw: DataFrame) -> DataFrame:
    """Cast the raw string columns. Invalid values become NULL instead of failing the job."""
    def money(c: str):
        return F.expr(f"try_cast(nullif(trim({c}), '') AS DECIMAL(18,2))")

    return (
        raw.withColumn("posting_date_d", F.to_date(F.try_to_timestamp(F.col("posting_date"), F.lit("yyyy-MM-dd"))))
        .withColumn("created_at_ts", F.try_to_timestamp(F.col("created_at"), F.lit("yyyy-MM-dd HH:mm:ss")))
        .withColumn("debit_d", money("debit"))
        .withColumn("credit_d", money("credit"))
        .withColumn("line_no_i", F.expr("try_cast(line_no AS INT)"))
        .withColumn("user_id", F.nullif(F.trim(F.col("user_id")), F.lit("")))
    )


def apply_rules(lines: DataFrame, accounts: DataFrame, users: DataFrame) -> DataFrame:
    """Add one boolean column per rule (True = passed) and an array of the failed rule codes."""
    acc = accounts.select(F.col("account_id").alias("_acc"))
    usr = users.select(F.col("user_id").alias("_usr"))
    t = (
        type_lines(lines)
        .join(F.broadcast(acc), F.col("account_id") == F.col("_acc"), "left")
        .join(F.broadcast(usr), F.col("user_id") == F.col("_usr"), "left")
    )

    def blank(c):
        return F.col(c).isNull() | (F.trim(F.col(c)) == "")

    checks = {
        "DQ01_ACCOUNT_EXISTS": F.col("_acc").isNotNull(),
        "DQ02_USER_EXISTS": F.col("_usr").isNotNull(),
        "DQ03_VALID_POSTING_DATE": F.col("posting_date_d").isNotNull()
        & (F.date_format("posting_date_d", "yyyy-MM") == F.col("fiscal_period")),
        "DQ04_VALID_CREATED_AT": F.col("created_at_ts").isNotNull(),
        "DQ05_ONE_SIDED_AMOUNT": ((F.col("debit_d") > 0) & F.col("credit_d").isNull())
        | ((F.col("credit_d") > 0) & F.col("debit_d").isNull()),
        "DQ06_VALID_AMOUNT": (blank("debit") | (F.col("debit_d").isNotNull() & (F.col("debit_d") >= 0)))
        & (blank("credit") | (F.col("credit_d").isNotNull() & (F.col("credit_d") >= 0))),
    }
    for code, cond in checks.items():
        t = t.withColumn(code, F.coalesce(cond, F.lit(False)))
    failed = F.array_compact(F.array(*[F.when(~F.col(c), F.lit(c)) for c in checks]))
    return t.withColumn("failed_rules", failed).drop("_acc", "_usr")


def split_valid_and_quarantine(checked: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Quarantine every line of an entry as soon as one of its lines fails a rule."""
    entry_status = checked.groupBy("entry_id").agg(
        F.array_distinct(F.flatten(F.collect_list("failed_rules"))).alias("entry_failed_rules")
    )
    joined = checked.join(entry_status, "entry_id")
    bad = F.size("entry_failed_rules") > 0

    valid = joined.filter(~bad).select(
        "entry_id",
        F.col("line_no_i").alias("line_no"),
        F.col("posting_date_d").alias("posting_date"),
        F.col("created_at_ts").alias("created_at"),
        "fiscal_period",
        "account_id",
        "description",
        F.coalesce("debit_d", F.lit(0).cast("decimal(18,2)")).alias("debit"),
        F.coalesce("credit_d", F.lit(0).cast("decimal(18,2)")).alias("credit"),
        "currency",
        "user_id",
        "source",
        "reference",
        "_ingested_at",
    )
    quarantine = joined.filter(bad).select(
        "entry_id", "line_no", "posting_date", "created_at", "fiscal_period", "account_id", "description",
        "debit", "credit", "currency", "user_id", "source", "reference",
        F.col("failed_rules").alias("line_failed_rules"), "entry_failed_rules",
        F.current_timestamp().alias("quarantined_at"),
    )
    return valid, quarantine


def dq_results(checked: DataFrame, run_id: str) -> DataFrame:
    """One row per rule: lines checked, lines failed, pass rate. Feeds the DQ dashboard."""
    total = F.count(F.lit(1))
    rows = [
        checked.agg(
            F.lit(code).alias("rule_code"),
            F.lit(desc).alias("rule_description"),
            total.alias("checked_rows"),
            F.sum(F.when(~F.col(code), 1).otherwise(0)).alias("failed_rows"),
        )
        for code, desc in RULES.items()
    ]
    out = rows[0]
    for r in rows[1:]:
        out = out.unionByName(r)
    return (
        out.withColumn("pass_rate_pct", F.round(100 * (1 - F.col("failed_rows") / F.col("checked_rows")), 3))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("run_at", F.current_timestamp())
    )
