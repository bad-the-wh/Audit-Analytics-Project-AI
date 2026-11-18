"""Gold layer: journal-line facts, journal-entry facts and dimensions, ready for audit tests and Power BI."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def dim_accounts(accounts: DataFrame) -> DataFrame:
    return accounts.select("account_id", "account_name", "account_type")


def dim_users(users: DataFrame) -> DataFrame:
    return users.select("user_id", "name", "department", "role", F.col("approval_limit").cast("decimal(18,2)").alias("approval_limit"))


def dim_periods(calendar: DataFrame) -> DataFrame:
    return calendar.select("fiscal_period", F.to_date("close_date").alias("close_date"))


def fact_journal_lines(silver_lines: DataFrame, accounts: DataFrame, users: DataFrame) -> DataFrame:
    return (
        silver_lines.join(F.broadcast(dim_accounts(accounts)), "account_id", "left")
        .join(F.broadcast(dim_users(users).select("user_id", "department")), "user_id", "left")
    )


def fact_journal_entries(fact_lines: DataFrame, users: DataFrame) -> DataFrame:
    """One row per journal entry, with totals and a signature of its lines (used to find duplicates)."""
    line_sig = F.concat_ws(":", "account_id", F.col("debit").cast("string"), F.col("credit").cast("string"))
    entries = fact_lines.groupBy("entry_id").agg(
        F.min("posting_date").alias("posting_date"),
        F.min("created_at").alias("created_at"),
        F.min("fiscal_period").alias("fiscal_period"),
        F.min("user_id").alias("user_id"),
        F.min("source").alias("source"),
        F.min("description").alias("description"),
        F.min("reference").alias("reference"),
        F.sum("debit").alias("total_debit"),
        F.sum("credit").alias("total_credit"),
        F.count(F.lit(1)).alias("n_lines"),
        F.concat_ws("|", F.array_sort(F.collect_list(line_sig))).alias("line_signature"),
    )
    return entries.join(F.broadcast(dim_users(users).select("user_id", "department", "approval_limit")), "user_id", "left")
