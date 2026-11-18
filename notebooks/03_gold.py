# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold
# MAGIC Business-ready tables for the audit tests and Power BI: a journal-line fact, a journal-entry fact
# MAGIC (one row per entry, with totals and a line signature) and the account, user and period dimensions.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from audit_lakehouse import gold

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "audit")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
t = lambda name: f"{catalog}.{schema}.{name}"  # noqa: E731


def save(df, name):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(name))
    print(f"{name:<28} {spark.table(t(name)).count():>10,} rows")

# COMMAND ----------

accounts = spark.table(t("bronze_accounts"))
users = spark.table(t("bronze_users"))
calendar = spark.table(t("bronze_period_calendar"))
silver = spark.table(t("silver_journal_lines"))

save(gold.dim_accounts(accounts), "gold_dim_accounts")
save(gold.dim_users(users), "gold_dim_users")
save(gold.dim_periods(calendar), "gold_dim_periods")
save(gold.fact_journal_lines(silver, accounts, users), "gold_fact_journal_lines")
save(gold.fact_journal_entries(spark.table(t("gold_fact_journal_lines")), users), "gold_fact_journal_entries")

# COMMAND ----------

# MAGIC %md
# MAGIC Every entry in gold must balance except the ones the audit tests are meant to catch. Quick look at the totals:

# COMMAND ----------

display(spark.sql(f"""
  SELECT fiscal_period, count(*) AS entries, sum(total_debit) AS debits, sum(total_credit) AS credits
  FROM {t('gold_fact_journal_entries')}
  GROUP BY fiscal_period ORDER BY fiscal_period
"""))
