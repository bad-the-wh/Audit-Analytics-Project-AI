# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze
# MAGIC Loads the raw CSV exports into Delta tables **exactly as received** (all strings), plus ingestion metadata.
# MAGIC Overwriting on each run keeps the load idempotent: re-running never duplicates data.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from audit_lakehouse import bronze

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "audit")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
raw = f"/Volumes/{catalog}/{schema}/raw"

# COMMAND ----------

files = {
    "bronze_journal_lines": "journal_lines.csv",
    "bronze_accounts": "accounts.csv",
    "bronze_users": "users.csv",
    "bronze_period_calendar": "period_calendar.csv",
    "ref_answer_key": "answer_key.csv",
    "ref_dq_answer_key": "dq_answer_key.csv",
}
for table, file in files.items():
    df = bronze.read_raw_csv(spark, f"{raw}/{file}")
    (df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.{table}"))
    print(f"{table:<26} {spark.table(f'{catalog}.{schema}.{table}').count():>10,} rows")
