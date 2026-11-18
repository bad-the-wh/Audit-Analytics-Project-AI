# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver
# MAGIC Types the data and applies six data-quality rules. Any entry with a failing line goes **entirely** to
# MAGIC quarantine with the failed rule codes: nothing is silently dropped. Rule results are appended to
# MAGIC `dq_results` so pass rates can be tracked run after run.

# COMMAND ----------

import os
import sys
import uuid

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from audit_lakehouse import dq

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "audit")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
t = lambda name: f"{catalog}.{schema}.{name}"  # noqa: E731
run_id = str(uuid.uuid4())

# COMMAND ----------

lines = spark.table(t("bronze_journal_lines"))
accounts = spark.table(t("bronze_accounts"))
users = spark.table(t("bronze_users"))

checked = dq.apply_rules(lines, accounts, users)
valid, quarantine = dq.split_valid_and_quarantine(checked)

valid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_journal_lines"))
quarantine.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_quarantine"))
dq.dq_results(checked, run_id).write.mode("append").saveAsTable(t("dq_results"))

# COMMAND ----------

bronze_n = lines.count()
silver_n = spark.table(t("silver_journal_lines")).count()
quarantine_n = spark.table(t("silver_quarantine")).count()
print(f"bronze {bronze_n:,} = silver {silver_n:,} + quarantine {quarantine_n:,}")
assert bronze_n == silver_n + quarantine_n, "Row counts do not reconcile between bronze and silver + quarantine"

# COMMAND ----------

display(spark.table(t("dq_results")).filter(f"run_id = '{run_id}'"))

# COMMAND ----------

display(spark.sql(f"""
  SELECT entry_id, entry_failed_rules, count(*) AS lines
  FROM {t('silver_quarantine')}
  GROUP BY entry_id, entry_failed_rules
  ORDER BY entry_id
"""))
