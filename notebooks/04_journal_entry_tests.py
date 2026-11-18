# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Journal entry tests
# MAGIC Runs eight audit rules over **100 % of entries** and ranks them with a weighted risk score, so auditors
# MAGIC start with the riskiest postings instead of a random sample.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from audit_lakehouse import gold, jet

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "audit")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
t = lambda name: f"{catalog}.{schema}.{name}"  # noqa: E731

# COMMAND ----------

entries = spark.table(t("gold_fact_journal_entries"))
lines = spark.table(t("gold_fact_journal_lines"))
periods = spark.table(t("gold_dim_periods"))

results = jet.run_journal_entry_tests(entries, lines, periods)
results.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_jet_results"))
jet.rule_summary(spark.table(t("gold_jet_results"))).write.mode("overwrite").saveAsTable(t("gold_jet_rule_summary"))

# COMMAND ----------

display(spark.table(t("gold_jet_rule_summary")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Where an auditor should start

# COMMAND ----------

display(spark.sql(f"""
  SELECT entry_id, posting_date, created_at, user_id, department, source, description,
         total_debit, risk_score, flags
  FROM {t('gold_jet_results')}
  WHERE n_flags > 0
  ORDER BY risk_score DESC, total_debit DESC
  LIMIT 50
"""))
