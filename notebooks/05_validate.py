# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Validate against the answer key
# MAGIC The generator recorded every anomaly and data-quality defect it planted. This notebook checks that the
# MAGIC pipeline found all of them and flagged nothing else. It **fails the job** if anything is missed.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from audit_lakehouse import validate

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "audit")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
t = lambda name: f"{catalog}.{schema}.{name}"  # noqa: E731

# COMMAND ----------

scores = validate.score_against_answer_key(spark.table(t("gold_jet_results")), spark.table(t("ref_answer_key")))
scores.write.mode("overwrite").saveAsTable(t("gold_validation"))
display(spark.table(t("gold_validation")))

# COMMAND ----------

q = validate.score_quarantine(spark.table(t("silver_quarantine")), spark.table(t("ref_dq_answer_key")))
display(q)

# COMMAND ----------

missed = spark.table(t("gold_validation")).agg({"missed": "sum"}).first()[0]
q_row = q.first()
assert missed == 0, f"{missed} planted anomalies were not detected"
assert q_row.missed == 0 and q_row.wrongly_quarantined == 0, "Quarantine does not match the defect answer key"
print("All planted anomalies detected; all defective entries quarantined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta time travel: the audit trail of the results table
# MAGIC Every write is versioned. An auditor can see exactly what the results looked like at any earlier run.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {t('gold_jet_results')}"))
