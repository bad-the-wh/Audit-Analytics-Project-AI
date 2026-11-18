# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the schema and the volume that receives the raw files.
# MAGIC
# MAGIC **After running this notebook:** generate the data locally (`python data_generator/generate_ledger.py --out data/raw`),
# MAGIC then upload the six CSV files to the volume: *Catalog → your catalog → audit → Volumes → raw → Upload to this volume*.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "audit")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw")
print(f"Upload the CSV files to /Volumes/{catalog}/{schema}/raw/")

# COMMAND ----------

display(dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/raw/"))
