# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 — Bronze Layer (Raw Ingestion)
# MAGIC
# MAGIC Reads raw CSV files from DBFS and writes them as a **Delta table** with:
# MAGIC - Schema enforcement (all columns as strings at this stage — no data loss)
# MAGIC - Audit columns (`_ingested_at`, `_source_file`)
# MAGIC - Idempotent overwrite so re-running is safe

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/airline_etl")   # adjust to your repo mount path

from config.project_config import RAW_DATA_PATH, BRONZE_PATH, BRONZE_TABLE
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType
)

# COMMAND ----------

# MAGIC %md ### 1 · Discover raw files

# COMMAND ----------

raw_files = [f.path for f in dbutils.fs.ls(RAW_DATA_PATH) if f.path.endswith(".csv")]
print(f"Found {len(raw_files)} CSV file(s):")
for p in raw_files:
    print(" ", p)

# COMMAND ----------

# MAGIC %md ### 2 · Read CSV — all columns as STRING (bronze = land as-is)

# COMMAND ----------

df_raw = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "false")   # keep everything as string in bronze
         .option("multiLine", "false")
         .csv(raw_files)
)

print(f"Raw rows : {df_raw.count():,}")
print(f"Columns  : {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md ### 3 · Add audit / lineage columns

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("_ingested_at",  F.current_timestamp())
    .withColumn("_source_file",  F.input_file_name())
)

# COMMAND ----------

# MAGIC %md ### 4 · Write to Delta (overwrite — idempotent)

# COMMAND ----------

(
    df_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.autoOptimize.autoCompact", "true")
    .save(BRONZE_PATH)
)

print(f"Bronze table written to: {BRONZE_PATH}")

# COMMAND ----------

# MAGIC %md ### 5 · Register as a metastore table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

# COMMAND ----------

# MAGIC %md ### 6 · Quick sanity check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*)            AS total_rows,
# MAGIC     COUNT(DISTINCT OP_UNIQUE_CARRIER) AS carriers,
# MAGIC     MIN(FL_DATE)        AS earliest_date,
# MAGIC     MAX(FL_DATE)        AS latest_date
# MAGIC FROM airline_etl.bronze_flights;

# COMMAND ----------

print("Bronze ingestion complete. Proceed to 03_transform_silver.")
