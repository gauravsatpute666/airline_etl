# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 — Setup
# MAGIC
# MAGIC **What this notebook does**
# MAGIC 1. Installs the Kaggle CLI inside the cluster
# MAGIC 2. Downloads the flight-delay dataset using Kaggle API credentials stored in Databricks Secrets
# MAGIC 3. Moves raw CSV files to DBFS
# MAGIC 4. Creates the `airline_etl` database
# MAGIC
# MAGIC **Pre-requisites**
# MAGIC - Create a Databricks Secret Scope named `kaggle` with keys `username` and `key`
# MAGIC   ```
# MAGIC   databricks secrets create-scope --scope kaggle
# MAGIC   databricks secrets put --scope kaggle --key username
# MAGIC   databricks secrets put --scope kaggle --key key
# MAGIC   ```
# MAGIC - The cluster must have internet access

# COMMAND ----------

import os, subprocess, shutil

# COMMAND ----------

# MAGIC %md ### 1 · Install Kaggle CLI

# COMMAND ----------

subprocess.run(["pip", "install", "-q", "kaggle"], check=True)

# COMMAND ----------

# MAGIC %md ### 2 · Inject Kaggle credentials from Databricks Secrets

# COMMAND ----------

kaggle_dir = "/root/.kaggle"
os.makedirs(kaggle_dir, exist_ok=True)

username = dbutils.secrets.get(scope="kaggle", key="username")
api_key  = dbutils.secrets.get(scope="kaggle", key="key")

with open(f"{kaggle_dir}/kaggle.json", "w") as f:
    import json
    json.dump({"username": username, "key": api_key}, f)

os.chmod(f"{kaggle_dir}/kaggle.json", 0o600)
print("Kaggle credentials configured.")

# COMMAND ----------

# MAGIC %md ### 3 · Download the dataset

# COMMAND ----------

DOWNLOAD_DIR = "/tmp/airline_raw"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

result = subprocess.run(
    ["kaggle", "datasets", "download", "-d", "robikscube/flight-delay-dataset-20182022",
     "--unzip", "-p", DOWNLOAD_DIR],
    capture_output=True, text=True
)
print(result.stdout)
if result.returncode != 0:
    raise RuntimeError(result.stderr)

print("Files downloaded:")
for f in os.listdir(DOWNLOAD_DIR):
    print(" ", f)

# COMMAND ----------

# MAGIC %md ### 4 · Copy raw files to DBFS

# COMMAND ----------

DBFS_RAW = "dbfs:/airline_etl/raw"
dbutils.fs.mkdirs(DBFS_RAW)

for fname in os.listdir(DOWNLOAD_DIR):
    local_path = f"file:{DOWNLOAD_DIR}/{fname}"
    dbfs_path  = f"{DBFS_RAW}/{fname}"
    dbutils.fs.cp(local_path, dbfs_path)
    print(f"Copied → {dbfs_path}")

# COMMAND ----------

# MAGIC %md ### 5 · Create database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS airline_etl
# MAGIC COMMENT 'Airline on-time performance — ETL pipeline';

# COMMAND ----------

print("Setup complete. Proceed to 02_ingest_bronze.")
