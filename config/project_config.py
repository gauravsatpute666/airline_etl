# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project Configuration
# MAGIC Central config for the Airline ETL pipeline. Import this in all notebooks.

# COMMAND ----------

# Kaggle dataset: https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022
KAGGLE_DATASET = "robikscube/flight-delay-dataset-20182022"

# DBFS paths (adjust to your Unity Catalog volume if UC is enabled)
RAW_DATA_PATH    = "dbfs:/airline_etl/raw"
BRONZE_PATH      = "dbfs:/airline_etl/bronze"
SILVER_PATH      = "dbfs:/airline_etl/silver"
GOLD_PATH        = "dbfs:/airline_etl/gold"

# Delta table names
BRONZE_TABLE = "airline_etl.bronze_flights"
SILVER_TABLE = "airline_etl.silver_flights"
GOLD_CARRIER_TABLE  = "airline_etl.gold_carrier_perf"
GOLD_ROUTE_TABLE    = "airline_etl.gold_route_delays"
GOLD_MONTHLY_TABLE  = "airline_etl.gold_monthly_trends"

# Null-fill defaults
NULL_DEFAULTS = {
    "DEP_DELAY"        : 0.0,
    "ARR_DELAY"        : 0.0,
    "CARRIER_DELAY"    : 0.0,
    "WEATHER_DELAY"    : 0.0,
    "NAS_DELAY"        : 0.0,
    "SECURITY_DELAY"   : 0.0,
    "LATE_AIRCRAFT_DELAY": 0.0,
    "CANCELLED"        : 0,
    "DIVERTED"         : 0,
    "AIR_TIME"         : 0.0,
    "DISTANCE"         : 0.0,
}
