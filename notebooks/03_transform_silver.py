# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — Silver Layer (Cleansed & Typed)
# MAGIC
# MAGIC Applies the following transformations on top of the bronze table:
# MAGIC
# MAGIC | # | Transformation | Detail |
# MAGIC |---|----------------|--------|
# MAGIC | 1 | **Cast types** | Numeric & boolean columns from STRING → proper types |
# MAGIC | 2 | **Parse dates** | `FL_DATE` → `DateType`; departure/arrival times → proper `TimestampType` |
# MAGIC | 3 | **Null handling** | Fill numeric nulls with 0, flag rows with missing critical fields |
# MAGIC | 4 | **Standardise strings** | Trim whitespace, upper-case carrier/airport codes |
# MAGIC | 5 | **Derived columns** | `IS_DELAYED`, `IS_CANCELLED`, `TOTAL_DELAY_MIN`, `FLIGHT_DURATION_HR` |
# MAGIC | 6 | **Drop junk rows** | Remove rows where origin = destination or distance <= 0 |

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/airline_etl")

from config.project_config import BRONZE_TABLE, SILVER_PATH, SILVER_TABLE, NULL_DEFAULTS
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, DateType, TimestampType, ShortType
)

# COMMAND ----------

# MAGIC %md ### 1 · Load bronze

# COMMAND ----------

df = spark.read.table(BRONZE_TABLE)
print(f"Bronze rows: {df.count():,}")

# COMMAND ----------

# MAGIC %md ### 2 · Cast numeric columns

# COMMAND ----------

INT_COLS = [
    "YEAR", "QUARTER", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK",
    "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "TAXI_OUT",
    "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME",
    "ARR_DELAY", "CANCELLED", "DIVERTED", "CRS_ELAPSED_TIME",
    "ACTUAL_ELAPSED_TIME", "AIR_TIME", "FLIGHTS", "DISTANCE",
    "DISTANCE_GROUP", "CARRIER_DELAY", "WEATHER_DELAY",
    "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY",
    "DIV_AIRPORT_LANDINGS", "DEP_DELAY_GROUP", "ARR_DELAY_GROUP",
]

df_typed = df
for col in INT_COLS:
    if col in df.columns:
        df_typed = df_typed.withColumn(col, F.col(col).cast(DoubleType()))

# COMMAND ----------

# MAGIC %md ### 3 · Parse dates and times

# COMMAND ----------

# FL_DATE arrives as "YYYY-MM-DD"
df_typed = df_typed.withColumn(
    "FL_DATE", F.to_date(F.col("FL_DATE"), "yyyy-MM-dd")
)

# DEP_TIME / ARR_TIME are stored as HHMM integers (e.g. 835 = 08:35)
# Reconstruct as proper timestamps using FL_DATE + time offset
def hhmm_to_minutes(col_name):
    """Convert HHMM integer column to minutes-since-midnight."""
    c = F.col(col_name)
    hours   = F.floor(c / 100).cast(IntegerType())
    minutes = (c % 100).cast(IntegerType())
    return hours * 60 + minutes

for time_col in ["DEP_TIME", "ARR_TIME", "CRS_DEP_TIME", "CRS_ARR_TIME", "WHEELS_OFF", "WHEELS_ON"]:
    if time_col in df_typed.columns:
        df_typed = df_typed.withColumn(
            f"{time_col}_TS",
            F.expr(f"TIMESTAMP(FL_DATE) + INTERVAL {hhmm_to_minutes(time_col)} MINUTES")
            if False   # placeholder — use the expression below
            else F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.date_format(F.col("FL_DATE"), "yyyy-MM-dd"),
                    F.lpad((F.col(time_col) / 100).cast(IntegerType()).cast("string"), 2, "0").cast("string")
                    + F.lit(":") +
                    F.lpad((F.col(time_col) % 100).cast(IntegerType()).cast("string"), 2, "0")
                ),
                "yyyy-MM-dd HH:mm"
            )
        )

# COMMAND ----------

# MAGIC %md ### 4 · Null handling

# COMMAND ----------

# Fill numeric nulls defined in config
df_clean = df_typed.fillna(NULL_DEFAULTS)

# Flag rows missing critical identity fields
df_clean = df_clean.withColumn(
    "_has_missing_critical",
    F.when(
        F.col("OP_UNIQUE_CARRIER").isNull() |
        F.col("FL_DATE").isNull() |
        F.col("ORIGIN").isNull() |
        F.col("DEST").isNull() |
        F.col("FL_NUM").isNull(),
        F.lit(True)
    ).otherwise(F.lit(False))
)

missing_count = df_clean.filter(F.col("_has_missing_critical")).count()
print(f"Rows with missing critical fields: {missing_count:,}")

# COMMAND ----------

# MAGIC %md ### 5 · Standardise string columns

# COMMAND ----------

STR_COLS = [
    "OP_UNIQUE_CARRIER", "OP_CARRIER_AIRLINE_ID", "OP_CARRIER",
    "TAIL_NUM", "ORIGIN", "DEST",
    "ORIGIN_CITY_NAME", "DEST_CITY_NAME",
    "ORIGIN_STATE_ABR", "DEST_STATE_ABR",
]

for col in STR_COLS:
    if col in df_clean.columns:
        df_clean = df_clean.withColumn(col, F.upper(F.trim(F.col(col))))

# COMMAND ----------

# MAGIC %md ### 6 · Derived / enrichment columns

# COMMAND ----------

df_silver = (
    df_clean

    # On-time flag: delayed if departure delay > 15 min (FAA definition)
    .withColumn("IS_DELAYED",   F.when(F.col("DEP_DELAY") > 15, True).otherwise(False))

    # Cancellation / diversion boolean
    .withColumn("IS_CANCELLED", F.col("CANCELLED").cast("boolean"))
    .withColumn("IS_DIVERTED",  F.col("DIVERTED").cast("boolean"))

    # Sum of all attributable delay minutes
    .withColumn(
        "TOTAL_DELAY_MIN",
        F.col("CARRIER_DELAY") + F.col("WEATHER_DELAY") +
        F.col("NAS_DELAY")     + F.col("SECURITY_DELAY") +
        F.col("LATE_AIRCRAFT_DELAY")
    )

    # Scheduled flight duration in hours (rounded to 2 dp)
    .withColumn(
        "SCHEDULED_DURATION_HR",
        F.round(F.col("CRS_ELAPSED_TIME") / 60, 2)
    )

    # Delay category bucket
    .withColumn(
        "DELAY_BUCKET",
        F.when(F.col("DEP_DELAY") <= 0,   "On Time")
         .when(F.col("DEP_DELAY") <= 15,  "Minor (<= 15 min)")
         .when(F.col("DEP_DELAY") <= 60,  "Moderate (16–60 min)")
         .when(F.col("DEP_DELAY") <= 180, "Severe (1–3 hr)")
         .otherwise("Extreme (> 3 hr)")
    )

    # Silver audit timestamp
    .withColumn("_silver_processed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md ### 7 · Remove obviously junk rows

# COMMAND ----------

rows_before = df_silver.count()

df_silver = df_silver.filter(
    (~F.col("_has_missing_critical")) &
    (F.col("ORIGIN") != F.col("DEST")) &
    (F.col("DISTANCE") > 0)
)

rows_after = df_silver.count()
print(f"Dropped {rows_before - rows_after:,} junk rows ({rows_before:,} → {rows_after:,})")

# COMMAND ----------

# MAGIC %md ### 8 · Write Silver Delta table

# COMMAND ----------

(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("YEAR", "MONTH")
    .option("delta.autoOptimize.optimizeWrite", "true")
    .option("delta.autoOptimize.autoCompact",   "true")
    .save(SILVER_PATH)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
    USING DELTA
    LOCATION '{SILVER_PATH}'
""")

print(f"Silver table written → {SILVER_PATH}")

# COMMAND ----------

# MAGIC %md ### 9 · Quick profile

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     YEAR,
# MAGIC     MONTH,
# MAGIC     COUNT(*)                                      AS total_flights,
# MAGIC     ROUND(AVG(DEP_DELAY), 2)                     AS avg_dep_delay_min,
# MAGIC     SUM(CAST(IS_DELAYED   AS INT))                AS delayed_flights,
# MAGIC     SUM(CAST(IS_CANCELLED AS INT))                AS cancelled_flights,
# MAGIC     ROUND(
# MAGIC         SUM(CAST(IS_DELAYED AS INT)) * 100.0 / COUNT(*), 2
# MAGIC     )                                             AS pct_delayed
# MAGIC FROM airline_etl.silver_flights
# MAGIC GROUP BY YEAR, MONTH
# MAGIC ORDER BY YEAR, MONTH;

# COMMAND ----------

print("Silver transformation complete. Proceed to 04_aggregate_gold.")
