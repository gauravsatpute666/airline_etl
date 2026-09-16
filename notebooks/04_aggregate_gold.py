# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 — Gold Layer (Business Aggregations)
# MAGIC
# MAGIC Builds three analytical tables from silver:
# MAGIC
# MAGIC | Table | Grain | Key metrics |
# MAGIC |-------|-------|-------------|
# MAGIC | `gold_carrier_perf` | Carrier × Year × Month | On-time %, avg delay, cancellation rate |
# MAGIC | `gold_route_delays` | Origin → Dest × Year | Avg delay, busiest routes |
# MAGIC | `gold_monthly_trends` | Year × Month | System-wide flight volume & delay trends |

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/airline_etl")

from config.project_config import (
    SILVER_TABLE,
    GOLD_PATH,
    GOLD_CARRIER_TABLE,
    GOLD_ROUTE_TABLE,
    GOLD_MONTHLY_TABLE,
)
from pyspark.sql import functions as F

# COMMAND ----------

df_silver = spark.read.table(SILVER_TABLE)

# COMMAND ----------

# MAGIC %md ### 1 · Carrier Performance

# COMMAND ----------

df_carrier = (
    df_silver
    .groupBy("OP_UNIQUE_CARRIER", "YEAR", "MONTH")
    .agg(
        F.count("*")                                                      .alias("total_flights"),
        F.sum(F.col("IS_CANCELLED").cast("int"))                          .alias("cancelled_flights"),
        F.sum(F.col("IS_DELAYED").cast("int"))                            .alias("delayed_flights"),
        F.round(F.avg("DEP_DELAY"), 2)                                    .alias("avg_dep_delay_min"),
        F.round(F.avg("ARR_DELAY"), 2)                                    .alias("avg_arr_delay_min"),
        F.round(F.avg("TOTAL_DELAY_MIN"), 2)                              .alias("avg_total_delay_min"),
        F.round(F.avg("DISTANCE"), 2)                                     .alias("avg_distance_miles"),
        F.max("DEP_DELAY")                                                .alias("max_dep_delay_min"),
        F.countDistinct("ORIGIN", "DEST")                                 .alias("unique_routes"),
    )
    .withColumn(
        "on_time_pct",
        F.round(
            (F.col("total_flights") - F.col("delayed_flights") - F.col("cancelled_flights"))
            * 100.0 / F.col("total_flights"),
            2
        )
    )
    .withColumn(
        "cancellation_rate_pct",
        F.round(F.col("cancelled_flights") * 100.0 / F.col("total_flights"), 2)
    )
    .withColumn("_gold_processed_at", F.current_timestamp())
)

(
    df_carrier
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/carrier_perf")
)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_CARRIER_TABLE}
    USING DELTA LOCATION '{GOLD_PATH}/carrier_perf'
""")
print(f"gold_carrier_perf rows: {df_carrier.count():,}")

# COMMAND ----------

# MAGIC %md ### 2 · Route-Level Delays

# COMMAND ----------

df_route = (
    df_silver
    .groupBy("ORIGIN", "DEST", "YEAR")
    .agg(
        F.count("*")                          .alias("total_flights"),
        F.round(F.avg("DEP_DELAY"), 2)        .alias("avg_dep_delay_min"),
        F.round(F.avg("ARR_DELAY"), 2)        .alias("avg_arr_delay_min"),
        F.round(F.avg("DISTANCE"), 0)         .alias("distance_miles"),
        F.sum(F.col("IS_CANCELLED").cast("int")) .alias("cancelled_flights"),
        F.round(F.avg("SCHEDULED_DURATION_HR"), 2) .alias("avg_scheduled_duration_hr"),
        F.round(
            F.sum(F.col("IS_CANCELLED").cast("int")) * 100.0 / F.count("*"), 2
        )                                     .alias("cancellation_rate_pct"),
    )
    # Only keep routes with at least 30 flights for statistical relevance
    .filter(F.col("total_flights") >= 30)
    .withColumn("_gold_processed_at", F.current_timestamp())
)

(
    df_route
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/route_delays")
)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_ROUTE_TABLE}
    USING DELTA LOCATION '{GOLD_PATH}/route_delays'
""")
print(f"gold_route_delays rows: {df_route.count():,}")

# COMMAND ----------

# MAGIC %md ### 3 · Monthly System-Wide Trends

# COMMAND ----------

df_monthly = (
    df_silver
    .groupBy("YEAR", "MONTH")
    .agg(
        F.count("*")                                          .alias("total_flights"),
        F.sum(F.col("IS_CANCELLED").cast("int"))              .alias("cancelled_flights"),
        F.sum(F.col("IS_DELAYED").cast("int"))                .alias("delayed_flights"),
        F.round(F.avg("DEP_DELAY"), 2)                        .alias("avg_dep_delay_min"),
        F.round(F.avg("ARR_DELAY"), 2)                        .alias("avg_arr_delay_min"),
        F.round(F.avg("TOTAL_DELAY_MIN"), 2)                  .alias("avg_total_delay_min"),
        F.countDistinct("OP_UNIQUE_CARRIER")                  .alias("active_carriers"),
        F.countDistinct("ORIGIN")                             .alias("active_origin_airports"),
        F.round(F.sum("DISTANCE") / 1e6, 2)                  .alias("total_distance_million_miles"),
    )
    .withColumn(
        "on_time_pct",
        F.round(
            (F.col("total_flights") - F.col("delayed_flights") - F.col("cancelled_flights"))
            * 100.0 / F.col("total_flights"),
            2
        )
    )
    .withColumn(
        "cancellation_rate_pct",
        F.round(F.col("cancelled_flights") * 100.0 / F.col("total_flights"), 2)
    )
    .orderBy("YEAR", "MONTH")
    .withColumn("_gold_processed_at", F.current_timestamp())
)

(
    df_monthly
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/monthly_trends")
)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_MONTHLY_TABLE}
    USING DELTA LOCATION '{GOLD_PATH}/monthly_trends'
""")
print(f"gold_monthly_trends rows: {df_monthly.count():,}")

# COMMAND ----------

# MAGIC %md ### 4 · Spot-check Gold tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 most delayed carriers (all years combined)
# MAGIC SELECT
# MAGIC     OP_UNIQUE_CARRIER,
# MAGIC     SUM(total_flights)           AS total_flights,
# MAGIC     ROUND(AVG(avg_dep_delay_min), 2) AS overall_avg_delay,
# MAGIC     ROUND(AVG(on_time_pct), 2)   AS avg_on_time_pct,
# MAGIC     ROUND(AVG(cancellation_rate_pct), 2) AS avg_cancel_rate
# MAGIC FROM airline_etl.gold_carrier_perf
# MAGIC GROUP BY OP_UNIQUE_CARRIER
# MAGIC ORDER BY overall_avg_delay DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5 busiest routes by flight count
# MAGIC SELECT
# MAGIC     ORIGIN,
# MAGIC     DEST,
# MAGIC     SUM(total_flights)           AS total_flights,
# MAGIC     ROUND(AVG(avg_dep_delay_min), 2) AS avg_delay_min
# MAGIC FROM airline_etl.gold_route_delays
# MAGIC GROUP BY ORIGIN, DEST
# MAGIC ORDER BY total_flights DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Year-over-year on-time trend
# MAGIC SELECT
# MAGIC     YEAR,
# MAGIC     ROUND(AVG(on_time_pct), 2)        AS avg_on_time_pct,
# MAGIC     ROUND(AVG(avg_dep_delay_min), 2)  AS avg_dep_delay_min,
# MAGIC     SUM(total_flights)                AS total_flights
# MAGIC FROM airline_etl.gold_monthly_trends
# MAGIC GROUP BY YEAR
# MAGIC ORDER BY YEAR;

# COMMAND ----------

print("Gold aggregation complete. ETL pipeline finished.")
