# Airline ETL — Databricks / Delta Lake

End-to-end ETL pipeline that ingests US airline on-time performance data from Kaggle
and processes it through a Medallion architecture (Bronze → Silver → Gold) on Databricks.

---

## Dataset

**Kaggle**: [Flight Delay Dataset 2018–2022](https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022)  
Source: US Bureau of Transportation Statistics (BTS) — ~30 M rows, ~110 columns.

---

## Architecture

```
Kaggle API
    │
    ▼
DBFS /airline_etl/raw/          ← raw CSV files
    │
    ▼
Bronze Delta Table              ← all columns as STRING + audit cols
    │  null-safe, no data loss
    ▼
Silver Delta Table              ← typed, cleaned, derived cols
    │  partitioned by YEAR / MONTH
    ▼
Gold Delta Tables               ← business aggregations
    ├── gold_carrier_perf
    ├── gold_route_delays
    └── gold_monthly_trends
```

---

## Notebooks (run in order)

| # | Notebook | Purpose |
|---|----------|---------|
| 01 | `notebooks/01_setup.py` | Install Kaggle CLI, download data, create database |
| 02 | `notebooks/02_ingest_bronze.py` | Land raw CSVs as Delta (all strings) |
| 03 | `notebooks/03_transform_silver.py` | Type cast, date parse, null fill, derive columns |
| 04 | `notebooks/04_aggregate_gold.py` | Carrier / route / monthly aggregations |

---

## Key Transformations (Silver)

| Transformation | Detail |
|----------------|--------|
| Type casting | 30+ numeric columns cast from STRING → DOUBLE |
| Date parsing | `FL_DATE` → `DateType` via `to_date("yyyy-MM-dd")` |
| Time parsing | HHMM integers → `TimestampType` (DEP_TIME, ARR_TIME, etc.) |
| Null filling | Delay columns filled with 0.0; boolean flags filled with 0 |
| Missing-row flag | `_has_missing_critical` marks rows lacking carrier/date/origin/dest |
| String cleaning | Trim + UPPER on all carrier & airport code columns |
| `IS_DELAYED` | True when `DEP_DELAY > 15` (FAA standard) |
| `IS_CANCELLED` | Cast from integer flag |
| `TOTAL_DELAY_MIN` | Sum of all five attributable delay buckets |
| `SCHEDULED_DURATION_HR` | `CRS_ELAPSED_TIME / 60` |
| `DELAY_BUCKET` | Categorical bucket: On Time / Minor / Moderate / Severe / Extreme |
| Junk row removal | Drop origin == dest, distance <= 0, missing critical fields |

---

## Pre-requisites

1. **Databricks workspace** with a cluster running DBR 13+ (Delta + PySpark included).
2. **Kaggle API credentials** stored as Databricks Secrets:
   ```
   databricks secrets create-scope --scope kaggle
   databricks secrets put --scope kaggle --key username   # your Kaggle username
   databricks secrets put --scope kaggle --key key        # your Kaggle API key
   ```
3. Clone/import this repo into Databricks Repos (`/Workspace/Repos/airline_etl`).

---

## DBFS Layout

```
dbfs:/airline_etl/
├── raw/            ← original CSVs
├── bronze/         ← Delta (all strings)
├── silver/         ← Delta, partitioned YEAR/MONTH
└── gold/
    ├── carrier_perf/
    ├── route_delays/
    └── monthly_trends/
```

---

## Gold Table Columns (quick reference)

**gold_carrier_perf** — grain: carrier × year × month  
`OP_UNIQUE_CARRIER, YEAR, MONTH, total_flights, cancelled_flights, delayed_flights,
avg_dep_delay_min, avg_arr_delay_min, avg_total_delay_min, on_time_pct, cancellation_rate_pct`

**gold_route_delays** — grain: origin × dest × year  
`ORIGIN, DEST, YEAR, total_flights, avg_dep_delay_min, distance_miles, cancellation_rate_pct`

**gold_monthly_trends** — grain: year × month  
`YEAR, MONTH, total_flights, cancelled_flights, delayed_flights, avg_dep_delay_min, on_time_pct`
