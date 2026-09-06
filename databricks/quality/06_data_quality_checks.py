# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC
# MAGIC This notebook validates data quality across the Silver and Gold layers.
# MAGIC
# MAGIC Checks include:
# MAGIC - Completeness
# MAGIC - Validity
# MAGIC - Uniqueness
# MAGIC - Consistency

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Quality Checks

# COMMAND ----------

silver_df = spark.read.table(
    "flight_streaming.silver.flight_events"
)

# COMMAND ----------

silver_total = silver_df.count()

silver_null_icao24 = silver_df.filter(
    F.col("icao24").isNull()
).count()

silver_null_timestamp = silver_df.filter(
    F.col("event_timestamp").isNull()
).count()

silver_invalid_latitude = silver_df.filter(
    ~F.col("latitude").between(-90, 90)
).count()

silver_invalid_longitude = silver_df.filter(
    ~F.col("longitude").between(-180, 180)
).count()

silver_duplicates = (
    silver_df
    .groupBy("icao24", "event_time")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Flight Positions Checks

# COMMAND ----------

positions_df = spark.read.table(
    "flight_streaming.gold.flight_positions"
)

positions_null_icao24 = positions_df.filter(
    F.col("icao24").isNull()
).count()

positions_null_timestamp = positions_df.filter(
    F.col("event_timestamp").isNull()
).count()

positions_invalid_latitude = positions_df.filter(
    ~F.col("latitude").between(-90, 90)
).count()

positions_invalid_longitude = positions_df.filter(
    ~F.col("longitude").between(-180, 180)
).count()

positions_duplicates = (
    positions_df
    .groupBy("icao24", "event_timestamp")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Aircraft Dimension Checks

# COMMAND ----------

aircraft_df = spark.read.table(
    "flight_streaming.gold.dim_aircraft"
)

aircraft_null_icao24 = aircraft_df.filter(
    F.col("icao24").isNull()
).count()

aircraft_duplicate_icao24 = (
    aircraft_df
    .groupBy("icao24")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Flight Metrics Checks

# COMMAND ----------

metrics_df = spark.read.table(
    "flight_streaming.gold.flight_metrics"
)

metrics_null_date = metrics_df.filter(
    F.col("event_date").isNull()
).count()

metrics_negative_observations = metrics_df.filter(
    F.col("total_observations") < 0
).count()

metrics_negative_aircraft = metrics_df.filter(
    F.col("active_aircraft") < 0
).count()

metrics_negative_velocity = metrics_df.filter(
    F.col("avg_velocity") < 0
).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Report

# COMMAND ----------

quality_results = [
    ("Silver - NULL ICAO24", silver_null_icao24),
    ("Silver - NULL event timestamp", silver_null_timestamp),
    ("Silver - Invalid latitude", silver_invalid_latitude),
    ("Silver - Invalid longitude", silver_invalid_longitude),
    ("Silver - Duplicate observations", silver_duplicates),

    ("Gold Positions - NULL ICAO24", positions_null_icao24),
    ("Gold Positions - NULL event timestamp", positions_null_timestamp),
    ("Gold Positions - Invalid latitude", positions_invalid_latitude),
    ("Gold Positions - Invalid longitude", positions_invalid_longitude),
    ("Gold Positions - Duplicate positions", positions_duplicates),

    ("Dim Aircraft - NULL ICAO24", aircraft_null_icao24),
    ("Dim Aircraft - Duplicate ICAO24", aircraft_duplicate_icao24),

    ("Gold Metrics - NULL event date", metrics_null_date),
    ("Gold Metrics - Negative observations", metrics_negative_observations),
    ("Gold Metrics - Negative aircraft count", metrics_negative_aircraft),
    ("Gold Metrics - Negative average velocity", metrics_negative_velocity)
]

quality_df = spark.createDataFrame(
    quality_results,
    ["check", "failed_records"]
)

display(quality_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overall Result

# COMMAND ----------

total_failed_checks = (
    quality_df
    .filter(F.col("failed_records") > 0)
    .count()
)

if total_failed_checks == 0:
    print("DATA QUALITY RESULT: PASS")
else:
    print(
        f"DATA QUALITY RESULT: FAIL - "
        f"{total_failed_checks} checks failed"
    )