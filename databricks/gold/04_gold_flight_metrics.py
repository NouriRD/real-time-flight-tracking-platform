# Databricks notebook source
from pyspark.sql import functions as F

silver_df = spark.read.table(
    "flight_streaming.silver.flight_events"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Flight Metrics**

# COMMAND ----------

metrics_df = (
    silver_df
    .withColumn(
        "event_date",
        F.to_date("event_timestamp")
    )
    .groupBy(
        "event_date",
        "origin_country"
    )
    .agg(
        F.count("*").alias(
            "total_observations"
        ),

        F.countDistinct("icao24").alias(
            "active_aircraft"
        ),

        F.round(
            F.avg("baro_altitude"),
            2
        ).alias(
            "avg_baro_altitude"
        ),

        F.round(
            F.avg("geo_altitude"),
            2
        ).alias(
            "avg_geo_altitude"
        ),

        F.round(
            F.avg("velocity"),
            2
        ).alias(
            "avg_velocity"
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write Gold Metrics**

# COMMAND ----------

metrics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "flight_streaming.gold.flight_metrics"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM flight_streaming.gold.flight_metrics
# MAGIC ORDER BY event_date DESC, active_aircraft DESC
# MAGIC LIMIT 20;