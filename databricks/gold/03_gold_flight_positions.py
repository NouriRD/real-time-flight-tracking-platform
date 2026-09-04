# Databricks notebook source
from pyspark.sql import functions as F

silver_df = spark.readStream.table(
    "flight_streaming.silver.flight_events"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Build Flight Positions Fact**

# COMMAND ----------

gold_positions_df = (
    silver_df
    .select(
        "icao24",
        "event_timestamp",
        "latitude",
        "longitude",
        "baro_altitude",
        "geo_altitude",
        "velocity",
        "true_track",
        "vertical_rate",
        "on_ground"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write Gold Fact**

# COMMAND ----------

GOLD_TABLE = "flight_streaming.gold.flight_positions"

GOLD_CHECKPOINT = (
    "/Volumes/flight_streaming/bronze/checkpoints/gold_flight_positions"
)

gold_query = (
    gold_positions_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        GOLD_CHECKPOINT
    )
    .trigger(availableNow=True)
    .toTable(GOLD_TABLE)
)

gold_query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS total_positions
# MAGIC FROM flight_streaming.gold.flight_positions;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM flight_streaming.gold.flight_positions
# MAGIC ORDER BY event_timestamp DESC
# MAGIC LIMIT 20;