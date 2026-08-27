# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC LIST '/Volumes/flight_streaming/landing/raw_events/';

# COMMAND ----------

SOURCE_PATH = "/Volumes/flight_streaming/landing/raw_events/"

CHECKPOINT_PATH = "/Volumes/flight_streaming/bronze/checkpoints/flight_events"

BRONZE_TABLE = "flight_streaming.bronze.flight_events"

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType
)

flight_schema = StructType([
    StructField("event_time", LongType(), True),
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("time_position", LongType(), True),
    StructField("last_contact", LongType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("baro_altitude", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    StructField("geo_altitude", DoubleType(), True),
    StructField("squawk", StringType(), True)
])

# COMMAND ----------

bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(flight_schema)
    .load(SOURCE_PATH)
)

# COMMAND ----------

query = (
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()

# COMMAND ----------

display(
    spark.read.table("flight_streaming.bronze.flight_events")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS total_events
# MAGIC FROM flight_streaming.bronze.flight_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM flight_streaming.bronze.flight_events
# MAGIC LIMIT 10;