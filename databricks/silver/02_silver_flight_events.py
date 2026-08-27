# Databricks notebook source
from pyspark.sql import functions as F

bronze_df = spark.readStream.table(
    "flight_streaming.bronze.flight_events"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transform**

# COMMAND ----------

silver_df = (
    bronze_df

    # Convert Unix timestamps
    .withColumn(
        "event_timestamp",
        F.to_timestamp(F.from_unixtime(F.col("event_time")))
    )

    .withColumn(
        "position_timestamp",
        F.to_timestamp(F.from_unixtime(F.col("time_position")))
    )

    .withColumn(
        "last_contact_timestamp",
        F.to_timestamp(F.from_unixtime(F.col("last_contact")))
    )

    # Clean callsign
    .withColumn(
        "callsign",
        F.upper(F.trim(F.col("callsign")))
    )

    # Clean country
    .withColumn(
        "origin_country",
        F.trim(F.col("origin_country"))
    )

    # Keep valid coordinates
    .filter(
        F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
    )

    # Keep valid aircraft identifier
    .filter(
        F.col("icao24").isNotNull()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Deduplication**

# COMMAND ----------

silver_df = silver_df.dropDuplicates(
    ["icao24", "event_time"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write Silver**

# COMMAND ----------

SILVER_TABLE = "flight_streaming.silver.flight_events"

SILVER_CHECKPOINT = (
    "/Volumes/flight_streaming/bronze/checkpoints/silver_flight_events"
)

silver_query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(SILVER_TABLE)
)

silver_query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS silver_events
# MAGIC FROM flight_streaming.silver.flight_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     icao24,
# MAGIC     callsign,
# MAGIC     origin_country,
# MAGIC     event_timestamp,
# MAGIC     latitude,
# MAGIC     longitude,
# MAGIC     baro_altitude,
# MAGIC     velocity
# MAGIC FROM flight_streaming.silver.flight_events
# MAGIC ORDER BY event_timestamp DESC
# MAGIC LIMIT 20;