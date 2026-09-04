# Databricks notebook source
from pyspark.sql import functions as F

silver_df = spark.read.table(
    "flight_streaming.silver.flight_events"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Build Aircraft Dimension**

# COMMAND ----------

dim_aircraft_df = (
    silver_df
    .select(
        "icao24",
        "callsign",
        "origin_country"
    )
    .filter(
        F.col("icao24").isNotNull()
    )
    .withColumn(
        "callsign",
        F.upper(F.trim(F.col("callsign")))
    )
    .withColumn(
        "origin_country",
        F.trim(F.col("origin_country"))
    )
    .dropDuplicates(
        ["icao24"]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write Aircraft Dimension**

# COMMAND ----------

DIM_TABLE = "flight_streaming.gold.dim_aircraft"

(
    dim_aircraft_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(DIM_TABLE)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS total_aircraft
# MAGIC FROM flight_streaming.gold.dim_aircraft;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM flight_streaming.gold.dim_aircraft
# MAGIC LIMIT 20;