# Databricks notebook source
# MAGIC %md
# MAGIC ## Step Configuration Operation

# COMMAND ----------

# MAGIC %run ./configuration

# COMMAND ----------

# MAGIC %run ./operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Files in the Raw Path

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest raw data

# COMMAND ----------

raw_movie_df = read_batch_raw(rawPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Raw Data

# COMMAND ----------

display(raw_movie_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Metadata

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

raw_movie_df = transform_raw(raw_movie_df)

# COMMAND ----------

display(raw_movie_df)

# COMMAND ----------

#dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Batch to a Bronze Table

# COMMAND ----------

from pyspark.sql.functions import col

(
    raw_movie_df.select(
        "datasource",
        "ingesttime",
        "value",
        "status",
        col("ingestdate").alias("p_ingestdate"),
    )
    .write.format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Bronze Table in the Metastore

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_bronze
"""
)

spark.sql(
    f"""
CREATE TABLE movie_bronze
USING DELTA
LOCATION "{bronzePath}"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Classic Bronze Table
# MAGIC 
# MAGIC Run this query to display the contents of the Classic Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------


