# Databricks notebook source

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

def read_batch_raw(rawPath: str) -> DataFrame:
    raw_movie_df = spark.read.format("json").option("multiline", "true").load(rawPath)
    return raw_movie_df.select(explode(raw_movie_df.movie))


# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
    return raw.select(
         col('col').alias("value"),
    lit("antraMoive").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate"),
    )
