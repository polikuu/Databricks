# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Table Updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Operation Functions

# COMMAND ----------

# MAGIC %run ./operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Quarantined Records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Load Quarantined Records from the Bronze Table

# COMMAND ----------

bronzeQuarantinedDF = spark.read.table("movie_bronze").filter(
    "status = 'quarantined'"
)
display(bronzeQuarantinedDF)

# COMMAND ----------

bronzeQuarantinedAugmentDF = bronzeQuarantinedDF.select("value", "value.*")
display(bronzeQuarantinedAugmentDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Correct Quarantined Data

# COMMAND ----------

repairDF = bronzeQuarantinedAugmentDF.withColumn("RunTime",bronzeQuarantinedAugmentDF.RunTime*-1)
display(repairDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Write the Repaired (formerly Quarantined) Records to the Silver Table

# COMMAND ----------

from pyspark.sql.functions import col, when


silverMovieFixedDF = (repairDF.select(
    "value",
    "BackdropUrl",
    "Budget",
    "CreatedBy",
    col("CreatedDate").cast("date").alias("p_CreatedDate"),
    "Id",
    "ImdbUrl",
    "OriginalLanguage",
    "Overview",
    "PosterUrl",
    "Price",
    col("ReleaseDate").cast("date"),
    "Revenue",
    col("RunTime").cast("INTEGER"),
    "Tagline",
    "Title",
    "TmdbUrl",
    "UpdatedBy",
    "UpdatedDate",
    lit(1).alias("languageID"),
    col("Genres.id").alias ("genreID"))).withColumn("Budget", when (repairDF.Budget < 1000000, 1000000).otherwise(repairDF.Budget)).distinct()

# COMMAND ----------

(
    silverMovieFixedDF.select(
    "BackdropUrl",
    "Budget",
    "p_CreatedDate",
    "Id",
    "ImdbUrl",
    "OriginalLanguage",
    "Overview",
    "PosterUrl",
    "Price",
    "ReleaseDate",
    "Revenue",
    "RunTime",
    "Tagline",
    "Title",
    "TmdbUrl",
    "UpdatedBy",
    "UpdatedDate",
    "genreID",
    "languageID"
    )
    .write.format("delta")
    .mode("append")
    .partitionBy("p_CreatedDate")
    .save(silverPath+"movie/")
)

# COMMAND ----------

from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silverMovieFixedDF.withColumn("status", lit("loaded"))

update_match = "bronze.value = clean.value"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Quarantined Records
# MAGIC 
# MAGIC If the update was successful, there should be no quarantined records
# MAGIC in the Bronze table.

# COMMAND ----------

display(spark.read.table("movie_bronze").filter(col("status") == "quarantined"))


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_silver

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
