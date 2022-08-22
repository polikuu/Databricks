# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver - ETL into a Silver table

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
# MAGIC ### Display the Files in the Bronze Paths

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

raw_movie_df = transform_raw (read_batch_raw(rawPath))

# COMMAND ----------

display(raw_movie_df)

# COMMAND ----------

raw_movie_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze to Silver Step

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(silverPath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load New Records from the Bronze Records
# MAGIC 
# MAGIC **EXERCISE**
# MAGIC 
# MAGIC Load all records from the Bronze table with a status of `"new"`.

# COMMAND ----------

bronzeDF = spark.read.table("movie_bronze").filter(col("status") == "new")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create the Silver DataFrame 

# COMMAND ----------

bronzeAugmentDF = bronzeDF.select("value", "value.*")

# COMMAND ----------

from pyspark.sql.functions import col, when


silver_movie = (bronzeAugmentDF.select(
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
    col("Genres.id").alias ("genreID"))).withColumn("Budget", when (bronzeAugmentDF.Budget < 1000000, 1000000).otherwise(bronzeAugmentDF.Budget))

# COMMAND ----------

display(silver_movie)

# COMMAND ----------

silver_language = bronzeAugmentDF.select( 
    lit(1).alias("languageID"),
    "OriginalLanguage").distinct()

# COMMAND ----------

display(silver_language)

# COMMAND ----------

from pyspark.sql.functions import explode

genreDF = bronzeAugmentDF.select (explode(bronzeAugmentDF.genres)
               )

# COMMAND ----------

display(genreDF)

# COMMAND ----------

genreAugment=genreDF.select("col.*")

# COMMAND ----------

silver_genre = genreAugment.distinct().select(
                col("id").alias ("genreID"),
                col("name").alias("genreName"))

# COMMAND ----------

display(silver_genre)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine the Bad Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split the Silver DataFrame

# COMMAND ----------

silver_movie_clean = silver_movie.filter(silver_movie.RunTime >= 0).distinct()
silver_movie_quarantine = silver_movie.filter(silver_movie.RunTime < 0).distinct()

# COMMAND ----------

display(silver_movie_clean)

# COMMAND ----------

silver_genre_clean = silver_genre.filter(silver_genre.genreName != "")
silver_genre_quarantine = silver_genre.filter(silver_genre.genreName == "")

# COMMAND ----------

display(silver_genre_clean)

# COMMAND ----------

display(silver_genre_quarantine)

# COMMAND ----------

silver_language_clean = silver_language

# COMMAND ----------

display(silver_language_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Clean Batch to a Silver Table

# COMMAND ----------

#write movie clean batch to a silver table
(
    silver_movie_clean.select(
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

spark.sql(
    """
DROP TABLE IF EXISTS movie_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movie_silver
USING DELTA
LOCATION "{silverPath+"movie/"}"
"""
)

# COMMAND ----------

#dbutils.fs.rm(silverPath+"movie/", recurse=True)

# COMMAND ----------

#write genre clean batch to a silver table, no need to fix genre quarantine bath because they are all duplicate and empty values in genre clean batch, and genre clean batch is lookup table, it has all genre ID and corresponding genre name. 
(
    silver_genre_clean.select(
    "genreID",
    "genreName"
    )
    .write.format("delta")
    .mode("append")
    .partitionBy("genreID")
    .save(silverPath+"genre/")
)


# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS genre_silver
"""
)

spark.sql(
    f"""
CREATE TABLE genre_silver
USING DELTA
LOCATION "{silverPath+"genre/"}"
"""
)

# COMMAND ----------


display(spark.sql("""select * from movie_silver"""))

# COMMAND ----------

display(spark.sql("""select * from genre_silver"""))

# COMMAND ----------

#write language clean batch to a silver table
(
    silver_language_clean
    .write.format("delta")
    .mode("append")
    .partitionBy("languageID")
    .save(silverPath+"language/")
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS language_silver
"""
)

spark.sql(
    f"""
CREATE TABLE language_silver
USING DELTA
LOCATION "{silverPath+"language/"}"
"""
)

# COMMAND ----------

display(spark.sql("""select * from language_silver"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Bronze table to Reflect the Loads

# COMMAND ----------


from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silver_movie_clean.withColumn("status", lit("loaded"))

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
# MAGIC **EXERCISE:** Update the records in the Bronze table to reflect updates.
# MAGIC 
# MAGIC ### Step 2: Update Quarantined records
# MAGIC Quarantined records should have their Bronze table `status` updated to `"quarantined"`.
# MAGIC 
# MAGIC 🕺🏻 **Hint** You are matching the `value` column in your quarantine Silver
# MAGIC DataFrame to the `value` column in the Bronze table.

# COMMAND ----------


silverAugmented = silver_movie_quarantine.withColumn(
    "status", lit("quarantined")
)

update_match = "bronze.value = quarantine.value"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
