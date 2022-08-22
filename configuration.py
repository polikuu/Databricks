# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC Define Data Paths.

# COMMAND ----------

rootPath= "/mnt/moviedb"

rawPath = "/mnt/blobstorage"


landingPath = rootPath + "/landing/"
bronzePath = rootPath + "/bronze/"
silverPath = rootPath + "/silver/"
silverQuarantinePath = rootPath + "/silverQuarantine/"


# COMMAND ----------

# MAGIC %md
# MAGIC Configure Database

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS movie")
spark.sql("USE movie")

# COMMAND ----------

# MAGIC %md
# MAGIC Import Utility Functions

# COMMAND ----------



