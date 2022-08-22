# Databricks notebook source


print(
    dbutils.fs.head(
        dbutils.fs.ls("/mnt/blobstorage")
    )
)

# COMMAND ----------

dbutils.fs.ls("/mnt/blobstorage")

# COMMAND ----------

dbutils.fs.rm("/mnt/blobstorage/bronze", True)

# COMMAND ----------


