# Databricks notebook source
for each in dbutils.fs.ls("/mnt/databrickudemy/raw/"):
  dbutils.fs.rm(each.path, True)

# COMMAND ----------

dbutils.fs.ls("/mnt/databrickudemy/raw/")

# COMMAND ----------

dbutils.widgets.text("sample", "")

# COMMAND ----------

dbutils.widgets.get("sample")

# COMMAND ----------


