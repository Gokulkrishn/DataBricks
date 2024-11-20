-- Databricks notebook source
create database if not exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database default;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df = spark.read.parquet("/mnt/databrickudemy/presentation/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("parquet").saveAsTable("demo.results_df_python")

-- COMMAND ----------


