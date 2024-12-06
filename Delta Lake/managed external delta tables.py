# Databricks notebook source
# MAGIC %md
# MAGIC Creating new Database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/databrickudemy/demo'

# COMMAND ----------

results_df = spark.read\
    .options(inferSchema=True)\
    .json("/mnt/databrickudemy/raw/2021-03-28/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Creating managed delta table

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.results_managed limit 3;

# COMMAND ----------

# MAGIC %md
# MAGIC Creating external delta table  -- Data cannot be deleted

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/databrickudemy/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/databrickudemy/demo/results_external';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.results_external limit 3;

# COMMAND ----------

# MAGIC %md
# MAGIC Partition by a column and creating table

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC Updating delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11- position
# MAGIC where position <=10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC Deleting delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE from f1_demo.results_managed
# MAGIC where position >=10;

# COMMAND ----------

# MAGIC %sql DELETE from f1_demo.results_managed where points =0; 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;
