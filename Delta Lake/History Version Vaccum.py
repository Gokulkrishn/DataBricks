# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at detailed history 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at particular version of table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge Version as of 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at particular timestamp

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", "2024-12-04T09:36:27.000+00:00").load("/mnt/databrickudemy/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Deleting data specify to delete without data retention

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 hours

# COMMAND ----------

# MAGIC %md
# MAGIC checking if data is deleted or not

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge Version as of 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Though data is deleted history is available

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Deleting one record to show that data can be pulled from previous version if it is accidentally deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE from f1_demo.drivers_merge where driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge version as of 30;

# COMMAND ----------

# MAGIC %md
# MAGIC Retrieving data from previous version

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING (select * from f1_demo.drivers_merge version as of 30) as src
# MAGIC ON tgt.driverId = src.driverId
# MAGIC when not matched
# MAGIC   then Insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

        
