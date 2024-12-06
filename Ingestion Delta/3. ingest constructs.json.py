# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json(f"{raw_folder}/{v_file_date}/constructors.json")

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_dropped_df)\
                                  .withColumnRenamed("constructorId","constructor_id")\
                                  .withColumnRenamed("constructorRef","constructor_ref")\
                                  .withColumn("ingestion_date",current_timestamp())\
                                  .withColumn("file_date", lit(v_file_date))                                  

# COMMAND ----------

# MAGIC %md
# MAGIC Write file to parquet form

# COMMAND ----------

constructor_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructor")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_processed.constructor;

# COMMAND ----------


