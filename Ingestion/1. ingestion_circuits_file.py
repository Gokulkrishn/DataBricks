# Databricks notebook source
from pyspark.sql.types import StructField,ShortType,IntegerType, StringType, DoubleType, StructType
from pyspark.sql.functions import col, lit

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.removeAll()  # Clears any existing widgets


# COMMAND ----------

try:
    v_data_source = dbutils.widgets.get("p_data_source")
    print(f"Debug: p_data_source value: {v_data_source}")
except Exception as e:
    print(f"Error: Widget 'p_data_source' not found. Details: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema

# COMMAND ----------

circuits_schema = StructType(fields=[
  StructField("circuitId", IntegerType(), True),
  StructField("circuitRef", StringType(), True),
  StructField("name", StringType(), True),
  StructField("location", StringType(), True),
  StructField("country", StringType(), True),
  StructField("lat", DoubleType(), True),
  StructField("lng", DoubleType(), True),
  StructField("alt", IntegerType(), True),
  StructField("url", StringType(), True)    
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## reading data from CSV

# COMMAND ----------

circuits_df = spark.read\
  .option("header",True)\
  .schema(circuits_schema)\
  .csv(f'{raw_folder}/circuits.csv')

# COMMAND ----------

circuits_df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Selecting columns

# COMMAND ----------

circuits_df.select('circuitId',"circuitRef","name","location","country","lat","lng").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Selecting columns

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng)

# COMMAND ----------

circuits_selected_df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Select column -- alias to rename a column

# COMMAND ----------

circuits_selected_df.select(circuits_selected_df["circuitId"].alias("circuit_Id")\
            ,circuits_selected_df["circuitRef"]\
            ,circuits_selected_df["name"]\
            ,circuits_selected_df["location"]\
            ,circuits_selected_df["country"]\
            ,circuits_selected_df["lat"].alias("latitude")\
            ,circuits_selected_df["lng"].alias("longitude"))

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Renaming columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
                                 .withColumnRenamed("circuitRef","circuit_ref")\
                                 .withColumnRenamed("name","name")\
                                 .withColumnRenamed("location","location")\
                                 .withColumnRenamed("country","country")\
                                 .withColumnRenamed("lat","latitude")\
                                 .withColumnRenamed("lng","longitude")\
                                 .withColumn("p_data_source",lit(v_data_source)) 

# COMMAND ----------

circuits_renamed_df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Adding new column

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

circuits_final_df.show(2)

# COMMAND ----------

circuits_final_df.write.mode('overwrite').parquet(f"{processed_folder}/circuits")

# COMMAND ----------

spark.read.parquet(f"{processed_folder}/circuits").show(2,truncate=False)

# COMMAND ----------


