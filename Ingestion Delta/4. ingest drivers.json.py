# Databricks notebook source
from pyspark.sql.functions import col,lit,current_timestamp, concat
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

drivers_df = spark.read.json(f"{raw_folder}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

drivers_df.show(2)

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True)
                         ,StructField("surname", StringType(), True)])

drivers_schema = StructType(fields=[StructField("driverId", StringType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", StringType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("dob", DateType(), True),                                
                                    StructField("name", name_schema, True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df= spark.read.schema(drivers_schema).json(f"{raw_folder}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_renamed_df = add_ingestion_date(drivers_df).withColumnRenamed("driverId", "driver_id")\
                               .withColumnRenamed("driverRef", "driver_ref")\
                               .withColumn("driver_name",concat(col("name.forename"), lit(" "), col("name.surname")))\
                               .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

driver_final_df = drivers_renamed_df.drop("url").drop("name")

# COMMAND ----------

driver_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_processed.drivers;

# COMMAND ----------


