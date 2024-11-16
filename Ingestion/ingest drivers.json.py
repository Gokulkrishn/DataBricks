# Databricks notebook source
from pyspark.sql.functions import col,lit,current_timestamp, concat
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType

# COMMAND ----------

drivers_df = spark.read.json("/mnt/databrickudemy/raw/drivers.json")

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

drivers_df= spark.read.schema(drivers_schema).json("/mnt/databrickudemy/raw/drivers.json")

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                               .withColumnRenamed("driverRef", "driver_ref")\
                               .withColumn("ingestion_date", current_timestamp())\
                               .withColumn("driver_name",concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

driver_final_df = drivers_renamed_df.drop("url").drop("name")

# COMMAND ----------

driver_final_df.show(2,truncate=False)

# COMMAND ----------

drivers_df.write.mode("overwrite").parquet("/mnt/databrickudemy/processed/drivers")

# COMMAND ----------

spark.read.parquet("/mnt/databrickudemy/processed/drivers").show(2,truncate=False)

# COMMAND ----------


