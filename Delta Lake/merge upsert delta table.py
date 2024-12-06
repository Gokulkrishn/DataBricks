# Databricks notebook source
from pyspark.sql.functions import upper,current_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC USE f1_demo;

# COMMAND ----------

driver_day1_df = spark.read \
    .option("inferSchema",True)\
    .json("/mnt/databrickudemy/raw/2021-03-28/drivers.json")\
    .filter("driverId <= 10")\
    .select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

driver_day1_df.createOrReplaceTempView("driver_day1_df")

# COMMAND ----------

# MAGIC %md
# MAGIC Updating and Inserting

# COMMAND ----------

driver_day2_df = spark.read \
    .option("inferSchema",True)\
    .json("/mnt/databrickudemy/raw/2021-03-28/drivers.json")\
    .filter("driverId between 6 and 15")\
    .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

driver_day2_df.createOrReplaceTempView("driver_day2_df")

# COMMAND ----------

driver_day3_df = spark.read \
    .option("inferSchema",True)\
    .json("/mnt/databrickudemy/raw/2021-03-28/drivers.json")\
    .filter("(driverId between 0 and 5) and (driverId between 16 and 20)")\
    .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

driver_day3_df.createOrReplaceTempView("driver_day3_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC ) USING DELTA
# MAGIC LOCATION '/mnt/databrickudemy/demo/drivers_merge'

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING driver_day1_df upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC When matched THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING driver_day2_df upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC When matched THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

from delta.tables import DeltaTable

drivers_merge = DeltaTable.forPath(spark, "/mnt/databrickudemy/demo/drivers_merge")

drivers_merge.alias("tgt").merge(
    driver_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId"
    ).whenMatchedUpdate(
        set={"dob": "upd.dob",
             "forename": "upd.forename", 
             "surname": "upd.surname",
             "updatedDate":current_timestamp()
            }
    ).whenNotMatchedInsert(
        values={"driverId": "upd.driverId",
                "dob": "upd.dob",
                "forename": "upd.forename",
                "surname": "upd.surname",
                "createdDate":current_timestamp()
                }
    ).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

drivers_merge.alias("tgt").merge(
    driver_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId"
    ).whenMatchedUpdate(
        set={"dob": "upd.dob",
             "forename": "upd.forename",
             "surname": "upd.surname",
             "updatedDate":current_timestamp()
            }  
    ).whenNotMatchedInsert(
        values={"driverId": "upd.driverId",
                "dob": "upd.dob",
                "forename": "upd.forename",
                "surname": "upd.surname",
                "createdDate":current_timestamp()
        }
    ).execute()
