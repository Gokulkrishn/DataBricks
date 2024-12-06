# Databricks notebook source
# MAGIC %md
# MAGIC Creating Delta table from Parquet table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_covert_to_delta (
# MAGIC driverId INT,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC USE f1_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_covert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_covert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_covert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/databrickudemy/demo/drivers_covert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/databrickudemy/demo/drivers_covert_to_delta_new`

# COMMAND ----------


