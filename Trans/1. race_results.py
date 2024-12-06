# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder}/races") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

processed_folder

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder}/constructors") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

ciruits_df = spark.read.parquet(f"{processed_folder}/circuits")\
  .withColumnRenamed("location","circuit_location")

# COMMAND ----------

race_circuit_df = races_df.join(ciruits_df, ciruits_df.circuit_id == races_df.circuit_id)\
  .select("race_id","race_year","race_name", "race_date", "circuit_location")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder}/results")\
  .withColumnRenamed("time","race_time")

# COMMAND ----------

results_df.show()

# COMMAND ----------

results_df.show()

# COMMAND ----------

constructor_df = spark.read.parquet(f"{processed_folder}/constructor")\
  .withColumnRenamed("name","team")

# COMMAND ----------

constructor_df = constructor_df.withColumnRenamed("constructorId", "constructor_id")

# COMMAND ----------

constructor_df.show()

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder}/drivers")\
  .withColumnRenamed("number","driver_number")\
  .withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

drivers_df.show()

# COMMAND ----------

race_result_df = results_df.join(race_circuit_df,race_circuit_df.race_id==results_df.race_id)\
                          .join(drivers_df,drivers_df.driver_id==results_df.driver_id)\
                          .join(constructor_df,constructor_df.constructor_id==results_df.constructor_id)\
                          .select("race_year","race_name","race_date","circuit_location","driver_name",\
                          "driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position")                            

# COMMAND ----------

final_df = add_ingestion_date(race_result_df)

# COMMAND ----------

dbutils.fs.rm(f"{presentation_folder}/race_results",True)

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------


