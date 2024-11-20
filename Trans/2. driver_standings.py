# Databricks notebook source
from pyspark.sql.functions import col, sum, when, count, rank, desc,partitioning
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder}/race_results")

# COMMAND ----------

driver_standings_df = race_results_df\
  .groupBy("race_year","driver_name","team")\
  .agg(sum("points").alias("total_points"),
    count(when(col("position") == 1,True)).alias("wins"))\
    .filter("race_year == 2020") 

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))

# COMMAND ----------

final_df = driver_standings_df.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
