# Databricks notebook source
from pyspark.sql.functions import col, sum, when, count, rank, desc,partitioning
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder}/race_results")


# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))    

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))

# COMMAND ----------

final_df = constructor_standings_df.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

final_df.filter("race_year = 2019").show()

# COMMAND ----------


