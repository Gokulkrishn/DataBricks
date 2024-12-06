# Databricks notebook source
from pyspark.sql.functions import avg,countDistinct,count,sum,col,rank,desc
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "/Workspace/Users/funbums1@gmail.com/DataBricks/Includes/configuration"

# COMMAND ----------

race_df = spark.read.parquet(f"{presentation_folder}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter

# COMMAND ----------

demo_df = race_df.filter("race_year = 2020")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Group By

# COMMAND ----------

demo_df.groupBy("driver_name").agg(
    countDistinct("race_name").alias("distinct_race_count"),
    sum("points").alias("total_points")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partition By

# COMMAND ----------



# COMMAND ----------

demo_df2 = race_df.filter("race_year in (2019,2020)")

# COMMAND ----------

demo_grouped_df = demo_df2.groupBy("race_year","driver_name")\
  .agg(countDistinct("race_name"),sum("points"))

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(col("sum(points)").desc())
demo_grouped_df.withColumn("rank",rank().over(driverRankSpec)).show(40)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Position

# COMMAND ----------

dbutils.notebook.run("test",0,{"sample":"Gokul"})

# COMMAND ----------


