# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(df):
  return df.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

def rearange_partition_columns(df,partition_column):
  col  = df.schema.names
  col.remove(partition_column)
  col.append(partition_column)
  return df.select(col)

# COMMAND ----------

def overwrite_partition(df,db_name,table_name,partition_column):
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  df = rearange_partition_columns(df,partition_column)
  if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
    df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

  from delta.tables import DeltaTable
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition) \
      .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll()\
      .execute()
  else:
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------


