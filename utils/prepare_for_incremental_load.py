# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_processed CASCADE;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed
# MAGIC LOCATION "/mnt/databrickudemy/processed"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_presentation CASCADE;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_presentation
# MAGIC LOCATION "/mnt/databrickudemy/presentation"

# COMMAND ----------


