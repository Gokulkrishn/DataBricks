# Databricks notebook source
def mount_containers(storage_account,container_name):
    tenant_id = dbutils.secrets.get(scope = "formula-1", key = "tenantID")
    client_id = dbutils.secrets.get(scope = "formula-1", key = "clientID")
    client_secret = dbutils.secrets.get(scope = "formula-1", key = "clientSecret")   
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account}/{container_name}",
    extra_configs = configs)

    return dbutils.fs.ls(f"/mnt/{storage_account}/{container_name}")

# COMMAND ----------

mount_containers("databrickudemy","demo")

# COMMAND ----------

dbutils.fs.ls("/mnt/databrickudemy/raw")

# COMMAND ----------


