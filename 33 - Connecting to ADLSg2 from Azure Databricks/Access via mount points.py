# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "59a63017-a581-4651-ba0f-fe18b30e9b8e",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="Demo-Scope",key="Dabricks-service-principal-secret"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/94032769-6d65-484f-842f-09d05b51c9ad/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://raw@tybuldatalakedemo.dfs.core.windows.net/",
  mount_point = "/mnt/raw",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/raw"))
