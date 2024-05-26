# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.tybuldatalakedemo.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.tybuldatalakedemo.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.tybuldatalakedemo.dfs.core.windows.net", "my-hardcoded-token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@tybuldatalakedemo.dfs.core.windows.net"))
