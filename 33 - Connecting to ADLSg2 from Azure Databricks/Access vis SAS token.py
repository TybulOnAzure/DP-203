# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.tybuldatalakedemo.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.tybuldatalakedemo.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.tybuldatalakedemo.dfs.core.windows.net", "sv=2022-11-02&ss=b&srt=c&sp=rwdlacyx&se=2024-04-19T14:14:35Z&st=2024-04-11T06:14:35Z&spr=https&sig=N1wHpDi9tfMOsND0e8xL1nHK0FRCfsboXizbeX60h9I%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@tybuldatalakedemo.dfs.core.windows.net"))
