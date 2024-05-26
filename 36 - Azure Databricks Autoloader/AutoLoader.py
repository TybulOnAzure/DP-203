# Databricks notebook source
# Import functions
from pyspark.sql.functions import col, current_timestamp

file_path = f"abfss://raw@tybuldatalakedemo.dfs.core.windows.net/AutoLoaderInput/"
target_path = f"abfss://conformed@tybuldatalakedemo.dfs.core.windows.net/AutoLoaderOutput"
checkpoint_path = f"abfss://checkpoint@tybuldatalakedemo.dfs.core.windows.net/"
table_name = "PiotrT_Demo.AutoLoaderDemo"

# Clear out data from previous demo execution
# spark.sql(f"DROP TABLE IF EXISTS {table_name}")
# dbutils.fs.rm(checkpoint_path, True)
# dbutils.fs.rm(target_path, True)

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  #Infer column types
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaEvolutionMode", "rescue")
  .load(file_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .option("path", target_path) 
  .option("mergeSchema", "true")
  #.trigger(processingTime='1 seconds')
  .trigger(availableNow=True)
  .toTable(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PiotrT_Demo.AutoLoaderDemo

# COMMAND ----------

file_path = f"abfss://raw@tybuldatalakedemo.dfs.core.windows.net/AutoLoaderInput/"
target_path = f"abfss://conformed@tybuldatalakedemo.dfs.core.windows.net/AutoLoaderOutput"
checkpoint_path = f"abfss://checkpoint@tybuldatalakedemo.dfs.core.windows.net/"
table_name = "PiotrT_Demo.AutoLoaderDemo"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)
dbutils.fs.rm(target_path, True)
