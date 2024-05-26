# Databricks notebook source
display(dbutils.fs.ls("abfss://raw@tybuldatalakedemo.dfs.core.windows.net"))

# COMMAND ----------

from pyspark.sql.functions import explode, col

df = (
    spark
    .read
    .format("json")
    .load("abfss://raw@tybuldatalakedemo.dfs.core.windows.net/Rebrickable/Minifigs/minifigs.json")
    .withColumn("explodedArray", explode(col("results")))
)

df.createOrReplaceTempView("myData")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   explodedArray.last_modified_dt AS LastModifiedDatetimeOriginal,
# MAGIC   to_date(explodedArray.last_modified_dt) AS LastModifiedDate,
# MAGIC   to_timestamp(explodedArray.last_modified_dt) AS LastModifiedDatetime,
# MAGIC   explodedArray.name AS Name,
# MAGIC   CASE
# MAGIC     WHEN explodedArray.name LIKE '%Toy%' THEN 'Toy'
# MAGIC     WHEN explodedArray.name LIKE '%Droid%' THEN 'Droid'
# MAGIC     ELSE 'Other'
# MAGIC   END AS MinifigType,
# MAGIC   cast(explodedArray.num_parts AS int) AS NumberOfParts,
# MAGIC   explodedArray.set_img_url AS ImageURL,
# MAGIC   explodedArray.set_num AS SetNumber,
# MAGIC   explodedArray.set_url AS SetURL
# MAGIC FROM
# MAGIC   myData

# COMMAND ----------

_sqldf \
    .write \
    .format("delta") \
    .save("abfss://curated@tybuldatalakedemo.dfs.core.windows.net/Rebrickable/Minifigs")

# COMMAND ----------

display(spark.read.format("delta").load("abfss://curated@tybuldatalakedemo.dfs.core.windows.net/Rebrickable/Minifigs"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE PiotrT_Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE PiotrT_Demo.Minifigs
# MAGIC AS
# MAGIC SELECT
# MAGIC   explodedArray.last_modified_dt AS LastModifiedDatetimeOriginal,
# MAGIC   to_date(explodedArray.last_modified_dt) AS LastModifiedDate,
# MAGIC   to_timestamp(explodedArray.last_modified_dt) AS LastModifiedDatetime,
# MAGIC   explodedArray.name AS Name,
# MAGIC   CASE
# MAGIC     WHEN explodedArray.name LIKE '%Toy%' THEN 'Toy'
# MAGIC     WHEN explodedArray.name LIKE '%Droid%' THEN 'Droid'
# MAGIC     ELSE 'Other'
# MAGIC   END AS MinifigType,
# MAGIC   cast(explodedArray.num_parts AS int) AS NumberOfParts,
# MAGIC   explodedArray.set_img_url AS ImageURL,
# MAGIC   explodedArray.set_num AS SetNumber,
# MAGIC   explodedArray.set_url AS SetURL
# MAGIC FROM
# MAGIC   myData

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PiotrT_Demo.minifigs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE PiotrT_Demo.Minifigs_External
# MAGIC LOCATION 'abfss://curated@tybuldatalakedemo.dfs.core.windows.net/Rebrickable/Minifigs_External'
# MAGIC AS
# MAGIC SELECT
# MAGIC   explodedArray.last_modified_dt AS LastModifiedDatetimeOriginal,
# MAGIC   to_date(explodedArray.last_modified_dt) AS LastModifiedDate,
# MAGIC   to_timestamp(explodedArray.last_modified_dt) AS LastModifiedDatetime,
# MAGIC   explodedArray.name AS Name,
# MAGIC   CASE
# MAGIC     WHEN explodedArray.name LIKE '%Toy%' THEN 'Toy'
# MAGIC     WHEN explodedArray.name LIKE '%Droid%' THEN 'Droid'
# MAGIC     ELSE 'Other'
# MAGIC   END AS MinifigType,
# MAGIC   cast(explodedArray.num_parts AS int) AS NumberOfParts,
# MAGIC   explodedArray.set_img_url AS ImageURL,
# MAGIC   explodedArray.set_num AS SetNumber,
# MAGIC   explodedArray.set_url AS SetURL
# MAGIC FROM
# MAGIC   myData

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM PiotrT_Demo.Minifigs_External

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO PiotrT_Demo.Minifigs_External(
# MAGIC     LastModifiedDatetimeOriginal,
# MAGIC     LastModifiedDate,
# MAGIC     LastModifiedDatetime,
# MAGIC     Name,
# MAGIC     MinifigType,
# MAGIC     NumberOfParts,
# MAGIC     ImageURL,
# MAGIC     SetNumber,
# MAGIC     SetURL
# MAGIC )
# MAGIC VALUES(
# MAGIC   "2020-05-27T21:47:00.694941Z",
# MAGIC   "2020-05-16",
# MAGIC   "2020-05-16T05:47:53.181+00:00",
# MAGIC   "New minifig 1",
# MAGIC   "Other",
# MAGIC   4,
# MAGIC   null,
# MAGIC   "fig-xxxx1",
# MAGIC   null
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PiotrT_Demo.Minifigs_External WHERE SetNumber = "fig-xxxx1"

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE
# MAGIC   PiotrT_Demo.Minifigs_External
# MAGIC SET
# MAGIC   Name = "Updated Minifig 1"
# MAGIC WHERE 
# MAGIC   SetNumber = "fig-xxxx1"

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM
# MAGIC   PiotrT_Demo.Minifigs_External
# MAGIC WHERE 
# MAGIC   SetNumber = "fig-xxxx1"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE PiotrT_Demo.Minifigs_Identity
# MAGIC (
# MAGIC   Id bigint GENERATED ALWAYS AS IDENTITY,
# MAGIC   LastModifiedDatetimeOriginal string,
# MAGIC   LastModifiedDate date,
# MAGIC   LastModifiedDatetime timestamp,
# MAGIC   Name string,
# MAGIC   MinifigType string,
# MAGIC   NumberOfParts int,
# MAGIC   ImageURL string,
# MAGIC   SetNumber string,
# MAGIC   SetURL string
# MAGIC )
# MAGIC LOCATION 'abfss://curated@tybuldatalakedemo.dfs.core.windows.net/Rebrickable/Minifigs_Identity'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO PiotrT_Demo.Minifigs_Identity(
# MAGIC     LastModifiedDatetimeOriginal,
# MAGIC     LastModifiedDate,
# MAGIC     LastModifiedDatetime,
# MAGIC     Name,
# MAGIC     MinifigType,
# MAGIC     NumberOfParts,
# MAGIC     ImageURL,
# MAGIC     SetNumber,
# MAGIC     SetURL
# MAGIC )
# MAGIC SELECT
# MAGIC   explodedArray.last_modified_dt AS LastModifiedDatetimeOriginal,
# MAGIC   to_date(explodedArray.last_modified_dt) AS LastModifiedDate,
# MAGIC   to_timestamp(explodedArray.last_modified_dt) AS LastModifiedDatetime,
# MAGIC   explodedArray.name AS Name,
# MAGIC   CASE
# MAGIC     WHEN explodedArray.name LIKE '%Toy%' THEN 'Toy'
# MAGIC     WHEN explodedArray.name LIKE '%Droid%' THEN 'Droid'
# MAGIC     ELSE 'Other'
# MAGIC   END AS MinifigType,
# MAGIC   cast(explodedArray.num_parts AS int) AS NumberOfParts,
# MAGIC   explodedArray.set_img_url AS ImageURL,
# MAGIC   explodedArray.set_num AS SetNumber,
# MAGIC   explodedArray.set_url AS SetURL
# MAGIC FROM
# MAGIC   myData

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PiotrT_Demo.Minifigs_Identity
