# Databricks notebook source
service_credential = dbutils.secrets.get(scope="Demo-Scope",key="Dabricks-service-principal-secret")

spark.conf.set("fs.azure.account.auth.type.tybuldatalakedemo.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.tybuldatalakedemo.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.tybuldatalakedemo.dfs.core.windows.net", "59a63017-a581-4651-ba0f-fe18b30e9b8e")
spark.conf.set("fs.azure.account.oauth2.client.secret.tybuldatalakedemo.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.tybuldatalakedemo.dfs.core.windows.net", "https://login.microsoftonline.com/94032769-6d65-484f-842f-09d05b51c9ad/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@tybuldatalakedemo.dfs.core.windows.net"))

# COMMAND ----------

df = spark.read.format("json").load("abfss://raw@tybuldatalakedemo.dfs.core.windows.net/Rebrickable/Minifigs/minifigs.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import explode

df2 = (
    df
    .withColumn("explodedArray", explode(df.results))
)

display(df2)


# COMMAND ----------

df2.createOrReplaceTempView("myData")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   myData

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   myData

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC   explodedArray.last_modified_dt AS LastModifiedDatetime,
# MAGIC   explodedArray.name AS Name,
# MAGIC   explodedArray.num_parts AS NumberOfParts,
# MAGIC   explodedArray.set_img_url AS ImageURL,
# MAGIC   explodedArray.set_num AS SetNumber,
# MAGIC   explodedArray.set_url AS SetURL
# MAGIC FROM
# MAGIC   myData

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC   coalesce(previous, 'Default Value') AS previousWithoutNulls,
# MAGIC   *
# MAGIC FROM
# MAGIC   myData
# MAGIC WHERE
# MAGIC   previous IS NOT NULL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC   explodedArray.last_modified_dt AS LastModifiedDatetimeOriginal,
# MAGIC   to_date(explodedArray.last_modified_dt) AS LastModifiedDate,
# MAGIC   to_timestamp(explodedArray.last_modified_dt) AS LastModifiedDatetime,
# MAGIC   explodedArray.name AS Name,
# MAGIC   CASE
# MAGIC     WHEN upper(explodedArray.name) LIKE '%TOY%' THEN 'Toy'
# MAGIC     WHEN upper(explodedArray.name) LIKE '%DROID%' THEN 'Droid'
# MAGIC     ELSE 'Unknown'
# MAGIC   END AS MinifigType,
# MAGIC   cast(explodedArray.num_parts AS int) AS NumberOfPartsInt,
# MAGIC   explodedArray.set_img_url AS ImageURL,
# MAGIC   explodedArray.set_num AS SetNumber,
# MAGIC   explodedArray.set_url AS SetURL
# MAGIC FROM
# MAGIC   myData

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   explodedArray.last_modified_dt AS LastModifiedDatetimeOriginal,
# MAGIC   explodedArray.name AS Name,
# MAGIC   cast(explodedArray.num_parts AS int) AS NumberOfPartsInt,
# MAGIC   explodedArray.set_img_url AS ImageURL,
# MAGIC   explodedArray.set_num AS SetNumber,
# MAGIC   explodedArray.set_url AS SetURL
# MAGIC FROM
# MAGIC   myData

# COMMAND ----------

from pyspark.sql.functions import explode

df3 = spark.read.format("json").load("abfss://raw@tybuldatalakedemo.dfs.core.windows.net/Rebrickable/Minifigs/*.json")

df_duplicates = (
    df3
    .withColumn("explodedArray", explode(df3.results))
)

df_duplicates.createOrReplaceTempView("myDuplicates")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   explodedArray.set_num,
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   myDuplicates
# MAGIC GROUP BY  
# MAGIC   explodedArray.set_num
# MAGIC HAVING  
# MAGIC   COUNT(*) > 1
# MAGIC ORDER BY 
# MAGIC   COUNT(*) DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC   explodedArray.last_modified_dt AS LastModifiedDatetime,
# MAGIC   explodedArray.name AS Name,
# MAGIC   explodedArray.num_parts AS NumberOfParts,
# MAGIC   explodedArray.set_img_url AS ImageURL,
# MAGIC   explodedArray.set_num AS SetNumber,
# MAGIC   explodedArray.set_url AS SetURL
# MAGIC FROM
# MAGIC   myDuplicates
# MAGIC WHERE
# MAGIC   explodedArray.set_num IN (
# MAGIC     SELECT
# MAGIC       explodedArray.set_num
# MAGIC     FROM
# MAGIC       myDuplicates
# MAGIC     GROUP BY  
# MAGIC       explodedArray.set_num
# MAGIC     HAVING  
# MAGIC       COUNT(*) > 1
# MAGIC   )
# MAGIC ORDER BY
# MAGIC   explodedArray.set_num

# COMMAND ----------

# MAGIC %sql
# MAGIC ;WITH Duplicates AS (
# MAGIC   SELECT  
# MAGIC     explodedArray.last_modified_dt AS LastModifiedDatetime,
# MAGIC     explodedArray.name AS Name,
# MAGIC     explodedArray.num_parts AS NumberOfParts,
# MAGIC     explodedArray.set_img_url AS ImageURL,
# MAGIC     explodedArray.set_num AS SetNumber,
# MAGIC     explodedArray.set_url AS SetURL,
# MAGIC     row_number() OVER(PARTITION BY explodedArray.set_num ORDER BY to_timestamp(explodedArray.last_modified_dt) DESC) AS RN
# MAGIC   FROM
# MAGIC     myDuplicates
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   Duplicates
# MAGIC
