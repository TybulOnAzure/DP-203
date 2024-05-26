# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC <h1>This is a  super notebook!</h1>
# MAGIC

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta/"))

# COMMAND ----------



# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta/")
display(df)

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.tybuldatalakedemo.dfs.core.windows.net",
    "tH9CpVSWNs+XhIvuBM7f16MzYcKPNgPkZ3gslxaH2o29S1H9zNYEW+XaaDj68/rQLfX77Cp+69D3+AStmfWAog==")

# COMMAND ----------

display(dbutils.fs.ls("bfss://raw@tybuldatalakedemo.dfs.core.windows.net/tybultrainingsql.database.windows.net/AdventureWorks/Year=2023/Month=10/Day=27/"))

# COMMAND ----------

spark.read.option("header", "true").csv("abfss://raw@tybuldatalakedemo.dfs.core.windows.net/tybultrainingsql.database.windows.net/AdventureWorks/Year=2023/Month=10/Day=27/\[SalesLT]_\[Customer].csv").printSchema()

# COMMAND ----------

df = spark \
    .read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("abfss://raw@tybuldatalakedemo.dfs.core.windows.net/tybultrainingsql.database.windows.net/AdventureWorks/Year=2023/Month=10/Day=27/\[SalesLT]_\[Customer].csv")

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT  
# MAGIC   *
# MAGIC FROM
# MAGIC   csv.`abfss://raw@tybuldatalakedemo.dfs.core.windows.net/tybultrainingsql.database.windows.net/AdventureWorks/Year=2023/Month=10/Day=27/\[SalesLT]_\[Customer].csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   myData

# COMMAND ----------

df.createOrReplaceTempView("myData")

# COMMAND ----------

newDF = spark.sql("SELECT * FROM myData")

# COMMAND ----------

display(newDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   LastName, 
# MAGIC   COUNT(*) AS NameCount
# MAGIC FROM
# MAGIC   myData
# MAGIC GROUP BY
# MAGIC   LastName
# MAGIC HAVING
# MAGIC   COUNT(*) > 5
# MAGIC ORDER BY  
# MAGIC   NameCount DESC
# MAGIC
