# Databricks notebook source
service_credential = dbutils.secrets.get(scope="Demo-Scope",key="Dabricks-service-principal-secret")

# Defining the service principal credentials for the Azure storage account
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "59a63017-a581-4651-ba0f-fe18b30e9b8e")
spark.conf.set("fs.azure.account.oauth2.client.secret", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/94032769-6d65-484f-842f-09d05b51c9ad/oauth2/token")

# Defining a separate set of service principal credentials for Azure Synapse Analytics (If not defined, the connector will use the Azure storage account credentials)
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.id", "59a63017-a581-4651-ba0f-fe18b30e9b8e")
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.secret", service_credential)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM piotrt_demo.minifigs

# COMMAND ----------

# DBTITLE 1,Write data to Dedicated SQL Pool
_sqldf.write \
  .format("sqldw") \
  .option("url", "jdbc:sqlserver://synapse-dp203-tybul-demo.sql.azuresynapse.net:1433;databaseName=DWHTest") \
  .option("tempDir", "abfss://stage@tybuldatalakedemo.dfs.core.windows.net/DatabricksStage") \
  .option("forwardSparkAzureStorageCredentials", "false") \
  .option("dbTable", "Rebrickable.Minifigs_real") \
  .option("enableServicePrincipalAuth", "true") \
  .mode("append") \
  .save()

# COMMAND ----------

# DBTITLE 1,Read the data from Synapse Dedicated SQL Pool
# Load data from an Azure Synapse query using Synapse Connector with OAuth authentication.
df2 = spark.read \
  .format("sqldw") \
  .option("url", "jdbc:sqlserver://synapse-dp203-tybul-demo.sql.azuresynapse.net:1433;databaseName=DWHTest") \
  .option("tempDir", "abfss://stage@tybuldatalakedemo.dfs.core.windows.net/DatabricksStage") \
  .option("forwardSparkAzureStorageCredentials", "false") \
  .option("dbTable", "Rebrickable.Minifigs_real") \
  .option("enableServicePrincipalAuth", "true") \
  .load()

display(df2)
