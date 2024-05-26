#!/usr/bin/env python
# coding: utf-8

# ## Notebook 2
# 
# 
# 

# In[14]:


get_ipython().run_cell_magic('pyspark', '', "df = spark.read.load('abfss://raw@tybuldatalakedemo.dfs.core.windows.net/Rebrickable/Minifigs/minifigs.json', format='json')\r\ndisplay(df.limit(10))\n")


# In[15]:


from pyspark.sql.functions import explode, col

df = (
    spark
    .read
    .format("json")
    .load("abfss://raw@tybuldatalakedemo.dfs.core.windows.net/Rebrickable/Minifigs/minifigs.json")
    .withColumn("explodedArray", explode(col("results")))
)

df.createOrReplaceTempView("myData")


# In[16]:


df2 = spark.sql("""
SELECT
  explodedArray.last_modified_dt AS LastModifiedDatetimeOriginal,
  to_date(explodedArray.last_modified_dt) AS LastModifiedDate,
  to_timestamp(explodedArray.last_modified_dt) AS LastModifiedDatetime,
  explodedArray.name AS Name,
  CASE
    WHEN explodedArray.name LIKE '%Toy%' THEN 'Toy'
    WHEN explodedArray.name LIKE '%Droid%' THEN 'Droid'
    ELSE 'Other'
  END AS MinifigType,
  cast(explodedArray.num_parts AS int) AS NumberOfParts,
  explodedArray.set_img_url AS ImageURL,
  explodedArray.set_num AS SetNumber,
  explodedArray.set_url AS SetURL
FROM
  myData
""")


# In[21]:


get_ipython().run_cell_magic('sql', '', '\r\nDESCRIBE TABLE EXTENDED piotrt_demo.minifigs_external\n')


# In[ ]:


df2 \
    .write \
    .format("delta") \
    .save("abfss://curated@tybuldatalakedemo.dfs.core.windows.net/Rebrickable/Minifigs")


# In[18]:


get_ipython().run_cell_magic('sql', '', "CREATE TABLE PiotrT_Demo.Minifigs_Delta\r\nUSING DELTA\r\nAS\r\nSELECT\r\n  explodedArray.last_modified_dt AS LastModifiedDatetimeOriginal,\r\n  to_date(explodedArray.last_modified_dt) AS LastModifiedDate,\r\n  to_timestamp(explodedArray.last_modified_dt) AS LastModifiedDatetime,\r\n  explodedArray.name AS Name,\r\n  CASE\r\n    WHEN explodedArray.name LIKE '%Toy%' THEN 'Toy'\r\n    WHEN explodedArray.name LIKE '%Droid%' THEN 'Droid'\r\n    ELSE 'Other'\r\n  END AS MinifigType,\r\n  cast(explodedArray.num_parts AS int) AS NumberOfParts,\r\n  explodedArray.set_img_url AS ImageURL,\r\n  explodedArray.set_num AS SetNumber,\r\n  explodedArray.set_url AS SetURL\r\nFROM\r\n  myData\n")


# In[20]:


get_ipython().run_cell_magic('sql', '', "CREATE TABLE PiotrT_Demo.Minifigs_External\r\nUSING DELTA\r\nLOCATION 'abfss://curated@tybuldatalakedemo.dfs.core.windows.net/Rebrickable/Minifigs_Synapse_External'\r\nAS\r\nSELECT\r\n  explodedArray.last_modified_dt AS LastModifiedDatetimeOriginal,\r\n  to_date(explodedArray.last_modified_dt) AS LastModifiedDate,\r\n  to_timestamp(explodedArray.last_modified_dt) AS LastModifiedDatetime,\r\n  explodedArray.name AS Name,\r\n  CASE\r\n    WHEN explodedArray.name LIKE '%Toy%' THEN 'Toy'\r\n    WHEN explodedArray.name LIKE '%Droid%' THEN 'Droid'\r\n    ELSE 'Other'\r\n  END AS MinifigType,\r\n  cast(explodedArray.num_parts AS int) AS NumberOfParts,\r\n  explodedArray.set_img_url AS ImageURL,\r\n  explodedArray.set_num AS SetNumber,\r\n  explodedArray.set_url AS SetURL\r\nFROM\r\n  myData\n")


# In[22]:


get_ipython().run_cell_magic('sql', '', 'SELECT COUNT(*) FROM PiotrT_Demo.Minifigs_External\n')

