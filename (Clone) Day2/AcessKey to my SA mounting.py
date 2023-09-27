# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://raw@adlsstoragesg.blob.core.windows.net",
  mount_point = "/mnt/adlsstoragesg/raw",
  extra_configs = {"fs.azure.account.key.adlsstoragesg.blob.core.windows.net":"uw8bEI4yl+iewO7I/keyTtEFsROgiVFQ84teP9SWRrhFsaoWerKBrOXtX71IomlTpnGUdjrk68sA+AStF7GNDA=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlsstoragesg/raw/json

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/adlsstoragesg/raw/json/14.8.2023.json")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import *
df1 = df.withColumn("ingestiondate",current_timestamp()).withColumn("path",input_file_name())

# COMMAND ----------

df1.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists json
# MAGIC

# COMMAND ----------

df1.write.mode("overwrite").option("path","dbfs:/mnt/adlsstoragesg/raw/output/sanjana/json").saveAsTable("json.bronze")

# COMMAND ----------



# COMMAND ----------


