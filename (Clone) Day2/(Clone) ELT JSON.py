# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/saunextadls/raw/json

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/saunextadls/raw/json")

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

df1.write.mode("overwrite").option("path","dbfs:/mnt/saunextadls/raw/output/sanjana/json").saveAsTable("json.bronze1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.bronze1

# COMMAND ----------

df2=spark.read.json("dbfs:/mnt/saunextadls/raw/json/15.8.23.json")

# COMMAND ----------


