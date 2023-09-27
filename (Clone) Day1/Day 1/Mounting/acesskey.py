# Databricks notebook source
dbutils.fs.mount(

  source = "wasbs://raw@sanly.blob.core.windows.net",

  mount_point = "/mnt/sanly/raw",

  extra_configs = {"fs.azure.account.key.sanly.blob.core.windows.net":"+wZyMJdwqiETIzCNMc/uvE0AJQ/2+fIGVKKvfx4um7lsUO0EPZjLx3efLhF9OihDdkaV1TBwq77j+AStSZRQ1Q=="})

# COMMAND ----------

# MAGIC %fs ls
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sanly/raw/Baby_Names.csv

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/sanly/raw/Baby_Names.csv")

# COMMAND ----------

output="dbfs:/mnt/sanly/raw/output"
df.write.mode("overwrite").parquet(f"{output}/Sanjana/babyname")

# COMMAND ----------

dbutils.fs.unmount("/mnt/sanly/raw")

# COMMAND ----------


