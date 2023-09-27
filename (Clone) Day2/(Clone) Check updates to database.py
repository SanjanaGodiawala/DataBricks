# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * from json.bronze1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from json.bronze1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from json.bronze1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from json.bronze1 where path = "dbfs:/mnt/saunextadls/raw/json/14.8.2023.json"

# COMMAND ----------


