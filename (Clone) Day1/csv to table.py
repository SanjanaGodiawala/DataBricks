# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/FileStore/tables/employees.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('test.employees_table')

# COMMAND ----------

df.write.mode('overwrite').option('delta.columnMapping.mode','name').saveAsTable('test.employees_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use test;
# MAGIC
# MAGIC select * from employees_table

# COMMAND ----------

# MAGIC %md
# MAGIC where is the meta data stored?

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/test.db

# COMMAND ----------


