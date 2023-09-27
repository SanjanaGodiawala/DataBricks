-- Databricks notebook source
create schema sample

-- COMMAND ----------

Create table emp(id int, name string, age int, dept string)

-- COMMAND ----------

describe detail emp

-- COMMAND ----------

describe extended emp

-- COMMAND ----------

drop table emp

-- COMMAND ----------

Create table emp(id int, name string, age int, dept string) location "dbfs:/mnt/saunextadls/raw/delta/sanjana/emp"

-- COMMAND ----------

Create table sample.emp(id int, name string, age int, dept string) location "dbfs:/mnt/adlsstoragesg/raw/delta/emp"

-- COMMAND ----------

describe extended sample.emp

-- COMMAND ----------

describe history sample.emp

-- COMMAND ----------

select * from sample.emp

-- COMMAND ----------

insert into table sample.emp values(1,'a',23,'DE')

-- COMMAND ----------

select * from emp

-- COMMAND ----------

insert into table sample.emp values(2,'b',23,'DE')

-- COMMAND ----------

insert into table sample.emp values(3,'c',23,'DE'),
                            (4,'d',23,'DE')

-- COMMAND ----------

select * from sample.emp

-- COMMAND ----------

delete from sample.emp where id= 3

-- COMMAND ----------

select * from sample.emp

-- COMMAND ----------

Update sample.emp
set dept='DS'
where id= 4

-- COMMAND ----------

select * from emp version as of 3

-- COMMAND ----------

select * from emp timestamp as of '2023-09-27T08:37:49.000+0000'

-- COMMAND ----------

create table sample.oldemp as 
select * from sample.emp version as of 3

-- COMMAND ----------

describe history sample.emp

-- COMMAND ----------

select * from sample.emp

-- COMMAND ----------


