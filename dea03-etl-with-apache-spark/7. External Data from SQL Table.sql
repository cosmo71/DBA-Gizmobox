-- Databricks notebook source
-- To access data from SQL server then you have to use 
-- 1. JDBC 
  -- You can only create external tables in hive metastore not in unity catalog as of now. (check)
-- 2. Lakehouse Federation (Not required for DBA Exam)

-- COMMAND ----------

-- Create Bronze Schema in Hize metastore

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS hive_metastore.bronze;

-- COMMAND ----------

-- DROP TABLE IF EXISTS hive_metastore.bronze.refunds;


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS hive_metastore.bronze.refunds
USING JDBC
OPTIONS (
  url 'jdbc:sqlserver://gizmobox-srv229.database.windows.net:1433;database=gizmobox;user=gizmobox@gizmobox-srv229',
  dbtable 'refunds',
  user 'gizmobox',
  password 'Soham@234'
)

-- COMMAND ----------

SELECT * FROM hive_metastore.bronze.refunds;


-- COMMAND ----------

DESCRIBE EXTENDED hive_metastore.bronze.refunds;