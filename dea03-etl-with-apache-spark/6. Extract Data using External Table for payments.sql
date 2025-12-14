-- Databricks notebook source
-- MAGIC %fs ls 'abfss://gizmobox@deacourseextdl229.dfs.core.windows.net/landing/external_data/payments'

-- COMMAND ----------

-- Create External Table, syntax is ame as when we create a table

CREATE TABLE IF NOT EXISTS gizmobox.bronze.payments
  (payment_id INTEGER, order_id INTEGER, payment_timestamp TIMESTAMP, payment_status INTEGER, payment_method STRING)
  USING CSV
  OPTIONS(
    header="true",
    delimeter=","
  )
  LOCATION 'abfss://gizmobox@deacourseextdl229.dfs.core.windows.net/landing/external_data/payments';

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.payments;

-- COMMAND ----------

DESCRIBE EXTENDED gizmobox.bronze.payments;

-- COMMAND ----------

-- What happens when you query the table and someone ADDS/DELETES/UPDATES the files under the external table. 

-- COMMAND ----------

-- If a file is deleted then we need to use REFRESH TABLE command so that spark looks at the folder again to see how many files there are. , If you dont do this you will get an error saying there is a file not present at that location. 

-- COMMAND ----------

REFRESH TABLE gizmobox.bronze.payments;

-- COMMAND ----------

-- When you drop the external table the files are not deleted but the metadata for the table is. 