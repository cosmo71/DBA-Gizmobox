-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####CREATE OR REPLACE FUNCTION catalog_name.schema_name.udf_name(param_name data_type)
-- MAGIC #####RETURNS return_type
-- MAGIC #####RETURN expression;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gizmobox.default.get_fullname(firstname STRING, surname STRING)
RETURNS STRING
RETURN CONCAT(initcap(firstname), ' ', initcap(surname));

-- COMMAND ----------

SELECT gizmobox.default.get_fullname('john', 'smith');

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED gizmobox.default.get_fullname;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gizmobox.default.get_payment_status(payment_status INT)
RETURNS STRING
return CASE payment_status
      WHEN 1 THEN 'Succes'
      WHEN 2 THEN 'Pending'
      WHEN 3 THEN 'Canceling'
      WHEN 4 THEN 'Failed'
      END ;


-- COMMAND ----------

SELECT 
  payment_id,
  order_id,
  CAST(date_format(payment_timestamp, 'yyy-MM-dd') AS DATE) as payment_date,
  date_format(payment_timestamp, 'HH:mm:ss') as payment_time,
  gizmobox.default.get_payment_status(payment_status) as payment_status,
  payment_method
FROM gizmobox.bronze.payments;

-- COMMAND ----------

