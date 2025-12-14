-- Databricks notebook source
--- Transform orders Data - String to. JSON
-- 1. Pr process the JSON String to fix the DQ issues
-- Transform JSON String to JSON object
-- Write transformed data to the silver schema

-- COMMAND ----------

SELECT *
FROM gizmobox.bronze.v_orders;

-- COMMAND ----------

-- 1. Pre Process the JSON strings to fix teh data quality issues

-- COMMAND ----------

SELECT value,
  regexp_replace(value, '"order_date": (\\d{4}-\\d{2}-\\d{2})', '"order_date": "\$1"') AS fixed_value
FROM gizmobox.bronze.v_orders;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW tv_orders_fixed
AS 
SELECT value,
  regexp_replace(value, '"order_date": (\\d{4}-\\d{2}-\\d{2})', '"order_date": "\$1"') AS fixed_value
FROM gizmobox.bronze.v_orders;

-- COMMAND ----------

-- Transform JSON String to JSON Object
----- schema_of_json
----- from_json

-- COMMAND ----------

SELECT CAST(schema_of_json(fixed_value) AS STRING) AS schema,
        fixed_value
FROM tv_orders_fixed
LIMIT 1;

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

SELECT from_json(fixed_value, "STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color: STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>") AS json_value,
        fixed_value
FROM  tv_orders_fixed

-- COMMAND ----------


CREATE OR REPLACE TABLE gizmobox.silver.orders_json
AS
SELECT from_json(fixed_value, "STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color: STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>") AS json_value
FROM  tv_orders_fixed;

-- COMMAND ----------

SELECT * FROM gizmobox.silver.orders_json;

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

