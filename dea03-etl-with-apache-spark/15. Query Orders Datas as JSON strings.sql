-- Databricks notebook source
SELECT *
FROM gizmobox.bronze.v_orders;

-- COMMAND ----------

-- Extract Top Leve lObject Values

-- COMMAND ----------

-- Top level object 
-- column_name:extraction_path

-- COMMAND ----------

SELECT  value:order_id AS order_id,
        value:order_date As order_date
FROM gizmobox.bronze.v_orders;

-- COMMAND ----------

-- 2. Extract Array Elements 

-- COMMAND ----------

SELECT  value:items[0] AS item1,
        value:items[1] AS item2
FROM gizmobox.bronze.v_orders;

-- COMMAND ----------

SELECT  value:items[0].item_id AS item1_item_id,
        value:items[0] AS item1
FROM gizmobox.bronze.v_orders;

-- COMMAND ----------

SELECT  value:items[0].item_id::INTEGER AS item1_item_id,
        value:items[0] AS item1
FROM gizmobox.bronze.v_orders;

-- COMMAND ----------

