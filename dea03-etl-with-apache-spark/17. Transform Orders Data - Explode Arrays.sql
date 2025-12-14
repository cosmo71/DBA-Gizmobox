-- Databricks notebook source
-- 1. Access elements from the JSON object
-- 2. Deduplicate Array Elements
-- 3. Explode Arrays
-- 4. Write the Transformed Data to Silver Schema

-- COMMAND ----------

SELECT *
FROM gizmobox.silver.orders_json;

-- COMMAND ----------

-- Access elements from the JSON Object

-- COMMAND ----------

SELECT json_value.order_id,
        json_value.order_status,
        json_value.payment_method,
        json_value.total_amount,
        json_value.transaction_timestamp,
        json_value.customer_id,
        json_value.items
FROM gizmobox.silver.orders_json;

-- COMMAND ----------


SELECT json_value.order_id,
        json_value.order_status,
        json_value.payment_method,
        json_value.total_amount,
        json_value.transaction_timestamp,
        json_value.customer_id,
        array_distinct(json_value.items)
FROM gizmobox.silver.orders_json;

-- COMMAND ----------

-- 3. Explode arrays

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW tv_orders_exploded
AS
SELECT json_value.order_id,
        json_value.order_status,
        json_value.payment_method,
        json_value.total_amount,
        json_value.transaction_timestamp,
        json_value.customer_id,
        explode(array_distinct(json_value.items)) AS item 
FROM gizmobox.silver.orders_json;

-- COMMAND ----------

SELECT order_id, 
    order_status,
    payment_method,
    total_amount,
    transaction_timestamp,
    customer_id,
    item.item_id, 
    item.name,
    item.price,
    item.quantity,
    item.category,
    item.details.brand,
    item.details.color
FROM tv_orders_exploded;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gizmobox.silver.orders
AS 
SELECT order_id, 
    order_status,
    payment_method,
    total_amount,
    transaction_timestamp,
    customer_id,
    item.item_id, 
    item.name,
    item.price,
    item.quantity,
    item.category,
    item.details.brand,
    item.details.color
FROM tv_orders_exploded;

-- COMMAND ----------

SELECT * FROM gizmobox.silver.orders;