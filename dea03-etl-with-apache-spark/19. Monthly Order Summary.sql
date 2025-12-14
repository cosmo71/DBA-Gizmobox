-- Databricks notebook source
-- Monthly Order Summary
-- For each of the customer, produce the following summary per month
    -- 1. Total Orders
    -- 2. Total items bought
    -- 3. total amount spent

-- COMMAND ----------

 SELECT * FROM gizmobox.silver.orders;

-- COMMAND ----------

SELECT date_format(transaction_timestamp, "yyyy-MM") as order_month,
        customer_id,
        COUNT(DISTINCT order_id) as total_orders,
        SUM(quantity) as total_items_bought,
        SUM(price * quantity) AS total_amount
 FROM gizmobox.silver.orders
 GROUP BY order_month, customer_id;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gizmobox.gold.order_summary_monthly
AS 
SELECT date_format(transaction_timestamp, "yyyy-MM") as order_month,
        customer_id,
        COUNT(DISTINCT order_id) as total_orders,
        SUM(quantity) as total_items_bought,
        SUM(price * quantity) AS total_amount
 FROM gizmobox.silver.orders
 GROUP BY order_month, customer_id;

-- COMMAND ----------

SELECT * FROM gizmobox.gold.order_summary_monthly;