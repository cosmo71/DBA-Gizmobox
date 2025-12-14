-- Databricks notebook source
SELECT * FROM gizmobox.bronze.v_memberships;

-- COMMAND ----------

-- Extract customer_id from the file path

-- COMMAND ----------

SELECT regexp_extract(path, '.*/([0-9]+)\\.png$', 1) AS customer_id,
        content AS membership_card
FROM gizmobox.bronze.v_memberships;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gizmobox.silver.members
AS 
SELECT regexp_extract(path, '.*/([0-9]+)\\.png$', 1) AS customer_id,
        content AS membership_card
FROM gizmobox.bronze.v_memberships;

-- COMMAND ----------

SELECT * FROM gizmobox.silver.members;