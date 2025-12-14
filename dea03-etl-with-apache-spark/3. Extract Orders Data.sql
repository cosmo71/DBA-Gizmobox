-- Databricks notebook source
SELECT * FROM json.`/Volumes/gizmobox/landing/operational_data/orders`;
-- /Volumes/Catalog/schema/Volume/Folder

-- COMMAND ----------

SELECT * FROM text.`/Volumes/gizmobox/landing/operational_data/orders`;

-- COMMAND ----------

-- Text format works better in this case because it does not parse just gets the data word for word
-- We want the data in the bronze layer to look as close as it can to the raw data
-- Based on your provider you can decide which parsing techniques to use like text & json

-- COMMAND ----------

CREATE OR REPLACE VIEW gizmobox.bronze.v_orders
AS
SELECT *  FROM text.`/Volumes/gizmobox/landing/operational_data/orders`;

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.v_orders;

-- COMMAND ----------

