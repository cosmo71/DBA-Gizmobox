-- Databricks notebook source
-- MAGIC %fs ls '/Volumes/gizmobox/landing/operational_data/memberships/'

-- COMMAND ----------

SELECT * FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`;

-- COMMAND ----------

CREATE OR REPLACE VIEW gizmobox.bronze.v_memberships
AS 
SELECT * FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`;

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.v_memberships;

-- COMMAND ----------

