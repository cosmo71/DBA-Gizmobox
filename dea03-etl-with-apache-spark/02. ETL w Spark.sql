-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Extract Data from the Customers JSON File
-- MAGIC 1. Query Single File
-- MAGIC 2. Query List of files using wirlcard characters
-- MAGIC 3. Query all the files in a Folder

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/gizmobox/landing/operational_data/customers/

-- COMMAND ----------

SELECT * FROM json.`dbfs:/Volumes/gizmobox/landing/operational_data/customers/customers_2024_10.json`

-- COMMAND ----------

----------------------------- WILDCARD *         --------------------------
SELECT * FROM json.`dbfs:/Volumes/gizmobox/landing/operational_data/customers/customers_2024_*.json`

-- COMMAND ----------

----------------------------- FOLDER --------------------------
SELECT * FROM json.`/Volumes/gizmobox/landing/operational_data/customers/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Select File Metadata

-- COMMAND ----------

SELECT _metadata.file_path as file_path,
*
FROM json.`/Volumes/gizmobox/landing/operational_data/customers`;

-- COMMAND ----------



-- COMMAND ----------

Something I just learned, we can use this metadata to tag all of the records to trace back to the exact file if there is some error with that record

use SELECT _metadata FROM `file_ext.path`;

-- COMMAND ----------

-- Creating views

-- COMMAND ----------

--  In Bronze layer you can have the data in multiple places: Tables, Views, Functions
-- View does not store the data, just the SELECT statement

-- COMMAND ----------

CREATE OR REPLACE VIEW gizmobox.bronze.v_customers -- (Three level namespace, catalog.schema.<view or table>)
AS 
SELECT 
      _metadata.file_path AS file_path, *
  FROM json.`/Volumes/gizmobox/landing/operational_data/customers`;

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.v_customers;

-- COMMAND ----------

--- Temp View

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW tv_customers 
AS 
SELECT 
      _metadata.file_path AS file_path, *
  FROM json.`/Volumes/gizmobox/landing/operational_data/customers`;

-- COMMAND ----------

SELECT * FROM tv_customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####When a notebook attached to a cluster -- spark sessions starts
-- MAGIC ####When a notebook detached from a cluster -- spark sessions ends

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Temp Views only exist until spark session ends
-- MAGIC ####Global Temp views exist until 

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW gtv_customers 
AS 
SELECT 
      _metadata.file_path AS file_path, *
  FROM json.`/Volumes/gizmobox/landing/operational_data/customers`;

-- COMMAND ----------

SELECT * FROM global_temp.gtv_customers; -- You need to use the global_temp schema when querying the global tmep views

-- COMMAND ----------

-- Global Temp views can be accessed from any notebook as long as cluster is running 