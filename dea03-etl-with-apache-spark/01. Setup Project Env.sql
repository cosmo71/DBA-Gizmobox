-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Set0up project env for Gizmo Box Data Lakehoues

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Access the container gizmobox

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://gizmobox@deacourseextdl229.core.windows.net/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Create External Location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS dea_course_ext_dl_gizmobox
  URL 'abfss://gizmobox@deacourseextdl229.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL dea_course-ext-sc)
  COMMENT 'External Locaation for Demo purposes'

-- COMMAND ----------

# We only need credential for the stroage account, then we can create a External Location in Unity Catalog with the URL and name it as we want

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://gizmobox@deacourseextdl229.dfs.core.windows.net/landing/operational_data/'

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Create the catalog - gizmobox

-- COMMAND ----------

-- In prod you would have to use sql to create catalogs -- this will be tested in the exam

SHOW CATALOGS;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS gizmobox
   MANAGED LOCATION 'abfss://gizmobox@deacourseextdl229.dfs.core.windows.net/' -- ADLS Location
   COMMENT 'This is the catalog for the Gizmobox Lakehouse';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Create Schemas
-- MAGIC 1. Landing
-- MAGIC 2. Bronze
-- MAGIC 3. Silver
-- MAGIC 4. Gold

-- COMMAND ----------

-- Check what catalog you are in before creating schemas
SELECT current_catalog();

-- COMMAND ----------

USE CATALOG gizmobox;

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

USE CATALOG gizmobox;

CREATE SCHEMA IF NOT EXISTS landing
   MANAGED LOCATION 'abfss://gizmobox@deacourseextdl229.dfs.core.windows.net/landing'; -- ADLS Location

CREATE SCHEMA IF NOT EXISTS bronze
   MANAGED LOCATION 'abfss://gizmobox@deacourseextdl229.dfs.core.windows.net/bronze'; -- ADLS Location

CREATE SCHEMA IF NOT EXISTS silver
   MANAGED LOCATION 'abfss://gizmobox@deacourseextdl229.dfs.core.windows.net/silver'; -- ADLS Location

CREATE SCHEMA IF NOT EXISTS gold
   MANAGED LOCATION 'abfss://gizmobox@deacourseextdl229.dfs.core.windows.net/gold'; -- ADLS Location
       
    
    -- Location is for hive metastore
    -- Managed Location is for ADLS

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Create Volume

-- COMMAND ----------

USE CATALOG gizmobox;
USE SCHEMA landing;


-- Volumes give you an easier way to reference your files and in unity catalog and you can do RBAC in catalog by going to volume, you can also now just use the catalog path instead of ADLS to access the data
CREATE EXTERNAL VOLUME IF NOT EXISTS operational_data
LOCATION 'abfss://gizmobox@deacourseextdl229.dfs.core.windows.net/landing/operational_data/';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/gizmobox/landing/operational_data

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

