-- Databricks notebook source
SELECT * FROM csv.`/Volumes/gizmobox/landing/operational_data/addresses`;

-- COMMAND ----------

-- Use the read_files function 

-- COMMAND ----------

SELECT * FROM read_files('/Volumes/gizmobox/landing/operational_data/addresses', 
                          format=>'csv',
                          delimiter=>'\t',
                          header => True
                          )

-- COMMAND ----------

CREATE OR REPLACE VIEW gizmobox.bronze.v_addresses
AS 
SELECT * FROM read_files('/Volumes/gizmobox/landing/operational_data/addresses', 
                          format=>'csv',
                          delimiter=>'\t',
                          header => True
                          );

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

