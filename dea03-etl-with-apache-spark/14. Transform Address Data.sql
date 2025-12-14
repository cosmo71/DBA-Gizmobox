-- Databricks notebook source
-- Transform Address data
--   1. Create one record for each cutomer with 2 sets of address columns, 1 for shipping and 1 for billing address
--   2. Write transformed data to the Silver Schema 

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.v_addresses;

-- COMMAND ----------

-- Create one one record for each customer which includes both address

-- COMMAND ----------

-- PIVOT is multiple rows to 1 col

-- COMMAND ----------

SELECT *
FROM (
  SELECT 
    customer_id,
    address_type,
    address_line_1,
    city,
    state,postcode
    FROM 
  gizmobox.bronze.v_addresses)
  PIVOT(MAX(address_line_1) as address_line_1,
        MAX(city) as city,
        MAX(state) as state,
        MAX(postcode) as postcode
        FOR address_type IN ('shipping','billing')
  );

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gizmobox.silver.addresses
AS 
SELECT *
FROM (
  SELECT 
    customer_id,
    address_type,
    address_line_1,
    city,
    state,postcode
    FROM 
  gizmobox.bronze.v_addresses)
  PIVOT(MAX(address_line_1) as address_line_1,
        MAX(city) as city,
        MAX(state) as state,
        MAX(postcode) as postcode
        FOR address_type IN ('shipping','billing')
  );

-- COMMAND ----------

SELECT * FROM gizmobox.silver.addresses;