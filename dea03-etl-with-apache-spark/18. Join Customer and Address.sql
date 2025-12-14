-- Databricks notebook source
SELECT * FROM gizmobox.silver.customers;

-- COMMAND ----------

SELECT * FROM gizmobox.silver.addresses;

-- COMMAND ----------

SELECT c.customer_id,
       c.customer_name,
       c.email,
       c.date_of_birth,
       c.member_since,
       c.telephone,
       a.shipping_address_line_1,
       a.shipping_city,
       a.shipping_state,
       a.shipping_postcode,
       a.billing_address_line_1,
       a.billing_city,
       a.billing_state,
       a.billing_postcode
FROM gizmobox.silver.customers c
INNER JOIN gizmobox.silver.addresses a 
    ON c.customer_id = a.customer_id;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gizmobox.gold.customer_address
SELECT c.customer_id,
       c.customer_name,
       c.email,
       c.date_of_birth,
       c.member_since,
       c.telephone,
       a.shipping_address_line_1,
       a.shipping_city,
       a.shipping_state,
       a.shipping_postcode,
       a.billing_address_line_1,
       a.billing_city,
       a.billing_state,
       a.billing_postcode
FROM gizmobox.silver.customers c
INNER JOIN gizmobox.silver.addresses a 
    ON c.customer_id = a.customer_id;

-- COMMAND ----------

SELECT * FROM gizmobox.gold.customer_address;