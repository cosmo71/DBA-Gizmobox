# Databricks notebook source
# Remove records with NULL customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gizmobox.bronze.v_customers
# MAGIC WHERE customer_id IS NOT NULL;

# COMMAND ----------

# Remove exact duplicate records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT *
# MAGIC FROM gizmobox.bronze.v_customers
# MAGIC WHERE customer_id IS NOT NULL
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT *
# MAGIC FROM gizmobox.bronze.v_customers
# MAGIC WHERE (customer_id, created_timestamp)  IN (
# MAGIC   SELECT customer_id, MAX(created_timestamp)
# MAGIC   FROM gizmobox.bronze.v_customers
# MAGIC   WHERE customer_id IS NOT NULL
# MAGIC   GROUP BY customer_id
# MAGIC )
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# Cast teh column values to the correct data types

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS (SELECT DISTINCT *
# MAGIC FROM gizmobox.bronze.v_customers
# MAGIC WHERE (customer_id, created_timestamp)  IN (
# MAGIC   SELECT customer_id, MAX(created_timestamp)
# MAGIC   FROM gizmobox.bronze.v_customers
# MAGIC   WHERE customer_id IS NOT NULL
# MAGIC   GROUP BY customer_id
# MAGIC )
# MAGIC ORDER BY customer_id
# MAGIC   )
# MAGIC   SELECT CAST(t.created_timestamp AS TIMESTAMP) AS created_timestamp,
# MAGIC   t.customer_id,
# MAGIC   t.customer_name,
# MAGIC   CAST(t.date_of_birth AS DATE) AS date_of_birth,
# MAGIC   t.email,
# MAGIC   CAST(t.member_since AS DATE) AS member_since,
# MAGIC   t.telephone
# MAGIC   FROM CTE t
# MAGIC
# MAGIC ;

# COMMAND ----------

# WRITE TABLE TO DELTA TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CETAS TABLE , this is a managed table , it takes longer because it stores data unline an external table
# MAGIC CREATE TABLE gizmobox.silver.customers 
# MAGIC AS 
# MAGIC WITH CTE AS (SELECT DISTINCT *
# MAGIC FROM gizmobox.bronze.v_customers
# MAGIC WHERE (customer_id, created_timestamp)  IN (
# MAGIC   SELECT customer_id, MAX(created_timestamp)
# MAGIC   FROM gizmobox.bronze.v_customers
# MAGIC   WHERE customer_id IS NOT NULL
# MAGIC   GROUP BY customer_id
# MAGIC )
# MAGIC ORDER BY customer_id
# MAGIC   )
# MAGIC   SELECT CAST(t.created_timestamp AS TIMESTAMP) AS created_timestamp,
# MAGIC   t.customer_id,
# MAGIC   t.customer_name,
# MAGIC   CAST(t.date_of_birth AS DATE) AS date_of_birth,
# MAGIC   t.email,
# MAGIC   CAST(t.member_since AS DATE) AS member_since,
# MAGIC   t.telephone
# MAGIC   FROM CTE t
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.customers ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED gizmobox.silver.customers;

# COMMAND ----------

# Managed - you created the table in Azure which stores data
# External table - you created the table over already stored data