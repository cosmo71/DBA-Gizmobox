# Databricks notebook source
# Extract Date and Time from payment_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   payment_id,
# MAGIC   order_id,
# MAGIC   CAST(date_format(payment_timestamp, 'yyy-MM-dd') AS DATE) as payment_date,
# MAGIC   date_format(payment_timestamp, 'HH:mm:ss') as payment_time,
# MAGIC   CASE 
# MAGIC       WHEN payment_status = 1 THEN 'Succes'
# MAGIC       WHEN payment_status = 2 THEN 'Pending'
# MAGIC       WHEN payment_status = 3 THEN 'Canceling'
# MAGIC       WHEN payment_status = 4 THEN 'Failed'
# MAGIC       END as payment_status,
# MAGIC   payment_method
# MAGIC FROM gizmobox.bronze.payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gizmobox.silver.payments
# MAGIC AS 
# MAGIC SELECT 
# MAGIC   payment_id,
# MAGIC   order_id,
# MAGIC   CAST(date_format(payment_timestamp, 'yyy-MM-dd') AS DATE) as payment_date,
# MAGIC   date_format(payment_timestamp, 'HH:mm:ss') as payment_time,
# MAGIC   CASE 
# MAGIC       WHEN payment_status = 1 THEN 'Succes'
# MAGIC       WHEN payment_status = 2 THEN 'Pending'
# MAGIC       WHEN payment_status = 3 THEN 'Canceling'
# MAGIC       WHEN payment_status = 4 THEN 'Failed'
# MAGIC       END as payment_status,
# MAGIC   payment_method
# MAGIC FROM gizmobox.bronze.payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.payments;