# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     refund_id, 
# MAGIC     payment_id,
# MAGIC     CAST(date_format(refund_timestamp, 'yyyy-MM-dd') AS DATE) as refund_date,
# MAGIC     CAST(date_format(refund_timestamp, 'HH-mm-ss') AS DATE) as refund_time,
# MAGIC
# MAGIC     refund_amount, 
# MAGIC     SPLIT(refund_reason, ':')[0] as refund_reason,
# MAGIC     SPLIT(refund_reason, ':')[1] as refund_source
# MAGIC FROM hive_metastore.bronze.refunds;

# COMMAND ----------

# Extract using regex, not going to write here but its also an option 
# syntax: regex_extract(col, pattern, idx)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS hive_metastore.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.silver.refunds
# MAGIC AS
# MAGIC SELECT 
# MAGIC     refund_id, 
# MAGIC     payment_id,
# MAGIC     CAST(date_format(refund_timestamp, 'yyyy-MM-dd') AS DATE) as refund_date,
# MAGIC     CAST(date_format(refund_timestamp, 'HH-mm-ss') AS DATE) as refund_time,
# MAGIC
# MAGIC     refund_amount, 
# MAGIC     SPLIT(refund_reason, ':')[0] as refund_reason,
# MAGIC     SPLIT(refund_reason, ':')[1] as refund_source
# MAGIC FROM hive_metastore.bronze.refunds;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED hive_metastore.silver.refunds;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- When you are creating a managed table in a schema and the location is not specfied in the hive_metastore, the files will be written to the root storage which will mix up with other projects that you are working on 
# MAGIC -- dbfs:/user/hive/warehouse/silver.db/refunds