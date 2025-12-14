# Databricks notebook source
# Profile data using UI 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.bronze.v_customers;

# COMMAND ----------

# Profile using BDUTILS packaage    

# COMMAND ----------

df = spark.table('gizmobox.bronze.v_customers')
dbutils.data.summarize(df)

# COMMAND ----------

# Profile Data Manually 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM gizmobox.bronze.v_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(customer_id), COUNT(email), COUNT(telephone)
# MAGIC FROM gizmobox.bronze.v_customers;

# COMMAND ----------

# COUNT IF Function 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT_IF(customer_id IS NULL), COUNT_IF(email IS NULL), COUNT_IF(telephone IS NULL)
# MAGIC FROM gizmobox.bronze.v_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM gizmobox.bronze.v_customers
# MAGIC WHERE customer_id IS NULL;

# COMMAND ----------

# DISTINCT Keyword 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT customer_id) == COUNT(*) FROM gizmobox.bronze.v_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT customer_id), COUNT(*) FROM gizmobox.bronze.v_customers;

# COMMAND ----------

