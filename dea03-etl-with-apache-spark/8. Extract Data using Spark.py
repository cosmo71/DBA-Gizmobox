# Databricks notebook source
df = spark.sql('<SQL Belongs here>')
display(df)

# COMMAND ----------

# Normal 

df = spark.read.format("json").load(path)

# COMMAND ----------

v_add = spark.table('gizmobox.bronze.v_addresses')

# COMMAND ----------

display(v_add)

# COMMAND ----------

