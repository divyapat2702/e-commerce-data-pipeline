# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists ecommerce
# MAGIC managed location 'abfss://uc-data@stgecomdevind21.dfs.core.windows.net/ecommerce-catalog'
# MAGIC comment 'Ecomm Project catalog central india'

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ecommerce

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ecommerce.bronze;
# MAGIC create schema if not exists ecommerce.silver;
# MAGIC create schema if not exists ecommerce.gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases from ecommerce

# COMMAND ----------

