# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog ecommerce;
# MAGIC
# MAGIC create schema if not exists raw;
# MAGIC
# MAGIC create external volume if not exists raw.raw_landing
# MAGIC   location 'abfss://ecomm-raw-data@stgecomdevind21.dfs.core.windows.net/'
# MAGIC   comment 'landing zone for raw data'

# COMMAND ----------

