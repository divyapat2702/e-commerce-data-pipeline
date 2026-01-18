# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

dbutils.widgets.text("catalog_name","ecommerce", "Catalog Name")
dbutils.widgets.text("storage_account_name","stgecomdevind21", "Storage Account Name")
dbutils.widgets.text("container_name","ecomm-raw-data", "Container Name")

# COMMAND ----------

storage_account_name = dbutils.widgets.get("storage_account_name")
catalog_name = dbutils.widgets.get("catalog_name")
container_name = dbutils.widgets.get("container_name")

# COMMAND ----------

#Azure Data Lake Storage Account

adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/order_shipments/landing/"

# Checkpoint folders for streaming (bronze, silver, gold)
bronze_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints/bronze/fact_order_shipments/"


# COMMAND ----------

spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", bronze_checkpoint_path) \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .option("header", "true") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("rescuedDataColumn", "_rescued_data") \
    .option("cloudFiles.includeExistingFiles", "true") \
    .option("pathGlobFilter", "*.csv") \
    .load(adls_path) \
    .withColumn("ingested_at", F.current_timestamp()) \
    .withColumn("source_file", F.col("_metadata.file_path")) \
    .writeStream \
    .outputMode("append") \
    .option("checkpointLocation", bronze_checkpoint_path) \
    .trigger(availableNow=True) \
    .table(f"{catalog_name}.bronze.brz_order_shipments") \
    .awaitTermination()

# COMMAND ----------

