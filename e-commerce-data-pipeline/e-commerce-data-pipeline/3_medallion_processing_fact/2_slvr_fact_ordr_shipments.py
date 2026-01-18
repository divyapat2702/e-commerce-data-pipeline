# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta.tables import DeltaTable


# COMMAND ----------

dbutils.widgets.text("catalog_name","ecommerce", "Catalog Name")
dbutils.widgets.text("storage_account_name","stgecomdevind21", "Storage Account Name")
dbutils.widgets.text("container_name","ecomm-raw-data", "Container Name")

# COMMAND ----------

storage_account_name = dbutils.widgets.get("storage_account_name")
catalog_name = dbutils.widgets.get("catalog_name")
container_name = dbutils.widgets.get("container_name")

#Silver Checkpoint Path
silver_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints/silver/fact_order_shipments/"

# COMMAND ----------

df = spark.readStream \
        .format("delta") \
        .table(f"{catalog_name}.bronze.brz_order_shipments")

# COMMAND ----------

#Converting the order_dt tp DateTyep

df = df.withColumn("order_dt", F.to_date(F.col("order_dt")))

# COMMAND ----------

#Converting 'carrier' column to UpperCase and trim whitespace

df = df.withColumn("carrier", F.upper(F.trim(F.col("carrier"))))

#Adding processed_time with current timestamp

df = df.withColumn("processed_time", F.current_timestamp())

# COMMAND ----------

def update_silver_table(microBatchDF, batch_id) :
  table_name = f"{catalog_name}.silver.slv_order_shipments"
  if not spark.catalog.tableExists(table_name):
    print("Creating New Table")
    microBatchDF.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(table_name)
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
  else:
    print("Updating Table")
    deltatable = DeltaTable.forName(spark,table_name)
    deltatable.alias("silver").merge(
        microBatchDF.alias("batch"), "silver.shipment_id == batch.shipment_id and silver.order_id == batch.order_id"
    ).whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

df.writeStream.trigger(availableNow=True).foreachBatch(
   update_silver_table 
).format("delta").option("checkpointLocation", silver_checkpoint_path).option("mergeSchema", "true"
).outputMode("update"
).trigger(once=True).start().awaitTermination()