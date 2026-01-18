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
silver_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints/silver/fact_order_returns/"

# COMMAND ----------

df = spark.readStream \
    .format("delta") \
    .table(f"{catalog_name}.bronze.brz_order_returns")

# COMMAND ----------

df = df.dropDuplicates(["order_id","order_dt","return_ts"])

# COMMAND ----------

#Converting the "order_dt" to date type and "return_ts" to timestamp
df = df.withColumn("order_dt", F.to_date(F.col("order_dt")))
df = df.withColumn("return_ts", F.to_timestamp(F.col("return_ts")))

# COMMAND ----------

#Converting "reason" column to Upper Case and triming the whitespace
df = df.withColumn("reason", F.upper(F.trim(F.col("reason"))))

# COMMAND ----------

#Adding processed_time column
df = df.withColumn("processed_time", F.current_timestamp())

# COMMAND ----------

def upsert_to_silver(microBatchDF, batchID):
  table_name = f"{catalog_name}.silver.slv_order_returns"
  if not spark.catalog.tableExists(table_name):
    print("creating new table")
    microBatchDF.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(table_name)
    spark.sql( f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
  else:
    deltaTable = DeltaTable.forName(spark, table_name)
    deltaTable.alias("silver_table").merge(
      microBatchDF.alias("batch_table"), "silver_table.order_id = batch_table.order_id AND silver_table.order_dt = batch_table.order_dt and silver_table.return_ts = batch_table.return_ts"
      ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

df.writeStream.trigger(availableNow=True).foreachBatch(
  upsert_to_silver
).format("delta").option("checkpointLocation", silver_checkpoint_path
                         ).option("mergeSchema", "true"
                                  ).outputMode("update"
                                               ).trigger(once=True).start().awaitTermination()

# COMMAND ----------

