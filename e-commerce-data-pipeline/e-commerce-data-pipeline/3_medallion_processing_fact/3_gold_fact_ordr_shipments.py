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
gold_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints/gold/fact_order_shipments/"

# COMMAND ----------

df = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .table(f"{catalog_name}.silver.slv_order_shipments")
   

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Two new coloumns 'carrier_group' and 'is_weekend_shipment' flag

# COMMAND ----------

#Adding Carrier Group Coloumn
df = df.withColumn("carrier_group", F.when(F.col("carrier").isin(["ECOMEXPRESS","DELHIVERY","XPRESSBEES","BLUEDART"]) , 
                                           F.lit("Domestic"))
                                    .otherwise(F.lit("International")))

# COMMAND ----------

#Adding is weekend shipment flag Coloumn

df = df.withColumn("is_weekend_shipment", F.when(F.dayofweek(F.col("order_dt")).isin([1,7]),F.lit("True"))
                                            .otherwise(F.lit("False")))

# COMMAND ----------

#Adding date_id Coloumn
df = df.withColumn("date_id",F.date_format(F.col("order_dt"),"yyyyMMdd").cast(T.IntegerType()))

# COMMAND ----------

df_gold = df.select("date_id",
                    "order_dt",
                    "order_id",
                    "shipment_id",
                    "is_weekend_shipment",
                    "carrier",
                    "carrier_group"
                    )

# COMMAND ----------

def upsert_to_gold(microBatchDF, batchID):
  table_name = f"{catalog_name}.gold.gld_fact_order_shipments"
  if not spark.catalog.tableExists(table_name):
    print("creating new table")
    microBatchDF.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(table_name)
    spark.sql( f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
  else:
    deltaTable = DeltaTable.forName(spark, table_name)
    deltaTable.alias("gold_table").merge(
      microBatchDF.alias("batch_table"), 
      "gold_table.order_id = batch_table.order_id and gold_table.shipment_id = batch_table.shipment_id and gold_table.order_dt = batch_table.order_dt"
      ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


 



# COMMAND ----------

df_gold.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_gold
).format("delta").option("checkpointLocation", gold_checkpoint_path).option("mergeSchema", "true"
).outputMode("update"
).trigger(once=True).start().awaitTermination()

# COMMAND ----------

