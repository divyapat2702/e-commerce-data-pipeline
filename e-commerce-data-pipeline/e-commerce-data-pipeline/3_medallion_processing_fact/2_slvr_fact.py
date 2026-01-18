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

silver_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints/silver/fact_order_items/"

# COMMAND ----------

df = spark.readStream \
    .format("delta") \
    .table(f"{catalog_name}.bronze.brz_order_items")

# COMMAND ----------

#df = df.dropDuplicates(["order_id","item_seq"])



# COMMAND ----------

#Transformation to convert "Two" to 2 in qunatity column 

df = df.withColumn("quantity", F.when(F.col("quantity") == "Two", 2) \
              .otherwise(F.col("quantity")).cast("int"))

# COMMAND ----------

# Transformation to Remove any '$' or other symbol from unit_price, keep only numric and cast to double

df = df.withColumn("unit_price", F.regexp_replace(F.col("unit_price"), "[$]", "").cast("double"))



# COMMAND ----------

# Transformation to Remove any '%' or other symbol from discount_pct, keep only numeric and cast to double

df = df.withColumn("discount_pct", F.regexp_replace(F.col("discount_pct"), "[%]", "").cast("double"))




# COMMAND ----------

#Transformation to convert coupon_code to lower case

df = df.withColumn("coupon_code", F.lower(F.trim(F.col("coupon_code"))))



# COMMAND ----------

#Transforming : chammel processing

df = df.withColumn("channel" ,
    F.when(F.col("channel") == "web", "Website") 
    .when(F.col("channel") == "app" , "Mobile") 
    .otherwise(F.col("channel")))



# COMMAND ----------

#Processed Time
df = df.withColumn("processed_time", F.current_timestamp())



# COMMAND ----------

# MAGIC %md
# MAGIC ###Writing to the Streaming Data To Silver Layer for Order_item Table

# COMMAND ----------

def upsert_to_silver(microBatchDF, batchID):
  table_name = f"{catalog_name}.silver.slv_order_items"
  if not spark.catalog.tableExists(table_name):
    print("creating new table")
    microBatchDF.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(table_name)
  else:
    deltaTable = DeltaTable.forName(spark, table_name)
    deltaTable.alias("silver_table").merge(
      microBatchDF.alias("batch_table"), "silver_table.order_id = batch_table.order_id AND silver_table.item_seq = batch_table.item_seq"
      ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    



# COMMAND ----------

df.writeStream.trigger(availableNow=True).foreachBatch(
  upsert_to_silver
).format("delta").option("checkpointLocation", silver_checkpoint_path
                         ).option("mergeSchema", "true"
                                  ).outputMode("update"
                                               ).trigger(once=True).start().awaitTermination()


# COMMAND ----------

