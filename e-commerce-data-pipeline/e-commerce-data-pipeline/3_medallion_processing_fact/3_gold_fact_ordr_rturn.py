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
gold_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints/gold/fact_order_returns/"

# COMMAND ----------

df = spark.readStream \
    .format("delta") \
    .option("readChangeFeed","true")\
    .option("checkpointLocation", gold_checkpoint_path)\
    .table(f"{catalog_name}.silver.slv_order_returns")

# COMMAND ----------

df = df.filter("_change_type in ('update_postimage', 'insert')")

# COMMAND ----------

df = df.withColumn("date_id", F.date_format(F.col("order_dt"), "yyyyMMdd").cast(T.IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Column required for Business Analytics
# MAGIC 1. return_days
# MAGIC 2. within_policy
# MAGIC 3. is_late_return

# COMMAND ----------

#Adding column return_days = return_ts - order_ts
df = df.withColumn("return_days", F.datediff(F.to_date(F.col("return_ts")), F.col("order_dt")))


# COMMAND ----------

#Adding column 'within_policy' & 'is_late_return'
df = df.withColumn("within_policy", F.when(F.col("return_days") <= 15, 1).otherwise(0))
df = df.withColumn("is_late_return", F.when(F.col("return_days") > 15, 1).otherwise(0))



# COMMAND ----------

# MAGIC %md
# MAGIC Rearaging the column in the final DF

# COMMAND ----------

return_gold_df = df.select("date_id",
                           "order_id",
                           "order_dt",
                           "return_ts",
                           "return_days",
                           "within_policy",
                           "is_late_return",
                           "reason")


# COMMAND ----------

def upsert_to_gold(microBatchDF, batchID):
  table_name = f"{catalog_name}.gold.gld_fact_order_return"
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
      "gold_table.order_id= batch_table.order_id and gold_table.order_dt = batch_table.order_dt"
      ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

return_gold_df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_gold
).format("delta").option("checkpointLocation", gold_checkpoint_path).option("mergeSchema", "true"
).outputMode("update"
).trigger(once=True).start().awaitTermination()