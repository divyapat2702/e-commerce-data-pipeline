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
gold_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints/gold/fact_order_items/"

# COMMAND ----------

df = spark.readStream \
    .format("delta") \
    .option("readChangeFeed","true")\
    .option("checkpointLocation", gold_checkpoint_path)\
    .table(f"{catalog_name}.silver.slv_order_items")
    

##To use the option("readChangeFeed","true") the table must be a Delta Table and have been created with the option("readChangeFeed","true"). it should have TBLPROPERTIES (delta.enableChangeDataFeed = true) 



# COMMAND ----------

# MAGIC %md
# MAGIC Setting the tableproperty 'delta.enableChangeDataFeed = true' in  the silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY ecommerce.silver.slv_order_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE ecommerce.silver.slv_order_items SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC Adding filter to get only the 'insert and 'update_postimage' records

# COMMAND ----------

df_union = df.filter("_change_type in ('update_postimage', 'insert')")

# COMMAND ----------

# MAGIC %md
# MAGIC Adding new column for the gold table
# MAGIC

# COMMAND ----------

#Adding gross_amonut
df_union = df_union.withColumn("gross_amount",
                    F.col("unit_price") * F.col("quantity")
                    )

# COMMAND ----------

#adding discount_amount
df_union = df_union.withColumn("discount_amount",
                    F.col("gross_amount") * (F.col("discount_pct") / 100.0)
)

#Adding sale_amount
df_union = df_union.withColumn("sale_amount",
                    F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
)

#Coupn flag . cupon_flag = 1 if coupon code is not null else 0
df_union = df_union.withColumn("coupon_flag",
                    F.when(F.col("coupon_code").isNotNull(), F.lit(1)).otherwise(F.lit(0))
)

#add date id
from pyspark.sql.types import IntegerType
#add date id
df_union = df_union.withColumn("date_id", F.date_format(F.col("dt"), "yyyyMMdd").cast(IntegerType()))



# COMMAND ----------

orders_gold_df = df_union.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount")
)

# COMMAND ----------

def upsert_to_gold(microBatchDF, batchID):
  table_name = f"{catalog_name}.gold.gld_fact_order_items"
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
      "gold_table.transaction_id = batch_table.transaction_id and gold_table.seq_no = batch_table.seq_no"
      ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


 

orders_gold_df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_gold
).format("delta").option("checkpointLocation", gold_checkpoint_path).option("mergeSchema", "true"
).outputMode("update"
).trigger(once=True).start().awaitTermination()

  

# COMMAND ----------

