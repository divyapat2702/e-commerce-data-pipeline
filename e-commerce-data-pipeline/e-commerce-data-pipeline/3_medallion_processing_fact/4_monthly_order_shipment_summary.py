# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta.tables import DeltaTable

# COMMAND ----------

catalog_name = "ecommerce"
table_name="order_shipments_summary"
days_cutoff = "30"

# COMMAND ----------

df = spark.read \
    .format("delta") \
    .table(f"{catalog_name}.gold.gld_fact_order_shipments")

# COMMAND ----------

df = df.withColumn("shipment_month", F.date_format("order_dt", "yyyy-MM"))


# COMMAND ----------

max_date = spark.sql(f"select max(order_dt) from {catalog_name}.gold.gld_fact_order_shipments").collect()[0][0]
print(max_date)

# COMMAND ----------

if spark.catalog.tableExists(f'{catalog_name}.gold.{table_name}') :
    df = df.filter(f"order_dt >= date_sub(date('{max_date}'),{days_cutoff})") \
    #where_clause = f"transaction_date >= date_sub(date('{max_date}'),{days_cutoff})" #max_date
   

# COMMAND ----------

df = df.groupBy(["shipment_month", "carrier"]).count().orderBy(["shipment_month","count"], ascending=False)



# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

if not spark.catalog.tableExists(f'{catalog_name}.gold.{table_name}') :
    df.write.format("delta").mode("overwrite").saveAsTable(f'{catalog_name}.gold.{table_name}')
    spark.sql(f"ALTER TABLE {catalog_name}.gold.{table_name} CLUSTER BY AUTO")
else :
    delta_table = DeltaTable.forName(spark,f'{catalog_name}.gold.{table_name}')
    delta_table.alias("gold_table").merge(df.alias("data_snapshot"),"gold_table.shipment_month = data_snapshot.shipment_month AND gold_table.carrier = data_snapshot.carrier").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# COMMAND ----------

