# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta.tables import DeltaTable

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

days_cutoff = 30
source_table_name = 'gld_fact_order_items'
table_name = 'gld_fact_daily_orders_summary'

# COMMAND ----------

max_date_row = spark.sql(f"""
                         select max(transaction_date) max_date
                         from {catalog_name}.gold.{source_table_name}
                         """).collect()[0]

max_date = max_date_row['max_date']
print(max_date)

# COMMAND ----------

if spark.catalog.tableExists(f'{catalog_name}.gold.{table_name}') :
    where_clause = f"transaction_date >= date_sub(date('{max_date}'),{days_cutoff})" #max_date
else:
    where_clause = "1=1"

# COMMAND ----------

summary_query = f"""
select
date_id,
unit_price_currency as currency,
sum(quantity) as total_quantity,
sum(gross_amount) as total_gross_amount,
sum(discount_amount) as total_discount_amount,
sum(tax_amount) as total_tax_amount,
sum(net_amount) as total_amount
from
{catalog_name}.gold.{source_table_name}
where {where_clause}
group by date_id, currency
order by date_id desc
"""

summary_df = spark.sql(summary_query)


# COMMAND ----------

if not spark.catalog.tableExists(f'{catalog_name}.gold.{table_name}') :
    summary_df.write.format("delta").mode("overwrite").saveAsTable(f'{catalog_name}.gold.{table_name}')
    spark.sql(f"ALTER TABLE {catalog_name}.gold.{table_name} CLUSTER BY AUTO")
else :
    delta_table = DeltaTable.forName(spark,f'{catalog_name}.gold.{table_name}')
    delta_table.alias("gold_table").merge(summary_df.alias("data_snapshot"),"gold_table.date_id = data_snapshot.date_id                                          AND gold_table.currency = data_snapshot.currency").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

