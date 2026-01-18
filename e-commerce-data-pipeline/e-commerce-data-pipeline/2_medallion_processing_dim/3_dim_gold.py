# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row

# COMMAND ----------

catalog_name = 'ecommerce'


# COMMAND ----------

df_prd = spark.table(f'{catalog_name}.silver.slv_products')
df_cat = spark.table(f'{catalog_name}.silver.slv_category')
df_brnd = spark.table(f'{catalog_name}.silver.slv_brands')


# COMMAND ----------

df_prd.createOrReplaceTempView('v_products')
df_cat.createOrReplaceTempView('v_category')
df_brnd.createOrReplaceTempView('v_brands')

# COMMAND ----------

#using the ecommerce catalog
spark.sql(f"use catalog {catalog_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating gold table dim_product by joining product table with brand and category tables
# MAGIC CREATE OR REPLACE TABLE ecommerce.gold.gld_dim_product AS
# MAGIC
# MAGIC WITH brnd_ctag AS (
# MAGIC   SELECT 
# MAGIC     b.brand_name,
# MAGIC     b.brand_code,
# MAGIC     c.category_name,
# MAGIC     c.category_code
# MAGIC   FROM 
# MAGIC     v_brands b
# MAGIC   JOIN 
# MAGIC     v_category c
# MAGIC     ON b.category_code = c.category_code
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   p.product_id,
# MAGIC   p.sku,
# MAGIC   p.category_code,
# MAGIC   COALESCE(bc.category_name, 'NA') AS category_name,
# MAGIC   p.brand_code,
# MAGIC   COALESCE(bc.brand_name, 'NA') AS brand_name,
# MAGIC   p.color,
# MAGIC   p.size,
# MAGIC   p.material,
# MAGIC   p.weight_grams,
# MAGIC   p.length_cm,
# MAGIC   p.width_cm,
# MAGIC   p.height_cm,
# MAGIC   p.rating_count,
# MAGIC   p._source_file,
# MAGIC   p.ingested_at
# MAGIC FROM v_products p
# MAGIC LEFT JOIN brnd_ctag bc
# MAGIC   ON p.category_code = bc.category_code
# MAGIC   and p.brand_code = bc.brand_code;

# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.slv_calendar')


# COMMAND ----------

df_gold = df_silver.withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))

#Add Month Name(e.g. Jan, Feb, Mar etc)
df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))

#is_weekened column

df_gold = df_gold.withColumn("is_weekend", F.when(F.dayofweek(F.col("date")) == 1, 1).otherwise(0))



# COMMAND ----------

#re-order the column 
desired_clmn_ordr = ["date_id","date","year","month_name","day_name","is_weekend","quarter","week","ingested_at","_source_file"]
df_gold = df_gold.select(desired_clmn_ordr)


# COMMAND ----------

#wrtie table to gold layer
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_calendar")

# COMMAND ----------

# India states
india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
}
# Australia states
australia_region = {
    "VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

# United States states
us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", 
    "NY": "NorthEast", "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

# Combine into a master dictionary
country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}  


# COMMAND ----------

row = []
for country, state_map in country_state_map.items():
    for state, region in state_map.items():
        row.append(Row(country = country, state = state, region = region))

# COMMAND ----------

# 2️ Create mapping DataFrame
df_region_mapping = spark.createDataFrame(row)

# Optional: show mapping
df_region_mapping.show(truncate=False)

# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.slv_customers')


# COMMAND ----------

df_gold = df_silver.join(df_region_mapping, on = ["country", "state"], how = "left")


# COMMAND ----------

reordr_clm = ["customer_id","phone","country_code","country","state","region","ingested_at","_source_file"]
df_gold = df_gold.select(reordr_clm)
df_gold = df_gold.fillna({'region': 'Other'})


# COMMAND ----------

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_customer")

# COMMAND ----------

