# Databricks notebook source
from pyspark.sql.types import *
import pyspark.sql.functions as F


# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading brz_brands Table in Bronze Layer ###

# COMMAND ----------

# Define schema for the brands data file
brand_schema = StructType([
  StructField('brand_code', StringType(), False),
  StructField('brand_name', StringType(), True),
  StructField("category_code", StringType(), True),
])

# COMMAND ----------

raw_data_path = f"/Volumes/{catalog_name}/raw/raw_landing/brands/*.csv"

df = spark.read.option("header", True).option("delimiter", ",").schema(brand_schema).csv(raw_data_path)

#add metadata columns
df = df.withColumn("_source_file", F.col("_metadata.file_path")) \
       .withColumn("ingested_at", F.current_timestamp())



# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.brz_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading brz_category  Table in Bronze Layer ###

# COMMAND ----------

# Define Schema for Category Data File
category_Schema = StructType([
  StructField('category_code', StringType(), False),
  StructField('category_name', StringType(), True),
])

# COMMAND ----------

ctgry_raw_data_path = f"/Volumes/{catalog_name}/raw/raw_landing/category/*.csv"

df = spark.read.option("header", True).option("delimeter", ",").schema(category_Schema).csv(ctgry_raw_data_path)

#add metadata columns
df = df.withColumn("_source_file", F.col("_metadata.file_path")) \
       .withColumn("ingested_at", F.col("_metadata.file_modification_time"))



# COMMAND ----------

df.write.format("delta") \
  .mode("overwrite") \
      .option("mergeSchema", "true") \
      .saveAsTable(f"{catalog_name}.bronze.brz_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading brz_customer table in Bronze Layer ###

# COMMAND ----------

cust_schema = StructType([
  StructField('customer_id', StringType(), False),
  StructField('phone', StringType(), True),
  StructField('country_code', StringType(), True),
  StructField('country', StringType(), True),
  StructField('state', StringType(), True),
])
cust_raw_data_path = f"/Volumes/{catalog_name}/raw/raw_landing/customers/*.csv"

df = spark.read.option("header", True).option("delimeter", ",").schema(cust_schema).csv(cust_raw_data_path)

#add metadata columns
df = df.withColumn("_source_file", F.col("_metadata.file_path")) \
       .withColumn("ingested_at", F.col("_metadata.file_modification_time"))



# COMMAND ----------

df.write.format("delta") \
  .mode("overwrite") \
      .option("mergeSchema", "true") \
      .saveAsTable(f"{catalog_name}.bronze.brz_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading brz_date table in Bronze Layer###

# COMMAND ----------

dt_schema = StructType([
  StructField('date', DateType(), False),
  StructField('year', StringType(), True),
  StructField('day_name', StringType(),True),
  StructField('quarter', IntegerType(), True),
  StructField('week_of_year', StringType(), True),
])

# COMMAND ----------

dt_raw_path = f"/Volumes/{catalog_name}/raw/raw_landing/date/*.csv"

df = spark.read.option("header", True).option("delimeter", ",").schema(dt_schema).csv(dt_raw_path)

#add metadata columns
df = df.withColumn("_source_file", F.col("_metadata.file_path")) \
       .withColumn("ingested_at", F.col("_metadata.file_modification_time"))




# COMMAND ----------

df.write.format("delta") \
  .mode("overwrite") \
      .option("mergeSchema", "true") \
      .saveAsTable(f"{catalog_name}.bronze.brz_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading brz_product table in Bronze Layer###

# COMMAND ----------

prd_schema = StructType([
  StructField('product_id', StringType(), False),
  StructField('sku',StringType(), True),
  StructField('category_code', StringType(), True),
  StructField('brand_code', StringType(), True),
  StructField('color', StringType(), True),
  StructField('size', StringType(), True),
  StructField('material', StringType(), True),
  StructField('weight_grams', StringType(), True),
  StructField('length_cm', StringType(), True),
  StructField('width_cm', DoubleType(), True),
  StructField('height_cm', DoubleType(), True),
  StructField('rating_count', IntegerType(), True),
])

prd_raw_path = f"/Volumes/{catalog_name}/raw/raw_landing/products/*.csv"

df = spark.read.option("header", True).option("delimeter", ",").schema(prd_schema).csv(prd_raw_path)

#add metadata columns 
df = df.withColumn("_source_file", F.col("_metadata.file_path")) \
       .withColumn("ingested_at", F.col("_metadata.file_modification_time"))



# COMMAND ----------

df.write.format("delta") \
  .mode("overwrite") \
      .option("mergeSchema", "true") \
      .saveAsTable(f"{catalog_name}.bronze.brz_products")

# COMMAND ----------

