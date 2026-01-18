# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *

catalog_name = 'ecommerce'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension Table Brands ###

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_brands')
df_bronze.show(10)

# COMMAND ----------

df_silver = df_bronze.withColumn("brand_name", F.trim("brand_name"))


# COMMAND ----------

df_silver = df_silver.withColumn("brand_code", F.regexp_replace(F.col("brand_code"), r'[^A-Za-z0-9]', ''))


# COMMAND ----------

df_not_alnum = df_silver.filter(~F.col("brand_code").rlike("^[A-Za-z0-9]+$"))


# COMMAND ----------

df_silver.select("category_code").distinct().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct category_code from ecommerce.bronze.brz_category

# COMMAND ----------

#Anomalies dictionary
anomalies = {
  "GROCERY": "GRCY",
  "BOOKS" : "BKS",
  "TOYS" : "TOY"
}

# PySpark replace is easy

df_silver = df_silver.replace(to_replace=anomalies, subset=["category_code"])

df_silver.select("category_code").distinct().show()


# COMMAND ----------

#Write raw data to the the silver layer (catalog : ecommerce, schema : silver table : slv_brands)
df_silver.write.format("delta")\
.mode("overwrite") \
.option("mergeSchema", "true")\
.saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension Table Category ###

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_category")
df_bronze.show(10)

# COMMAND ----------

#To Find Duplicate #
df_bronze.groupBy("category_code").count().filter("count > 1").show()

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(["category_code"])



# COMMAND ----------

df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code")))



# COMMAND ----------

#Write raw data to the the silver layer (catalog : ecommerce, schema : silver table : slv_category)
df_silver.write.format("delta")\
.mode("overwrite") \
.option("mergeSchema", "true")\
.saveAsTable(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension Table Customer ###

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_customers")


# COMMAND ----------

#Handling Null value in the phone column
df_silver = df_bronze.withColumn("phone", F.coalesce(F.col("phone"), F.lit("NA")))


# COMMAND ----------

#Dropping the rows that have customer_id as null
df_silver = df_silver.dropna(subset=["customer_id"])
#Writing the clean data to the silver table slv_customers


# COMMAND ----------

#writing the clean data to the silver table slv_customers
df_silver.write.format("delta")\
.mode("overwrite") \
.option("mergeSchema", "true")\
.saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension Product ###

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_products")


# COMMAND ----------

#Trimimg the proudct_name column
df_silver = df_bronze.withColumn("sku", F.trim(F.col("sku")))


# COMMAND ----------

#Converting the category_code and brand_code coloumn to upper case
df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code")))
df_silver = df_silver.withColumn("brand_code", F.upper(F.col("brand_code")))


# COMMAND ----------

df_silver.select("material").distinct().show()



# COMMAND ----------

df_silver = df_silver.withColumn("material", F.when(F.col("material") == "Coton" , "Cotton")
                                              .when(F.col("material") == "Alumium", "Aluminum")
                                              .when(F.col("material") == "Ruber", "Rubber")
                                              .otherwise(F.col("material")))
df_silver.select("material").distinct().show()

# COMMAND ----------

#Removing 'g' fromt he weight_grams column and coverting it to int
df_silver = df_silver.withColumn("weight_grams", F.regexp_replace(F.col("weight_grams"), "g", ""))
df_silver = df_silver.withColumn("weight_grams", F.col("weight_grams").cast("int"))


# COMMAND ----------

#Replacing ',' with '.' in the length_cm column and converting it to double
df_silver = df_silver.withColumn("length_cm", F.regexp_replace(F.col("length_cm"), ",", "."))
df_silver = df_silver.withColumn("length_cm", F.col("length_cm").cast("double"))



# COMMAND ----------

#Coverting -ve rating_count to postive rating_count column
df_silver = df_silver.withColumn("rating_count", F.when(F.col("rating_count").isNotNull() , F.abs(F.col("rating_count")))
                                              .otherwise(F.lit(0)))


# COMMAND ----------

#writing data to silver product table
df_silver.write.format("delta")\
.mode("overwrite") \
.option("mergeSchema", "true")\
.saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dim date###

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_date")


# COMMAND ----------

#Converting Day Name in caps
df_silver = df_bronze.withColumn("day_name", F.initcap(F.col("day_name")))


# COMMAND ----------

df_silver = df_silver.withColumn("week_of_year", F.abs(F.col("week_of_year")))
df_silver = df_silver.withColumn("week_of_year", F.col("week_of_year").cast("int"))


                                 

# COMMAND ----------

df_silver = df_silver.dropDuplicates(["date"])


# COMMAND ----------

df_silver = df_silver.withColumn("quarter", F.concat_ws("", F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year"))))

df_silver = df_silver.withColumn("week_of_year", F.concat_ws("-", F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year"))))



# COMMAND ----------

df_silver = df_silver.withColumnRenamed("week_of_year", "week")

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_calendar)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_calendar")