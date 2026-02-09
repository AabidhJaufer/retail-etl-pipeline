from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr, current_date
import os

# Initialize Spark
spark = SparkSession.builder.appName("RetailStarSchema").master("local[*]").getOrCreate()

# Define Paths
BASE_DIR = os.path.expanduser("~/retail_pipeline")
RAW_DIR = os.path.join(BASE_DIR, "data/raw")
PROC_DIR = os.path.join(BASE_DIR, "data/processed")

print("--- Starting Transformation ---")

# 1. Dim Product
df_products = spark.read.json(os.path.join(RAW_DIR, "products.json"))
df_products.select(
    col("id").alias("product_key"),
    col("title").alias("product_name"),
    col("category"),
    col("price").cast("double"),
    col("brand")
).write.mode("overwrite").parquet(os.path.join(PROC_DIR, "dim_product"))
print("✅ dim_product created.")

# 2. Dim Customer
df_users = spark.read.json(os.path.join(RAW_DIR, "users.json"))
df_users.select(
    col("id").alias("customer_key"),
    col("firstName"),
    col("lastName"),
    col("email"),
    col("address.city").alias("city")
).write.mode("overwrite").parquet(os.path.join(PROC_DIR, "dim_customer"))
print("✅ dim_customer created.")

# 3. Fact Sales
df_carts = spark.read.json(os.path.join(RAW_DIR, "carts.json"))
df_carts.withColumn("item", explode(col("products"))).select(
    col("id").alias("order_id"),
    col("userId").alias("customer_key"),
    col("item.id").alias("product_key"),
    col("item.quantity").alias("quantity"),
    col("item.total").alias("total_amount"),
    expr("date_sub(current_date(), cast(rand() * 30 as int))").alias("transaction_date")
).write.mode("overwrite").parquet(os.path.join(PROC_DIR, "fact_sales"))
print("✅ fact_sales created.")