"""
Silver layer transformation.
Reads raw CSVs from S3 bronze, cleans and types each table, writes Parquet to S3 silver.
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType

load_dotenv()

ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
BUCKET = os.getenv("S3_BUCKET", "ecommerce-data")
BRONZE = f"s3a://{BUCKET}/bronze"
SILVER = f"s3a://{BUCKET}/silver"


# ─── Spark session ────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ecommerce-silver")
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_URL)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.367")
        # Force SDK v1 credentials provider (matches hadoop-aws 3.4.x)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # PySpark 4.x enables ANSI mode by default — invalid casts throw instead
        # of returning NULL. Disable it so malformed CSV data (e.g. shifted columns
        # in order_reviews) silently produces NULL rather than crashing the job.
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def print_section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def audit(df: DataFrame, label: str):
    """Print row count, schema, sample rows, and null counts."""
    print(f"\n--- {label} ---")
    print(f"  Row count : {df.count()}")
    print(f"  Schema    :")
    df.printSchema()
    print(f"  Sample rows:")
    df.show(5, truncate=False)
    print(f"  Null counts per column:")
    df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
    ]).show(truncate=False)


def report_dropped(before: int, after: int, reason: str):
    dropped = before - after
    print(f"  Dropped {dropped} rows ({reason})")


def write_silver(df: DataFrame, table: str):
    path = f"{SILVER}/{table}"
    df.write.mode("overwrite").parquet(path)
    print(f"\n  Written to {path}")


# ─── Table transforms ─────────────────────────────────────────────────────────

def transform_orders(spark: SparkSession):
    print_section("ORDERS")
    df = spark.read.csv(f"{BRONZE}/olist_orders_dataset.csv", header=True, inferSchema=False)
    audit(df, "Raw orders")

    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    df_clean = df
    for col in date_cols:
        df_clean = df_clean.withColumn(col, F.to_timestamp(F.col(col), "yyyy-MM-dd HH:mm:ss"))

    before = df_clean.count()
    df_clean = df_clean.dropDuplicates(["order_id"])
    report_dropped(before, df_clean.count(), "duplicate order_id")

    # Drop rows missing the mandatory foreign key
    before = df_clean.count()
    df_clean = df_clean.filter(F.col("customer_id").isNotNull())
    report_dropped(before, df_clean.count(), "null customer_id")

    audit(df_clean, "Clean orders")
    write_silver(df_clean, "orders")


def transform_customers(spark: SparkSession):
    print_section("CUSTOMERS")
    df = spark.read.csv(f"{BRONZE}/olist_customers_dataset.csv", header=True, inferSchema=False)
    audit(df, "Raw customers")

    df_clean = df.withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
    df_clean = df_clean.withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))

    before = df_clean.count()
    df_clean = df_clean.dropDuplicates(["customer_id"])
    report_dropped(before, df_clean.count(), "duplicate customer_id")

    audit(df_clean, "Clean customers")
    write_silver(df_clean, "customers")


def transform_order_items(spark: SparkSession):
    print_section("ORDER ITEMS")
    df = spark.read.csv(f"{BRONZE}/olist_order_items_dataset.csv", header=True, inferSchema=False)
    audit(df, "Raw order_items")

    df_clean = (
        df
        .withColumn("price", F.col("price").cast(FloatType()))
        .withColumn("freight_value", F.col("freight_value").cast(FloatType()))
        .withColumn("order_item_id", F.col("order_item_id").cast(IntegerType()))
        .withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date"), "yyyy-MM-dd HH:mm:ss"))
    )

    before = df_clean.count()
    df_clean = df_clean.filter(F.col("price").isNotNull() & F.col("order_id").isNotNull())
    report_dropped(before, df_clean.count(), "null price or order_id")

    audit(df_clean, "Clean order_items")
    write_silver(df_clean, "order_items")


def transform_order_payments(spark: SparkSession):
    print_section("ORDER PAYMENTS")
    df = spark.read.csv(f"{BRONZE}/olist_order_payments_dataset.csv", header=True, inferSchema=False)
    audit(df, "Raw order_payments")

    df_clean = (
        df
        .withColumn("payment_value", F.col("payment_value").cast(FloatType()))
        .withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
        .withColumn("payment_sequential", F.col("payment_sequential").cast(IntegerType()))
    )

    before = df_clean.count()
    df_clean = df_clean.filter(F.col("order_id").isNotNull() & F.col("payment_value").isNotNull())
    report_dropped(before, df_clean.count(), "null order_id or payment_value")

    audit(df_clean, "Clean order_payments")
    write_silver(df_clean, "order_payments")


def transform_order_reviews(spark: SparkSession):
    print_section("ORDER REVIEWS")
    df = spark.read.csv(f"{BRONZE}/olist_order_reviews_dataset.csv", header=True, inferSchema=False)
    audit(df, "Raw order_reviews")

    df_clean = (
        df
        .withColumn("review_score", F.col("review_score").cast(IntegerType()))
        .withColumn("review_creation_date", F.to_timestamp(F.col("review_creation_date"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("review_answer_timestamp", F.to_timestamp(F.col("review_answer_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        # Null out empty comment strings
        .withColumn("review_comment_title", F.when(F.trim(F.col("review_comment_title")) == "", None).otherwise(F.col("review_comment_title")))
        .withColumn("review_comment_message", F.when(F.trim(F.col("review_comment_message")) == "", None).otherwise(F.col("review_comment_message")))
    )

    before = df_clean.count()
    df_clean = df_clean.dropDuplicates(["review_id"])
    report_dropped(before, df_clean.count(), "duplicate review_id")

    audit(df_clean, "Clean order_reviews")
    write_silver(df_clean, "order_reviews")


def transform_products(spark: SparkSession):
    print_section("PRODUCTS")
    df = spark.read.csv(f"{BRONZE}/olist_products_dataset.csv", header=True, inferSchema=False)
    audit(df, "Raw products")

    numeric_cols = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]

    df_clean = df
    for col in numeric_cols:
        df_clean = df_clean.withColumn(col, F.col(col).cast(FloatType()))

    df_clean = df_clean.withColumn(
        "product_category_name",
        F.lower(F.trim(F.col("product_category_name")))
    )

    before = df_clean.count()
    df_clean = df_clean.dropDuplicates(["product_id"])
    report_dropped(before, df_clean.count(), "duplicate product_id")

    audit(df_clean, "Clean products")
    write_silver(df_clean, "products")


def transform_sellers(spark: SparkSession):
    print_section("SELLERS")
    df = spark.read.csv(f"{BRONZE}/olist_sellers_dataset.csv", header=True, inferSchema=False)
    audit(df, "Raw sellers")

    df_clean = (
        df
        .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
        .withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))
    )

    before = df_clean.count()
    df_clean = df_clean.dropDuplicates(["seller_id"])
    report_dropped(before, df_clean.count(), "duplicate seller_id")

    audit(df_clean, "Clean sellers")
    write_silver(df_clean, "sellers")


def transform_geolocation(spark: SparkSession):
    print_section("GEOLOCATION")
    df = spark.read.csv(f"{BRONZE}/olist_geolocation_dataset.csv", header=True, inferSchema=False)
    audit(df, "Raw geolocation")

    df_clean = (
        df
        .withColumn("geolocation_lat", F.col("geolocation_lat").cast(FloatType()))
        .withColumn("geolocation_lng", F.col("geolocation_lng").cast(FloatType()))
        .withColumn("geolocation_state", F.upper(F.trim(F.col("geolocation_state"))))
        .withColumn("geolocation_city", F.lower(F.trim(F.col("geolocation_city"))))
    )

    before = df_clean.count()
    df_clean = df_clean.dropDuplicates(["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"])
    report_dropped(before, df_clean.count(), "duplicate zip+lat+lng")

    audit(df_clean, "Clean geolocation")
    write_silver(df_clean, "geolocation")


def transform_category_translation(spark: SparkSession):
    print_section("CATEGORY TRANSLATION")
    df = spark.read.csv(f"{BRONZE}/product_category_name_translation.csv", header=True, inferSchema=False)
    audit(df, "Raw category_translation")

    df_clean = (
        df
        .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
        .withColumn("product_category_name_english", F.lower(F.trim(F.col("product_category_name_english"))))
    )

    before = df_clean.count()
    df_clean = df_clean.dropDuplicates(["product_category_name"])
    report_dropped(before, df_clean.count(), "duplicate category name")

    audit(df_clean, "Clean category_translation")
    write_silver(df_clean, "category_translation")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    transform_orders(spark)
    transform_customers(spark)
    transform_order_items(spark)
    transform_order_payments(spark)
    transform_order_reviews(spark)
    transform_products(spark)
    transform_sellers(spark)
    transform_geolocation(spark)
    transform_category_translation(spark)

    print("\n" + "="*60)
    print("  Silver layer complete.")
    print("="*60)
    spark.stop()
