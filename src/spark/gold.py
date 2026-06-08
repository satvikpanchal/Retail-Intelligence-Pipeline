"""
Gold layer transformation.
Reads clean Parquet from S3 silver, produces business-ready aggregates
and dimensional models, writes Parquet to S3 gold.

Tables produced:
  fact_orders          — one row per order, all key metrics joined in
  sales_by_month       — monthly revenue trend
  sales_by_state       — revenue + orders by customer state
  top_categories       — revenue, orders, avg price per English category
  seller_performance   — per-seller KPIs
  customer_metrics     — per-customer LTV and behaviour
  payment_breakdown    — payment method split
  review_distribution  — score 1-5 counts
  dim_products         — product + English category name
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

load_dotenv()

ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
BUCKET       = os.getenv("S3_BUCKET", "ecommerce-data")
SILVER       = f"s3a://{BUCKET}/silver"
GOLD         = f"s3a://{BUCKET}/gold"


# ─── Spark session ────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ecommerce-gold")
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_URL)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.367")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def section(title: str):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")

def write_gold(df: DataFrame, table: str):
    path = f"{GOLD}/{table}"
    df.write.mode("overwrite").parquet(path)
    print(f"  → Written {df.count()} rows to {path}")


# ─── Load silver tables ────────────────────────────────────────────────────────

def load_silver(spark: SparkSession):
    section("Loading silver tables")
    tables = {}
    names = [
        "orders", "customers", "order_items", "order_payments",
        "order_reviews", "products", "sellers", "category_translation",
    ]
    for name in names:
        # .persist() materialises the data in Spark memory so later plan
        # executions never go back to S3 — avoids stale-listing errors when
        # LocalStack's directory cache is inconsistent after a silver overwrite.
        df = spark.read.parquet(f"{SILVER}/{name}").persist()
        count = df.count()   # triggers the persist and warms the cache
        tables[name] = df
        print(f"  Loaded {name}: {count:,} rows")
    return tables


# ─── Transforms ───────────────────────────────────────────────────────────────

def build_fact_orders(t: dict) -> DataFrame:
    section("fact_orders")

    # Aggregate payments to one row per order (sum value, take dominant type)
    payments = (
        t["order_payments"]
        .groupBy("order_id")
        .agg(
            F.round(F.sum("payment_value"), 2).alias("total_payment"),
            F.first("payment_type").alias("payment_type"),
            F.max("payment_installments").alias("max_installments"),
        )
    )

    # Aggregate items to one row per order
    items = (
        t["order_items"]
        .groupBy("order_id")
        .agg(
            F.count("order_item_id").alias("item_count"),
            F.round(F.sum("price"), 2).alias("items_revenue"),
            F.round(F.sum("freight_value"), 2).alias("freight_revenue"),
        )
    )

    # Aggregate reviews to one row per order (some orders have multiple reviews)
    reviews = (
        t["order_reviews"]
        .groupBy("order_id")
        .agg(F.round(F.avg("review_score"), 2).alias("avg_review_score"))
    )

    # Join everything onto orders
    fact = (
        t["orders"]
        .join(t["customers"].select("customer_id", "customer_state", "customer_city"),
              on="customer_id", how="left")
        .join(payments, on="order_id", how="left")
        .join(items,    on="order_id", how="left")
        .join(reviews,  on="order_id", how="left")
        .withColumn("total_revenue",
                    F.round(F.col("items_revenue") + F.col("freight_revenue"), 2))
        # Delivery time in days (actual)
        .withColumn("delivery_days",
                    F.when(
                        F.col("order_delivered_customer_date").isNotNull(),
                        F.datediff(
                            F.col("order_delivered_customer_date"),
                            F.col("order_purchase_timestamp")
                        )
                    ))
        # Was it delivered on time?
        .withColumn("is_on_time",
                    F.when(
                        F.col("order_delivered_customer_date").isNotNull() &
                        F.col("order_estimated_delivery_date").isNotNull(),
                        (F.col("order_delivered_customer_date") <=
                         F.col("order_estimated_delivery_date")).cast("boolean")
                    ))
        # Convenience date parts
        .withColumn("purchase_year",  F.year("order_purchase_timestamp"))
        .withColumn("purchase_month", F.month("order_purchase_timestamp"))
        .withColumn("purchase_month_str",
                    F.date_format("order_purchase_timestamp", "yyyy-MM"))
        .select(
            "order_id", "customer_id", "customer_state", "customer_city",
            "order_status", "order_purchase_timestamp",
            "purchase_year", "purchase_month", "purchase_month_str",
            "item_count", "items_revenue", "freight_revenue", "total_revenue",
            "payment_type", "max_installments", "total_payment",
            "avg_review_score", "delivery_days", "is_on_time",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        )
    )

    return fact


def build_sales_by_month(fact: DataFrame) -> DataFrame:
    section("sales_by_month")
    df = (
        fact
        .filter(F.col("order_status") == "delivered")
        .groupBy("purchase_month_str")
        .agg(
            F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.round(F.avg("total_revenue"), 2).alias("avg_order_value"),
            F.round(F.avg("avg_review_score"), 2).alias("avg_review_score"),
        )
        .orderBy("purchase_month_str")
    )
    df.show(5)
    return df


def build_sales_by_state(fact: DataFrame) -> DataFrame:
    section("sales_by_state")
    df = (
        fact
        .filter(F.col("order_status") == "delivered")
        .groupBy("customer_state")
        .agg(
            F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("customer_count"),
            F.round(F.avg("total_revenue"), 2).alias("avg_order_value"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    df.show(10)
    return df


def build_top_categories(t: dict, fact: DataFrame) -> DataFrame:
    section("top_categories")

    # Join products with English category names
    products_en = (
        t["products"]
        .join(t["category_translation"],
              on="product_category_name", how="left")
        .withColumn(
            "category_english",
            F.coalesce(
                F.col("product_category_name_english"),
                F.col("product_category_name"),
                F.lit("unknown")
            )
        )
        .select("product_id", "category_english")
    )

    # Join items → products → fact (for order status filter)
    items_with_cat = (
        t["order_items"]
        .join(products_en, on="product_id", how="left")
        .join(fact.select("order_id", "order_status"),
              on="order_id", how="left")
        .filter(F.col("order_status") == "delivered")
    )

    df = (
        items_with_cat
        .groupBy("category_english")
        .agg(
            F.round(F.sum("price"), 2).alias("total_revenue"),
            F.count("order_item_id").alias("order_count"),
            F.round(F.avg("price"), 2).alias("avg_price"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    df.show(10)
    return df


def build_seller_performance(t: dict, fact: DataFrame) -> DataFrame:
    section("seller_performance")

    seller_items = (
        t["order_items"]
        .join(fact.select("order_id", "order_status", "avg_review_score",
                          "delivery_days", "is_on_time"),
              on="order_id", how="left")
        .filter(F.col("order_status") == "delivered")
    )

    df = (
        seller_items
        .groupBy("seller_id")
        .agg(
            F.round(F.sum("price"), 2).alias("total_revenue"),
            F.count("order_item_id").alias("items_sold"),
            F.countDistinct("order_id").alias("order_count"),
            F.round(F.avg("avg_review_score"), 2).alias("avg_review_score"),
            F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
            F.round(
                F.sum(F.col("is_on_time").cast("int")) /
                F.count("is_on_time") * 100, 1
            ).alias("on_time_pct"),
        )
        .join(t["sellers"].select("seller_id", "seller_state", "seller_city"),
              on="seller_id", how="left")
        .orderBy(F.desc("total_revenue"))
    )
    df.show(5)
    return df


def build_customer_metrics(t: dict, fact: DataFrame) -> DataFrame:
    section("customer_metrics")

    # Only need customer_unique_id — customer_state is already in fact
    cust_map = t["customers"].select("customer_id", "customer_unique_id")

    df = (
        fact
        .filter(F.col("order_status") == "delivered")
        .join(cust_map, on="customer_id", how="left")
        .groupBy("customer_unique_id", "customer_state")
        .agg(
            F.count("order_id").alias("order_count"),
            F.round(F.sum("total_revenue"), 2).alias("total_spent"),
            F.round(F.avg("total_revenue"), 2).alias("avg_order_value"),
            F.round(F.avg("avg_review_score"), 2).alias("avg_review_score"),
        )
        .orderBy(F.desc("total_spent"))
    )
    df.show(5)
    return df


def build_payment_breakdown(fact: DataFrame) -> DataFrame:
    section("payment_breakdown")
    df = (
        fact
        .filter(F.col("order_status") == "delivered")
        .groupBy("payment_type")
        .agg(
            F.count("order_id").alias("order_count"),
            F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("max_installments"), 1).alias("avg_installments"),
        )
        .orderBy(F.desc("order_count"))
    )
    df.show()
    return df


def build_review_distribution(fact: DataFrame) -> DataFrame:
    section("review_distribution")
    df = (
        fact
        .filter(F.col("avg_review_score").isNotNull())
        .withColumn("review_score", F.round(F.col("avg_review_score"), 0).cast("int"))
        .groupBy("review_score")
        .agg(F.count("order_id").alias("order_count"))
        .orderBy("review_score")
    )
    df.show()
    return df


def build_dim_products(t: dict) -> DataFrame:
    section("dim_products")
    df = (
        t["products"]
        .join(t["category_translation"],
              on="product_category_name", how="left")
        .withColumn(
            "category_english",
            F.coalesce(
                F.col("product_category_name_english"),
                F.col("product_category_name"),
                F.lit("unknown")
            )
        )
        .drop("product_category_name_english")
    )
    print(f"  dim_products: {df.count()} rows")
    return df


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    t    = load_silver(spark)
    fact = build_fact_orders(t)

    write_gold(fact,                              "fact_orders")
    write_gold(build_sales_by_month(fact),        "sales_by_month")
    write_gold(build_sales_by_state(fact),        "sales_by_state")
    write_gold(build_top_categories(t, fact),     "top_categories")
    write_gold(build_seller_performance(t, fact), "seller_performance")
    write_gold(build_customer_metrics(t, fact),   "customer_metrics")
    write_gold(build_payment_breakdown(fact),     "payment_breakdown")
    write_gold(build_review_distribution(fact),   "review_distribution")
    write_gold(build_dim_products(t),             "dim_products")

    print("\n" + "="*60)
    print("  Gold layer complete.")
    print("="*60)
    spark.stop()
