"""
Load gold layer Parquet files from S3 into PostgreSQL analytics tables.
Runs after spark/gold.py completes. Uses pandas + psycopg2 (no Spark needed).
Each table is fully replaced on every run (truncate + insert).
"""

import os
import boto3
import pandas as pd
import psycopg2
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

# ─── Config ──────────────────────────────────────────────────────────────────

S3_ENDPOINT  = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
BUCKET       = os.getenv("S3_BUCKET", "ecommerce-data")
GOLD_PREFIX  = "gold"

PG_HOST      = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT      = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB        = os.getenv("POSTGRES_DB", "ecommerce")
PG_USER      = os.getenv("POSTGRES_USER", "ecommerce")
PG_PASSWORD  = os.getenv("POSTGRES_PASSWORD")

TABLES = [
    "fact_orders",
    "sales_by_month",
    "sales_by_state",
    "top_categories",
    "seller_performance",
    "customer_metrics",
    "payment_breakdown",
    "review_distribution",
    "dim_products",
]


# ─── S3 helpers ──────────────────────────────────────────────────────────────

def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name="us-east-1",
    )


def read_gold_table(s3, table: str) -> pd.DataFrame:
    """Read all part-*.parquet files for a gold table into a single DataFrame."""
    prefix = f"{GOLD_PREFIX}/{table}/"
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    objects = response.get("Contents", [])

    part_files = [o["Key"] for o in objects if o["Key"].endswith(".parquet")]
    if not part_files:
        raise FileNotFoundError(f"No parquet files found at s3://{BUCKET}/{prefix}")

    frames = []
    for key in part_files:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        frames.append(pd.read_parquet(BytesIO(obj["Body"].read())))

    df = pd.concat(frames, ignore_index=True)
    print(f"  Read {table}: {len(df):,} rows, {len(df.columns)} columns")
    return df


# ─── Postgres helpers ─────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD
    )


def create_table_from_df(cur, table: str, df: pd.DataFrame):
    """Auto-create table from DataFrame dtypes, dropping if exists."""
    type_map = {
        "int64":   "BIGINT",
        "int32":   "INTEGER",
        "float64": "DOUBLE PRECISION",
        "float32": "REAL",
        "bool":    "BOOLEAN",
        "object":  "TEXT",
    }

    cols = []
    for col, dtype in df.dtypes.items():
        pg_type = type_map.get(str(dtype), "TEXT")
        cols.append(f'"{col}" {pg_type}')

    ddl = f'DROP TABLE IF EXISTS "{table}"; CREATE TABLE "{table}" ({", ".join(cols)});'
    cur.execute(ddl)
    print(f"  Created table: {table}")


def insert_df(cur, table: str, df: pd.DataFrame):
    """Bulk-insert DataFrame using executemany."""
    if df.empty:
        print(f"  Skipped {table} — empty DataFrame")
        return

    # Replace NaN and NaT with None for psycopg2.
    # astype(object) is required first — .where() on typed datetime columns
    # leaves NaT as the pandas sentinel instead of converting to None.
    df = df.astype(object).where(df.notna(), None)

    cols         = [f'"{c}"' for c in df.columns]
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql          = f'INSERT INTO "{table}" ({", ".join(cols)}) VALUES ({placeholders})'
    rows         = [tuple(row) for row in df.itertuples(index=False, name=None)]

    cur.executemany(sql, rows)
    print(f"  Inserted {len(rows):,} rows → {table}")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "="*60)
    print("  Loading gold → PostgreSQL")
    print("="*60)

    s3   = get_s3()
    conn = get_conn()
    conn.autocommit = False
    cur  = conn.cursor()

    try:
        for table in TABLES:
            print(f"\n── {table} ──")
            df = read_gold_table(s3, table)
            create_table_from_df(cur, table, df)
            insert_df(cur, table, df)

        conn.commit()
        print("\n  All tables committed to PostgreSQL ✓")

    except Exception as e:
        conn.rollback()
        print(f"\n  ERROR: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    print("="*60)
