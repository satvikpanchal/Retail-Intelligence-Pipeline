"""
Uploads raw Olist CSVs to the S3 bronze layer.
Idempotent — safe to run multiple times.
"""

import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BUCKET = os.getenv("S3_BUCKET", "ecommerce-data")
BRONZE_PREFIX = "bronze"

DATA_DIR = Path(__file__).parent.parent.parent / "data"


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        region_name=REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )


def ensure_bucket(client):
    try:
        client.head_bucket(Bucket=BUCKET)
    except ClientError:
        client.create_bucket(Bucket=BUCKET)
        print(f"Created bucket: {BUCKET}")


def upload_csvs(client):
    csv_files = sorted(DATA_DIR.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSVs found in {DATA_DIR}")

    for csv_path in csv_files:
        key = f"{BRONZE_PREFIX}/{csv_path.name}"
        client.upload_file(str(csv_path), BUCKET, key)
        print(f"  uploaded: {key}")

    print(f"\nDone — {len(csv_files)} files uploaded to s3://{BUCKET}/{BRONZE_PREFIX}/")


if __name__ == "__main__":
    client = get_client()
    ensure_bucket(client)
    upload_csvs(client)
