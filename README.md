# E-Commerce Analytics Data Platform

An end-to-end data engineering pipeline built on the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Raw CSV files are ingested into a local S3-compatible store, processed through a medallion architecture using PySpark, and orchestrated by Apache Airflow.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Apache Airflow                           │
│   upload_bronze  ──►  spark_silver  ──►  spark_gold  ──►  load  │
└────────┬────────────────────┬───────────────────────────────────┘
         │                    │
         ▼                    ▼
  ┌─────────────────────────────────┐
  │        LocalStack S3            │
  │  s3://ecommerce-data/           │
  │  ├── bronze/   (raw CSVs)       │
  │  ├── silver/   (clean Parquet)  │
  │  └── gold/     (analytics)      │
  └─────────────────────────────────┘
                                        ┌──────────────┐
                                        │  PostgreSQL  │
                                        │  (analytics  │
                                        │   tables)    │
                                        └──────────────┘
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.13 | Primary language |
| PySpark | Distributed data transformation |
| Apache Airflow | Pipeline orchestration |
| LocalStack | Local AWS S3 emulation |
| PostgreSQL | Analytics serving layer |
| Docker Compose | Local environment management |
| boto3 | S3 ingestion |

---

## Data Layers (Medallion Architecture)

| Layer | Location | Format | Description |
|---|---|---|---|
| **Bronze** | `s3://ecommerce-data/bronze/` | CSV | Raw files, exactly as received — never modified |
| **Silver** | `s3://ecommerce-data/silver/` | Parquet | Cleaned, typed, deduplicated per table |
| **Gold** | `s3://ecommerce-data/gold/` | Parquet | Business-ready aggregates and dimensional models |

---

## Project Structure

```
ecommerce-analytics-platform/
├── docker-compose.yml          # LocalStack + Airflow + Postgres
├── .env.example                # Environment variable template
├── requirements.txt            # Python dependencies
├── Makefile                    # One-command shortcuts
├── data/                       # Raw Olist CSVs (gitignored — see Setup)
├── airflow/
│   └── dags/
│       └── ecommerce_dag.py    # Pipeline DAG definition
└── src/
    ├── ingestion/
    │   └── upload_bronze.py    # CSV → S3 bronze
    └── spark/
        ├── silver.py           # Bronze → Silver (clean + type)
        └── gold.py             # Silver → Gold (aggregates + models)
```

---

## Dataset

This project uses the **Olist Brazilian E-Commerce** dataset (100k orders, 2016–2018).

| Table | Description |
|---|---|
| `olist_orders_dataset.csv` | Order header — status, timestamps |
| `olist_customers_dataset.csv` | Customer location and ID |
| `olist_order_items_dataset.csv` | Line items — product, seller, price |
| `olist_order_payments_dataset.csv` | Payment method and value |
| `olist_order_reviews_dataset.csv` | Customer review scores and comments |
| `olist_products_dataset.csv` | Product dimensions and category |
| `olist_sellers_dataset.csv` | Seller location |
| `olist_geolocation_dataset.csv` | Zip code to lat/lng mapping |
| `product_category_name_translation.csv` | Portuguese → English category names |

**Download:** https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Place all CSVs in the `data/` directory before running.

---

## Setup

### Prerequisites
- Docker Desktop
- Python 3.13
- Java 17 (required by PySpark)
- AWS CLI (for verifying S3 contents)

### 1. Clone the repo
```bash
git clone https://github.com/your-username/ecommerce-analytics-platform.git
cd ecommerce-analytics-platform
```

### 2. Configure environment
```bash
cp .env.example .env
# Open .env and fill in your LOCALSTACK_AUTH_TOKEN
```

### 3. Start LocalStack
```bash
docker compose up -d
docker compose ps   # wait until status = healthy
```

### 4. Create Python environment
```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 5. Download the dataset
Download from Kaggle and place all CSVs in `data/`.

### 6. Run the pipeline
```bash
# Upload raw CSVs to S3 bronze
python src/ingestion/upload_bronze.py

# Run silver transformation
python src/spark/silver.py

# Verify S3 contents
aws --endpoint-url=http://localhost:4566 s3 ls s3://ecommerce-data/ --recursive
```

---

## Gold Layer Outputs

| Table | Description |
|---|---|
| `fact_orders` | Order-level fact table |
| `dim_customers` | Customer dimension |
| `dim_products` | Product dimension with English category names |
| `dim_sellers` | Seller dimension |
| `sales_by_state` | Revenue aggregated by customer state |
| `sales_by_month` | Monthly revenue trends |
| `top_product_categories` | Revenue and order count by category |
| `seller_performance` | Per-seller revenue, ratings, delivery times |
| `customer_metrics` | LTV, order frequency, average spend per customer |

---

## Design Decisions

**Why medallion architecture?**
Separating raw, clean, and aggregated data means bugs in transformation logic never corrupt the source. Bronze is immutable — any layer can always be reprocessed from scratch.

**Why Parquet for silver/gold?**
Columnar format is significantly faster for analytical queries than CSV. It also preserves types, so no re-casting on every read.

**Why LocalStack instead of real AWS?**
Allows full local development at zero cost. The S3 interface is identical — swapping to real AWS only requires changing the endpoint URL and credentials.

**Why PySpark over pandas?**
The Olist dataset fits in memory, but the pipeline is designed to scale. PySpark handles the same logic on a 100GB dataset without code changes.
