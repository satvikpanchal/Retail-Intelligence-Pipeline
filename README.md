# E-Commerce Analytics Data Platform

An end-to-end data engineering pipeline built on the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Raw CSV files flow through a medallion architecture — bronze → silver → gold — orchestrated by Apache Airflow, stored in a local S3-compatible store, loaded into PostgreSQL, and visualised in a Streamlit dashboard.

---

## Architecture

```
                        Apache Airflow
          ┌─────────────────────────────────────────────────┐
          │  upload_bronze → spark_silver → spark_gold       │
          │                                   → load_postgres│
          └──────────┬──────────────┬────────────────────────┘
                     │              │
                     ▼              ▼
          ┌──────────────────────────────────┐
          │         LocalStack S3            │
          │   s3://ecommerce-data/           │
          │   ├── bronze/   (raw CSVs)       │
          │   ├── silver/   (clean Parquet)  │
          │   └── gold/     (aggregates)     │
          └──────────────────────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   PostgreSQL 16  │
                       │  (gold tables)   │
                       └────────┬─────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │    Streamlit     │
                       │   dashboard      │
                       └──────────────────┘
```

---

## Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.13 | Primary language |
| PySpark | 4.1.2 | Distributed data transformation |
| Apache Airflow | 3.2.1 | Pipeline orchestration |
| LocalStack | latest | Local AWS S3 emulation |
| PostgreSQL | 16 | Analytics serving layer |
| Streamlit | latest | Analytics dashboard UI |
| Docker Compose | — | LocalStack + Postgres containers |
| boto3 | — | S3 read/write from Python |
| psycopg2 | — | PostgreSQL loading |

---

## Data Layers (Medallion Architecture)

| Layer | Location | Format | Description |
|---|---|---|---|
| **Bronze** | `s3://ecommerce-data/bronze/` | CSV | Raw files as received — never modified |
| **Silver** | `s3://ecommerce-data/silver/` | Parquet | Cleaned, typed, deduplicated per table |
| **Gold** | `s3://ecommerce-data/gold/` | Parquet | Business-ready aggregates and dimensional models |
| **Serving** | PostgreSQL `ecommerce` database | Tables | Gold layer materialised for low-latency queries |

---

## Gold Layer Tables

| Table | Rows | Description |
|---|---|---|
| `fact_orders` | 99,441 | Order-level fact — revenue, delivery days, on-time flag, review score |
| `sales_by_month` | 23 | Monthly revenue trend (delivered orders only) |
| `sales_by_state` | 27 | Revenue, order count, customer count by Brazilian state |
| `top_categories` | 74 | Revenue, order count, avg price per English product category |
| `seller_performance` | 2,970 | Per-seller KPIs — revenue, items sold, avg rating, on-time % |
| `customer_metrics` | 93,396 | Customer LTV — total spent, order count, avg order value |
| `payment_breakdown` | 5 | Split by payment method (credit card, boleto, voucher, debit) |
| `review_distribution` | 5 | Order count per review score (1–5) |
| `dim_products` | 32,951 | Product dimension with English category names |

---

## Project Structure

```
.
├── docker-compose.yml              # LocalStack + PostgreSQL containers
├── .env.example                    # Environment variable template
├── conf/
│   └── spark-defaults.conf         # Spark/S3 credential provider config
├── data/                           # Raw Olist CSVs (gitignored — see Setup)
├── airflow/
│   └── dags/
│       └── ecommerce_pipeline.py   # 4-task Airflow DAG
└── src/
    ├── ingestion/
    │   └── upload_bronze.py        # CSV → S3 bronze
    ├── spark/
    │   ├── silver.py               # Bronze → Silver (clean + type)
    │   └── gold.py                 # Silver → Gold (aggregates + models)
    ├── load/
    │   └── load_postgres.py        # Gold Parquet → PostgreSQL
    └── ui/
        └── app.py                  # Streamlit analytics dashboard
```

---

## Dataset

The **Olist Brazilian E-Commerce** dataset covers ~100k orders placed between 2016 and 2018.

| File | Description |
|---|---|
| `olist_orders_dataset.csv` | Order header — status, timestamps |
| `olist_customers_dataset.csv` | Customer location and ID |
| `olist_order_items_dataset.csv` | Line items — product, seller, price, freight |
| `olist_order_payments_dataset.csv` | Payment method, value, instalments |
| `olist_order_reviews_dataset.csv` | Customer review scores and comments |
| `olist_products_dataset.csv` | Product dimensions and category |
| `olist_sellers_dataset.csv` | Seller location |
| `olist_geolocation_dataset.csv` | Zip code → lat/lng mapping |
| `product_category_name_translation.csv` | Portuguese → English category names |

**Download:** https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce  
Place all CSVs in `data/` before running.

---

## Setup

### Prerequisites
- Docker Desktop
- Python 3.13 + [uv](https://github.com/astral-sh/uv) (or pip)
- Java 17 (`brew install openjdk@17` on macOS)

### 1. Clone and enter the repo
```bash
git clone https://github.com/your-username/ecommerce-analytics-platform.git
cd ecommerce-analytics-platform
```

### 2. Configure environment
```bash
cp .env.example .env
# Fill in POSTGRES_PASSWORD (and LOCALSTACK_AUTH_TOKEN if using LocalStack Pro)
```

### 3. Start infrastructure
```bash
docker compose up -d
docker compose ps   # wait until localstack and postgres are healthy
```

### 4. Create Python environment
```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 5. Download the dataset
Download from Kaggle and place all CSVs in `data/`.

### 6. Run manually (one step at a time)
```bash
# Upload raw CSVs to S3 bronze
python src/ingestion/upload_bronze.py

# Clean and type — writes Parquet to silver
python src/spark/silver.py

# Build aggregates — writes Parquet to gold
python src/spark/gold.py

# Load gold into PostgreSQL
python src/load/load_postgres.py

# Launch the dashboard
streamlit run src/ui/app.py
```

### 7. Run via Airflow (recommended)
```bash
# macOS — required to avoid fork() crash
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export no_proxy='*'

airflow standalone
# Open http://localhost:8080, trigger the ecommerce_pipeline DAG
```

The DAG runs all four steps in sequence: `upload_bronze → spark_silver → spark_gold → load_postgres`.

---

## Dashboard

The Streamlit dashboard connects directly to PostgreSQL and renders:

- **KPI row** — total orders, revenue, avg order value, avg review score, avg delivery days, on-time rate
- **Revenue by Month** — line chart of monthly revenue across 2016–2018
- **Payment Methods** — breakdown by credit card, boleto, voucher, debit card
- **Revenue by State** — top 15 Brazilian states by revenue
- **Top Product Categories** — top 15 categories by revenue
- **Review Score Distribution** — bar chart of 1–5 star counts
- **Top 10 Sellers** — leaderboard with revenue, items sold, avg rating, delivery days, on-time %
- **Customer LTV** — top 20 customers by lifetime spend

---

## Design Decisions

**Why medallion architecture?**  
Separating raw, clean, and aggregated data means bugs in transformation logic never corrupt the source. Bronze is immutable — any layer can always be reprocessed from scratch.

**Why Parquet for silver/gold?**  
Columnar format is significantly faster for analytical queries than CSV. It also preserves types, so no re-casting on every read.

**Why LocalStack instead of real AWS?**  
Allows full local development at zero cost. The S3 interface is identical — swapping to real AWS only requires changing the endpoint URL and credentials in `.env`.

**Why PySpark over pandas?**  
The Olist dataset fits in memory, but the pipeline is designed to scale. PySpark handles the same logic on a 100GB dataset without code changes.

**Why PostgreSQL as a serving layer?**  
Gold Parquet on S3 is great for Spark but slow for ad-hoc queries. Materialising into Postgres gives the dashboard sub-second response times and a clean SQL interface.

**Why Airflow standalone (not in Docker)?**  
Airflow needs to launch PySpark subprocesses that inherit the local Java and Python environment. Running it in Docker adds a layer of volume-mounting complexity with no benefit for a local dev setup.
