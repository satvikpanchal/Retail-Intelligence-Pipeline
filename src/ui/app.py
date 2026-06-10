"""
Olist E-Commerce Analytics Dashboard
Streamlit app connecting directly to PostgreSQL gold layer tables.

Run with:
    streamlit run src/ui/app.py
"""

import os
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ─── Page config ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Olist Analytics",
    page_icon="🛒",
    layout="wide",
)

# ─── DB connection ────────────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "ecommerce"),
        user=os.getenv("POSTGRES_USER", "ecommerce"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql(sql, conn)


# ─── Header ──────────────────────────────────────────────────────────────────

st.title("🛒 Olist E-Commerce Analytics")
st.caption("Brazilian e-commerce dataset · 2016–2018 · Medallion architecture on LocalStack S3")
st.divider()

# ─── KPI row ─────────────────────────────────────────────────────────────────

kpis = query("""
    SELECT
        COUNT(*)                                   AS total_orders,
        ROUND(SUM(total_revenue)::numeric, 2)      AS total_revenue,
        ROUND(AVG(total_revenue)::numeric, 2)      AS avg_order_value,
        ROUND(AVG(avg_review_score)::numeric, 2)   AS avg_review_score,
        ROUND(AVG(delivery_days)::numeric, 1)      AS avg_delivery_days,
        ROUND(
            100.0 * SUM(CASE WHEN is_on_time::boolean THEN 1 ELSE 0 END)
            / NULLIF(COUNT(is_on_time), 0), 1
        )                                          AS on_time_pct
    FROM fact_orders
    WHERE order_status = 'delivered'
""")

k = kpis.iloc[0]
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total Orders",      f"{int(k['total_orders']):,}")
c2.metric("Total Revenue",     f"R${float(k['total_revenue']):,.0f}")
c3.metric("Avg Order Value",   f"R${float(k['avg_order_value']):,.2f}")
c4.metric("Avg Review Score",  f"⭐ {float(k['avg_review_score']):.2f}")
c5.metric("Avg Delivery Days", f"{float(k['avg_delivery_days']):.1f} days")
c6.metric("On-Time Rate",      f"{float(k['on_time_pct']):.1f}%")

st.divider()

# ─── Revenue over time ────────────────────────────────────────────────────────

col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("📈 Revenue by Month")
    monthly = query("""
        SELECT purchase_month_str AS month, total_revenue, order_count
        FROM sales_by_month
        ORDER BY month
    """)
    st.line_chart(monthly.set_index("month")[["total_revenue"]], height=280)

with col_right:
    st.subheader("💳 Payment Methods")
    payments = query("""
        SELECT payment_type, order_count, total_revenue
        FROM payment_breakdown
        ORDER BY order_count DESC
    """)
    st.dataframe(
        payments.rename(columns={
            "payment_type":  "Method",
            "order_count":   "Orders",
            "total_revenue": "Revenue (R$)",
        }),
        hide_index=True,
        use_container_width=True,
    )

st.divider()

# ─── State + categories ───────────────────────────────────────────────────────

col_a, col_b = st.columns(2)

with col_a:
    st.subheader("🗺️ Revenue by State")
    states = query("""
        SELECT customer_state AS state, total_revenue, order_count
        FROM sales_by_state
        ORDER BY total_revenue DESC
        LIMIT 15
    """)
    st.bar_chart(states.set_index("state")["total_revenue"], height=320)

with col_b:
    st.subheader("📦 Top Product Categories")
    cats = query("""
        SELECT category_english AS category,
               total_revenue,
               order_count,
               avg_price
        FROM top_categories
        ORDER BY total_revenue DESC
        LIMIT 15
    """)
    st.bar_chart(cats.set_index("category")["total_revenue"], height=320)

st.divider()

# ─── Review distribution ──────────────────────────────────────────────────────

col_r, col_s = st.columns([1, 2])

with col_r:
    st.subheader("⭐ Review Score Distribution")
    reviews = query("""
        SELECT review_score::text AS score, order_count
        FROM review_distribution
        ORDER BY review_score
    """)
    st.bar_chart(reviews.set_index("score")["order_count"], height=260)

with col_s:
    st.subheader("🏆 Top 10 Sellers by Revenue")
    sellers = query("""
        SELECT seller_id, seller_state,
               total_revenue,
               items_sold,
               avg_review_score,
               avg_delivery_days,
               on_time_pct
        FROM seller_performance
        ORDER BY total_revenue DESC
        LIMIT 10
    """)
    sellers["seller_id"] = sellers["seller_id"].str[:8] + "..."
    st.dataframe(
        sellers.rename(columns={
            "seller_id":         "Seller",
            "seller_state":      "State",
            "total_revenue":     "Revenue (R$)",
            "items_sold":        "Items Sold",
            "avg_review_score":  "Avg ⭐",
            "avg_delivery_days": "Avg Days",
            "on_time_pct":       "On Time %",
        }),
        hide_index=True,
        use_container_width=True,
    )

st.divider()

# ─── Customer metrics ─────────────────────────────────────────────────────────

st.subheader("👥 Customer Lifetime Value — Top 20")
customers = query("""
    SELECT customer_unique_id, customer_state,
           order_count, total_spent, avg_order_value, avg_review_score
    FROM customer_metrics
    ORDER BY total_spent DESC
    LIMIT 20
""")
customers["customer_unique_id"] = customers["customer_unique_id"].str[:10] + "..."
st.dataframe(
    customers.rename(columns={
        "customer_unique_id": "Customer",
        "customer_state":     "State",
        "order_count":        "Orders",
        "total_spent":        "Total Spent (R$)",
        "avg_order_value":    "Avg Order (R$)",
        "avg_review_score":   "Avg ⭐",
    }),
    hide_index=True,
    use_container_width=True,
)

st.caption("Pipeline: LocalStack S3 (bronze → silver → gold) · Apache Airflow · PySpark · PostgreSQL · Streamlit")
