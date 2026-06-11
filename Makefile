PYTHON  := .venv/bin/python
AIRFLOW := .venv/bin/airflow

# ── Environment ───────────────────────────────────────────────────────────────

.env:
	@echo "Creating .env from template — fill in your values before continuing."
	cp .env.example .env

venv:
	python3.13 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt

# ── Infrastructure ────────────────────────────────────────────────────────────

up:
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@until docker inspect --format='{{.State.Health.Status}}' localstack 2>/dev/null | grep -q healthy; do sleep 2; done
	@until docker inspect --format='{{.State.Health.Status}}' ecommerce_postgres 2>/dev/null | grep -q healthy; do sleep 2; done
	@echo "LocalStack + PostgreSQL ready."

down:
	docker compose down

# ── Airflow ───────────────────────────────────────────────────────────────────

airflow-init:
	AIRFLOW_HOME=$(PWD)/airflow $(AIRFLOW) db migrate

airflow-start:
	AIRFLOW_HOME=$(PWD)/airflow $(AIRFLOW) standalone

# ── Pipeline steps (run individually) ────────────────────────────────────────

bronze:
	$(PYTHON) src/ingestion/upload_bronze.py

silver:
	$(PYTHON) src/spark/silver.py

gold:
	$(PYTHON) src/spark/gold.py

load:
	$(PYTHON) src/load/load_postgres.py

# Run the full pipeline end-to-end without Airflow
pipeline: bronze silver gold load

# ── Dashboard ─────────────────────────────────────────────────────────────────

ui:
	.venv/bin/streamlit run src/ui/app.py

# ── Verify S3 contents ────────────────────────────────────────────────────────

ls-bronze:
	aws --endpoint-url=http://localhost:4566 s3 ls s3://ecommerce-data/bronze/ --recursive

ls-silver:
	aws --endpoint-url=http://localhost:4566 s3 ls s3://ecommerce-data/silver/ --recursive

ls-gold:
	aws --endpoint-url=http://localhost:4566 s3 ls s3://ecommerce-data/gold/ --recursive

# ── Teardown ──────────────────────────────────────────────────────────────────

clean-s3:
	aws --endpoint-url=http://localhost:4566 s3 rm s3://ecommerce-data/ --recursive

clean: down
	rm -rf .localstack/ .postgres/

.PHONY: venv up down airflow-init airflow-start bronze silver gold load pipeline ui \
        ls-bronze ls-silver ls-gold clean-s3 clean
