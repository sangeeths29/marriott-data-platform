# Local development. Everything runs free on a laptop: local Spark + Iceberg (Hadoop
# catalog) + DuckDB for dbt. In AWS the same code runs on Glue / Athena / MWAA.
DATE    ?= $(shell date +%F)
START   ?= 2026-11-01
PY      ?= .venv/bin/python
SPARK   ?= .venv/bin/spark-submit
DBT     ?= ../.venv/bin/dbt
ICEBERG := org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1
REFERENCE_SOURCES := datafiniti_reviews europe_reviews osm_properties sec_financials
export DATA_ROOT ?= ./data
# spark-submit must use this project's Python, not whatever python3 is first on PATH (e.g. conda base)
export PYSPARK_PYTHON ?= $(CURDIR)/.venv/bin/python
export PYSPARK_DRIVER_PYTHON ?= $(CURDIR)/.venv/bin/python

# date arithmetic helper: $(call add_days,2026-11-01,2) -> 2026-11-03
add_days = $(shell $(PY) -c "import datetime as t;print(t.date.fromisoformat('$(1)')+t.timedelta(days=$(2)))")

.PHONY: help setup lint test test-all package sample ingest bronze silver crs-init crs-day reservations \
        dbt demo freshness \
        aws-whoami bootstrap tf-init tf-plan tf-apply tf-destroy aws-env hybrid-env clean

help:             ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/ -/'

setup:            ## venv + deps (Python 3.10-3.12, Java 17+)
	python3 -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt

lint:             ## ruff
	.venv/bin/ruff check pipeline scripts tests airflow

test:             ## fast unit tests (no Spark)
	.venv/bin/pytest -q -m "not spark"

test-all:         ## unit + Spark/Iceberg integration tests
	.venv/bin/pytest -q

package:          ## zip pipeline/ for Glue --extra-py-files and spark-submit --py-files
	@mkdir -p build && rm -f build/pipeline.zip && zip -qr build/pipeline.zip pipeline -x '*/__pycache__/*'

# ---------------------------------------------------------------- reference data
sample:           ## synthetic reference files (exact upstream schemas) -> landing
	USE_SAMPLE_DATA=1 $(MAKE) ingest

ingest:           ## download reference sources -> landing (real data: Kaggle token + SEC_USER_AGENT)
	for s in $(REFERENCE_SOURCES); do $(PY) -m pipeline.ingestion.ingest --source $$s --date $(DATE) || exit 1; done

bronze:           ## validate landing -> bronze (data contracts, quarantine)
	for s in $(REFERENCE_SOURCES); do $(PY) -m pipeline.ingestion.validate_and_load --source $$s --date $(DATE) || exit 1; done

silver: package   ## Spark: reviews/properties/financials -> silver
	$(SPARK) $(SPARK_EXTRA) --py-files build/pipeline.zip pipeline/jobs/bronze_to_silver.py --date $(DATE) --data-root "$(DATA_ROOT)"
	$(PY) scripts/register_silver_tables.py

# ---------------------------------------------------------------- reservations (SIMULATED source)
crs-init:         ## simulate the reservation system + full-load extract (needs silver/properties)
	$(PY) -m pipeline.simulator.crs init --start $(START) --days 30
	$(PY) -m pipeline.ingestion.validate_and_load --source crs_reservations --date $(call add_days,$(START),-1)

crs-day:          ## publish + validate one day's CDC extract (DATE=...)
	$(PY) -m pipeline.simulator.crs advance --date $(DATE)
	$(PY) -m pipeline.ingestion.validate_and_load --source crs_reservations --date $(DATE)

reservations: package  ## Spark: incremental CDC MERGE into Iceberg (catches up to DATE)
	$(SPARK) $(if $(SPARK_EXTRA),$(SPARK_EXTRA),--packages $(ICEBERG)) --py-files build/pipeline.zip \
	  pipeline/jobs/reservations_cdc_merge.py --date $(DATE) --data-root "$(DATA_ROOT)"

# ---------------------------------------------------------------- gold + ops
dbt:              ## seeds, SCD2 snapshot, models + tests (DuckDB)
	cd dbt && $(DBT) build --profiles-dir .

freshness:
	$(PY) scripts/check_freshness.py

demo:             ## end-to-end on synthetic data: reference + 3 days of reservations + gold
	$(MAKE) sample bronze silver DATE=$(START)
	$(MAKE) crs-init
	$(MAKE) crs-day DATE=$(START)
	$(MAKE) crs-day DATE=$(call add_days,$(START),1)
	$(MAKE) crs-day DATE=$(call add_days,$(START),2)
	$(MAKE) reservations DATE=$(call add_days,$(START),2)
	$(MAKE) dbt freshness

# ---------------------------------------------------------------- AWS (Terraform)
ENV        ?= dev
AWS_ACCOUNT = $(shell aws sts get-caller-identity --query Account --output text)
TF_BACKEND  = -backend-config="bucket=marriott-dp-tfstate-$(AWS_ACCOUNT)" -backend-config="key=$(ENV)/main.tfstate"

aws-whoami:       ## show which AWS account/identity you're signed in as
	aws sts get-caller-identity --query '{Account:Account,Identity:Arn}' --output table

bootstrap:        ## one-time: state bucket + GitHub OIDC roles (local state)
	cd terraform/bootstrap && terraform init -input=false && terraform apply

tf-init:          ## init the main stack against remote state (ENV=dev|prod)
	cd terraform && terraform init -input=false -reconfigure $(TF_BACKEND)

tf-plan: package tf-init  ## plan ENV
	cd terraform && terraform plan -input=false -var-file=envs/$(ENV).tfvars -out=$(ENV).tfplan

tf-apply:         ## apply the saved plan from tf-plan
	cd terraform && terraform apply -input=false $(ENV).tfplan

tf-destroy: package tf-init  ## tear down ENV (asks for confirmation)
	cd terraform && terraform destroy -var-file=envs/$(ENV).tfvars

aws-env:          ## print the env vars that point local tools/Airflow at the deployed stack
	@cd terraform && terraform output -json airflow_env | python3 -c "import json,sys; [print(f'export {k}={v}') for k,v in json.load(sys.stdin).items()]"

hybrid-env:       ## write .env.hybrid: Spark on this machine, data/catalog/queries on AWS (then: source .env.hybrid)
	@$(MAKE) -s aws-env | grep -v PIPELINE_ENV > .env.hybrid
	@echo "export PIPELINE_ENV=local LOCAL_SPARK_S3=1 DBT_TARGET=dev" >> .env.hybrid
	@$(PY) -c "from pipeline.common.spark import LOCAL_S3_PACKAGES as p; print(f\"export SPARK_EXTRA='--packages {p}'\")" >> .env.hybrid
	@echo "wrote .env.hybrid - load it with: source .env.hybrid"

clean:
	rm -rf build reports dbt/target dbt/logs data/warehouse.duckdb
