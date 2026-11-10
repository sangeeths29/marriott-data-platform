# Local Airflow image for the demo stack. In AWS, MWAA runs the same DAGs and Spark
# runs on Glue, so this image is for laptops / CI only.
FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless zip make \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
# Pipeline runtime deps go into Airflow's env (constrained to Airflow's pins).
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.11.txt" \
      apache-airflow-providers-amazon kaggle vaderSentiment "pyspark>=3.5,<4" duckdb
# dbt has conflicting pins with Airflow: isolate it in its own venv (standard practice).
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core>=1.10,<2" "dbt-duckdb>=1.9" "dbt-athena-community>=1.9"
ENV DBT_BIN=/home/airflow/dbt-venv/bin/dbt
