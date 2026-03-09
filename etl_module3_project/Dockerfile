FROM apache/airflow:2.7.0

USER airflow

ARG AIRFLOW_VERSION=2.7.0
ARG PYTHON_VERSION=3.8
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir \
    apache-airflow-providers-mongo \
    pymongo \
    psycopg2-binary \
    Faker \
    --constraint "${CONSTRAINT_URL}"