from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    "owner": "student",
    "start_date": datetime(2026, 2, 2)
}

sql = """

TRUNCATE mart_user_activity;

INSERT INTO mart_user_activity
SELECT
    user_id,
    COUNT(DISTINCT session_id),
    AVG(EXTRACT(EPOCH FROM (end_time - start_time))),
    COUNT(DISTINCT device)
FROM staging_user_sessions
GROUP BY user_id;


TRUNCATE mart_support_efficiency;

INSERT INTO mart_support_efficiency
SELECT
    issue_type,
    status,
    COUNT(*),
    AVG(resolution_hours)
FROM staging_support_tickets
GROUP BY issue_type, status;

"""


with DAG(
        dag_id="build_analytical_marts",
        default_args=default_args,
        schedule="@daily",
        catchup=False
) as dag:

    create_tables = PostgresOperator(
        task_id="create_marts",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS mart_user_activity(
            user_id VARCHAR,
            total_sessions INT,
            avg_session_duration_sec FLOAT,
            devices_used INT
        );

        CREATE TABLE IF NOT EXISTS mart_support_efficiency(
            issue_type VARCHAR,
            status VARCHAR,
            ticket_count INT,
            avg_resolution_time FLOAT
        );
        """
    )

    refresh = PostgresOperator(
        task_id="refresh_marts",
        postgres_conn_id="postgres_default",
        sql=sql
    )

    create_tables >> refresh