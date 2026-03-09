from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'student',
    'start_date': datetime(2024, 1, 1),
}

sql_create_mart_activity = """
CREATE TABLE IF NOT EXISTS mart_user_activity AS
SELECT 
    user_id,
    COUNT(DISTINCT session_id) as total_sessions,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_session_duration_sec,
    COUNT(DISTINCT device) as devices_used
FROM staging_user_sessions
GROUP BY user_id;
"""

sql_create_mart_support = """
CREATE TABLE IF NOT EXISTS mart_support_efficiency AS
SELECT 
    issue_type,
    status,
    COUNT(*) as ticket_count,
    AVG(resolution_hours) as avg_resolution_time
FROM staging_support_tickets
GROUP BY issue_type, status;
"""

sql_refresh_marts = """
TRUNCATE TABLE mart_user_activity;
INSERT INTO mart_user_activity 
SELECT 
    user_id,
    COUNT(DISTINCT session_id),
    AVG(EXTRACT(EPOCH FROM (end_time - start_time))),
    COUNT(DISTINCT device)
FROM staging_user_sessions
GROUP BY user_id;

TRUNCATE TABLE mart_support_efficiency;
INSERT INTO mart_support_efficiency
SELECT 
    issue_type,
    status,
    COUNT(*),
    AVG(resolution_hours)
FROM staging_support_tickets
GROUP BY issue_type, status;
"""

with DAG('build_analytical_marts', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    init_marts = PostgresOperator(
        task_id='init_marts_tables',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS mart_user_activity (
                user_id VARCHAR, total_sessions INT, avg_session_duration_sec FLOAT, devices_used INT
            );
            CREATE TABLE IF NOT EXISTS mart_support_efficiency (
                issue_type VARCHAR, status VARCHAR, ticket_count INT, avg_resolution_time FLOAT
            );
        """
    )
    
    refresh_marts = PostgresOperator(
        task_id='refresh_marts_data',
        postgres_conn_id='postgres_default',
        sql=sql_refresh_marts
    )
    
    init_marts >> refresh_marts