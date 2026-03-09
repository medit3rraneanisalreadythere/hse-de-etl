from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def extract_transform_load_sessions(**context):
    mongo_hook = MongoHook(mongo_conn_id='mongo_default')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    docs = mongo_hook.find(collection_name='UserSessions', query={})
    data = list(docs)
    
    seen_ids = set()
    clean_data = []
    for doc in data:
        if doc['session_id'] not in seen_ids:
            seen_ids.add(doc['session_id'])
            pages = ','.join(doc.get('pages_visited', []))
            actions = ','.join(doc.get('actions', []))
            clean_data.append((
                doc['session_id'], doc['user_id'], doc['start_time'], 
                doc['end_time'], pages, doc['device'], actions
            ))
    
    insert_query = """
        INSERT INTO staging_user_sessions 
        (session_id, user_id, start_time, end_time, pages_visited, device, actions)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (session_id) DO NOTHING;
    """
    pg_hook.run(insert_query, parameters=clean_data)

def extract_transform_load_tickets(**context):
    mongo_hook = MongoHook(mongo_conn_id='mongo_default')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    docs = mongo_hook.find(collection_name='SupportTickets', query={})
    data = list(docs)
    
    seen_ids = set()
    clean_data = []
    for doc in data:
        if doc['ticket_id'] not in seen_ids:
            seen_ids.add(doc['ticket_id'])
            # Calculate resolution time
            created = doc['created_at']
            updated = doc['updated_at']
            resolution_hours = (updated - created).total_seconds() / 3600 if created and updated else 0
            
            clean_data.append((
                doc['ticket_id'], doc['user_id'], doc['status'], 
                doc['issue_type'], created, updated, resolution_hours
            ))
            
    insert_query = """
        INSERT INTO staging_support_tickets 
        (ticket_id, user_id, status, issue_type, created_at, updated_at, resolution_hours)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticket_id) DO NOTHING;
    """
    pg_hook.run(insert_query, parameters=clean_data)

with DAG('etl_mongo_to_postgres', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    task_sessions = PythonOperator(
        task_id='load_sessions',
        python_callable=extract_transform_load_sessions
    )
    
    task_tickets = PythonOperator(
        task_id='load_tickets',
        python_callable=extract_transform_load_tickets
    )

    task_sessions >> task_tickets