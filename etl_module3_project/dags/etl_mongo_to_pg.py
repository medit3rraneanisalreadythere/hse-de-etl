from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "student",
    "start_date": datetime(2026, 2, 2),
    "retries": 1
}


def load_sessions():
    mongo = MongoHook(mongo_conn_id="mongo_default")
    pg = PostgresHook(postgres_conn_id="postgres_default")

    collection = mongo.get_collection("UserSessions")
    docs = list(collection.find({}))

    rows = []
    for doc in docs:
        if "session_id" not in doc or "user_id" not in doc:
            continue
            
        doc.pop("_id", None)
        
        pages = ",".join(str(p) for p in doc.get("pages_visited", []))
        actions = ",".join(str(a) for a in doc.get("actions", []))
        
        start_time = doc.get("start_time")
        end_time = doc.get("end_time")
        
        if hasattr(start_time, 'isoformat'):
            start_time = start_time.isoformat()
        if hasattr(end_time, 'isoformat'):
            end_time = end_time.isoformat()

        rows.append((
            doc.get("session_id", ""),
            doc.get("user_id", ""),
            start_time,
            end_time,
            pages,
            str(doc.get("device", "")),
            actions
        ))

    if rows:
        insert_query = """
            INSERT INTO staging_user_sessions 
            (session_id, user_id, start_time, end_time, pages_visited, device, actions)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO NOTHING
        """
        
        conn = pg.get_conn()
        cursor = conn.cursor()
        cursor.executemany(insert_query, rows)
        conn.commit()
        cursor.close()
        
        print(f"Inserted {len(rows)} sessions")
    else:
        print("No valid documents to insert")


def load_tickets():
    mongo = MongoHook(mongo_conn_id="mongo_default")
    pg = PostgresHook(postgres_conn_id="postgres_default")

    collection = mongo.get_collection("SupportTickets")
    docs = list(collection.find({}))

    rows = []
    for doc in docs:
        if "ticket_id" not in doc:
            continue
            
        doc.pop("_id", None)
        
        created = doc["created_at"]
        updated = doc["updated_at"]
        
        if hasattr(created, 'isoformat'):
            created_str = created.isoformat()
        else:
            created_str = created
            
        if hasattr(updated, 'isoformat'):
            updated_str = updated.isoformat()
        else:
            updated_str = updated
        
        try:
            if hasattr(created, 'isoformat') and hasattr(updated, 'isoformat'):
                resolution = (updated - created).total_seconds() / 3600
            else:
                resolution = 0
        except:
            resolution = 0

        rows.append((
            doc["ticket_id"],
            doc["user_id"],
            doc["status"],
            doc["issue_type"],
            created_str,
            updated_str,
            resolution
        ))

    if rows:
        insert_query = """
            INSERT INTO staging_support_tickets 
            (ticket_id, user_id, status, issue_type, created_at, updated_at, resolution_hours)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO NOTHING
        """
        
        conn = pg.get_conn()
        cursor = conn.cursor()
        cursor.executemany(insert_query, rows)
        conn.commit()
        cursor.close()
        
        print(f"Inserted {len(rows)} tickets")
    else:
        print("No valid documents to insert")


with DAG(
    dag_id="etl_mongo_to_postgres",
    default_args=default_args,
    schedule="@daily",
    catchup=False
) as dag:

    sessions = PythonOperator(
        task_id="load_sessions",
        python_callable=load_sessions
    )

    tickets = PythonOperator(
        task_id="load_tickets",
        python_callable=load_tickets
    )

    sessions >> tickets