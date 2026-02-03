from datetime import datetime
import pandas as pd
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

INPUT_PATH = "/opt/airflow/data/IOT-temp.csv"
OUTPUT_DIR = "/opt/airflow/data/"

def parse_and_transform():
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    raw_records = re.split(r'__export__\.temp_log_', content)
    records = []

    for part in raw_records:
        if not part.strip():
            continue

        match = re.match(r'^[a-zA-Z0-9_]+,(.+),(\d{2}-\d{2}-\d{4} \d{2}:\d{2}),(\d+),([InOut]+)$', part.strip())
        if match:
            room, noted_date, temp, in_out = match.groups()
            records.append({
                "room": room,
                "noted_date": noted_date,
                "temperature": int(temp),
                "in_out": in_out
            })

    df = pd.DataFrame(records)

    df = df[df['in_out'] == 'In'].copy()

    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M', errors='coerce')
    df = df.dropna(subset=['noted_date'])
    df['date'] = df['noted_date'].dt.date

    lower = df['temperature'].quantile(0.05)
    upper = df['temperature'].quantile(0.95)
    df = df[(df['temperature'] >= lower) & (df['temperature'] <= upper)]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(os.path.join(OUTPUT_DIR, "transformed_data.csv"), index=False)

    daily = df.groupby('date')['temperature'].mean().reset_index()

    hottest = daily.nlargest(5, 'temperature')
    coldest = daily.nsmallest(5, 'temperature')

    print("5 самых жарких дней:")
    print(hottest)
    print("\n5 самых холодных дней:")
    print(coldest)

    hottest.to_csv(os.path.join(OUTPUT_DIR, "hottest_days.csv"), index=False)
    coldest.to_csv(os.path.join(OUTPUT_DIR, "coldest_days.csv"), index=False)

with DAG(
    dag_id="iot_temperature_dag",
    start_date=datetime(2026, 2, 1)
) as dag:

    etl_task = PythonOperator(
        task_id="parse_and_transform_iot_temperature",
        python_callable=parse_and_transform
    )