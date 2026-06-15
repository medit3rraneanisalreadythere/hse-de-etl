import csv
import os
from datetime import datetime

import ydb
import ydb.iam


ENDPOINT = "grpcs://ydb.serverless.yandexcloud.net:2135"
DATABASE = "/ru-central1/b1ghedftaaq4desokdp3/etn5u7ra9ts58n3leij6"
KEY_FILE = "authorized_key.json"

CSV_FILE = "generated_data/transactions_v2.csv"
TABLE_NAME = "transactions_v2"


def parse_bool(value):
    return str(value).lower() == "true"


def parse_datetime(value):
    dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    return int(dt.timestamp())


def create_table(pool):
    query = f"""
    CREATE TABLE `{TABLE_NAME}` (
        call_id Utf8 NOT NULL,
        call_time Utf8,
        client_id Utf8,
        region_code Utf8,
        campaign_type Utf8,
        call_status Utf8,
        client_response Utf8,
        duration_sec Int32,
        follow_up_required Bool,
        PRIMARY KEY (call_id)
    );
    """

    def callee(session):
        session.execute_scheme(query)

    try:
        pool.retry_operation_sync(callee)
        print("Table created")
    except Exception as e:
        print("Table create skipped or failed:", e)


def upload_csv(driver):
    column_types = ydb.BulkUpsertColumns()
    column_types.add_column("call_id", ydb.PrimitiveType.Utf8)
    column_types.add_column("call_time", ydb.PrimitiveType.Utf8)
    column_types.add_column("client_id", ydb.PrimitiveType.Utf8)
    column_types.add_column("region_code", ydb.PrimitiveType.Utf8)
    column_types.add_column("campaign_type", ydb.PrimitiveType.Utf8)
    column_types.add_column("call_status", ydb.PrimitiveType.Utf8)
    column_types.add_column("client_response", ydb.PrimitiveType.Utf8)
    column_types.add_column("duration_sec", ydb.PrimitiveType.Int32)
    column_types.add_column("follow_up_required", ydb.PrimitiveType.Bool)

    table_path = os.path.join(DATABASE, TABLE_NAME)

    batch = []
    batch_size = 1000
    total = 0

    with open(CSV_FILE, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            batch.append({
                "call_id": row["call_id"],
                "call_time": row["call_time"],
                "client_id": row["client_id"],
                "region_code": row["region_code"],
                "campaign_type": row["campaign_type"],
                "call_status": row["call_status"],
                "client_response": row["client_response"],
                "duration_sec": int(row["duration_sec"]),
                "follow_up_required": parse_bool(row["follow_up_required"])
            })

            if len(batch) >= batch_size:
                driver.table_client.bulk_upsert(table_path, batch, column_types)
                total += len(batch)
                print(f"Uploaded rows: {total}")
                batch = []

        if batch:
            driver.table_client.bulk_upsert(table_path, batch, column_types)
            total += len(batch)
            print(f"Uploaded rows: {total}")

    print("Upload finished")


def main():
    credentials = ydb.iam.ServiceAccountCredentials.from_file(KEY_FILE)

    driver = ydb.Driver(
        endpoint=ENDPOINT,
        database=DATABASE,
        credentials=credentials
    )

    driver.wait(fail_fast=True, timeout=10)

    pool = ydb.SessionPool(driver)

    create_table(pool)
    upload_csv(driver)

    driver.stop()


if __name__ == "__main__":
    main()