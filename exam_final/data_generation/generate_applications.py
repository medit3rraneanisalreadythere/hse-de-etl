import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

output_dir = Path("generated_data")
output_dir.mkdir(exist_ok=True)

output_file = output_dir / "applications.csv"

regions = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW", "DE-HB"]
products = ["cash_loan", "credit_card", "mortgage", "car_loan", "consumer_loan"]
risk_levels = ["low", "medium", "high"]
decision_statuses = ["approved", "rejected", "manual_review"]
channels = ["mobile", "web", "office", "call_center"]

rows_count = 650_000
start_time = datetime(2026, 5, 1, 9, 0, 0)

with output_file.open("w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    writer.writerow([
        "application_id",
        "event_time",
        "customer_id",
        "region_code",
        "product_type",
        "requested_amount",
        "term_months",
        "credit_score",
        "risk_level",
        "decision_status",
        "approved_amount",
        "channel",
        "employee_review_flag",
        "processing_time_sec"
    ])

    for i in range(rows_count):
        requested_amount = random.randint(1000, 75000)
        decision_status = random.choice(decision_statuses)

        if decision_status == "approved":
            approved_amount = requested_amount
        elif decision_status == "manual_review":
            approved_amount = random.randint(0, requested_amount)
        else:
            approved_amount = 0

        event_time = start_time + timedelta(seconds=random.randint(0, 31 * 24 * 60 * 60))

        writer.writerow([
            f"app_20260501_{i:07d}",
            event_time.strftime("%Y-%m-%d %H:%M:%S"),
            f"cust_{random.randint(10000, 999999)}",
            random.choice(regions),
            random.choice(products),
            requested_amount,
            random.choice([6, 12, 24, 36, 48, 60]),
            random.randint(300, 850),
            random.choice(risk_levels),
            decision_status,
            approved_amount,
            random.choice(channels),
            random.choice(["true", "false"]),
            random.randint(1, 180)
        ])

print(f"Generated file: {output_file}")
print(f"Size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")