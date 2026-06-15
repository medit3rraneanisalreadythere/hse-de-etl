import json
import random
from datetime import datetime, timedelta
from pathlib import Path

output_dir = Path("generated_data")
output_dir.mkdir(exist_ok=True)

output_file = output_dir / "kafka_messages.jsonl"

regions = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW", "DE-HB"]
risk_levels = ["low", "medium", "high"]
decision_statuses = ["approved", "rejected", "manual_review"]
document_statuses = ["verified", "pending", "rejected"]

rows_count = 5_000
start_time = datetime(2026, 5, 1, 10, 0, 0)

with output_file.open("w", encoding="utf-8") as file:
    for i in range(rows_count):
        message = {
            "application_id": f"loan_{i:07d}",
            "customer": {
                "customer_id": f"cust_{random.randint(1000, 999999)}",
                "region": random.choice(regions)
            },
            "loan": {
                "amount": random.randint(1000, 75000),
                "term_months": random.choice([6, 12, 24, 36, 48, 60])
            },
            "scoring": {
                "score": random.randint(300, 850),
                "risk_level": random.choice(risk_levels)
            },
            "documents": [
                {
                    "type": "passport",
                    "status": random.choice(document_statuses)
                },
                {
                    "type": "income_statement",
                    "status": random.choice(document_statuses)
                }
            ],
            "decision_status": random.choice(decision_statuses),
            "submitted_at": (
                start_time + timedelta(seconds=random.randint(0, 31 * 24 * 60 * 60))
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        file.write(json.dumps(message, ensure_ascii=False) + "\n")

print(f"Generated file: {output_file}")
print(f"Size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")