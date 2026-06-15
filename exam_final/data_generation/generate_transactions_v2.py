import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

output_dir = Path("generated_data")
output_dir.mkdir(exist_ok=True)

output_file = output_dir / "transactions_v2.csv"

regions = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW", "DE-HB"]
campaign_types = [
    "credit_card_offer",
    "cash_loan_offer",
    "mortgage_offer",
    "insurance_offer",
    "deposit_offer"
]
call_statuses = ["answered", "missed", "busy", "failed"]
client_responses = ["interested", "not_interested", "callback", "no_response"]

rows_count = 450_000
start_time = datetime(2026, 5, 1, 8, 0, 0)

with output_file.open("w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    writer.writerow([
        "call_id",
        "call_time",
        "client_id",
        "region_code",
        "campaign_type",
        "call_status",
        "client_response",
        "duration_sec",
        "follow_up_required"
    ])

    for i in range(rows_count):
        call_time = start_time + timedelta(seconds=random.randint(0, 31 * 24 * 60 * 60))

        writer.writerow([
            f"call_202605_{i:07d}",
            call_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            f"client_{random.randint(1000, 999999)}",
            random.choice(regions),
            random.choice(campaign_types),
            random.choice(call_statuses),
            random.choice(client_responses),
            random.randint(5, 900),
            random.choice(["true", "false"])
        ])

print(f"Generated file: {output_file}")
print(f"Size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")