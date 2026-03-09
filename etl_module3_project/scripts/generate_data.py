from pymongo import MongoClient
from faker import Faker
import random
from datetime import datetime, timedelta

client = MongoClient("mongodb://localhost:27017/")
db = client["etl_project_db"]
fake = Faker()

def generate_sessions(count=100):
    sessions = []
    for i in range(count):
        start = fake.date_time_this_year()
        end = start + timedelta(minutes=random.randint(5, 60))
        sessions.append({
            "session_id": f"sess_{i:04d}",
            "user_id": f"user_{random.randint(1, 50):03d}",
            "start_time": start,
            "end_time": end,
            "pages_visited": [fake.uri_path() for _ in range(random.randint(1, 5))],
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "actions": [random.choice(["login", "view_product", "add_to_cart", "logout", "search"]) for _ in range(random.randint(1, 4))]
        })
    return sessions

def generate_events(count=200):
    events = []
    for i in range(count):
        events.append({
            "event_id": f"evt_{i:04d}",
            "timestamp": fake.date_time_this_year(),
            "event_type": random.choice(["click", "scroll", "hover", "submit"]),
            "details": {"page": fake.uri_path()}
        })
    return events

def generate_tickets(count=50):
    tickets = []
    for i in range(count):
        created = fake.date_time_this_year()
        updated = created + timedelta(hours=random.randint(1, 48))
        tickets.append({
            "ticket_id": f"ticket_{i:03d}",
            "user_id": f"user_{random.randint(1, 50):03d}",
            "status": random.choice(["open", "closed", "pending"]),
            "issue_type": random.choice(["payment", "login", "bug", "feature"]),
            "messages": [
                {"sender": "user", "message": fake.sentence(), "timestamp": created.isoformat()},
                {"sender": "support", "message": fake.sentence(), "timestamp": updated.isoformat()}
            ],
            "created_at": created,
            "updated_at": updated
        })
    return tickets

def generate_recommendations(count=50):
    recs = []
    for i in range(count):
        recs.append({
            "user_id": f"user_{i:03d}",
            "recommended_products": [f"prod_{random.randint(1, 100):03d}" for _ in range(3)],
            "last_updated": fake.date_time_this_year()
        })
    return recs

def generate_reviews(count=50):
    reviews = []
    for i in range(count):
        reviews.append({
            "review_id": f"rev_{i:03d}",
            "user_id": f"user_{random.randint(1, 50):03d}",
            "product_id": f"prod_{random.randint(1, 100):03d}",
            "review_text": fake.paragraph(),
            "rating": random.randint(1, 5),
            "moderation_status": random.choice(["pending", "approved", "rejected"]),
            "flags": ["contains_images"] if random.random() > 0.7 else [],
            "submitted_at": fake.date_time_this_year()
        })
    return reviews

if __name__ == "__main__":
    db.UserSessions.insert_many(generate_sessions())
    db.EventLogs.insert_many(generate_events())
    db.SupportTickets.insert_many(generate_tickets())
    db.UserRecommendations.insert_many(generate_recommendations())
    db.ModerationQueue.insert_many(generate_reviews())
    print("Data generated successfully in MongoDB!")