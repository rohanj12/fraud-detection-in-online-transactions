import json
import os
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
from collections import defaultdict


from faker import Faker
from kafka import KafkaProducer

# ---------- Config (override via env) ----------
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "5"))

fake = Faker()
Faker.seed(42)
random.seed(42)

# Track last timestamp & rolling counts to simulate velocity features
last_txn_ts_by_card = defaultdict(lambda: None)
rolling_counts_10min = defaultdict(int)

merchant_categories = [
    "electronics", "groceries", "fashion", "restaurants", "travel",
    "gas", "pharmacy", "entertainment", "digital_goods", "home_improvement"
]
channels = ["web", "mobile", "pos"]
entry_modes = ["chip", "swipe", "online"]
countries = [
    ("US", (37.0902, -95.7129)), ("GB", (55.3781, -3.4360)),
    ("IN", (20.5937, 78.9629)), ("DE", (51.1657, 10.4515)),
    ("AU", (-25.2744, 133.7751))
]

def merchant_risk(category: str) -> float:
    base = {
        "digital_goods": 0.6, "travel": 0.45, "electronics": 0.4, "fashion": 0.35,
        "entertainment": 0.3, "restaurants": 0.25, "groceries": 0.15,
        "pharmacy": 0.1, "gas": 0.1, "home_improvement": 0.2
    }
    noise = random.uniform(-0.05, 0.05)
    return max(0.0, min(1.0, base.get(category, 0.2) + noise))

def jitter_coords(lat, lon, max_jitter=1.2):
    return lat + random.uniform(-max_jitter, max_jitter), lon + random.uniform(-max_jitter, max_jitter)

def generate_event() -> dict:
    now = datetime.now(timezone.utc)
    country, (clat, clon) = random.choice(countries)
    lat, lon = jitter_coords(clat, clon)
    category = random.choice(merchant_categories)

    user_id = f"user_{random.randint(1000, 9999)}"
    card_id = f"card_{random.randint(10000, 99999)}"

    # Rolling velocity for this card in last 10 mins
    prev_ts = last_txn_ts_by_card[card_id]
    if prev_ts and (now - prev_ts) <= timedelta(minutes=10):
        rolling_counts_10min[card_id] += 1
    else:
        rolling_counts_10min[card_id] = 1
    last_txn_ts_by_card[card_id] = now

    amount = round(random.uniform(2.0, 1500.0), 2)
    is_international = country != "US" and random.random() < 0.5
    entry_mode = "online" if random.random() < 0.55 else random.choice(entry_modes)

    return {
        "transaction_id": str(uuid.uuid4()),
        "event_ts": now.isoformat(),
        "user_id": user_id,
        "card_id": card_id,
        "merchant_id": f"m_{random.randint(100, 999)}",
        "merchant_category": category,
        "amount": amount,
        "currency": "USD",
        "country": country,
        "city": fake.city(),
        "ip_address": fake.ipv4_public(),
        "device_id": f"dev_{random.randint(10000, 99999)}",
        "channel": random.choice(channels),
        "entry_mode": entry_mode,
        "is_international": is_international,
        "lat": round(lat, 4),
        "lon": round(lon, 4),
        "previous_txn_ts": prev_ts.isoformat() if prev_ts else None,
        "velocity_10min": rolling_counts_10min[card_id],
        "merchant_risk_score": round(merchant_risk(category), 3),
        "label_training_only": None  # populated only in historical backfills
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=50,
        retries=5
    )
    print(f"[producer] sending to topic={TOPIC} at ~{EVENTS_PER_SEC} events/sec")

    try:
        while True:
            event = generate_event()
            # Use card_id as key to keep per-card ordering
            producer.send(TOPIC, key=event["card_id"], value=event)
            time.sleep(1.0 / EVENTS_PER_SEC)
    except KeyboardInterrupt:
        print("\n[producer] stopped")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
