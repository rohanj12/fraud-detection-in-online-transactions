import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
import sys
print("[consumer] module loaded; python:", sys.version, flush=True)

from kafka import KafkaConsumer
# import boto3  # enable if you set USE_S3=true

TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")

# Local landing config
BASE_DIR = Path(os.getenv("LANDING_DIR", "data/landing"))
MAX_RECORDS_PER_FILE = int(os.getenv("MAX_RECORDS_PER_FILE", "5000"))
MAX_SECONDS_PER_FILE = int(os.getenv("MAX_SECONDS_PER_FILE", "120"))

# Optional S3
USE_S3 = os.getenv("USE_S3", "false").lower() == "true"
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = os.getenv("S3_PREFIX", "landing/transactions/")

def make_sink_path(now: datetime) -> Path:
    # Partition by date/hour → efficient downstream Spark reads
    p = BASE_DIR / f"YYYY={now.year}-MM={now.month:02d}-DD={now.day:02d}" / f"HH={now.hour:02d}"
    p.mkdir(parents=True, exist_ok=True)
    return p

def main():
    print(f"[consumer] starting; topic={TOPIC}; bootstrap={BOOTSTRAP_SERVERS}", flush=True)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000
    )
    print(f"[consumer] reading topic={TOPIC}; landing to {BASE_DIR.resolve()}")

    current_file = None
    current_path = None
    records_in_file = 0
    file_start_time = time.time()

    # s3 = boto3.client("s3") if USE_S3 else None

    try:
        while True:
            now = datetime.now(timezone.utc)
            sink_dir = make_sink_path(now)

            # Rotate file if needed
            need_rotate = (
                current_file is None
                or records_in_file >= MAX_RECORDS_PER_FILE
                or (time.time() - file_start_time) >= MAX_SECONDS_PER_FILE
            )
            if need_rotate:
                if current_file:
                    current_file.flush()
                    current_file.close()
                    print(f"[consumer] closed {current_path} ({records_in_file} records)")
                    # if USE_S3 and s3:
                    #     s3_key = f"{S3_PREFIX}{current_path.relative_to(BASE_DIR)}"
                    #     s3.upload_file(str(current_path), S3_BUCKET, s3_key)
                    #     print(f"[consumer] uploaded to s3://{S3_BUCKET}/{s3_key}")
                current_path = sink_dir / f"transactions_{int(time.time())}.jsonl"
                current_file = open(current_path, "a", encoding="utf-8")
                records_in_file = 0
                file_start_time = time.time()
                print(f"[consumer] opened  {current_path}")

            polled = False
            for msg in consumer:
                polled = True
                event = msg.value
                current_file.write(json.dumps(event) + "\n")
                records_in_file += 1

                if records_in_file >= MAX_RECORDS_PER_FILE:
                    break

            if not polled:
                time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n[consumer] stopped")
    finally:
        if current_file:
            current_file.flush()
            current_file.close()
            print(f"[consumer] closed {current_path} ({records_in_file} records)")

if __name__ == "__main__":
    main()