"""
Batch curation (one-off or ad-hoc backfills):
- Reads all landing JSONL recursively (not streaming).
- Casts/derives fields via shared helper.
- Dedupes by transaction_id (batch version).
- Writes Parquet partitioned by event_date/event_hour.
- Uses dynamic partition overwrite so re-runs are safe.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
)
from src.processing.transformations import cast_and_enrich
from pyspark.sql.functions import col

# -----------------------------
# Config (override via env vars)
# -----------------------------
LANDING_DIR = os.getenv("LANDING_DIR", "data/landing")
CURATED_DIR = os.getenv("CURATED_DIR", "data/curated/transactions")
OVERWRITE = os.getenv("OVERWRITE", "true").lower() == "true"  # dynamic partition overwrite

# -----------------------------
# Spark session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("fraud-transactions-batch-curation")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Dynamic partition overwrite lets us overwrite only touched partitions
if OVERWRITE:
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# -----------------------------
# Landing schema (JSONL)
# -----------------------------
schema = StructType([
    StructField("transaction_id",       StringType(), False),
    StructField("event_ts",             StringType(), False),
    StructField("user_id",              StringType(), True),
    StructField("card_id",              StringType(), True),
    StructField("merchant_id",          StringType(), True),
    StructField("merchant_category",    StringType(), True),
    StructField("amount",               DoubleType(), True),
    StructField("currency",             StringType(), True),
    StructField("country",              StringType(), True),
    StructField("city",                 StringType(), True),
    StructField("ip_address",           StringType(), True),
    StructField("device_id",            StringType(), True),
    StructField("channel",              StringType(), True),
    StructField("entry_mode",           StringType(), True),
    StructField("is_international",     BooleanType(), True),
    StructField("lat",                  DoubleType(), True),
    StructField("lon",                  DoubleType(), True),
    StructField("previous_txn_ts",      StringType(), True),
    StructField("velocity_10min",       IntegerType(), True),
    StructField("merchant_risk_score",  DoubleType(), True),
    StructField("label_training_only",  IntegerType(), True)
])

print(f"[batch] reading landing (recursive) from: {LANDING_DIR}")

# Read all nested landing files (YYYY=…/HH=…/)
landing_df = (
    spark.read
    .schema(schema)
    .option("recursiveFileLookup", "true")
    .json(LANDING_DIR)
)

input_count = landing_df.count()
print(f"[batch] input rows: {input_count}")

# Transform (cast, derive, minimal DQ)
curatable_df = cast_and_enrich(landing_df)

# Batch dedupe by transaction_id (keep first; order is arbitrary in batch)
deduped_df = curatable_df.dropDuplicates(["transaction_id"])
deduped_count = deduped_df.count()
dupes_removed = input_count - deduped_count
print(f"[batch] deduped rows: {deduped_count}  (removed {dupes_removed} duplicates)")

# Optional: quick sanity checks on amounts and timestamps
bad_amounts = deduped_df.filter(col("amount") < 0).count()
null_dates = deduped_df.filter(col("event_date").isNull() | col("event_hour").isNull()).count()
print(f"[batch] sanity: negative amounts={bad_amounts}, null event partitions={null_dates}")

# Write Parquet partitioned by event_date/hour
write_mode = "overwrite" if OVERWRITE else "append"
print(f"[batch] writing Parquet to: {CURATED_DIR}  (mode={write_mode})")
(
    deduped_df
    .write
    .mode(write_mode)
    .partitionBy("event_date", "event_hour")
    .parquet(CURATED_DIR)
)

print("[batch] DONE. Parquet available at:", CURATED_DIR)
