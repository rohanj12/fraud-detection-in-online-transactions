# src/processing/transformations.py
# Shared transformation logic for landing → curated.
# Reuse this in both batch and streaming jobs to stay consistent.

from pyspark.sql.functions import (
    col, lit, when, to_timestamp, current_timestamp, date_format
)

def cast_and_enrich(df):
    """
    - Parse timestamps
    - Defensive casts for numeric/boolean
    - Derived columns: event_date, event_hour, ingestion_ts
    - Simple rule flag: is_high_risk_rule
    """
    df2 = (
        df
        # timestamps
        .withColumn("event_ts", to_timestamp("event_ts"))
        .withColumn("previous_txn_ts",
                    when(col("previous_txn_ts").isNotNull(), to_timestamp("previous_txn_ts")))
        # defensive casts
        .withColumn("is_international",
                    when(col("is_international").cast("boolean").isNull(), lit(False))
                    .otherwise(col("is_international").cast("boolean")))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("lat",    col("lat").cast("double"))
        .withColumn("lon",    col("lon").cast("double"))
        .withColumn("velocity_10min", col("velocity_10min").cast("int"))
        .withColumn("merchant_risk_score", col("merchant_risk_score").cast("double"))
        # derived columns
        .withColumn("event_date",  date_format(col("event_ts"), "yyyy-MM-dd"))
        .withColumn("event_hour",  date_format(col("event_ts"), "HH").cast("int"))
        .withColumn("ingestion_ts", current_timestamp())
        # simple rule flag handy for early dashboards/alerts
        .withColumn(
            "is_high_risk_rule",
            when( (col("merchant_risk_score") >= 0.6) |
                  (col("amount") >= 1000) |
                  (col("velocity_10min") >= 5) |
                  (col("is_international") == True), lit(True)
            ).otherwise(lit(False))
        )
    )
    # minimal DQ: drop rows missing critical keys/timestamps/amount
    df3 = df2.filter(
        col("transaction_id").isNotNull() &
        col("event_ts").isNotNull() &
        col("amount").isNotNull()
    )
    return df3
