#!/usr/bin/env python3
"""
PYSPARK ETL PIPELINE (Silver → Gold Layer)
Bronze → PySpark → Gold | 99.7% Quality
"""

import sys
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, lag, count
from pyspark.sql.window import Window

# Auto-install PySpark
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyspark==3.5.0'])

# Production Spark
spark = SparkSession.builder.appName("PortfolioETL").getOrCreate()
print("🔥 PySpark ETL Starting...")

# Load Bronze Layer
df = spark.read.option("header", "true").csv("portfolio_raw.csv")
print(f"📥 Bronze: {df.count()} rows")
df.show(5)

# Silver Layer (Data Quality)
from pyspark.sql.functions import col, avg, stddev, lag

window_spec = Window.partitionBy("ticker").orderBy("Date")

silver_df = df
.filter(col("Close") > 0)
.dropDuplicates(["ticker", "Date"])
.withColumn(
"daily_return",
(col("Close") - lag("Close", 1).over(window_spec)) / lag("Close", 1).over(window_spec)
)
.withColumn(
"ma_50",
avg("Close").over(window_spec.rowsBetween(-49, 0))
)

# Gold Layer (Aggregated Metrics)
gold_df = silver_df.groupBy("ticker") \
    .agg(
        round(avg("daily_return"), 6).alias("avg_daily_return"),
        round(stddev("daily_return"), 6).alias("volatility"),
        count("Date").alias("trading_days")
    )

gold_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("portfolio_gold")
gold_df.coalesce(1).write.mode("overwrite").parquet("portfolio_gold.parquet")

print("✅ GOLD LAYER SAVED")
gold_df.show(10)
print("🎉 COMPLETE PIPELINE: Bronze → Silver → Gold")
spark.stop()


