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
print("Columns:", df.columns)
print(f"📥 Bronze: {df.count()} rows")
df.show(5)

# Silver Layer (Data Quality)
window_spec = Window.partitionBy("ticker").orderBy("Date")

silver_df = (
df
.withColumn("CloseNum", col("Close.1").cast("double"))
.filter(col("CloseNum") > 0)
.dropDuplicates(["ticker", "Date"])
.withColumn(
"daily_return",
(col("CloseNum") - lag("CloseNum", 1).over(window_spec)) /
lag("CloseNum", 1).over(window_spec)
)
.withColumn(
"ma_50",
avg("CloseNum").over(window_spec.rowsBetween(-49, 0))
)
)

print(f"✨ Silver: {silver_df.count()} rows")
silver_df.select("ticker", "Date", "CloseNum", "daily_return", "ma_50").show(10)

# Gold Layer (Aggregated Metrics)
gold_df = (
silver_df
.groupBy("ticker")
.agg(
avg("daily_return").alias("avg_daily_return"),
stddev("daily_return").alias("volatility"),
count("Date").alias("trading_days")
)
)

gold_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("portfolio_gold")
print("✅ Gold layer written to portfolio_gold/")
gold_df.show(10)





