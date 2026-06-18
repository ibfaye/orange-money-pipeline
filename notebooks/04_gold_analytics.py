"""
Databricks Notebook 04 — Gold Analytics & Fraud Detection

Builds Gold-layer aggregate tables and runs fraud pattern detection.
Designed for visualization in Databricks SQL dashboards or Power BI.

Databricks Runtime: 14.3 LTS or later
"""

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer — Analytics & Fraud Detection

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/xamxamgraph/orange-money-pipeline/src")

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold.daily_aggregates import GoldAggregator
from gold.fraud_patterns import FraudDetector

spark = SparkSession.builder.getOrCreate()
print(f"✅ Spark {spark.version}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Build Gold Aggregates

# COMMAND ----------
aggregator = GoldAggregator(spark)
results = aggregator.build_all(days_back=30)

print("📊 Gold Aggregation Results:")
for k, v in results.items():
    print(f"   {k}: {v:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Daily Summary Dashboard

# COMMAND ----------
daily = spark.table("orange_money.gold.daily_summary")

# Show latest 7 days
display(
    daily.filter(F.col("_date") >= F.date_sub(F.current_date(), 7))
    .orderBy("_date", ascending=False)
)

# COMMAND ----------
# MAGIC %md

# COMMAND ----------
# Key KPIs
print("📊 Key Performance Indicators (Last 7 Days):")
kpis = daily.filter(F.col("_date") >= F.date_sub(F.current_date(), 7)).agg(
    F.sum("transaction_count").alias("total_txns"),
    F.sum("total_volume_xof").alias("total_volume"),
    F.avg("success_rate").alias("avg_success_rate"),
    F.sum("total_fees_xof").alias("total_fees"),
    F.avg("avg_latency_seconds").alias("avg_latency"),
).collect()[0]

print(f"   Total Transactions: {kpis['total_txns']:,.0f}")
print(f"   Total Volume: {kpis['total_volume']:,.0f} XOF")
print(f"   Avg Success Rate: {kpis['avg_success_rate']:.2f}%")
print(f"   Total Fees: {kpis['total_fees']:,.0f} XOF")
print(f"   Avg Processing Latency: {kpis['avg_latency']:.1f}s")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Region Performance

# COMMAND ----------
display(
    daily.groupBy("region").agg(
        F.sum("transaction_count").alias("txn_count"),
        F.sum("total_volume_xof").alias("volume_xof"),
        F.avg("success_rate").alias("avg_success_pct"),
    ).orderBy("txn_count", ascending=False)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Merchant Activity

# COMMAND ----------
merchant = spark.table("orange_money.gold.merchant_activity")

# Top 10 merchants by volume
display(
    merchant.groupBy("merchant_name", "merchant_category").agg(
        F.sum("total_volume_xof").alias("total_volume"),
        F.sum("transaction_count").alias("txn_count"),
        F.avg("avg_transaction_xof").alias("avg_txn"),
        F.sum("unique_customers").alias("customers"),
    ).orderBy("total_volume", ascending=False).limit(10)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Fraud Pattern Detection

# COMMAND ----------
detector = FraudDetector(spark)
flags = detector.detect_all()

print(f"🚨 Fraud Detection Results:")
print(f"   Total flags: {flags.count():,}")

# Pattern breakdown
display(
    flags.groupBy("flag_pattern").count()
    .withColumnRenamed("count", "flag_count")
    .orderBy("flag_count", ascending=False)
)

# COMMAND ----------
# MAGIC %md

# COMMAND ----------
# Show flagged transactions
display(flags.orderBy("amount", ascending=False).limit(50))

# COMMAND ----------
# MAGIC %md

# COMMAND ----------
# Velocity anomaly details
velocity = flags.filter(F.col("flag_pattern") == "VELOCITY_ANOMALY")
print(f"🚨 Velocity Anomalies: {velocity.count():,}")
if velocity.count() > 0:
    display(
        velocity.select(
            "transaction_id", "sender_phone", "amount",
            "flag_detail_1", "region", "channel"
        ).orderBy(F.col("flag_detail_1").cast("int").desc()).limit(20)
    )

# COMMAND ----------
# MAGIC %md

# COMMAND ----------
# Amount spike details
spikes = flags.filter(F.col("flag_pattern") == "AMOUNT_SPIKE")
print(f"🚨 Amount Spikes: {spikes.count():,}")
if spikes.count() > 0:
    display(
        spikes.select(
            "transaction_id", "sender_phone", "amount",
            "flag_detail_1", "flag_detail_2", "region"
        ).orderBy("amount", ascending=False).limit(20)
    )

# COMMAND ----------
print("\n🎉 Gold analytics and fraud detection complete")
print("\n📊 Next Steps:")
print("   1. Connect Databricks SQL Dashboard to gold.daily_summary")
print("   2. Set up alerts on fraud detection flags")
print("   3. Schedule pipeline: ingestion → normalization → aggregation → detection")
print("   4. Integrate with Power BI / Tableau for executive reporting")
