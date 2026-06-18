"""
Databricks Notebook 03 — Silver Transformation

Demonstrates Bronze → Silver normalization:
- Schema enforcement
- Deduplication
- Data quality validation
- Enrichment (date dimensions, amount buckets, peak hours)

Databricks Runtime: 14.3 LTS or later
"""

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Layer — Normalization & Quality

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/xamxamgraph/orange-money-pipeline/src")

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver.transaction_normalizer import SilverNormalizer

spark = SparkSession.builder.getOrCreate()
print(f"✅ Spark {spark.version}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Run Silver Normalization

# COMMAND ----------
normalizer = SilverNormalizer(spark)

# Normalize the data we ingested in Notebook 02
metrics = normalizer.normalize(
    start_date=datetime(2025, 1, 15),
    end_date=datetime(2025, 1, 20),
)

print("📊 Silver Normalization Metrics:")
for k, v in metrics.items():
    print(f"   {k}: {v}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Verify Silver Data

# COMMAND ----------
silver_df = spark.table("orange_money.silver.normalized_transactions")

print(f"📊 Silver table: orange_money.silver.normalized_transactions")
print(f"   Total rows: {silver_df.count():,}")
print(f"   Columns: {len(silver_df.columns)}")

# COMMAND ----------
# MAGIC %md

# COMMAND ----------
# Show enriched columns
display(
    silver_df.select(
        "transaction_id", "amount", "status",
        "_date", "_hour", "_day_of_week", "_is_weekend",
        "_is_peak_hour", "_amount_bucket", "_is_merchant_txn",
        "_processing_latency_seconds", "_quality_flag"
    ).limit(20)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Quality Report

# COMMAND ----------
quality = normalizer.get_quality_report()
display(quality)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Quality Failures Investigation

# COMMAND ----------
# Investigate rows that failed quality checks
failures = silver_df.filter(F.col("_quality_flag") == "FAIL")

print(f"Quality failures: {failures.count():,} ({failures.count() / silver_df.count() * 100:.2f}%)")
print(f"\nFailure breakdown:")

for rule in ["_quality_amount_positive", "_quality_valid_status", "_quality_has_initiated_at"]:
    fails = failures.filter(F.col(rule) == False).count()
    print(f"  {rule}: {fails:,}")

display(failures.limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Amount Bucket Distribution

# COMMAND ----------
display(
    silver_df.groupBy("_amount_bucket").count()
    .withColumnRenamed("count", "txn_count")
    .withColumn("pct", F.round(F.col("txn_count") / silver_df.count() * 100, 1))
    .orderBy("txn_count", ascending=False)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Peak vs Off-Peak Volume Comparison

# COMMAND ----------
display(
    silver_df.groupBy("_is_peak_hour").agg(
        F.count("*").alias("txn_count"),
        F.sum("amount").alias("total_volume_xof"),
        F.avg("amount").alias("avg_amount_xof"),
    )
)

# COMMAND ----------
print("\n🎉 Silver normalization verified — ready for Gold aggregation")
