"""
Databricks Notebook 02 — Bronze Ingestion

Demonstrates ingesting Orange Money transaction data into the Bronze Delta table.
Shows: schema creation, append-only writes, audit columns, mergeSchema.

Databricks Runtime: 14.3 LTS or later
Cluster: All-Purpose (single-node OK for mock data)
"""

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Layer — Raw Transaction Landing
# MAGIC
# MAGIC This notebook demonstrates the Bronze ingestion pattern:
# MAGIC 1. Initialize Spark session with Delta Lake support
# MAGIC 2. Ingest Orange Money transactions into `orange_money.bronze.raw_transactions`
# MAGIC 3. Verify data landed correctly
# MAGIC 4. Demonstrate incremental ingestion

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/senanalytics/orange-money-pipeline/src")

from datetime import datetime, timedelta
from pyspark.sql import SparkSession

from ingestion.config import config
from ingestion.orange_money_client import OrangeMoneyClient
from bronze.raw_landing import BronzeIngestor

# Initialize Spark (Databricks provides the session)
spark = SparkSession.builder.getOrCreate()
print(f"✅ Spark {spark.version} — Delta Lake supported: {spark.conf.get('spark.sql.extensions', 'none')}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Create Catalog and Schema (if first run)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Create the Orange Money catalog and schemas
# MAGIC CREATE CATALOG IF NOT EXISTS orange_money;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS orange_money.bronze
# MAGIC   COMMENT 'Raw Orange Money transactions — append-only, schema-on-read';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS orange_money.silver
# MAGIC   COMMENT 'Normalized, deduplicated, quality-checked transactions';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS orange_money.gold
# MAGIC   COMMENT 'Business aggregates and fraud detection';

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Initial Ingestion (3 days of mock data)

# COMMAND ----------
client = OrangeMoneyClient()
ingestor = BronzeIngestor(spark, client)

start = datetime(2025, 1, 15)
end = datetime(2025, 1, 17)

print(f"Ingesting Orange Money data: {start.date()} → {end.date()}")
rows = ingestor.ingest_date_range(start, end)
print(f"\n✅ Bronze ingestion complete: {rows:,} rows written to `{config.bronze_path}`")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Verify Bronze Data

# COMMAND ----------
# Inspect the Bronze table
bronze_df = spark.table(config.bronze_path)

print(f"📊 Bronze table: {config.bronze_path}")
print(f"   Total rows: {bronze_df.count():,}")
print(f"   Date range: {bronze_df.select('initiated_at').agg({'initiated_at': 'min'}).collect()[0][0]} → {bronze_df.select('initiated_at').agg({'initiated_at': 'max'}).collect()[0][0]}")
print(f"\nSchema:")
bronze_df.printSchema()

# COMMAND ----------
# MAGIC %md

# COMMAND ----------
# Sample rows
display(bronze_df.limit(10))

# COMMAND ----------
# MAGIC %md

# COMMAND ----------
# Transaction status distribution
display(
    bronze_df.groupBy("status").count()
    .withColumnRenamed("count", "txn_count")
    .orderBy("txn_count", ascending=False)
)

# COMMAND ----------
# MAGIC %md

# COMMAND ----------
# Channel + Region breakdown
display(
    bronze_df.groupBy("channel", "region").count()
    .withColumnRenamed("count", "txn_count")
    .orderBy("txn_count", ascending=False)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Incremental Ingestion

# COMMAND ----------
# Demonstrate incremental ingestion
latest = ingestor.get_latest_ingestion_date()
print(f"Latest transaction in Bronze: {latest}")

if latest:
    # Ingest from latest date + 1 day
    new_start = latest + timedelta(days=1)
    new_end = new_start + timedelta(days=2)
    print(f"Ingesting new data: {new_start.date()} → {new_end.date()}")
    new_rows = ingestor.ingest_date_range(new_start, new_end)
    print(f"✅ Added {new_rows:,} new rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Audit Trail

# COMMAND ----------
# Show batch metadata
display(
    bronze_df.select("_batch_id", "_ingested_at", "_source_file")
    .distinct()
    .orderBy("_ingested_at")
)

# COMMAND ----------
# Verify no duplicates by transaction_id
total = bronze_df.count()
unique = bronze_df.select("transaction_id").distinct().count()
print(f"Total rows: {total:,}")
print(f"Unique transaction_ids: {unique:,}")
print(f"Duplicates: {total - unique:,}")
print(f"✅ {'No duplicates' if total == unique else f'WARNING: {total - unique} duplicates found'}")

# COMMAND ----------
print("\n🎉 Bronze ingestion pipeline verified and ready for Silver normalization")
