"""
Databricks Notebook 01 — Orange Money API Exploration

Explores the Orange Money API client in mock mode.
Run this FIRST to understand the data model and transaction patterns.

Databricks Runtime: 14.3 LTS or later
Cluster: Single-node (dev/exploration)
"""

# COMMAND ----------
# MAGIC %md
# MAGIC # Orange Money API — Data Exploration
# MAGIC
# MAGIC This notebook explores the Orange Money transaction data model using the mock client.
# MAGIC It demonstrates:
# MAGIC - API client initialization (mock mode)
# MAGIC - Transaction data model (Pydantic)
# MAGIC - CDP-compliant phone tokenization
# MAGIC - Realistic Senegalese transaction patterns
# MAGIC
# MAGIC **Prerequisites:** None. Mock mode requires no API credentials.

# COMMAND ----------
# Setup
import sys
sys.path.append("/Workspace/Repos/xamxamgraph/orange-money-pipeline/src")

from datetime import datetime, timedelta
from pprint import pprint
import pandas as pd

from ingestion.config import config
from ingestion.orange_money_client import OrangeMoneyClient, MockTransactionGenerator

print(f"✅ Configuration loaded")
print(f"   Mock mode: {config.mock_mode}")
print(f"   CDP tokenization: {config.cdp_tokenize_phone_numbers}")
print(f"   Catalog: {config.catalog_name}.{config.bronze_schema} → {config.gold_schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Generate Sample Transactions

# COMMAND ----------
# Generate a single day of mock transactions
gen = MockTransactionGenerator(seed=42)  # Fixed seed for reproducibility
sample_date = datetime(2025, 1, 15)

print(f"Generating transactions for {sample_date.date()}...")
sample_txns = list(gen.generate_day(sample_date, count=20))

print(f"\nGenerated {len(sample_txns)} transactions")
print(f"\nSample transaction:")
tx = sample_txns[0]
print(f"  ID: {tx.transaction_id}")
print(f"  Type: {tx.transaction_type}")
print(f"  Amount: {tx.amount:,.0f} XOF ({tx.currency})")
print(f"  Fee: {tx.fee:,.0f} XOF")
print(f"  Sender: {tx.sender_phone} → Recipient: {tx.recipient_phone}")
print(f"  Status: {tx.status}")
print(f"  Time: {tx.initiated_at}")
print(f"  Channel: {tx.channel}")
print(f"  Region: {tx.region}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Transaction Pattern Analysis

# COMMAND ----------
# Analyze the generated batch
df = pd.DataFrame([t.model_dump() for t in sample_txns])

print("📊 Transaction Type Distribution")
print(df["transaction_type"].value_counts().to_string())
print(f"\n📊 Channel Distribution")
print(df["channel"].value_counts().to_string())
print(f"\n📊 Region Distribution")
print(df["region"].value_counts().to_string())
print(f"\n💰 Amount Statistics")
print(f"   Total Volume: {df['amount'].sum():,.0f} XOF")
print(f"   Avg Transaction: {df['amount'].mean():,.0f} XOF")
print(f"   Median: {df['amount'].median():,.0f} XOF")
print(f"   Min: {df['amount'].min():,.0f} XOF")
print(f"   Max: {df['amount'].max():,.0f} XOF")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. CDP Tokenization Demo

# COMMAND ----------
from ingestion.orange_money_client import PhoneTokenizer

tokenizer = PhoneTokenizer()

test_phones = ["771234567", "781234567", "761234567", "701234567"]
for phone in test_phones:
    token = tokenizer.tokenize(phone)
    masked = tokenizer.mask_display(phone)
    print(f"  {masked} → {token}")

print("\n✅ Same input always produces same token (deterministic)")
print(f"   771234567 → {tokenizer.tokenize('771234567')} (first call)")
print(f"   771234567 → {tokenizer.tokenize('771234567')} (second call)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. API Client with Full Pagination

# COMMAND ----------
client = OrangeMoneyClient()
start = datetime(2025, 1, 15)
end = datetime(2025, 1, 17)

print(f"Fetching transactions {start.date()} → {end.date()}...")
total = 0
tx_types = {}
regions = {}

for page in client.fetch_transactions(start, end, page_size=50):
    for tx in page.transactions:
        total += 1
        tx_types[tx.transaction_type] = tx_types.get(tx.transaction_type, 0) + 1
        regions[tx.region] = regions.get(tx.region, 0) + 1

print(f"\n📊 Total fetched: {total:,} transactions")
print(f"\nTransaction Types:")
for t, c in sorted(tx_types.items(), key=lambda x: -x[1]):
    print(f"  {t}: {c:,} ({c/total*100:.1f}%)")

print(f"\nTop Regions:")
for r, c in sorted(regions.items(), key=lambda x: -x[1])[:5]:
    print(f"  {r}: {c:,} ({c/total*100:.1f}%)")

print("\n✅ API client exploration complete")
