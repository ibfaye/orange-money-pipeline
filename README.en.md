# Orange Money Pipeline — English Reference

> This is the English reference for the [Orange Money Pipeline](https://github.com/senanalytics/orange-money-pipeline). The primary README is in French. Below is a summary.

## What This Is

A production-grade **Azure Databricks Medallion Architecture** pipeline for Orange Money mobile payment transactions. Deployable as reference infrastructure for any Senegalese or West African bank, fintech, or enterprise processing mobile money data.

## Why This Exists

Orange Money processes billions of XOF monthly across 14M+ users in Senegal. Most enterprises lack a robust data architecture to ingest, normalize, and analyze this transaction flow. This repository provides the **reference implementation** — demonstrating that world-class data engineering is achievable for West African financial infrastructure.

## Architecture at a Glance

| Layer | Table | Purpose |
|-------|-------|---------|
| **Bronze** | `orange_money.bronze.raw_transactions` | Raw API payloads, append-only, CDP-tokenized PII |
| **Silver** | `orange_money.silver.normalized_transactions` | Deduplicated, quality-checked, enriched with time/amount dimensions |
| **Gold** | `orange_money.gold.daily_summary` | Daily KPIs by region, channel, peak/off-peak |
| **Gold** | `orange_money.gold.merchant_activity` | Merchant performance metrics |
| **Gold** | `orange_money.gold.fraud_patterns` | Anomaly detection flags |

## CDP Compliance

This pipeline implements Senegal's **Commission de Protection des Données Personnelles** requirements:

- **Art. 44 (Pseudonymization)**: Phone numbers tokenized via HMAC-SHA256 at ingestion edge
- **Art. 62 (Retention)**: Configurable retention periods with date-partitioned deletion
- **Art. 50 (Consent)**: Consent metadata tracked in Bronze schema

For full CDP compliance architecture, [contact Sen'Analytics](https://senanalytics.sn).

## Quick Reference

```bash
# Clone
git clone https://github.com/senanalytics/orange-money-pipeline.git
cd orange-money-pipeline

# Install
pip install -e ".[dev]"

# Test
pytest tests/ -v

# Deploy infrastructure
cd terraform && terraform init && terraform apply
```

## License

MIT © [Sen'Analytics](https://senanalytics.sn)
