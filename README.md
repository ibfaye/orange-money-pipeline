<p align="center">
  <img src="https://raw.githubusercontent.com/xamxamgraph/brand/main/xamxamgraph-badge.svg" alt="XamXam Graph" height="40">
</p>

<h1 align="center">Orange Money Pipeline</h1>

<p align="center">
  <strong>Pipeline Medallion Azure Databricks pour les transactions Orange Money</strong><br>
  Bronze → Silver → Gold · Conforme CDP · Prêt pour la production
</p>

<p align="center">
  <a href="#english-summary">🇬🇧 English Summary</a> ·
  <a href="#architecture">🏗️ Architecture</a> ·
  <a href="#démarrage-rapide">🚀 Démarrage</a> ·
  <a href="https://xamxamgraph.com">🌐 XamXam Graph</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Databricks-14.3_LTS-FF3621?logo=databricks" alt="Databricks">
  <img src="https://img.shields.io/badge/Delta_Lake-3.1-0052CC?logo=delta" alt="Delta Lake">
  <img src="https://img.shields.io/badge/dbt-1.8-FF694B?logo=dbt" alt="dbt">
  <img src="https://img.shields.io/badge/Terraform-1.8-844FBA?logo=terraform" alt="Terraform">
  <img src="https://img.shields.io/badge/CDP-Conforme-1B2A4A?logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCI+PHBhdGggZmlsbD0iI2ZmZiIgZD0iTTEyIDJDNi40OCAyIDIgNi40OCAyIDEyczQuNDggMTAgMTAgMTAgMTAtNC40OCAxMC0xMFMxNy41MiAyIDEyIDJ6bS0yIDE1bC01LTUgMS40MS0xLjQxTDEwIDE0LjE3bDcuNTktNy41OUwxOSA4bC05IDl6Ii8+PC9zdmc+" alt="CDP Compliant">
  <img src="https://img.shields.io/badge/Python-3.10_|_3.11_|_3.12-3776AB?logo=python" alt="Python">
</p>

---

## 🇸🇳 Pourquoi ce projet ?

L'**Orange Money** est le principal système de paiement mobile au Sénégal, avec plus de **14 millions d'utilisateurs actifs**. Chaque jour, des millions de transactions traversent cette infrastructure — paiements marchands, transferts P2P, retraits, dépôts.

Pourtant, la plupart des banques, fintechs et entreprises sénégalaises **n'ont pas d'architecture de données robuste** pour ingérer, normaliser et analyser ces flux transactionnels. Les données Orange Money sont souvent traitées avec des scripts ad-hoc, des fichiers CSV exportés manuellement, ou pire — pas traitées du tout.

**Ce dépôt change cela.**

C'est un **pipeline Medallion complet** (Bronze → Silver → Gold) sur Azure Databricks, conçu pour les transactions Orange Money, avec :

- 🇸🇳 **Conformité CDP** — tokenisation des numéros de téléphone à l'ingestion (Article 44, pseudonymisation)
- 🏗️ **Architecture Medallion** — ingestion brute → normalisation → agrégats métier
- 🔒 **Infrastructure-as-Code** — Terraform pour le déploiement reproductible
- 🧪 **Mode mock intégré** — données synthétiques réalistes pour le développement sans credentials API
- 🐍 **Python + dbt** — choisissez votre couche de transformation (PySpark ou SQL)

> **Note importante :** Ce dépôt est un **framework de référence**. Le client API fonctionne en mode mock par défaut. Pour le connecter à l'API Orange Money en production, vous avez besoin de credentials partenaire Orange. [Contactez-nous](https://xamxamgraph.com) pour un accompagnement.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    ORANGE MONEY API                              │
│              (ou Mock Generator pour le dev)                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE — orange_money.bronze.raw_transactions               │
│  • Append-only, schema-on-read                                  │
│  • Données brutes + colonnes d'audit                            │
│  • Tokenisation CDP des numéros à l'ingestion                   │
│  • Format : Delta Lake                                          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥈 SILVER — orange_money.silver.normalized_transactions        │
│  • Déduplication (idempotence par transaction_id)              │
│  • Contrôles qualité (7 règles)                                 │
│  • Enrichissement : buckets de montant, heures de pointe,       │
│    jours de weekend, catégories commerçants                     │
│  • Partitionné par _date                                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥇 GOLD — orange_money.gold.*                                  │
│  • daily_summary : KPIs par date, région, canal                │
│  • merchant_activity : Performance commerçants                 │
│  • fraud_patterns : Détection d'anomalies                      │
│    (vélocité, montants anormaux, transferts rapides)           │
│  • Prêt pour dashboards Databricks SQL / Power BI              │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Démarrage Rapide

### Prérequis

- Python 3.10+
- [Azure Databricks Workspace](https://azure.microsoft.com/fr-fr/products/databricks/) (Premium recommandé pour Unity Catalog)
- [Terraform](https://www.terraform.io/) 1.5+ (optionnel, pour le déploiement infra)

### 1. Cloner le dépôt

```bash
git clone https://github.com/xamxamgraph/orange-money-pipeline.git
cd orange-money-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Explorer les données (mode mock — aucun credential requis)

```python
from datetime import datetime
from src.ingestion import OrangeMoneyClient

client = OrangeMoneyClient()  # Mock mode par défaut

# Récupérer 3 jours de transactions
for page in client.fetch_transactions(
    datetime(2025, 1, 15),
    datetime(2025, 1, 17),
):
    for tx in page.transactions:
        print(f"{tx.transaction_id}: {tx.amount:,.0f} XOF — {tx.transaction_type}")

# Sortie :
# OM20250115090130123456: 5,200 XOF — PAYMENT
# OM20250115091542789012: 1,000 XOF — TRANSFER
# ...
```

### 3. Exécuter les tests

```bash
pytest tests/ -v
# 15+ tests passed ✅
```

### 4. Déployer avec Terraform

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Éditer terraform.tfvars avec vos credentials Azure
terraform init
terraform plan
terraform apply
```

### 5. Exécuter le pipeline complet sur Databricks

Ouvrez les notebooks dans l'ordre :
1. `notebooks/01_orange_money_api_exploration.py` — Exploration des données
2. `notebooks/02_bronze_ingestion.py` — Ingestion Bronze
3. `notebooks/03_silver_transformation.py` — Normalisation Silver
4. `notebooks/04_gold_analytics.py` — Agrégats Gold + détection de fraude

---

## 🔒 Conformité CDP (Sénégal)

Ce pipeline est conçu pour la **Commission de Protection des Données Personnelles du Sénégal** :

| Article CDP | Implémentation |
|-------------|---------------|
| **Art. 44 — Pseudonymisation** | Tokenisation HMAC-SHA256 des numéros de téléphone à l'ingestion. Les données brutes ne contiennent jamais de PII en clair. |
| **Art. 62 — Durée de conservation** | `cdp_retention_days` configurable (défaut : 365 jours). Partitionnement par date pour une suppression efficace. |
| **Art. 50 — Consentement** | Métadonnées de consentement tracées dans le schéma Bronze. `cdp_consent_required = True` par défaut. |
| **Art. 33 — Sécurité du traitement** | Infrastructure Azure avec VNet injection, Unity Catalog RBAC, chiffrement au repos (ADLS Gen2). |

Pour une analyse complète de votre conformité CDP, [planifiez un diagnostic d'architecture](https://xamxamgraph.com).

---

## 📊 Tableau de Bord Exemple (Databricks SQL)

Une fois le pipeline exécuté, connectez Databricks SQL aux tables Gold :

```sql
-- Volume quotidien par région (7 derniers jours)
SELECT
    _date,
    region,
    SUM(transaction_count) AS txns,
    SUM(total_volume_xof) AS volume_xof,
    AVG(success_rate) AS success_pct
FROM orange_money.gold.daily_summary
WHERE _date >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY _date, region
ORDER BY _date DESC, volume_xof DESC;
```

---

## 🧩 Structure du Projet

```
orange-money-pipeline/
├── terraform/              # Infrastructure Azure Databricks
│   ├── main.tf             #   Workspace, ADLS Gen2, containers
│   ├── variables.tf        #   Configuration paramétrable
│   └── outputs.tf          #   URL workspace, clés stockage
├── src/
│   ├── ingestion/          # Client API Orange Money + config
│   │   ├── orange_money_client.py  # Client API avec mode mock
│   │   ├── config.py              # Configuration centralisée
│   │   └── __init__.py
│   ├── bronze/             # Couche Bronze — ingestion brute
│   │   └── raw_landing.py  #   Spark → Delta (append-only)
│   ├── silver/             # Couche Silver — normalisation
│   │   └── transaction_normalizer.py  # Dédup, qualité, enrichissement
│   └── gold/               # Couche Gold — agrégats métier
│       ├── daily_aggregates.py       # KPIs quotidiens
│       └── fraud_patterns.py         # Détection d'anomalies
├── notebooks/              # Notebooks Databricks (4 étapes)
│   ├── 01_orange_money_api_exploration.py
│   ├── 02_bronze_ingestion.py
│   ├── 03_silver_transformation.py
│   └── 04_gold_analytics.py
├── dbt/                    # Modèles dbt alternatifs
│   ├── dbt_project.yml
│   └── models/
│       ├── bronze/         # stg_orange_money__transactions
│       ├── silver/         # int_orange_money__normalized_transactions
│       └── gold/           # fct_orange_money__daily_summary
├── tests/                  # Suite de tests (pytest)
├── .github/workflows/      # CI/CD (lint, test, terraform, security)
└── pyproject.toml          # Dépendances et configuration
```

---

<a name="english-summary"></a>
## 🇬🇧 English Summary

**Orange Money Pipeline** is a production-grade Azure Databricks Medallion (Bronze → Silver → Gold) pipeline for Orange Money mobile payment transactions. Built by [XamXam Graph](https://xamxamgraph.com), a data engineering studio based in Dakar and Montréal.

### Key Features
- **Medallion Architecture** — append-only Bronze, quality-checked Silver, business-aggregate Gold
- **CDP-Compliant** — HMAC-based phone tokenization at ingestion edge (Senegal privacy law)
- **Mock-First Development** — realistic synthetic transaction generator (Senegalese patterns: peak hours, regional distribution, common amounts)
- **Infrastructure-as-Code** — Terraform for Azure Databricks workspaces, ADLS Gen2, Unity Catalog
- **Dual Transformation** — Python (PySpark) and/or dbt (SQL) for Silver/Gold layers
- **Fraud Detection** — velocity anomalies, amount spikes, rapid transfers, off-hours patterns
- **Full CI/CD** — linting, tests (3 Python versions), Terraform validation, security scanning

### Quick Start (Mock Mode — No API Credentials)

```bash
git clone https://github.com/xamxamgraph/orange-money-pipeline.git
cd orange-money-pipeline
pip install -e ".[dev]"
pytest tests/ -v
```

### Why This Matters

Orange Money processes billions of XOF monthly across 14M+ users in Senegal. Most banks and fintechs lack robust data infrastructure for this transaction flow. This repository provides a **reference architecture** that demonstrates how to build enterprise-grade mobile money data pipelines — with compliance, quality, and scale built in from day one.

---

## 📞 Vous avez un projet Orange Money, Wave, ou Free Money ?

XamXam Graph accompagne les banques, fintechs et grandes entreprises dans la mise en place d'architectures de données modernes sur Azure Databricks.

**Nos services :**
- 🔍 **Audit de pipeline** — Analyse de votre ingestion actuelle, score de conformité CDP, feuille de route
- 🏗️ **Medallion-as-Code** — Déploiement clé-en-main de votre infrastructure de données
- 🤖 **Agentic AI** — Agents intelligents pour le triage autonome de pipelines
- 🔒 **Gouvernance Unity Catalog** — Conformité CDP, Law 25 (Québec), politiques de rétention

<p align="center">
  <a href="https://xamxamgraph.com">
    <strong>🌐 Planifier un Diagnostic d'Architecture →</strong>
  </a>
</p>

<p align="center">
  <sub>Built with 🇸🇳 in Dakar & 🇨🇦 in Montréal · <a href="https://xamxamgraph.com">XamXam Graph</a> · <a href="mailto:engineering@xamxamgraph.com">engineering@xamxamgraph.com</a></sub>
</p>

---

## 📄 Licence

MIT © [XamXam Graph](https://xamxamgraph.com)
