---
title: "Architecture Medallion pour Orange Money : Pipeline de Données sur Azure Databricks"
description: "Comment ingérer, normaliser et analyser des millions de transactions Orange Money avec une architecture Medallion (Bronze → Silver → Gold) conforme à la CDP du Sénégal."
date: 2025-06-16
author: "Ibrahim Faye"
authorTitle: "Founder & Principal Solution Architect, XamXam Graph"
tags:
  - databricks
  - medallion-architecture
  - orange-money
  - delta-lake
  - senegal
  - mobile-money
  - cdp-compliance
  - data-engineering
  - pyspark
  - dbt
featured: true
canonical: https://xamxamgraph.com/journal/architecture-medallion-orange-money-databricks
---

# Architecture Medallion pour Orange Money : Pipeline de Données sur Azure Databricks

**14 millions d'utilisateurs. Des milliards de FCFA par mois. Zéro architecture de données robuste chez la plupart des entreprises qui en dépendent.**

Voilà le paradoxe Orange Money au Sénégal.

Les banques, fintechs et entreprises traitent ces flux transactionnels avec des scripts Python ad-hoc, des exports CSV manuels, ou pire — pas du tout. Pendant ce temps, les données s'accumulent sans gouvernance, sans normalisation, sans conformité.

Cet article présente une **architecture de référence complète** — un pipeline Medallion sur Azure Databricks, conçu spécifiquement pour les transactions Orange Money, avec la conformité CDP intégrée dès l'ingestion.

---

## Le vrai problème n'est pas l'API

Orange Money fournit une API REST pour les partenaires. Obtenir les transactions n'est pas le défi. Le défi, c'est ce qui vient après :

1. **Idempotence** : La même transaction peut apparaître deux fois dans deux appels API successifs. Sans déduplication, vos agrégats sont faux.
2. **Qualité** : 2 à 5% des transactions ont des montants nuls, des statuts invalides, ou des timestamps manquants. Sans contrôle qualité, votre Gold Layer est contaminé.
3. **Conformité CDP** : Les numéros de téléphone sont des données personnelles au sens de l'Article 4 de la loi sénégalaise. Les stocker en clair dans un Data Lake, c'est une non-conformité immédiate.
4. **Volume** : 5000+ transactions/jour pour un commerçant moyen, 500 000+ pour une banque. Sans partitionnement, vos requêtes Gold mettent 30 secondes au lieu de 2.

---

## L'Architecture Medallion : Pourquoi trois couches ?

L'architecture Medallion (Bronze → Silver → Gold), popularisée par Databricks, n'est pas un buzzword. C'est une réponse directe aux quatre problèmes ci-dessus.

```
┌────────────────────────────────────────────────────────┐
│                 ORANGE MONEY API                        │
│            (ou Mock Generator pour le dev)              │
└───────────────────────┬────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────┐
│  🥉 BRONZE — raw_transactions                          │
│  • Append-only, jamais modifié                         │
│  • Données brutes + colonnes d'audit                   │
│  • Tokenisation PII à l'ingestion (CDP Art. 44)       │
│  • Format : Delta Lake, partitionné par date           │
└───────────────────────┬────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────┐
│  🥈 SILVER — normalized_transactions                   │
│  • Déduplication (ROW_NUMBER par transaction_id)       │
│  • 7 règles de qualité automatiques                    │
│  • Enrichissement : buckets, heures de pointe, canaux  │
└───────────────────────┬────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────┐
│  🥇 GOLD — daily_summary, merchant_activity,           │
│           fraud_patterns                                │
│  • KPIs prêts pour dashboards Databricks SQL / Power BI │
│  • Détection d'anomalies (vélocité, spikes, rapidité)  │
│  • Partitionné, aggregé, documenté                      │
└────────────────────────────────────────────────────────┘
```

**La règle d'or** : Bronze est immuable. Silver est fiable. Gold est actionnable.

---

## Bronze : L'ingestion qui protège les données personnelles

La couche Bronze reçoit les transactions brutes de l'API Orange Money. Mais avant d'écrire la moindre ligne dans Delta Lake, une étape critique se produit : **la tokenisation des numéros de téléphone**.

```python
# src/ingestion/orange_money_client.py — tokenisation CDP à l'ingestion

class PhoneTokenizer:
    """Tokenisation HMAC-SHA256 pour conformité CDP (Art. 44)."""

    def tokenize(self, phone: str) -> str:
        """Tokenise un numéro. Déterministe — même entrée → même token."""
        return hmac.new(
            self.secret, phone.encode(), hashlib.sha256
        ).hexdigest()[:16]

# Avant écriture dans Bronze
if self._tokenizer:
    tx.sender_phone = self._tokenizer.tokenize(tx.sender_phone)
    tx.recipient_phone = self._tokenizer.tokenize(tx.recipient_phone)
```

**Pourquoi c'est important** : La CDP sénégalaise (Article 44) exige la pseudonymisation des données personnelles. En tokenisant à l'ingestion — avant que les données n'atteignent le stockage — vous éliminez le risque de PII en clair dans votre Data Lake. Même un accès non autorisé à la table Bronze ne révèle aucun numéro de téléphone réel.

La table Bronze ajoute également des colonnes d'audit automatiques :

| Colonne | Rôle |
|---|---|
| `_ingested_at` | Horodatage d'ingestion (UTC) |
| `_source_file` | Fichier source ou endpoint API |
| `_batch_id` | Identifiant de lot pour la traçabilité |

---

## Silver : La normalisation qui fait la différence

C'est ici que la plupart des projets échouent. On passe de Bronze à Gold directement — et on obtient des dashboards qui affichent des totaux inexacts à cause de doublons et de données corrompues.

La couche Silver applique **trois transformations en séquence** :

### 1. Déduplication

```python
# Déduplication par transaction_id — garde la version la plus récente
window = Window.partitionBy("transaction_id") \
    .orderBy(F.col("_ingested_at").desc())

deduped = bronze \
    .withColumn("_row_num", F.row_number().over(window)) \
    .filter(F.col("_row_num") == 1) \
    .drop("_row_num")
```

Simple, déterministe, efficace. Une transaction Orange Money qui apparaît dans deux appels API successifs ne sera comptée qu'une fois.

### 2. Contrôles qualité

```python
QUALITY_RULES = {
    "amount_positive": F.col("amount") > 0,
    "amount_reasonable": F.col("amount") < 50_000_000,  # 50M FCFA
    "valid_status": F.col("status").isin(
        "SUCCESS", "PENDING", "FAILED", "REVERSED"
    ),
    "has_sender": F.col("sender_phone").isNotNull(),
    "fee_non_negative": F.col("fee") >= 0,
}
```

Chaque règle génère une colonne booléenne `_quality_{rule}`. Une colonne composite `_quality_flag` (`PASS` / `FAIL`) est calculée. Les lignes qui échouent ne sont **pas rejetées** — elles sont conservées avec le flag `FAIL`, ce qui permet un audit sans perte de données.

### 3. Enrichissement

La couche Silver ajoute des dimensions dérivées qui seront utilisées par les agrégats Gold :

| Colonne enrichie | Exemple | Utilité |
|---|---|---|
| `_date` | `2025-01-15` | Partitionnement |
| `_hour` | `14` | Analyse horaire |
| `_is_peak_hour` | `True` | Heures de pointe (9h-12h, 15h-18h) |
| `_is_weekend` | `False` | Patterns week-end vs semaine |
| `_amount_bucket` | `MEDIUM` | Segmentation (MICRO → XLARGE) |
| `_is_merchant_txn` | `True` | Transaction commerçant vs P2P |
| `_processing_latency_seconds` | `3` | Délai initiation → complétion |

---

## Gold : Les agrégats qui parlent aux décideurs

La couche Gold transforme les données normalisées en **KPIs métier directement exploitables** par des dashboards Databricks SQL, Power BI ou Tableau.

### daily_summary — Le pouls quotidien

```sql
-- Volume par région et canal (7 derniers jours)
SELECT
    _date,
    region,
    SUM(transaction_count)     AS total_txns,
    SUM(total_volume_xof)      AS volume_fcfa,
    ROUND(AVG(success_rate), 1) AS success_pct
FROM orange_money.gold.daily_summary
WHERE _date >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY _date, region
ORDER BY _date DESC, volume_fcfa DESC;
```

Résultat typique pour un marchand moyen :

| _date | region | total_txns | volume_fcfa | success_pct |
|-------|--------|------------|-------------|-------------|
| 2025-01-21 | Dakar | 3,842 | 52,340,500 | 98.2 |
| 2025-01-21 | Thiès | 1,204 | 14,230,000 | 97.8 |
| 2025-01-21 | Diourbel | 487 | 5,120,000 | 96.5 |

### Détection de fraude

La couche Gold exécute également **quatre patterns de détection d'anomalies** à chaque exécution :

| Pattern | Règle | Exemple |
|---|---|---|
| `VELOCITY_ANOMALY` | > 20 transactions/jour par émetteur | Compte émet 45 transferts en une journée |
| `AMOUNT_SPIKE` | Transaction > 5× la moyenne mobile 7 jours | Transaction de 500 000 FCFA alors que la moyenne est de 45 000 |
| `RAPID_TRANSFER` | Transferts multiples au même destinataire < 10 min | 3 transferts de 50 000 FCFA en 4 minutes |
| `OFF_HOURS_LARGE` | Transaction > 100 000 FCFA hors heures de pointe | Paiement de 200 000 FCFA à 3h du matin |

Les transactions flaggées sont stockées dans une table dédiée avec le pattern détecté et les détails contextuels (montant, écart, intervalle). Elles peuvent alimenter un système d'alerte temps réel ou une file de révision manuelle.

---

## Infrastructure-as-Code : Terraform pour la reproductibilité

Le pipeline est livré avec des modules Terraform qui provisionnent l'infrastructure complète :

```hcl
# Azure Databricks Workspace (Premium — requis pour Unity Catalog)
resource "azurerm_databricks_workspace" "this" {
  name                = "orange-money-databricks"
  resource_group_name = azurerm_resource_group.this.name
  location            = var.location
  sku                 = "premium"
}

# ADLS Gen2 avec trois containers — un par couche Medallion
resource "azurerm_storage_container" "bronze" { ... }
resource "azurerm_storage_container" "silver" { ... }
resource "azurerm_storage_container" "gold"  { ... }
```

**Pourquoi Terraform** : Parce qu'un pipeline qui fonctionne sur le poste du consultant mais pas en production n'a aucune valeur. L'infrastructure se déploie en une commande :

```bash
terraform init && terraform apply
```

---

## Mode Mock : Développer sans credentials API

Un pipeline de données transactionnelles a un problème structurel : vous ne pouvez pas développer et tester sans credentials de production — ce qui bloque les équipes, ralentit les itérations, et rend les contributions open-source impossibles.

Notre solution : un **générateur de données synthétiques** qui modélise les patterns réels du marché sénégalais.

```python
from src.ingestion import OrangeMoneyClient

# Aucun credential requis — mode mock par défaut
client = OrangeMoneyClient()

for page in client.fetch_transactions(
    datetime(2025, 1, 15),
    datetime(2025, 1, 17)
):
    for tx in page.transactions:
        print(f"{tx.transaction_id}: {tx.amount:,.0f} FCFA — {tx.region}")
```

Le générateur respecte :
- **Distribution régionale** : Dakar ~45%, Thiès ~15%, autres régions proportionnelles
- **Heures de pointe** : 9h-12h et 15h-18h GMT
- **Canaux** : USSD 65%, APP 25%, WEB 5%, AGENT 5%
- **Montants** : Clusters autour de 500, 1 000, 2 000, 5 000, 10 000 FCFA

---

## Ce que cette architecture apporte concrètement

| Avant | Après |
|---|---|
| Scripts Python ad-hoc par employé | Pipeline unique, versionné, testé |
| Numéros de téléphone en clair dans les logs | Tokenisation HMAC-SHA256 à l'ingestion |
| Doublons non détectés → KPIs erronés | Déduplication déterministe dans Silver |
| 2-5% de données corrompues dans les rapports | Quality flag sur chaque ligne, audit trail complet |
| Dashboard = 30 secondes de chargement | Partitionnement → 2 secondes |
| Pas de détection de fraude | 4 patterns d'anomalies automatiques |
| Déploiement manuel, non reproductible | `terraform apply` → infrastructure complète |

---

## Code source et prochaines étapes

L'intégralité du code est open-source (MIT) :

🔗 [**github.com/ibfaye/orange-money-pipeline**](https://github.com/ibfaye/orange-money-pipeline)

Le dépôt contient :
- Le pipeline Python complet (Bronze → Silver → Gold)
- Les modèles dbt alternatifs (SQL)
- Les modules Terraform pour le déploiement
- Quatre notebooks Databricks exécutables
- Une suite de tests complète
- Une CI/CD GitHub Actions

---

## Vous avez un projet Orange Money, Wave, ou Free Money ?

La plupart des architectures de données pour le mobile money en Afrique de l'Ouest sont construites avec des scripts fragiles et une conformité inexistante. Ça ne devrait pas être le cas.

**XamXam Graph** intervient à deux niveaux :

- **Audit de pipeline** — On analyse votre ingestion actuelle, on score votre conformité CDP, on vous donne une feuille de route chiffrée.
- **Medallion-as-Code** — On déploie votre infrastructure de données clé-en-main, avec gouvernance Unity Catalog et conformité intégrée.

<p align="center">
  <a href="https://xamxamgraph.com">
    <strong>🌐 Planifier un Diagnostic d'Architecture →</strong>
  </a>
</p>

---

*Ibrahim Faye est le fondateur de XamXam Graph, un studio d'architecture de données basé à Dakar et Montréal, spécialisé dans les pipelines Medallion sur Azure Databricks et l'IA agentique pour l'infrastructure de données en Afrique de l'Ouest.*
