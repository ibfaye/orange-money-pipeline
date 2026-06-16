-- ═══════════════════════════════════════════════════════════════════
-- Silver: Normalized Transactions
-- ═══════════════════════════════════════════════════════════════════
-- Deduplicated, quality-flagged, enriched transaction table.
-- This is a pure-SQL alternative to the Python SilverNormalizer.

{{ config(
    materialized='table',
    schema='silver',
    partition_by='_date',
    file_format='delta',
    tags=['silver', 'normalized', 'orange_money'],
) }}

WITH

-- 1. Read from Bronze staging
bronze AS (
    SELECT * FROM {{ ref('stg_orange_money__transactions') }}
),

-- 2. Deduplicate by transaction_id (keep latest _ingested_at)
deduped AS (
    SELECT *
    FROM bronze
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY transaction_id
        ORDER BY _ingested_at DESC
    ) = 1
),

-- 3. Apply data quality checks and enrich
normalized AS (
    SELECT
        *,
        -- Quality checks
        CASE
            WHEN amount > 0
             AND status IN ('SUCCESS', 'PENDING', 'FAILED', 'REVERSED')
             AND initiated_at IS NOT NULL
            THEN 'PASS'
            ELSE 'FAIL'
        END AS _quality_flag,

        CASE WHEN amount > 0 THEN TRUE ELSE FALSE END AS _quality_amount_positive,
        CASE WHEN amount < 50000000 THEN TRUE ELSE FALSE END AS _quality_amount_reasonable,
        CASE WHEN status IN ('SUCCESS', 'PENDING', 'FAILED', 'REVERSED') THEN TRUE ELSE FALSE END AS _quality_valid_status,
        CASE WHEN initiated_at IS NOT NULL THEN TRUE ELSE FALSE END AS _quality_has_initiated_at,

        -- Date dimensions
        CAST(initiated_at AS DATE) AS _date,
        HOUR(initiated_at) AS _hour,
        DAYOFWEEK(initiated_at) AS _day_of_week,

        -- Derived flags
        CASE WHEN DAYOFWEEK(initiated_at) IN (1, 7) THEN TRUE ELSE FALSE END AS _is_weekend,
        CASE WHEN HOUR(initiated_at) BETWEEN 9 AND 12
              OR HOUR(initiated_at) BETWEEN 15 AND 18 THEN TRUE ELSE FALSE END AS _is_peak_hour,
        CASE WHEN merchant_code IS NOT NULL THEN TRUE ELSE FALSE END AS _is_merchant_txn,

        -- Amount bucket
        CASE
            WHEN amount < 1000 THEN 'MICRO'
            WHEN amount < 5000 THEN 'SMALL'
            WHEN amount < 20000 THEN 'MEDIUM'
            WHEN amount < 100000 THEN 'LARGE'
            ELSE 'XLARGE'
        END AS _amount_bucket,

        -- Processing latency
        UNIX_TIMESTAMP(completed_at) - UNIX_TIMESTAMP(initiated_at) AS _processing_latency_seconds

    FROM deduped
)

SELECT * FROM normalized
