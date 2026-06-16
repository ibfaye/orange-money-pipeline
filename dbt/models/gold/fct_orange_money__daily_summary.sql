-- ═══════════════════════════════════════════════════════════════════
-- Gold: Daily Summary
-- ═══════════════════════════════════════════════════════════════════

{{ config(
    materialized='table',
    schema='gold',
    partition_by='_date',
    file_format='delta',
    tags=['gold', 'aggregated', 'dashboard', 'orange_money'],
) }}

WITH normalized AS (
    SELECT *
    FROM {{ ref('int_orange_money__normalized_transactions') }}
    WHERE _quality_flag = 'PASS'
)

SELECT
    _date,
    region,
    channel,
    _is_weekend,
    _is_peak_hour,

    -- Volume metrics
    COUNT(*)                                    AS transaction_count,
    SUM(amount)                                 AS total_volume_xof,
    ROUND(AVG(amount), 2)                       AS avg_transaction_xof,
    PERCENTILE(amount, 0.5)                     AS median_transaction_xof,
    SUM(fee)                                    AS total_fees_xof,

    -- Success metrics
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END)   AS failed_count,
    COUNT(CASE WHEN status = 'REVERSED' THEN 1 END) AS reversed_count,
    ROUND(
        SAFE_DIVIDE(
            COUNT(*) - COUNT(CASE WHEN status = 'FAILED' THEN 1 END),
            COUNT(*)
        ) * 100, 2
    )                                           AS success_rate,

    -- Merchant metrics
    COUNT(CASE WHEN _is_merchant_txn THEN 1 END)    AS merchant_txn_count,
    COUNT(DISTINCT merchant_code)                   AS unique_merchants,
    SUM(CASE WHEN _is_merchant_txn THEN amount END) AS merchant_volume_xof,
    ROUND(
        SAFE_DIVIDE(
            COUNT(CASE WHEN _is_merchant_txn THEN 1 END),
            COUNT(*)
        ) * 100, 2
    )                                           AS merchant_share_pct,

    -- Performance
    ROUND(AVG(_processing_latency_seconds), 1)  AS avg_latency_seconds,

    -- Metadata
    CURRENT_TIMESTAMP()                         AS _updated_at

FROM normalized
GROUP BY
    _date,
    region,
    channel,
    _is_weekend,
    _is_peak_hour
