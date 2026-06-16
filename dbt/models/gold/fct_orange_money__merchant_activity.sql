-- ═══════════════════════════════════════════════════════════════════
-- Gold: Merchant Activity
-- ═══════════════════════════════════════════════════════════════════

{{ config(
    materialized='table',
    schema='gold',
    partition_by='_date',
    file_format='delta',
    tags=['gold', 'aggregated', 'merchant', 'orange_money'],
) }}

WITH normalized AS (
    SELECT *
    FROM {{ ref('int_orange_money__normalized_transactions') }}
    WHERE _is_merchant_txn
      AND _quality_flag = 'PASS'
)

SELECT
    merchant_code,
    merchant_name,
    merchant_category,
    _date,

    COUNT(*)                                    AS transaction_count,
    SUM(amount)                                 AS total_volume_xof,
    ROUND(AVG(amount), 2)                       AS avg_transaction_xof,
    SUM(fee)                                    AS total_fees_xof,
    COUNT(DISTINCT sender_token)                AS unique_customers,

    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count,

    ROUND(
        SAFE_DIVIDE(
            COUNT(*) - COUNT(CASE WHEN status = 'FAILED' THEN 1 END),
            COUNT(*)
        ) * 100, 2
    )                                           AS success_rate,

    ROUND(
        SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT sender_token)), 2
    )                                           AS avg_txn_per_customer,

    CURRENT_TIMESTAMP()                         AS _updated_at

FROM normalized
GROUP BY
    merchant_code,
    merchant_name,
    merchant_category,
    _date
