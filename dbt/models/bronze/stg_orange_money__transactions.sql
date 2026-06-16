-- ═══════════════════════════════════════════════════════════════════
-- Bronze: Raw Transaction View
-- ═══════════════════════════════════════════════════════════════════
-- Thin view over the raw Delta table ingested by the Python pipeline.
-- Adds no transformation — pure passthrough with audit column visibility.

{{ config(
    materialized='view',
    schema='bronze',
    tags=['bronze', 'raw', 'orange_money'],
) }}

SELECT
    transaction_id,
    external_id,
    transaction_type,
    amount,
    currency,
    fee,
    -- Phone numbers are tokenized at ingestion edge (CDP compliance)
    sender_phone    AS sender_token,
    recipient_phone AS recipient_token,
    merchant_code,
    merchant_name,
    merchant_category,
    status,
    failure_reason,
    initiated_at,
    completed_at,
    channel,
    agent_code,
    region,
    -- Audit columns
    _ingested_at,
    _source_file,
    _batch_id

FROM {{ source('orange_money', 'raw_transactions') }}
