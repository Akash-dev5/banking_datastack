{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        v:id::NUMBER AS transaction_id,
        v:account_id::NUMBER AS account_id,
        v:amount::NUMBER(18,2) AS amount,
        v:txn_type::VARCHAR AS transaction_type,
        v:related_account_id::NUMBER AS related_account_id,
        v:status::VARCHAR AS status,
        v:created_at::TIMESTAMP_TZ AS transaction_time,
        CURRENT_TIMESTAMP AS load_timestamp,

        ROW_NUMBER() OVER (PARTITION BY v:id::NUMBER ORDER BY v:created_at::TIMESTAMP_TZ DESC) AS rn

    FROM {{ source('banking', 'transactions') }}
)

SELECT
    transaction_id,
    account_id,
    amount,
    transaction_type,
    related_account_id,
    status,
    transaction_time,
    load_timestamp
FROM ranked
WHERE rn = 1