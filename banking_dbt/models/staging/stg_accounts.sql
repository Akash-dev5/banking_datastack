WITH ranked AS (
    SELECT 
        v:id::NUMBER AS account_id,
        v:customer_id::NUMBER AS customer_id,
        v:account_type::VARCHAR AS account_type,
        v:balance::NUMBER(18,2) AS balance,
        v:currency::VARCHAR AS currency,
        v:created_at::TIMESTAMP_TZ AS created_at,
        CURRENT_TIMESTAMP AS load_timestamp,
        ROW_NUMBER() OVER (PARTITION BY v:id::NUMBER ORDER BY v:created_at::TIMESTAMP_TZ DESC) AS rn

    FROM {{ source('banking', 'accounts') }}
)

SELECT 
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    load_timestamp
FROM ranked
WHERE rn = 1 