{{config( materialized='incremental', unique_key='transaction_id')}}

WITH enriched_transactions AS (
    SELECT
        t.transaction_id,
        t.account_id,
        a.customer_id,
        t.amount,
        t.related_account_id,
        t.status,
        t.transaction_type,
        t.transaction_time,
        CURRENT_TIMESTAMP() AS load_timestamp

    FROM {{ref('stg_transactions')}} AS t

    LEFT JOIN {{ref('dim_accounts')}} AS a
        ON t.account_id = a.account_id
        AND a.is_current = TRUE)


SELECT * 
FROM enriched_transactions