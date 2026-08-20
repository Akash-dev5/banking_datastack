SELECT
    --Transaction
    f.transaction_id,
    f.transaction_time,
    f.transaction_type,
    f.status,
    f.amount,

    --Account
    f.account_id,
    a.account_type,
    a.currency,

    --Customer
    f.customer_id,
    c.first_name,
    c.last_name,
    c.email, 

    --Reporting fields
    CAST(f.transaction_time AS DATE) AS transaction_date,
    YEAR(f.transaction_time) AS transaction_year,
    MONTH(f.transaction_time) AS transaction_month,
    DAYOFWEEK(f.transaction_time) AS transaction_day_of_week,

    --Dervied business flag
    CASE WHEN f.transaction_type = 'TRANSFER' THEN TRUE
    ELSE FALSE END AS is_transfer

FROM {{ ref('fct_transactions') }} AS f

LEFT JOIN {{ ref('dim_accounts') }} AS a
    ON f.account_id = a.account_id
    AND a.is_current = TRUE

LEFT JOIN {{ ref('dim_customers') }} AS c
    ON f.customer_id = c.customer_id
    AND c.is_current = TRUE