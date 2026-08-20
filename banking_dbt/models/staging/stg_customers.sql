WITH ranked AS (
    SELECT 
        v:id::NUMBER AS customer_id,
        v:first_name::VARCHAR AS first_name,
        v:last_name::VARCHAR AS last_name,
        v:email::VARCHAR AS email,
        v:created_at::TIMESTAMP_TZ AS created_at,
        CURRENT_TIMESTAMP AS load_timestamp,
        ROW_NUMBER() OVER (PARTITION BY v:id::NUMBER ORDER BY v:created_at::TIMESTAMP_TZ DESC) AS rn

    FROM {{ source('banking', 'customers') }}
)

SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    load_timestamp
FROM ranked
WHERE rn = 1