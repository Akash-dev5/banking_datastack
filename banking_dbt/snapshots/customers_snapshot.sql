{% snapshot customers_snapshot %}

{{ config(
    unique_key='customer_id',
    strategy='check',
    check_cols=['first_name', 'last_name', 'email']
) }}

SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    created_at

FROM {{ ref('stg_customers') }}
{% endsnapshot %}