SELECT DISTINCT
    customer_id,
    customer_name,
    city,
    -- SCD Type 2 fields
    MIN(event_date) OVER (PARTITION BY customer_id) AS first_seen_date,
    MAX(event_date) OVER (PARTITION BY customer_id) AS last_seen_date,
    TRUE                                             AS is_current
FROM {{ ref('stg_orders') }}