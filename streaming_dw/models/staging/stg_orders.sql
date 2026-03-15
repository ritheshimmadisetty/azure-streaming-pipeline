SELECT
    order_id,
    customer_id,
    TRIM(customer_name)                         AS customer_name,
    TRIM(UPPER(city))                           AS city,
    product_id,
    TRIM(product_name)                          AS product_name,
    TRIM(category)                              AS category,
    unit_price,
    quantity,
    ROUND(total_amount, 2)                      AS total_amount,
    TRIM(LOWER(status))                         AS status,
    TRIM(LOWER(platform))                       AS platform,
    event_timestamp,
    event_date,
    event_hour,
    event_year,
    event_month
FROM {{ source('streaming', 'RAW_ORDERS') }}
WHERE order_id      IS NOT NULL
  AND customer_id   IS NOT NULL
  AND total_amount  > 0