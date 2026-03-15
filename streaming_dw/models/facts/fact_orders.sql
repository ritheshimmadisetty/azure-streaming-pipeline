SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.event_date                                AS order_date,
    o.event_timestamp                           AS order_timestamp,
    o.event_hour,
    o.event_year,
    o.event_month,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.status,
    o.platform,
    o.city
FROM {{ ref('stg_orders') }} o