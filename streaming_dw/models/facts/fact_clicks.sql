SELECT
    c.session_id,
    c.customer_id,
    c.product_id,
    c.page,
    c.action,
    c.city,
    c.device,
    c.event_date,
    c.event_timestamp,
    c.event_hour
FROM {{ ref('stg_clicks') }} c