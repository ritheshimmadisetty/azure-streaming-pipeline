SELECT DISTINCT
    product_id,
    product_name,
    category,
    AVG(unit_price) OVER (PARTITION BY product_id) AS avg_unit_price
FROM {{ ref('stg_orders') }}