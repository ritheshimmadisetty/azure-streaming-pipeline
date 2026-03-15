SELECT
    session_id,
    customer_id,
    TRIM(LOWER(page))    AS page,
    product_id,
    TRIM(LOWER(action))  AS action,
    TRIM(UPPER(city))    AS city,
    TRIM(LOWER(device))  AS device,
    event_timestamp,
    event_date,
    event_hour
FROM {{ source('streaming', 'RAW_CLICKS') }}
WHERE session_id   IS NOT NULL
  AND customer_id  IS NOT NULL