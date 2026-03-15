WITH RECURSIVE date_spine AS (
    SELECT '2023-01-01'::DATE AS date_day
    UNION ALL
    SELECT DATEADD(DAY, 1, date_day)
    FROM date_spine
    WHERE date_day < '2025-12-31'::DATE
)
SELECT
    date_day                                                          AS date_id,
    DAY(date_day)                                                     AS day_of_month,
    MONTH(date_day)                                                   AS month_number,
    MONTHNAME(date_day)                                               AS month_name,
    QUARTER(date_day)                                                 AS quarter,
    YEAR(date_day)                                                    AS year,
    DAYNAME(date_day)                                                 AS day_name,
    CASE WHEN DAYOFWEEK(date_day) IN (1,7) THEN 1 ELSE 0 END         AS is_weekend
FROM date_spine