MERGE INTO dim_date AS target
USING (
    SELECT
        $1:date_key::INT      AS date_key,
        $1:date::DATE         AS date,
        $1:year::INT          AS year,
        $1:month::INT         AS month,
        $1:day::INT           AS day,
        $1:quarter::INT       AS quarter,
        $1:day_of_week::VARCHAR AS day_of_week
    FROM @processed_zone_stage/dim_date/
) AS source
ON target.date_key = source.date_key
WHEN NOT MATCHED THEN INSERT (date_key, date, year, month, day, quarter, day_of_week)
    VALUES (source.date_key, source.date, source.year, source.month, source.day,
            source.quarter, source.day_of_week);
-- date dimension rows never change once written, so no UPDATE branch needed here
