CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INT PRIMARY KEY,
    date          DATE,
    year          INT,
    month         INT,
    day           INT,
    quarter       INT,
    day_of_week   VARCHAR(10)
);
