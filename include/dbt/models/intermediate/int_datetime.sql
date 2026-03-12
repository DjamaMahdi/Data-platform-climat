-- int_datetime.sql

-- Create a CTE to extract date and time components
WITH datetime_cte AS (
    SELECT DISTINCT
        datetime_id,
        strptime(datetime_id,'%d/%m/%Y') as date_part,

FROM {{ ref('stg_General_1') }} 
)

SELECT
  datetime_id,
  date_part as datetime,
  EXTRACT(YEAR FROM date_part) AS year,
  EXTRACT(MONTH FROM date_part) AS month,
  strftime('%m/%Y', date_part) AS month_year,
  EXTRACT(DAY FROM date_part) AS day,
  EXTRACT(DAYOFWEEK FROM date_part) AS weekday
FROM datetime_cte
ORDER BY date_part