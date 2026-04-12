-- int_datetime.sql

-- Create a CTE to extract date and time components
WITH datetime_cte AS (
    {{ dbt_utils.date_spine(
          datepart="day",
          start_date="cast('1940-01-01' as date)",
          end_date="current_date()"
        )
    }}
)

SELECT
  strftime(date_day, '%d/%m/%Y') as datetime_id,
  date_day as datetime,
  EXTRACT(YEAR FROM date_day) AS year,
  EXTRACT(MONTH FROM date_day) AS month,
  strftime('%m/%Y', date_day) AS month_year,
  EXTRACT(DAY FROM date_day) AS day,
  EXTRACT(DAYOFWEEK FROM date_day) AS weekday
FROM datetime_cte
ORDER BY date_day