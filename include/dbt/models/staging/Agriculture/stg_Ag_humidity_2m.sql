WITH Ag_humidity_2m AS (
SELECT 
  lon,
  lat,
  strftime("time",'%d/%m/%Y') AS datetime_id,
  ROUND(Relative_Humidity_2m_12h / 100, 2) AS Relative_Humidity_12h,

FROM {{ source('Bronze', 'Ag_humidity_2m') }} 
)

SELECT
  {{dbt_utils.generate_surrogate_key(['lat', 'lon', 'datetime_id'])}} as key_id,
  {{dbt_utils.generate_surrogate_key(['lat', 'lon'])}} as coord_id,  
    *
FROM Ag_humidity_2m
