WITH stg_Ag_wind_10m as (
SELECT 
  lon,
  lat,
  strftime("time",'%d/%m/%Y') AS datetime_id,
  ROUND(Wind_Speed_10m_Mean_24h, 2) AS Wind_Speed_10m_Mean_24h
FROM {{ source('Bronze', 'Ag_wind_10m') }}
)

SELECT
  {{dbt_utils.generate_surrogate_key(['lat', 'lon', 'datetime_id'])}} as key_id,
  {{dbt_utils.generate_surrogate_key(['lat', 'lon'])}} as coord_id,  
    *
FROM stg_Ag_wind_10m
