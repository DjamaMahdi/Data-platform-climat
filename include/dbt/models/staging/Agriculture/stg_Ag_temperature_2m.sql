WITH stg_Ag_temperature_2m AS (
SELECT 
  lon,
  lat,
  strftime("time",'%d/%m/%Y') AS datetime_id,
  ROUND(Temperature_Air_2m_Max_24h - 273.15, 2) AS Temp_Max_24h,
  ROUND(Temperature_Air_2m_Mean_24h - 273.15, 2) AS Temp_Mean_24h,
  ROUND(Temperature_Air_2m_Min_24h - 273.15, 2) AS Temp_Min_24h,
FROM {{ source('Bronze', 'Ag_temperature_2m') }} 
)

SELECT
  {{dbt_utils.generate_surrogate_key(['lat', 'lon', 'datetime_id'])}} as key_id,
  {{dbt_utils.generate_surrogate_key(['lat', 'lon'])}} as coord_id,  
    *
FROM stg_Ag_temperature_2m

