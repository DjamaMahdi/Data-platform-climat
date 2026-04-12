SELECT
  datetime,
  year,
  month_year,
  lat as latitude,
  lon as longitude,
  Region,
  Temp_Max_24h,
  Temp_Mean_24h,
  Temp_Min_24h,
  Wind_Speed_10m_Mean_24h,
  Reference_Evapotranspiration,
  Precipitation_Flux,
  Relative_Humidity_12h
FROM {{ ref('int_AgricultureERA5') }}
ORDER BY datetime