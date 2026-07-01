SELECT
  datetime as "Date",
  year as "Années",
  month_year as "Mois_Années",
  lat as latitude,
  lon as longitude,
  Region,
  Temp_Max_24h,
  Temp_Mean_24h as Temp_Moyenne_24h,
  Temp_Min_24h,
  Wind_Speed_10m_Mean_24h as Vitesse_vent_10m_Moyenne,
  Reference_Evapotranspiration,
  Precipitation_Flux as Precipitation_Flux,
  Relative_Humidity_12h as "Humidité_Relative"
FROM {{ ref('int_AgricultureERA5') }}
ORDER BY datetime