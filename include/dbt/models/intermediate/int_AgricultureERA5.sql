SELECT
  t1.key_id,
  t1.coord_id,
  t1.datetime_id,
  dt.datetime,
  dt.year,
  dt.month_year,
  mr.Region,
  t1.lat,
  t1.lon,
  t1.Temp_Max_24h,
  t1.Temp_Mean_24h,
  t1.Temp_Min_24h,
  t2.Wind_Speed_10m_Mean_24h,
  t3.Reference_Evapotranspiration,
  t4.Precipitation_Flux,
  t5.Relative_Humidity_12h
FROM {{ ref('stg_Ag_temperature_2m') }} t1


INNER JOIN {{ ref('stg_Ag_wind_10m') }} t2 ON t1.key_id = t2.key_id
INNER JOIN {{ ref('stg_Ag_reference_et') }} t3 ON t1.key_id = t3.key_id
INNER JOIN {{ ref('stg_Ag_precip_flux') }} t4 ON t1.key_id = t4.key_id
INNER JOIN {{ ref('stg_Ag_humidity_2m') }} t5 ON t1.key_id = t5.key_id
INNER JOIN {{ ref('int_datetime') }} dt ON t1.datetime_id = dt.datetime_id
INNER JOIN {{ ref('int_Ag_mappingRegion') }} mr ON t1.coord_id = mr.coord_id