WITH stg2 as (
SELECT 
  strftime(valid_time,'%d/%m/%Y') AS datetime_id,
  latitude,
  longitude,
  t2m - 273.15 as temperature, -- conversion en °C
  d2m - 273.15 as temperature_point_rosee, -- conversion en °C
  si10 as vitesse_vent,
  stl1 - 273.15 as temperature_sol_couche1, -- conversion en °C
  swvl1 as humidite_volumique_sol1,
  sst - 273.15 as temperature_surface_mer, -- conversion en °C
  sp / 100 as pression_surface, -- conversion en hPa
  msl / 100 as pression_niveau_mer, -- conversion en hPa
  expver as versionERA5
FROM {{ source('Bronze', 'General2') }}
WHERE number=0
)
SELECT 
  {{dbt_utils.generate_surrogate_key(['latitude', 'longitude', 'datetime_id'])}} as key_id,
  {{dbt_utils.generate_surrogate_key(['latitude', 'longitude'])}} as coord_id,
  *
  FROM stg2
