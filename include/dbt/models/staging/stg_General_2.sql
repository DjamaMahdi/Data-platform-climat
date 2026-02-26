SELECT 
  valid_time AS datetime_id,
  {{dbt_utils.generate_surrogate_key(['latitude', 'longitude', 'valid_time'])}} as key_id,
  latitude,
  longitude,
  t2m as temperature,
  d2m as temperature_point_rosee,
  si10 as vitesse_vent,
  stl1 as temperature_sol_couche1,
  swvl1 as humidite_volumique_sol1,
  expver as versionERA5
FROM {{ source('Bronze', 'General2') }}
WHERE number=0