SELECT 
  valid_time AS datetime_id,
  {{dbt_utils.generate_surrogate_key(['latitude', 'longitude', 'valid_time'])}} as key_id,
  latitude,
  longitude,
  tp as total_precipitation,
  ssrd as rayonnement_solaire_surface,
  e as evaporation_reelle,
  pev as evaporation_potentielle,
  ro as ruissellement,
  expver as versionERA5
FROM {{ source('Bronze', 'General1') }} 
WHERE number=0