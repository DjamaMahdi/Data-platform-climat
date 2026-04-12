WITH  stg1 as (
SELECT 
  strftime(valid_time,'%d/%m/%Y') AS datetime_id,
  latitude,
  longitude,
  valid_time,
  tp * 1000 as total_precipitation, -- conversion en mm
  ssrd / 86400 as rayonnement_solaire_surface, -- conversion en W/m²
  e * -1000 as evaporation_reelle, -- conversion en mm
  pev * -1000 as evaporation_potentielle, -- conversion en mm
  ro * 1000 as ruissellement, -- conversion en mm
  expver as versionERA5
FROM {{ source('Bronze', 'General1') }} 
WHERE number=0
)
SELECT 
  {{dbt_utils.generate_surrogate_key(['latitude', 'longitude', 'datetime_id'])}} as key_id,
  {{dbt_utils.generate_surrogate_key(['latitude', 'longitude'])}} as coord_id,  
  *
FROM stg1