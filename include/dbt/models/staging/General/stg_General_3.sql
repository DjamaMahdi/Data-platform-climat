WITH stg3 as (
SELECT 
  strftime(valid_time,'%d/%m/%Y') AS datetime_id,
  latitude,
  longitude,
  mwd  as direction_moyenne_vagues,
  mwp  as periode_moyenne_vagues, 
  expver as versionERA5
FROM {{ source('Bronze', 'General3') }}
WHERE number=0
)
SELECT 
  {{dbt_utils.generate_surrogate_key(['latitude', 'longitude', 'datetime_id'])}} as key_id,
  {{dbt_utils.generate_surrogate_key(['latitude', 'longitude'])}} as coord_id,
  *
  FROM stg3