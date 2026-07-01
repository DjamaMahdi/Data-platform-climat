-- dim_vagues.sql
SELECT DISTINCT
    key_id,
    datetime_id as Date,
    latitude,
    longitude,
    direction_moyenne_vagues,
    periode_moyenne_vagues,
    versionERA5
FROM {{ ref('stg_General_3') }}
WHERE direction_moyenne_vagues  IS NOT NULL
AND periode_moyenne_vagues IS NOT NULL
ORDER BY datetime_id

