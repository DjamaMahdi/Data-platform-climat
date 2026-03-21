-- int_GeneralEra5.sql
SELECT
    t1.key_id,
    t1.datetime_id,
    dt.datetime,
    dt.year,
    dt.month_year,
    t1.latitude,
    t1.longitude,
    mr.Region,
    t1.total_precipitation,
    t1.rayonnement_solaire_surface,
    t1.evaporation_reelle,
    t1.evaporation_potentielle,
    t1.ruissellement,
    t1.versionERA5,
    t1.coord_id,
    t2.temperature,
    t2.temperature_point_rosee,
    t2.vitesse_vent,
    t2.temperature_sol_couche1,
    t2.humidite_volumique_sol1,
    t3.direction_moyenne_vagues,
    t3.periode_moyenne_vagues,
FROM {{ ref('stg_General_1') }} t1

FULL JOIN {{ ref('stg_General_2') }} t2 ON t1.key_id = t2.key_id
FULL JOIN {{ ref('stg_General_3') }} t3 ON t1.key_id = t3.key_id
INNER JOIN {{ ref('int_datetime') }} dt ON t1.datetime_id = dt.datetime_id
INNER JOIN {{ ref('int_mappingRegion') }} mr ON t1.coord_id = mr.coord_id