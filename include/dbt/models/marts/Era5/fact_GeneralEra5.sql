-- fact_GeneralEra5.sql

SELECT
    datetime,
    year,
    month_year,
    latitude,
    longitude,
    Region,
    total_precipitation,
    rayonnement_solaire_surface,
    evaporation_reelle,
    evaporation_potentielle,
    ruissellement,
    versionERA5,
    temperature,
    temperature_point_rosee,
    vitesse_vent,
    temperature_sol_couche1,
    humidite_volumique_sol1,
    direction_moyenne_vagues,
    periode_moyenne_vagues,
FROM {{ ref('int_GeneralEra5') }} 