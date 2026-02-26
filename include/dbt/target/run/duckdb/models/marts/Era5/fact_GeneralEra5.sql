
  
    
    

    create  table
      "Climate_Era5"."main"."fact_GeneralEra5__dbt_tmp"
  
    as (
      -- fact_general.sql

WITH stg_General_1 AS (
    SELECT
        key_id,
        datetime_id,
        latitude,
        longitude,
        total_precipitation,
        rayonnement_solaire_surface,
        evaporation_reelle,
        evaporation_potentielle,
        ruissellement,
        VersionERA5
    FROM "Climate_Era5"."main"."stg_General_1"
)

SELECT
t1.*,
t2.*
FROM stg_General_1 AS t1
LEFT JOIN "Climate_Era5"."main"."stg_General_2" AS t2 ON t1.key_id = t2.key_id
    );
  
  