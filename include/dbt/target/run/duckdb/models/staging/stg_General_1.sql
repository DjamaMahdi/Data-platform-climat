
  
  create view "Climate_Era5"."main"."stg_General_1__dbt_tmp" as (
    SELECT 
  valid_time AS datetime_id,
  md5(cast(coalesce(cast(latitude as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(longitude as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(valid_time as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as key_id,
  latitude,
  longitude,
  tp as total_precipitation,
  ssrd as rayonnement_solaire_surface,
  e as evaporation_reelle,
  pev as evaporation_potentielle,
  ro as ruissellement,
  expver as versionERA5
FROM "Climate_Era5"."Bronze"."General1" 
WHERE number=0
  );
