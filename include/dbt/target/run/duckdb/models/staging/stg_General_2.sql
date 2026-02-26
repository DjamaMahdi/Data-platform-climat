
  
  create view "Climate_Era5"."main"."stg_General_2__dbt_tmp" as (
    SELECT 
  valid_time AS datetime_id,
  md5(cast(coalesce(cast(latitude as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(longitude as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(valid_time as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as key_id,
  latitude,
  longitude,
  t2m as temperature,
  d2m as temperature_point_rosee,
  si10 as vitesse_vent,
  stl1 as temperature_sol_couche1,
  swvl1 as humidite_volumique_sol1,
  expver as versionERA5
FROM "Climate_Era5"."Bronze"."General2"
WHERE number=0
  );
