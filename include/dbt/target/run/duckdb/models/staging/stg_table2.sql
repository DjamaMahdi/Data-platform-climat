
  
    
    

    create  table
      "Climate_Era5"."main"."stg_table2__dbt_tmp"
  
    as (
      SELECT 
valid_time AS datetime_id,
latitude,
longitude,
t2m as temperature,
d2m as temperature_point_rosee,
si10 as vitesse_vent,
stl1 as temperature_sol_couche1,
swvl1 as humidite_volumique_sol1
FROM "Climate_Era5"."Bronze"."Table2"
WHERE number=0
    );
  
  