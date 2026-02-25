SELECT 
  valid_time AS datetime_id,
  latitude, 
  longitude,
  tp as total_precipitation,
  ssrd as rayonnement_solaire_surface,
  e as evaporation_reelle,
  pev as evaporation_potentielle,
  ro as ruissellement
FROM "Climate_Era5"."Bronze"."Table1" 
WHERE number=0