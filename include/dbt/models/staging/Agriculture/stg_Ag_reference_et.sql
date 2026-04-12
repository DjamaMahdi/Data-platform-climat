WITH Ag_reference_et as (
SELECT 
  lon,
  lat,
  strftime("time",'%d/%m/%Y') AS datetime_id,
  ROUND(ReferenceET_PenmanMonteith_FAO56, 2) AS Reference_Evapotranspiration
FROM {{ source('Bronze', 'Ag_reference_et') }} 
)

SELECT
  {{dbt_utils.generate_surrogate_key(['lat', 'lon', 'datetime_id'])}} as key_id,
  {{dbt_utils.generate_surrogate_key(['lat', 'lon'])}} as coord_id,  
    *
FROM  Ag_reference_et