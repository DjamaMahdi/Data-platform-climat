WITH stg_Ag_precip_flux as (
SELECT 
  lon,
  lat,
  strftime("time",'%d/%m/%Y') AS datetime_id,
  ROUND(Precipitation_Flux, 2) AS Precipitation_Flux
FROM {{ source('Bronze', 'Ag_precip_flux') }} 
)

SELECT
  {{dbt_utils.generate_surrogate_key(['lat', 'lon', 'datetime_id'])}} as key_id,
  {{dbt_utils.generate_surrogate_key(['lat', 'lon'])}} as coord_id,  
    *
FROM stg_Ag_precip_flux
