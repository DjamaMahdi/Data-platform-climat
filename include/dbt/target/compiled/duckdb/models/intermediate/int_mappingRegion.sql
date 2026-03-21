WITH grid_cells AS (
    -- Each grid cell is defined as a 0.25 x 0.25 degree square centered on the latitude and longitude points and we calculate the area of overlap with the regions
    SELECT DISTINCT
        latitude,
        longitude,
        ST_MakeEnvelope(
            longitude - 0.125,
            latitude  - 0.125,
            longitude + 0.125,
            latitude  + 0.125
        ) AS cell_geom
    FROM "Climate_Era5"."Silver"."stg_General_1"
),

overlaps AS (
    -- Measure how much each grid cell overlaps with the regions
    SELECT
        g.latitude,
        g.longitude,
        d.Region,
        ST_Area(ST_Intersection(g.cell_geom, d.coordinates)) AS overlap_area
    FROM grid_cells g
    CROSS JOIN "Climate_Era5"."Silver"."stg_Djibouti_Region" d
    WHERE ST_Intersects(g.cell_geom, d.coordinates)
)

-- For each grid cell, assign the region with the largest overlap area
SELECT
    md5(cast(coalesce(cast(latitude as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(longitude as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS coord_id,
    latitude,
    longitude,
    Region
FROM overlaps
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY latitude, longitude
    ORDER BY overlap_area DESC
) = 1