WITH grid_cells AS (
    -- Each grid cell is defined as a 0.1 x 0.1 degree square centered on the latitude and longitude points and we calculate the area of overlap with the regions
    SELECT DISTINCT
        lat AS latitude,
        lon AS longitude,
        ST_MakeEnvelope(
            lon - 0.05,
            lat  - 0.05,
            lon + 0.05,
            lat  + 0.05
        ) AS cell_geom
    FROM {{ ref('stg_Ag_temperature_2m') }}
),

region_overlaps AS (
    -- Measure how much each grid cell overlaps with the regions
    SELECT
        g.latitude,
        g.longitude,
        d.Region,
        ST_Area(ST_Intersection(g.cell_geom, d.coordinates)) AS overlap_area
    FROM grid_cells g
    CROSS JOIN {{ ref('stg_Djibouti_Region') }} d
    WHERE ST_Intersects(g.cell_geom, d.coordinates)
)

-- For each grid cell, assign the region with the largest overlap area
SELECT
    {{dbt_utils.generate_surrogate_key(['latitude', 'longitude'])}} AS coord_id,
    latitude,
    longitude,
    Region
FROM region_overlaps
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY latitude, longitude
    ORDER BY overlap_area DESC
) = 1
