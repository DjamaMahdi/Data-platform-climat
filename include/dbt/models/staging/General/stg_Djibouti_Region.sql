SELECT
CASE 
    WHEN NAME_1 = 'Djiboutii' THEN 'Djibouti'
    WHEN NAME_1 = 'AliSabieh' THEN 'Ali Sabieh'
    ELSE NAME_1 
END as Region,
geom as coordinates
FROM {{ source('Bronze', 'Djibouti_Region') }}