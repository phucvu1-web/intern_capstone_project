{{
    config(materialized='view')
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_pets')}}
),

cleaned AS (
    SELECT 
        CAST(pet_id AS INTEGER) AS pet_id,

        INITCAP(TRIM(pet_name)) AS pet_name,
        INITCAP(TRIM(LOWER(species))) AS species,
        TRIM(breed) AS breed,

        CAST(COALESCE(NULLIF(TRIM(age), ''), '0') AS INTEGER) AS age,
        
        CASE 
            WHEN UPPER(TRIM(vaccinated)) IN ('Y', 'YES') THEN 'Yes'
            WHEN UPPER(TRIM(vaccinated)) IN ('N', 'NO') THEN 'No'
            ELSE 'Unknown'
        END AS vaccinated,

        CAST(owner_id AS INTEGER) AS owner_id
    FROM source
)

SELECT * FROM cleaned
