{{
    config(materialized='view')
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_owners') }}

),

cleaned AS (
    SELECT
        CAST(owner_id AS INTEGER) AS owner_id,
        INITCAP(TRIM(first_name)) AS first_name,
        INITCAP(TRIM(last_name)) AS last_name,
        first_name || ' ' || last_name AS full_name,

        COALESCE(NULLIF(TRIM(email), ''), 'N/A') AS email,
        COALESCE(NULLIF(TRIM(phone), ''), 'N/A') AS phone,
        COALESCE(NULLIF(TRIM(city), ''), 'Unknown') AS city
    FROM source
)

SELECT * FROM cleaned