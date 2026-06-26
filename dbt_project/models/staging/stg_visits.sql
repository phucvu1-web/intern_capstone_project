{{
    config(materialized='view')

}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_visits') }}
),
cleaned AS (
    SELECT
        CAST(visit_id AS INTEGER) AS visit_id,
        CAST(pet_id AS INTEGER) AS pet_id,
        CASE 
            WHEN visit_date ~ '^\d{4}-\d{2}-\d{2}$'
                THEN CAST(visit_date AS DATE)
            WHEN visit_date ~ '^\d{4}/\d{2}/\d{2}$'
                THEN CAST(REPLACE(visit_date, '/', '-') AS DATE)
            WHEN visit_date ~ '^\d{2}-\d{2}-\d{4}$'
                THEN TO_DATE(visit_date, 'DD-MM-YYYY') 
            ELSE NULL
        END AS visit_date,
        TRIM(reason) As reason,

        CAST(COALESCE(NULLIF(TRIM(cost), ''), '0') AS  NUMERIC) AS cost,
        COALESCE(NULLIF(TRIM(vet_name), ''), '0') AS vet_name,
        COALESCE(NULLIF(TRIM(notes), ''), '0') AS notes
    FROM source
)
SELECT * FROM cleaned