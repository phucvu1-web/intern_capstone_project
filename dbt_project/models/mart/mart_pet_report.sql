{{
    config(materialized='table')
}}

WITH pets AS (
    SELECT * FROM {{ ref('stg_pets') }}
),
owners AS (
    SELECT * FROM {{ref('stg_owners')}}
),
visits AS (
    SELECT * FROM {{ref('stg_visits')}}
),
pet_visit_stats AS(
    SELECT
        pet_id, COUNT(*) AS total_visits,
        SUM(cost) AS total_cost,
        MAX(visit_date) AS last_visit,
        MIN(visit_date) AS first_visit,
        STRING_AGG(DISTINCT reason, ', ' ORDER BY reason) AS visit_reasons
    FROM visits
    GROUP BY pet_id
),
final AS(
    SELECT
        p.pet_id,
        p.pet_name,
        p.species,
        p.age, p.vaccinated, o.full_name, o.email, o.phone, o.city,
        COALESCE(s.total_visits, 0) as total_visits, COALESCE(s.total_cost, 0) as total_cost, s.first_visit, s.last_visit, COALESCE(s.visit_reasons, 'no visits') AS visit_reasons
    FROM pet_visit_stats s 
    RIGHT JOIN pets p ON p.pet_id = s.pet_id
    INNER JOIN owners o ON o.owner_id = p.owner_id

)

SELECT * FROM final