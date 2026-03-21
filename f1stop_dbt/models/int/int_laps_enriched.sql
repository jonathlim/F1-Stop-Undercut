{{ config(
    tags = ['int']
) }}

WITH stage AS (
    SELECT * FROM {{ ref('stg_race_results')}}
    WHERE 
)

SELECT * FROM stage