-- Model: mart_tyre_strategy
-- Description: Tyre stint analysis per driver per race

{{ config(
    tags = ['mart'],
    materialized = 'incremental',
    incremental_strategy = 'append',
    unique_key = ['year', 'round_number', 'driver', 'stint']
) }}

WITH laps AS (
    SELECT * FROM {{ ref('int_laps_enriched') }}
    WHERE lap_time_sec IS NOT NULL
    AND is_pit_lap = false
    AND is_outlap = false
    AND tyre_compound IS NOT NULL
),


-- Pre-calculate stint lap count so we can reference it in degradation logic
stint_counts AS (
    SELECT
        year,
        round_number,
        driver,
        stint,
        COUNT(lap_number) AS stint_lap_count
    FROM laps
    GROUP BY year, round_number, driver, stint
),

-- Join stint count back to laps so it's available as a column
laps_with_count AS (
    SELECT
        l.*,
        s.stint_lap_count
    FROM laps l
    LEFT JOIN stint_counts s
        ON l.year = s.year
        AND l.round_number = s.round_number
        AND l.driver = s.driver
        AND l.stint = s.stint
),

stint_metrics AS (
    SELECT
        year,
        round_number,
        event_name,
        driver,
        team,
        stint,
        tyre_compound,

        MIN(lap_number)                                         AS stint_start_lap,
        MAX(lap_number)                                         AS stint_end_lap,
        MAX(stint_lap_count)                                    AS stint_lap_count,
        MAX(tyre_life_laps)                                     AS max_tyre_age,

        MIN(lap_time_sec)                                       AS fastest_lap_sec,
        AVG(lap_time_sec)                                       AS avg_lap_time_sec,

        AVG(CASE WHEN stint_lap_number <= 3 
            THEN lap_time_sec END)                              AS avg_first_3_laps_sec,
        AVG(CASE WHEN stint_lap_number > stint_lap_count - 3 
            THEN lap_time_sec END)                              AS avg_last_3_laps_sec,
        AVG(CASE WHEN stint_lap_number > stint_lap_count - 3 
            THEN lap_time_sec END) -
        AVG(CASE WHEN stint_lap_number <= 3 
            THEN lap_time_sec END)                              AS deg_first_to_last_sec,

        AVG(CASE WHEN is_green_flag_lap = true 
            THEN lap_time_sec END)                              AS avg_green_flag_lap_sec,

        MAX(loaded_at)                                          AS loaded_at

    FROM laps_with_count
    GROUP BY year, round_number, event_name, driver, team, stint, tyre_compound
)

SELECT * FROM stint_metrics

{% if is_incremental() %}
    WHERE (year, round_number, driver, stint) NOT IN (
        SELECT year, round_number, driver, Stint
        FROM {{ this }}
    )
{% endif %}