-- Model: mart_race_summary
-- High level race overview per driver per race

{{ config(
    tags = ['mart'],
    materialized = 'incremental',
    incremental_strategy = 'append',
    unique_key = ['year', 'round_number', 'driver']
)}}

WITH laps AS (
    SELECT * FROM {{ ref('int_laps_enriched') }}
),

first_and_last_lap AS (
    SELECT
        year,
        round_number,
        driver,
        MAX(CASE WHEN lap_number = last_lap_number THEN position END)  AS finish_position,
        MAX(CASE WHEN lap_number = first_lap_number THEN position END) AS start_position
    FROM (
        SELECT
            year,
            round_number,
            driver,
            position,
            lap_number,
            MAX(lap_number) OVER (
                PARTITION BY year, round_number, driver
            ) AS last_lap_number,
            MIN(lap_number) OVER (
                PARTITION BY year, round_number, driver
            ) AS first_lap_number
        FROM laps
    )
    GROUP BY year, round_number, driver
),

race_summary AS (
    SELECT
        -- Keys
        l.year,
        l.round_number,
        l.event_name,
        l.driver,
        l.team,

        -- Race positions
        la.start_position,
        la.finish_position,
        la.start_position - la.finish_position          AS positions_gained,

        -- Lap counts
        COUNT(l.lap_number)                            AS total_laps,
        SUM(CASE WHEN l.is_green_flag_lap = true 
            THEN 1 ELSE 0 END)                         AS green_flag_laps,
        SUM(CASE WHEN l.is_pit_lap = true 
            THEN 1 ELSE 0 END)                         AS pit_stop_count,

        -- Pace
        MIN(CASE WHEN l.is_accurate = true 
            THEN l.lap_time_sec END)                   AS fastest_lap_sec,
        AVG(CASE WHEN l.is_green_flag_lap = true
            AND l.is_accurate = true
            AND l.is_pit_lap = false
            AND l.is_outlap = false
            THEN l.lap_time_sec END)                   AS avg_green_flag_lap_sec,

        -- Stint and tyre info
        MAX(l.stint)                                   AS total_stints,
        COLLECT_SET(l.tyre_compound)                   AS compounds_used,

        -- Metadata
        MAX(l.loaded_at)                               AS loaded_at

    FROM laps l
    LEFT JOIN first_and_last_lap la
        ON l.year = la.year
        AND l.round_number = la.round_number
        AND l.driver = la.driver

    GROUP BY
        l.year,
        l.round_number,
        l.event_name,
        l.driver,
        l.team,
        la.start_position,
        la.finish_position
),

final AS (
    SELECT
        year,
        round_number,
        event_name,
        driver,
        team,
        start_position,
        finish_position,
        positions_gained,
        total_laps,
        green_flag_laps,
        pit_stop_count,
        fastest_lap_sec,
        avg_green_flag_lap_sec,
        total_stints,
        compounds_used,
        loaded_at
    FROM race_summary
)

SELECT * FROM final

{% if is_incremental() %}
    WHERE (year, round_number, driver) NOT IN (
        SELECT year, round_number, driver
        FROM {{ this }}
    )
{% endif %}