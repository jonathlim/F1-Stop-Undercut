-- Model: int_laps_enriched

{{ config(
    tags = ['int'],
    materialized = 'incremental',
    incremental_strategy = 'append',
    unique_key = ['year', 'round_number', 'driver', 'lap_number']
) }}

WITH stage AS (
    SELECT * FROM {{ ref('stg_race_results')}}
),

enriched AS (
    SELECT
        -- Keys
        year,
        round_number,
        event_name,
        session_type,
        driver,
        driver_number,
        team,
        lap_number,
        position,

        -- Lap times
        lap_time_sec,
        sector_1_time_sec,
        sector_2_time_sec,
        sector_3_time_sec,

        -- Session times
        session_time_sec,
        lap_start_time_sec,
        lap_start_date,

        -- Pit info
        pit_in_time_sec,
        pit_out_time_sec,

        -- Tyre info
        tyre_compound,
        tyre_life_laps,
        is_fresh_tyre,
        stint,

        -- Speed traps
        speed_trap_i1,
        speed_trap_i2,
        speed_trap_fl,
        speed_trap_st,

        -- Flags from staging
        track_status,
        is_personal_best,
        is_deleted,
        deleted_reason,
        is_fastf1_generated,
        is_accurate,

        -- Derived flags
        NOT (
            track_status LIKE '%2%' OR   -- yellow flag
            track_status LIKE '%3%' OR   -- SC deployed
            track_status LIKE '%4%' OR   -- SC on track
            track_status LIKE '%5%' OR   -- red flag
            track_status LIKE '%6%' OR   -- VSC
            track_status LIKE '%7%'      -- VSC ending
        )                                   AS is_green_flag_lap,
        pit_in_time_sec IS NOT NULL         AS is_pit_lap,
        pit_out_time_sec IS NOT NULL        AS is_outlap,

        -- Lap number within current stint (resets after each pit stop)
        ROW_NUMBER() OVER (
            PARTITION BY year, round_number, driver, stint
            ORDER BY lap_number
        )                                   AS stint_lap_number,

        -- Gap to fastest lap in that race
        lap_time_sec - MIN(lap_time_sec) OVER (
            PARTITION BY year, round_number
        )                                   AS gap_to_fastest_lap_sec,

        -- Metadata
        loaded_at

    FROM stage
)


SELECT * FROM enriched

{% if is_incremental() %}
    WHERE (year, round_number, driver, lap_number) NOT IN (
        SELECT year, round_number, driver, lap_number
        FROM {{ this }}
    )
{% endif %}