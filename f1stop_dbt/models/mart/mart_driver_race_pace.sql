-- Model: mart_driver_race_pace
-- Description: Driver pace analysis per race, green flag laps only

{{ config(
    tags = ['mart'],
    materialized = 'table',
    incremental_strategy = 'append',
    unique_key = ['year','round_number','driver']
)}}

WITH green_flag_laps AS (
    -- Filter to only clean, accurate, green flag laps
    -- This is the foundation for all pace analysis
    SELECT * FROM {{ ref('int_laps_enriched') }}
    WHERE is_green_flag_lap = true
    AND is_accurate = true
    AND lap_time_sec IS NOT NULL
    AND is_pit_lap = false
    AND is_outlap = false
),

pace_metrics AS (
    SELECT
        -- Keys
        year,
        round_number,
        event_name,
        driver,
        team,

        -- Lap count context
        COUNT(lap_number)                                   AS total_green_flag_laps,

        -- Pace metrics
        MIN(lap_time_sec)                                   AS fastest_lap_sec,
        AVG(lap_time_sec)                                   AS avg_lap_time_sec,
        PERCENTILE(lap_time_sec, 0.5)                       AS median_lap_time_sec,

        -- Consistency metric
        -- Lower stddev = more consistent driver
        STDDEV(lap_time_sec)                                AS lap_time_stddev,

        -- Sector breakdowns
        AVG(sector_1_time_sec)                              AS avg_sector_1_sec,
        AVG(sector_2_time_sec)                              AS avg_sector_2_sec,
        AVG(sector_3_time_sec)                              AS avg_sector_3_sec,
        MIN(sector_1_time_sec)                              AS best_sector_1_sec,
        MIN(sector_2_time_sec)                              AS best_sector_2_sec,
        MIN(sector_3_time_sec)                              AS best_sector_3_sec,

        -- Speed trap averages
        AVG(speed_trap_i1)                                  AS avg_speed_trap_i1,
        AVG(speed_trap_i2)                                  AS avg_speed_trap_i2,
        AVG(speed_trap_fl)                                  AS avg_speed_trap_fl,
        AVG(speed_trap_st)                                  AS avg_speed_trap_st,

        -- Gap to fastest driver in race (pace delta)
        AVG(gap_to_fastest_lap_sec)                         AS avg_gap_to_fastest_lap_sec,

        -- Metadata
        MAX(loaded_at)                                      AS loaded_at

    FROM green_flag_laps
    GROUP BY year, round_number, event_name, driver, team
)

SELECT * FROM pace_metrics

{% if is_incremental() %}
    WHERE (year, round_number, driver) NOT IN (
        SELECT year, round_number, driver
        FROM {{ this }}
    )
{% endif %}