-- Model: stg_race_results

{{ config( 
    tags = ['stg'],
    materialized = 'incremental',
    incremental_strategy = 'append',
    unique_key = ['year', 'round_number', 'driver', 'lap_number']
)}}

WITH source AS (
    SELECT * FROM {{ source('f1stop_raw', 'raw_race_results') }}
),

renamed_cast AS (
    SELECT

        -- Time as seconds
        CAST(Time AS DOUBLE)                AS session_time_sec,
        CAST(LapTime AS DOUBLE)             AS lap_time_sec,
        CAST(PitOutTime AS DOUBLE)          AS pit_out_time_sec,
        CAST(PitInTime AS DOUBLE)           AS pit_in_time_sec,
        CAST(Sector1Time AS DOUBLE)         AS sector_1_time_sec,
        CAST(Sector2Time AS DOUBLE)         AS sector_2_time_sec,
        CAST(Sector3Time AS DOUBLE)         AS sector_3_time_sec,
        CAST(Sector1SessionTime AS DOUBLE)  AS sector_1_session_time_sec,
        CAST(Sector2SessionTime AS DOUBLE)  AS sector_2_session_time_sec,
        CAST(Sector3SessionTime AS DOUBLE)  AS sector_3_session_time_sec,
        CAST(LapStartTime AS DOUBLE)        AS lap_start_time_sec,

        -- Timestamp
        TRY_CAST(LapStartDate AS TIMESTAMP)     AS lap_start_date,

        -- Driver info
        Driver                              AS driver,
        DriverNumber                        AS driver_number,
        Team                                AS team,

        -- Lap info
        CAST(CAST(NULLIF(LapNumber, 'nan') AS DOUBLE) AS INT)               AS lap_number,
        CAST(CAST(NULLIF(Stint, 'nan') AS DOUBLE) AS INT)              AS stint,
        CAST(CAST(NULLIF(Position, 'nan') AS DOUBLE) AS INT)              AS position,

        -- Speed trap
        CAST(SpeedI1 AS DOUBLE)             AS speed_trap_I1,
        CAST(SpeedI2 AS DOUBLE)             AS speed_trap_I2,
        CAST(SpeedFL AS DOUBLE)             AS speed_trap_FL,
        CAST(SpeedST AS DOUBLE)             AS speed_trap_ST,

        -- Tyre info
        NULLIF(NULLIF(Compound, 'nan'), 'None')             AS tyre_compound,
        CAST(CAST(NULLIF(TyreLife, 'nan') AS DOUBLE) AS INT)             AS tyre_life_laps,
        TRY_CAST(FreshTyre AS BOOLEAN)          AS is_fresh_tyre,
        
        -- Track info
        TrackStatus                         AS track_status, -- can have multiple statuses, keeping as string
        TRY_CAST(IsPersonalBest AS BOOLEAN)     AS is_personal_best,
        TRY_CAST(Deleted AS BOOLEAN)            AS is_deleted,
        NULLIF(NULLIF(DeletedReason, 'nan'), '')     AS deleted_reason,
        TRY_CAST(FastF1Generated AS BOOLEAN)    AS is_fastf1_generated,
        TRY_CAST(IsAccurate AS BOOLEAN)         AS is_accurate,
        
        -- Race info
        CAST(year AS INT)                   AS year,
        CAST(round_number AS INT)           AS round_number,
        event_name                          AS event_name,	
        session_type                        AS session_type,	
        
        -- Metadata
        CAST(loaded_at AS TIMESTAMP)        AS loaded_at
    FROM source
)

SELECT * FROM renamed_cast

{% if is_incremental() %}
    WHERE (year, round_number, driver, lap_number) NOT IN (
        SELECT year, round_number, driver, lap_number
        from {{ this }}
    )
{% endif %}