-- Model: stg_race_results

{{ config( 
    tags = ['stg']
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
        CAST(LapStartDate AS TIMESTAMP)     AS lap_start_date,

        -- Driver info
        Driver                              AS driver,
        DriverNumber                        AS driver_number,
        Team                                AS team,

        -- Lap info
        CAST(LapNumber AS INT)              AS lap_number,
        CAST(Stint AS INT)                  AS stint,
        CAST(Position AS INT)               AS position,

        -- Speed trap
        CAST(SpeedI1 AS DOUBLE)             AS speed_trap_I1,
        CAST(SpeedI2 AS DOUBLE)             AS speed_trap_I2,
        CAST(SpeedFL AS DOUBLE)             AS speed_trap_FL,
        CAST(SpeedST AS DOUBLE)             AS speed_trap_ST,

        -- Tyre info
        Compound                            AS tyre_compound,
        CAST(TyreLife AS INT)               AS tyre_life_laps,
        CAST(FreshTyre AS BOOLEAN)          AS is_fresh_tyre,
        
        -- Track info
        TrackStatus                         AS track_status, -- can have multiple statuses, keeping as string
        CAST(IsPersonalBest AS BOOLEAN)     AS is_personal_best,
        CAST(Deleted AS BOOLEAN)            AS is_deleted,
        DeletedReason                       AS deleted_reason,
        CAST(FastF1Generated AS BOOLEAN)    AS is_fastf1_generated,
        CAST(IsAccurate AS BOOLEAN)         AS is_accurate,
        
        -- Race info
        CAST(year AS INT)                   AS year,
        CAST(round_number AS INT)           AS round_number,
        event_name                          AS event_name,	
        session_type                        AS session_type,	
        
        -- Metadata
        loaded_at
    FROM source
)

SELECT * FROM renamed_cast