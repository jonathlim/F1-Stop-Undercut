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
        CAST(Time AS DOUBLE)                                AS session_time_sec,

        {{ cast_nan_double('LapTime') }}                    AS lap_time_sec,
        {{ cast_nan_double('PitOutTime') }}                 AS pit_out_time_sec,
        {{ cast_nan_double('PitInTime') }}                  AS pit_in_time_sec,
        {{ cast_nan_double('Sector1Time') }}                AS sector_1_time_sec,
        {{ cast_nan_double('Sector2Time') }}                AS sector_2_time_sec,
        {{ cast_nan_double('Sector3Time') }}                AS sector_3_time_sec,
        {{ cast_nan_double('Sector1SessionTime') }}         AS sector_1_session_time_sec,
        {{ cast_nan_double('Sector2SessionTime') }}         AS sector_2_session_time_sec,
        {{ cast_nan_double('Sector3SessionTime') }}         AS sector_3_session_time_sec,
        {{ cast_nan_double('LapStartTime') }}               AS lap_start_time_sec,

        -- Timestamp
        TRY_CAST(LapStartDate AS TIMESTAMP)                 AS lap_start_date,

        -- Driver info
        Driver                                              AS driver,
        DriverNumber                                        AS driver_number,
        Team                                                AS team,

        -- Lap info
        {{ cast_nan_int('LapNumber') }}                     AS lap_number,
        {{ cast_nan_int('Stint') }}                         AS stint,
        {{ cast_nan_int('Position') }}                      AS position,

        -- Speed trap
        {{ cast_nan_double('SpeedI1') }}                    AS speed_trap_i1,
        {{ cast_nan_double('SpeedI2') }}                    AS speed_trap_i2,
        {{ cast_nan_double('SpeedFL') }}                    AS speed_trap_fl,
        {{ cast_nan_double('SpeedST') }}                    AS speed_trap_st,

        -- Tyre info
        NULLIF(NULLIF(Compound, 'nan'), 'None')             AS tyre_compound,
        {{ cast_nan_int('TyreLife') }}                      AS tyre_life_laps,
        TRY_CAST(FreshTyre AS BOOLEAN)                      AS is_fresh_tyre,
        
        -- Track info
        TrackStatus                                         AS track_status, -- can have multiple statuses, keeping as string
        TRY_CAST(IsPersonalBest AS BOOLEAN)                 AS is_personal_best,
        TRY_CAST(Deleted AS BOOLEAN)                        AS is_deleted,
        NULLIF(NULLIF(DeletedReason, 'nan'), '')            AS deleted_reason,
        TRY_CAST(FastF1Generated AS BOOLEAN)                AS is_fastf1_generated,
        TRY_CAST(IsAccurate AS BOOLEAN)                     AS is_accurate,
        
        -- Race info
        CAST(year AS INT)                                   AS year,
        CAST(round_number AS INT)                           AS round_number,
        event_name                                          AS event_name,	
        session_type                                        AS session_type,	
        
        -- Metadata
        CAST(loaded_at AS TIMESTAMP)                        AS loaded_at
    FROM source
)

SELECT * FROM renamed_cast

{% if is_incremental() %}
    WHERE (year, round_number, driver, lap_number) NOT IN (
        SELECT year, round_number, driver, lap_number
        from {{ this }}
    )
{% endif %}