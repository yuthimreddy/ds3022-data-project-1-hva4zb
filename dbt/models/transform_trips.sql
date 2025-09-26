
-- Model: Performing ALL the required transformations (6 new columns)
-- Combines both yellow and green taxi data and joins it with the emissions data


WITH all_trips AS (
    -- selectig all the columns from yellow taxi table and adding yellow as the cab_type
    SELECT
        'yellow' AS taxi_type,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance
    FROM {{source('main', 'yellow_trips')}}

--  Combining BOTH yellow and green data
    UNION ALL 

-- selecting all the columns from the green taxi table now
    SELECT
        'green' AS taxi_type,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance
    FROM {{source('main', 'green_trips')}}

),

emissions AS (
    SELECT * FROM {{ source('main', 'vehicle_emissions') }}
)



-- Joining the unified trips with the emissions data and performing the following
-- calculations to get the required columns
SELECT
    t.taxi_type,
    t.pickup_datetime,
    t.trip_distance, 

    -- 1) Calculating CO2 Per Trip in KG:
    -- (distance * grams_per_mile) / 1000 = kg of CO2
    (t.trip_distance * e.co2_grams_per_mile) / 1000 AS trip_co2_kgs,


    -- 2 Calculating AVG MPH Per Trip:
    CASE
        WHEN (julian(t.dropoff_datetime) - julian(t.pickup_datetime)) > 0
        THEN t.trip_distance / ((julian(t.dropoff_datetime) - julian(t.pickup_datetime))*24)
        ELSE 0
    END AS avg_mph_per_trip,
    -- 3) Calculating Column Trip HOUR using (0-23):
    EXTRACT(HOUR FROM t.pickup_datetime) AS trip_hour,

    -- 4) Calculating Column Trip Day of Week (1-7):
    EXTRACT(dayofweek FROM t.pickup_datetime) AS trip_day_of_week,

    -- 5) Calculating Column Week Number (1-53):
    EXTRACT(week FROM t.pickup_datetime) AS trip_week_number,

    -- 6) Calculating Column Trip Month (1-12):
    EXTRACT(month FROM t.pickup_datetime) AS trip_month


FROM all_trips t
-- Joining trips to the emission table

JOIN emissions e ON t.taxi_type = TRIM(REPLACE(e.vehicle_type, '_taxi', ''))

