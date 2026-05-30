/* ============================================================
   STAR SCHEMA TABLES
   ============================================================ */

-- Drop fact first (because it depends on dimensions)
DROP TABLE IF EXISTS fact_environmental_conditions;
DROP TABLE IF EXISTS dim_datetime;
DROP TABLE IF EXISTS dim_environmental_factors;

CREATE TABLE IF NOT EXISTS dim_datetime (
    datetime_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp_local TIMESTAMP,
    year INT,
    month INT,
    day INT,
    hour INT,
    weekday VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_environmental_factors (
    env_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    latitude FLOAT,
    longitude FLOAT,
    location_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS fact_environmental_conditions (
    fact_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Foreign keys
    datetime_id BIGINT REFERENCES dim_datetime(datetime_id),
    env_id BIGINT REFERENCES dim_environmental_factors(env_id),

    temperature_2m FLOAT,
    wind_speed_10m FLOAT,
    precipitation_probability FLOAT,
    soil_temperature_0cm FLOAT,
    soil_moisture_0_to_1cm FLOAT,

    pm25 FLOAT,
    ozone FLOAT,
    us_aqi INT,

    grass_pollen FLOAT,
    ragweed_pollen FLOAT,
    birch_pollen FLOAT,
    alder_pollen FLOAT,
    mugwort_pollen FLOAT,
    olive_pollen FLOAT,

    planting_readiness FLOAT,
    allergy_risk FLOAT,

    high_wind_flag BOOLEAN,
    rain_expected_flag BOOLEAN,
    soil_too_wet_flag BOOLEAN,
    poor_air_quality_flag BOOLEAN,
    high_pollen_flag BOOLEAN,
    heat_stress_flag BOOLEAN,
    respiratory_risk_flag BOOLEAN,
    best_overall_day_flag BOOLEAN
);

/* ============================================================
   STAGING TABLE (MATCHES CSV EXACTLY)
   ============================================================ */

CREATE TABLE IF NOT EXISTS staging_environmental_raw (
    date TIMESTAMP,
    temperature_2m FLOAT,
    relative_humidity_2m FLOAT,
    precipitation_probability FLOAT,
    precipitation FLOAT,
    wind_speed_10m FLOAT,
    soil_temperature_0cm FLOAT,
    soil_moisture_0_to_1cm FLOAT,
    pm10 FLOAT,
    pm2_5 FLOAT,
    carbon_monoxide FLOAT,
    ozone FLOAT,
    nitrogen_dioxide FLOAT,
    sulphur_dioxide FLOAT,
    us_aqi FLOAT,
    us_aqi_pm2_5 FLOAT,
    us_aqi_pm10 FLOAT,
    us_aqi_nitrogen_dioxide FLOAT,
    us_aqi_carbon_monoxide FLOAT,
    us_aqi_ozone FLOAT,
    us_aqi_sulphur_dioxide FLOAT,
    grass_pollen FLOAT,
    ragweed_pollen FLOAT,
    olive_pollen FLOAT,
    mugwort_pollen FLOAT,
    birch_pollen FLOAT,
    alder_pollen FLOAT,
    planting_readiness FLOAT,
    allergy_risk FLOAT,
    high_wind_flag INT,
    rain_expected_flag INT,
    soil_too_wet_flag INT,
    poor_air_quality_flag INT,
    high_pollen_flag INT,
    heat_stress_flag INT,
    respiratory_risk_flag INT,
    best_overall_day_flag INT
);

/* ============================================================
   POPULATE DIMENSIONS
   ============================================================ */

-- dim_datetime
INSERT INTO dim_datetime (timestamp_local, year, month, day, hour, weekday)
SELECT DISTINCT
    date,
    EXTRACT(YEAR FROM date)::INT,
    EXTRACT(MONTH FROM date)::INT,
    EXTRACT(DAY FROM date)::INT,
    EXTRACT(HOUR FROM date)::INT,
    TO_CHAR(date, 'Day')
FROM staging_environmental_raw
ORDER BY date;

-- dim_environmental_factors (single location)
INSERT INTO dim_environmental_factors (latitude, longitude, location_name)
VALUES (38.2527, -85.7585, 'Louisville, KY');

/* ============================================================
   POPULATE FACT TABLE
   ============================================================ */

INSERT INTO fact_environmental_conditions (
    datetime_id,
    env_id,
    temperature_2m,
    wind_speed_10m,
    precipitation_probability,
    soil_temperature_0cm,
    soil_moisture_0_to_1cm,
    pm25,
    ozone,
    us_aqi,
    grass_pollen,
    ragweed_pollen,
    birch_pollen,
    alder_pollen,
    mugwort_pollen,
    olive_pollen,
    planting_readiness,
    allergy_risk,
    high_wind_flag,
    rain_expected_flag,
    soil_too_wet_flag,
    poor_air_quality_flag,
    high_pollen_flag,
    heat_stress_flag,
    respiratory_risk_flag,
    best_overall_day_flag
)
SELECT
    d.datetime_id,
    1 AS env_id,
    s.temperature_2m,
    s.wind_speed_10m,
    s.precipitation_probability,
    s.soil_temperature_0cm,
    s.soil_moisture_0_to_1cm,
    s.pm2_5 AS pm25,
    s.ozone,
    s.us_aqi::INT,
    s.grass_pollen,
    s.ragweed_pollen,
    s.birch_pollen,
    s.alder_pollen,
    s.mugwort_pollen,
    s.olive_pollen,
    s.planting_readiness,
    s.allergy_risk,
    (s.high_wind_flag = 1),
    (s.rain_expected_flag = 1),
    (s.soil_too_wet_flag = 1),
    (s.poor_air_quality_flag = 1),
    (s.high_pollen_flag = 1),
    (s.heat_stress_flag = 1),
    (s.respiratory_risk_flag = 1),
    (s.best_overall_day_flag = 1)
FROM staging_environmental_raw s
JOIN dim_datetime d
  ON d.timestamp_local = s.date;

/* ============================================================
   VALIDATION COUNTS
   ============================================================ */

SELECT COUNT(*) AS staging_rows FROM staging_environmental_raw;
SELECT COUNT(*) AS dim_datetime_rows FROM dim_datetime;
SELECT COUNT(*) AS fact_rows FROM fact_environmental_conditions;

SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM staging_environmental_raw;

/* ============================================================
   VIEW TABLES & STRUCTURE
   ============================================================ */

-- List all tables
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

-- Column definitions
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_datetime';

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_environmental_factors';

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'fact_environmental_conditions';

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'staging_environmental_raw';

-- Preview rows
SELECT * FROM dim_datetime LIMIT 5;
SELECT * FROM dim_environmental_factors LIMIT 5;
SELECT * FROM fact_environmental_conditions LIMIT 5;
SELECT * FROM staging_environmental_raw LIMIT 5;

-- Show foreign keys
SELECT
    tc.constraint_name,
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table,
    ccu.column_name AS foreign_column
FROM 
    information_schema.table_constraints AS tc
JOIN 
    information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
JOIN 
    information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
WHERE 
    tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_name;

-- Adding a JSONB column to store raw JSON data
ALTER TABLE staging_environmental_raw
ADD COLUMN raw_data JSONB;

-- Or, if you need a new table to store specific JSON elements
CREATE TABLE IF NOT EXISTS api_data (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    extracted_at TIMESTAMP DEFAULT NOW(),
    some_value TEXT,
    nested_value JSONB
);
