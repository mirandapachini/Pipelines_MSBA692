-- ============================================
-- DIMENSION: Date/Time
-- ============================================
CREATE TABLE dim_datetime (
    datetime_id SERIAL PRIMARY KEY,
    timestamp_local TIMESTAMP NOT NULL,
    year INT,
    month INT,
    day INT,
    hour INT,
    weekday VARCHAR(10)
);

-- ============================================
-- DIMENSION: Environmental Factors
-- ============================================
CREATE TABLE dim_environmental_factors (
    env_id SERIAL PRIMARY KEY,
    latitude FLOAT,
    longitude FLOAT,
    location_name VARCHAR(100)
);

-- ============================================
-- FACT TABLE: Environmental Conditions
-- ============================================
CREATE TABLE fact_environmental_conditions (
    fact_id SERIAL PRIMARY KEY,
    datetime_id INT REFERENCES dim_datetime(datetime_id),
    env_id INT REFERENCES dim_environmental_factors(env_id),

    -- Weather
    temperature_2m FLOAT,
    wind_speed_10m FLOAT,
    precipitation_probability FLOAT,

    -- Soil
    soil_temperature_0cm FLOAT,
    soil_moisture_0_to_1cm FLOAT,

    -- Air Quality
    pm25 FLOAT,
    ozone FLOAT,
    us_aqi FLOAT,

    -- Pollen
    grass_pollen FLOAT,
    ragweed_pollen FLOAT,
    birch_pollen FLOAT,
    alder_pollen FLOAT,
    mugwort_pollen FLOAT,
    olive_pollen FLOAT,

    -- Composite Scores
    planting_readiness FLOAT,
    allergy_risk FLOAT,

    -- Flags
    high_wind_flag INT,
    rain_expected_flag INT,
    soil_too_wet_flag INT,
    poor_air_quality_flag INT,
    high_pollen_flag INT,
    heat_stress_flag INT,
    respiratory_risk_flag INT,
    best_overall_day_flag INT
);
