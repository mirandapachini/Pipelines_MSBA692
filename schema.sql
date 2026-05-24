
/* ============================================================
   SCHEMA DOCUMENTATION
   Project: Environmental Conditions Database
   Author: Miranda Pachini
   Description:
     This database stores environmental, weather, air quality,
     and pollen data for daily analysis and risk scoring.
     The design follows a star schema with two dimension tables
     and one fact table to support analytics and reporting.
   ============================================================ */

/* ============================================================
   TABLE: dim_datetime
   ============================================================ */

CREATE TABLE dim_datetime (
    datetime_id INT PRIMARY KEY,
    timestamp_local TIMESTAMP,
    year INT,
    month INT,
    day INT,
    hour INT,
    weekday VARCHAR(10)
);

/* ============================================================
   TABLE: dim_environmental_factors
   ============================================================ */

CREATE TABLE dim_environmental_factors (
    env_id INT PRIMARY KEY,
    latitude FLOAT,
    longitude FLOAT,
    location_name VARCHAR(100)
);

/* ============================================================
   TABLE: fact_environmental_conditions
   ============================================================ */

CREATE TABLE fact_environmental_conditions (
    fact_id SERIAL PRIMARY KEY,
    datetime_id INT REFERENCES dim_datetime(datetime_id),
    env_id INT REFERENCES dim_environmental_factors(env_id),

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
