/* ============================================================
   STAR SCHEMA TABLES
   ============================================================ */
DROP TABLE IF EXISTS dim_datetime CASCADE;
DROP TABLE IF EXISTS dim_environmental_factors CASCADE;
DROP TABLE IF EXISTS dim_environmental_factors CASCADE;


CREATE TABLE IF NOT EXISTS dim_datetime (
    datetime_id SERIAL PRIMARY KEY,
    timestamp_local TIMESTAMP,
    year INT,
    month INT,
    day INT,
    hour INT,
    weekday VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_environmental_factors (
    env_id SERIAL PRIMARY KEY,
    latitude FLOAT,
    longitude FLOAT,
    location_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS fact_environmental_conditions (
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


-- dim_environmental_factors (single location for your dataset)
INSERT INTO dim_environmental_factors (latitude, longitude, location_name)
VALUES (38.2527, -85.7585, 'Louisville, KY')
ON CONFLICT DO NOTHING;

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

SELECT COUNT(*) FROM staging_environmental_raw;
SELECT COUNT(*) FROM dim_datetime;
SELECT COUNT(*) FROM fact_environmental_conditions;

SELECT MIN(date), MAX(date) FROM staging_environmental_raw;

