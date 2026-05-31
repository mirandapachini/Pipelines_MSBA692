"""
Open-Meteo Weather & Air Quality ETL Pipeline

This script extracts weather, soil, air quality, and pollen data from the Open-Meteo API,
merges and transforms it with derived metrics and risk flags, and loads it into a destination
(PostgreSQL or Unity Catalog).

Features:
- Dynamic date range (today + 7 days)
- Graceful error handling with tuple returns
- Planting readiness and allergy risk scoring
- Operational and health risk flags
- Data quality diagnostics and cleanup

Usage:
    python openmeteo_etl_pipeline.py
    
Dependencies:
    pip install openmeteo-requests requests-cache retry-requests numpy pandas sqlalchemy psycopg2-binary
"""

import logging
import os
from datetime import date, timedelta

import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


# ============================================================================
# CONFIGURATION & LOGGING
# ============================================================================

def setup_logging():
    """Configure logging to both console and file."""
    log_dir = "/tmp/etl_logs"
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f"{log_dir}/open_meteo.log")
        ],
    )


# ============================================================================
# DATA EXTRACTION
# ============================================================================

def extract_data():
    """
    Extract weather/soil data from the Open-Meteo API using dynamic dates.
    Returns (DataFrame, None) on success or (None, error_message) on failure.
    """
    logging.info("Starting extraction from Open-Meteo API")

    # Setup Open-Meteo client with caching + retry logic
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Dynamic date range (today → 7 days ahead)
    today = date.today()
    seven_days = today + timedelta(days=7)

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": 38.2527,
        "longitude": -85.7585,
        "hourly": [
            "temperature_2m", "relative_humidity_2m", "precipitation_probability",
            "precipitation", "wind_speed_10m", "soil_temperature_0cm",
            "soil_moisture_0_to_1cm"
        ],
        "timezone": "auto",
        "wind_speed_unit": "mph",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "start_date": today.isoformat(),
        "end_date": seven_days.isoformat(),
    }

    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]

        hourly = response.Hourly()

        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ).tz_convert(response.Timezone().decode())
        }

        # Assign variables in same order as requested
        hourly_data["temperature_2m"] = hourly.Variables(0).ValuesAsNumpy()
        hourly_data["relative_humidity_2m"] = hourly.Variables(1).ValuesAsNumpy()
        hourly_data["precipitation_probability"] = hourly.Variables(2).ValuesAsNumpy()
        hourly_data["precipitation"] = hourly.Variables(3).ValuesAsNumpy()
        hourly_data["wind_speed_10m"] = hourly.Variables(4).ValuesAsNumpy()
        hourly_data["soil_temperature_0cm"] = hourly.Variables(5).ValuesAsNumpy()
        hourly_data["soil_moisture_0_to_1cm"] = hourly.Variables(6).ValuesAsNumpy()

        df_raw = pd.DataFrame(hourly_data)

        logging.info("Extraction complete: %d rows", len(df_raw))
        return df_raw, None

    except Exception as e:
        logging.exception("Extraction failed: %s", e)
        return None, "API request failed — please try again later."


def extract_air_quality_pollen():
    """
    Extract air quality + pollen data from the Open-Meteo Air Quality API.
    Returns (DataFrame, None) on success or (None, error_message) on failure.
    """
    logging.info("Starting extraction from Open-Meteo Air Quality API")

    # Setup Open-Meteo client with caching + retry logic
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Dynamic date handling with API limit
    today = date.today()
    seven_days = today + timedelta(days=7)
    api_max_date = date(2026, 6, 5)
    end_date = min(seven_days, api_max_date)

    url = "https://air-quality-api.open-meteo.com/v1/air-quality"

    params = {
        "latitude": 38.2527,
        "longitude": -85.7585,
        "hourly": [
            "pm10", "pm2_5", "carbon_monoxide", "ozone", "nitrogen_dioxide",
            "sulphur_dioxide", "us_aqi", "us_aqi_pm2_5", "us_aqi_pm10",
            "us_aqi_nitrogen_dioxide", "us_aqi_carbon_monoxide",
            "us_aqi_ozone", "us_aqi_sulphur_dioxide",
            "grass_pollen", "ragweed_pollen", "olive_pollen",
            "mugwort_pollen", "birch_pollen", "alder_pollen"
        ],
        "timezone": "auto",
        "domains": "cams_global",
        "start_date": today.isoformat(),
        "end_date": end_date.isoformat(),
    }

    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]

        # Extract hourly block
        hourly = response.Hourly()

        # Build date column
        dates = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ).tz_convert(response.Timezone().decode())

        # Build dictionary for DataFrame
        hourly_data = {
            "date": dates,
            "pm10": hourly.Variables(0).ValuesAsNumpy(),
            "pm2_5": hourly.Variables(1).ValuesAsNumpy(),
            "carbon_monoxide": hourly.Variables(2).ValuesAsNumpy(),
            "ozone": hourly.Variables(3).ValuesAsNumpy(),
            "nitrogen_dioxide": hourly.Variables(4).ValuesAsNumpy(),
            "sulphur_dioxide": hourly.Variables(5).ValuesAsNumpy(),
            "us_aqi": hourly.Variables(6).ValuesAsNumpy(),
            "us_aqi_pm2_5": hourly.Variables(7).ValuesAsNumpy(),
            "us_aqi_pm10": hourly.Variables(8).ValuesAsNumpy(),
            "us_aqi_nitrogen_dioxide": hourly.Variables(9).ValuesAsNumpy(),
            "us_aqi_carbon_monoxide": hourly.Variables(10).ValuesAsNumpy(),
            "us_aqi_ozone": hourly.Variables(11).ValuesAsNumpy(),
            "us_aqi_sulphur_dioxide": hourly.Variables(12).ValuesAsNumpy(),
            "grass_pollen": hourly.Variables(13).ValuesAsNumpy(),
            "ragweed_pollen": hourly.Variables(14).ValuesAsNumpy(),
            "olive_pollen": hourly.Variables(15).ValuesAsNumpy(),
            "mugwort_pollen": hourly.Variables(16).ValuesAsNumpy(),
            "birch_pollen": hourly.Variables(17).ValuesAsNumpy(),
            "alder_pollen": hourly.Variables(18).ValuesAsNumpy()
        }

        df_raw = pd.DataFrame(hourly_data)
        df_raw["date"] = pd.to_datetime(df_raw["date"])

        logging.info("Air quality + pollen extraction complete: %d rows", len(df_raw))
        return df_raw, None

    except Exception as e:
        logging.exception("Air quality extraction failed: %s", e)
        return None, "API request failed — please try again later."


# ============================================================================
# DATA TRANSFORMATION
# ============================================================================

def merge_weather_air_quality(weather_df, air_df):
    """
    Merge weather/soil data with air quality + pollen data on the datetime column.
    Returns a merged, chronologically sorted DataFrame.
    """
    logging.info("Starting merge of weather/soil with air quality/pollen")

    # Ensure datetime columns are parsed and timezone-free
    weather_df["date"] = pd.to_datetime(weather_df["date"]).dt.tz_localize(None)
    air_df["date"] = pd.to_datetime(air_df["date"]).dt.tz_localize(None)

    # Merge on the shared timestamp
    merged = weather_df.merge(air_df, on="date", how="inner")

    # Sort chronologically
    merged = merged.sort_values("date")

    # Final datetime normalization
    merged["date"] = pd.to_datetime(merged["date"])

    logging.info("Merge complete: %d rows", len(merged))
    return merged


def transform_data(merged):
    """
    Apply transformations to merged data:
    - Standardize column names
    - Compute derived metrics (planting_readiness, allergy_risk)
    - Create operational and health risk flags
    - Data quality cleanup
    """
    # Standardize column names
    merged.columns = (
        merged.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("-", "_")
            .str.replace("__+", "_", regex=True)
    )

    # Derived metrics
    merged["planting_readiness"] = (
        (merged["soil_temperature_0cm"].clip(50, 80) - 50) / 30 * 40 +
        (1 - merged["precipitation_probability"] / 100) * 20 +
        (1 - merged["wind_speed_10m"] / 20).clip(0, 1) * 20 +
        (1 - merged["soil_moisture_0_to_1cm"].clip(0, 0.5) / 0.5) * 20
    ).clip(0, 100)

    merged["allergy_risk"] = (
        (merged["pm2_5"] / 35).clip(0, 1) * 25 +
        (merged["ozone"] / 70).clip(0, 1) * 15 +
        (merged[[
            "grass_pollen", "ragweed_pollen", "birch_pollen",
            "alder_pollen", "mugwort_pollen", "olive_pollen"
        ]].fillna(0).mean(axis=1) / 200).clip(0, 1) * 60
    ).clip(0, 100)

    # Operational & Health Risk Flags
    merged["high_wind_flag"] = (merged["wind_speed_10m"] > 15).astype(int)
    merged["rain_expected_flag"] = (merged["precipitation_probability"] > 50).astype(int)
    merged["soil_too_wet_flag"] = (merged["soil_moisture_0_to_1cm"] > 0.35).astype(int)
    merged["poor_air_quality_flag"] = (merged["us_aqi"] > 100).astype(int)
    merged["high_pollen_flag"] = (
        merged[[
            "grass_pollen", "ragweed_pollen", "birch_pollen",
            "alder_pollen", "mugwort_pollen", "olive_pollen"
        ]].fillna(0).mean(axis=1) > 150
    ).astype(int)
    merged["heat_stress_flag"] = (merged["temperature_2m"] >= 85).astype(int)
    merged["respiratory_risk_flag"] = (merged["allergy_risk"] >= 60).astype(int)

    merged["best_overall_day_flag"] = (
        (merged["planting_readiness"] >= 65) &
        (merged["allergy_risk"] <= 40) &
        (merged["rain_expected_flag"] == 0) &
        (merged["high_wind_flag"] == 0) &
        (merged["poor_air_quality_flag"] == 0)
    ).astype(int)

    # Data Quality Diagnostics
    logging.info("Merged DataFrame Summary Statistics:\n%s", merged.describe(include="all"))
    logging.info("Null Values Count:\n%s", merged.isnull().sum())
    logging.info("Duplicate Rows Count: %d", merged.duplicated().sum())

    # Final cleanup
    before_dupes = len(merged)
    merged = merged.drop_duplicates()
    logging.info("Duplicate rows removed: %d", before_dupes - len(merged))

    merged = merged.ffill().bfill()
    merged["date"] = pd.to_datetime(merged["date"])
    numeric_cols = merged.columns.drop("date")
    merged[numeric_cols] = merged[numeric_cols].apply(pd.to_numeric, errors="coerce")
    merged = merged.sort_values("date").reset_index(drop=True)

    logging.info("Transformation cleanup complete. Rows: %d, Columns: %d",
                 merged.shape[0], merged.shape[1])

    return merged


# ============================================================================
# DATA LOADING
# ============================================================================

def load_to_postgres(df, table_name="open_meteo_merged"):
    """
    Load the final merged DataFrame into PostgreSQL.
    Replaces the table on each run (idempotent ETL behavior).
    
    Note: Update connection string with your actual PostgreSQL credentials.
    """
    logging.info("Starting load into PostgreSQL")

    # Build connection string
    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"
    )

    try:
        # Write DataFrame to PostgreSQL (replace = idempotent)
        df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False
        )

        logging.info(
            "Load complete. Table '%s' now contains %d rows and %d columns.",
            table_name,
            df.shape[0],
            df.shape[1]
        )

    except SQLAlchemyError as e:
        logging.exception("PostgreSQL load failed: %s", e)
        raise


def load_to_unity_catalog(df, table_name="main.weather.hourly_merged"):
    """
    Load the final merged DataFrame into Unity Catalog.
    Alternative to PostgreSQL for Databricks-native storage.
    
    Args:
        df: Pandas DataFrame to save
        table_name: Fully qualified table name (catalog.schema.table)
    """
    logging.info("Starting load into Unity Catalog")
    
    try:
        # Convert pandas to Spark DataFrame
        spark_df = spark.createDataFrame(df)
        
        # Write to Unity Catalog
        spark_df.write.mode("overwrite").saveAsTable(table_name)
        
        logging.info(
            "Load complete. Table '%s' now contains %d rows and %d columns.",
            table_name,
            df.shape[0],
            df.shape[1]
        )
        
    except Exception as e:
        logging.exception("Unity Catalog load failed: %s", e)
        raise


# ============================================================================
# PIPELINE ORCHESTRATION
# ============================================================================

def run_pipeline(load_destination="skip", table_name=None):
    """
    Full ETL pipeline orchestration:
    1. Extract weather + soil data
    2. Extract air quality + pollen data
    3. Merge datasets
    4. Transform (clean, engineer features, validate)
    5. Load into destination
    
    Args:
        load_destination: "postgres", "unity_catalog", or "skip"
        table_name: Target table name (optional, uses defaults if None)
    
    Returns:
        DataFrame: Transformed data (if load_destination="skip")
    """
    logging.info("===== ETL PIPELINE STARTED =====")

    # 1. Extract
    weather_df, error = extract_data()
    if error:
        logging.error("Pipeline aborted: %s", error)
        return None

    air_df, error = extract_air_quality_pollen()
    if error:
        logging.error("Pipeline aborted: %s", error)
        return None

    # 2. Merge
    merged = merge_weather_air_quality(weather_df, air_df)

    # 3. Transform
    merged = transform_data(merged)

    # 4. Load
    if load_destination == "postgres":
        if table_name:
            load_to_postgres(merged, table_name)
        else:
            load_to_postgres(merged)
    elif load_destination == "unity_catalog":
        if table_name:
            load_to_unity_catalog(merged, table_name)
        else:
            load_to_unity_catalog(merged)
    else:
        logging.info("Load skipped (destination: %s)", load_destination)

    logging.info("===== ETL PIPELINE COMPLETED =====")
    
    return merged


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Setup logging
    setup_logging()
    
    # Run pipeline
    # Options for load_destination: "postgres", "unity_catalog", "skip"
    result_df = run_pipeline(load_destination="skip")
    
    # Optionally preview results
    if result_df is not None:
        print("\n" + "="*80)
        print("Pipeline completed successfully!")
        print(f"Final dataset: {result_df.shape[0]} rows × {result_df.shape[1]} columns")
        print("="*80)
        print("\nFirst 5 rows:")
        print(result_df.head())
