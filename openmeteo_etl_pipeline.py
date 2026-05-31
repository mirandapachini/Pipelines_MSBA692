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
    """
    Configure logging to both console and file.
    
    Logs are written to /tmp/etl_logs/open_meteo.log for persistent debugging.
    Console output is also enabled for interactive monitoring during execution.
    """
    log_dir = "/tmp/etl_logs"
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),  # Print to console
            logging.FileHandler(f"{log_dir}/open_meteo.log")  # Save to file
        ],
    )


# ============================================================================
# DATA EXTRACTION
# ============================================================================

def extract_data():
    """
    Extract weather/soil data from the Open-Meteo API using dynamic dates.
    
    Returns:
        tuple: (DataFrame, None) on success or (None, error_message) on failure
        
    API Details:
        - Endpoint: https://api.open-meteo.com/v1/forecast
        - Free tier, no API key required
        - Location: Louisville, KY (38.2527°N, -85.7585°W)
        - Data: Hourly weather and soil metrics for 7-day forecast
        
    Why tuple returns?
        This pattern allows the caller to check for errors without catching exceptions.
        Makes it easy to gracefully handle API failures and continue pipeline execution.
    """
    logging.info("Starting extraction from Open-Meteo API")

    # Setup client with caching to avoid redundant API calls during development/testing
    # Cache expires after 1 hour (3600 seconds) to ensure relatively fresh data
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    
    # Retry logic handles transient network errors (5 attempts with exponential backoff)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Dynamic date range: today → 7 days ahead
    # This ensures the pipeline always gets the latest forecast data when run
    today = date.today()
    seven_days = today + timedelta(days=7)

    url = "https://api.open-meteo.com/v1/forecast"

    # Location: Louisville, KY
    # Units: Fahrenheit (temperature), mph (wind), inch (precipitation)
    # Why these variables? Selected for agricultural planning and health risk assessment
    params = {
        "latitude": 38.2527,
        "longitude": -85.7585,
        "hourly": [
            "temperature_2m",                # Air temperature at 2 meters
            "relative_humidity_2m",          # Relative humidity %
            "precipitation_probability",     # % chance of rain (0-100)
            "precipitation",                 # Rainfall amount in inches
            "wind_speed_10m",                # Wind speed at 10 meters in mph
            "soil_temperature_0cm",          # Surface soil temperature (°F)
            "soil_moisture_0_to_1cm"         # Top 1cm soil moisture (0-1 scale)
        ],
        "timezone": "auto",                  # Automatically detect timezone from coordinates
        "wind_speed_unit": "mph",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "start_date": today.isoformat(),     # YYYY-MM-DD format
        "end_date": seven_days.isoformat(),
    }

    try:
        # API returns a list of responses (one per location)
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]  # We only requested one location

        # Extract the hourly forecast data block
        hourly = response.Hourly()

        # Build timezone-aware datetime index
        # Why tz_convert? Open-Meteo returns UTC timestamps; we convert to local time
        # for easier interpretation and compatibility with downstream systems
        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"  # Include start time, exclude end time
            ).tz_convert(response.Timezone().decode())
        }

        # IMPORTANT: Variables must be accessed in the same order as requested in params
        # Index 0 = first variable in the hourly list, index 1 = second, etc.
        hourly_data["temperature_2m"] = hourly.Variables(0).ValuesAsNumpy()
        hourly_data["relative_humidity_2m"] = hourly.Variables(1).ValuesAsNumpy()
        hourly_data["precipitation_probability"] = hourly.Variables(2).ValuesAsNumpy()
        hourly_data["precipitation"] = hourly.Variables(3).ValuesAsNumpy()
        hourly_data["wind_speed_10m"] = hourly.Variables(4).ValuesAsNumpy()
        hourly_data["soil_temperature_0cm"] = hourly.Variables(5).ValuesAsNumpy()
        hourly_data["soil_moisture_0_to_1cm"] = hourly.Variables(6).ValuesAsNumpy()

        df_raw = pd.DataFrame(hourly_data)

        logging.info("Extraction complete: %d rows", len(df_raw))
        return df_raw, None  # Success: return DataFrame and None for error

    except Exception as e:
        # Log the full traceback for debugging
        logging.exception("Extraction failed: %s", e)
        # Return user-friendly error message instead of raising exception
        return None, "API request failed — please try again later."


def extract_air_quality_pollen():
    """
    Extract air quality + pollen data from the Open-Meteo Air Quality API.
    
    Returns:
        tuple: (DataFrame, None) on success or (None, error_message) on failure
        
    API Details:
        - Endpoint: https://air-quality-api.open-meteo.com/v1/air-quality
        - Free tier, no API key required
        - Data source: CAMS (Copernicus Atmosphere Monitoring Service)
        - Includes: PM2.5, PM10, gases, US AQI, and pollen forecasts
        
    IMPORTANT CONSTRAINT:
        Air quality forecast data is only available until 2026-06-05.
        This is a known API limitation. The code caps the end date to avoid errors.
    """
    logging.info("Starting extraction from Open-Meteo Air Quality API")

    # Same caching and retry strategy as weather extraction
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Dynamic date handling with API constraint
    today = date.today()
    seven_days = today + timedelta(days=7)
    
    # CRITICAL: Air quality API has a hard limit of 2026-06-05
    # If we try to request data beyond this date, the API will fail
    api_max_date = date(2026, 6, 5)
    end_date = min(seven_days, api_max_date)  # Use the earlier of the two dates

    url = "https://air-quality-api.open-meteo.com/v1/air-quality"

    # Why these variables?
    # - PM2.5/PM10: Fine particulate matter, key drivers of air quality index
    # - Gases: NOx, SO2, CO, O3 — all contribute to respiratory issues
    # - US AQI: Standard air quality index used by EPA (0-500 scale)
    # - Pollen: Six major allergen types tracked for allergy risk assessment
    params = {
        "latitude": 38.2527,
        "longitude": -85.7585,
        "hourly": [
            # Air quality pollutants (μg/m³)
            "pm10", "pm2_5", "carbon_monoxide", "ozone", "nitrogen_dioxide",
            "sulphur_dioxide",
            # US Air Quality Index (0-500 scale)
            "us_aqi",
            # Individual pollutant AQI scores
            "us_aqi_pm2_5", "us_aqi_pm10",
            "us_aqi_nitrogen_dioxide", "us_aqi_carbon_monoxide",
            "us_aqi_ozone", "us_aqi_sulphur_dioxide",
            # Pollen forecasts (grains/m³)
            "grass_pollen", "ragweed_pollen", "olive_pollen",
            "mugwort_pollen", "birch_pollen", "alder_pollen"
        ],
        "timezone": "auto",
        "domains": "cams_global",  # Use CAMS global model for air quality
        "start_date": today.isoformat(),
        "end_date": end_date.isoformat(),
    }

    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]

        # Extract hourly data block
        hourly = response.Hourly()

        # Build timezone-aware datetime index (same pattern as weather extraction)
        dates = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ).tz_convert(response.Timezone().decode())

        # IMPORTANT: Variables accessed in the order they appear in params["hourly"]
        # If you change the order in params, you must update these indices
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
        return df_raw, None  # Success

    except Exception as e:
        logging.exception("Air quality extraction failed: %s", e)
        return None, "API request failed — please try again later."


# ============================================================================
# DATA TRANSFORMATION
# ============================================================================

def merge_weather_air_quality(weather_df, air_df):
    """
    Merge weather/soil data with air quality + pollen data on the datetime column.
    
    Args:
        weather_df: DataFrame with weather/soil metrics
        air_df: DataFrame with air quality/pollen metrics
        
    Returns:
        DataFrame: Inner join on timestamp, chronologically sorted
        
    Why inner join?
        - Weather API may return more rows than air quality API (due to date limits)
        - Inner join ensures we only keep timestamps where BOTH datasets have data
        - This prevents null values in critical columns downstream
    """
    logging.info("Starting merge of weather/soil with air quality/pollen")

    # Strip timezone info to avoid merge conflicts
    # Both APIs return timezone-aware timestamps, but pandas merge requires timezone-naive
    weather_df["date"] = pd.to_datetime(weather_df["date"]).dt.tz_localize(None)
    air_df["date"] = pd.to_datetime(air_df["date"]).dt.tz_localize(None)

    # Inner join: only keep rows where timestamps match in both DataFrames
    merged = weather_df.merge(air_df, on="date", how="inner")

    # Sort chronologically for easier inspection and time-series analysis
    merged = merged.sort_values("date")

    # Ensure date column is in proper datetime format after merge
    merged["date"] = pd.to_datetime(merged["date"])

    logging.info("Merge complete: %d rows", len(merged))
    return merged


def transform_data(merged):
    """
    Apply transformations to merged data:
    - Standardize column names (lowercase, underscores)
    - Compute derived metrics (planting_readiness, allergy_risk)
    - Create operational and health risk flags
    - Data quality cleanup (duplicates, missing values, type coercion)
    
    Args:
        merged: DataFrame with weather + air quality data
        
    Returns:
        DataFrame: Transformed data ready for loading
        
    Business Logic:
        - Planting Readiness: 0-100 score for ideal outdoor planting conditions
        - Allergy Risk: 0-100 score for respiratory health risk
        - Flags: Binary indicators (0/1) for specific thresholds
    """
    # Standardize column names: lowercase, replace spaces/hyphens with underscores
    # This ensures compatibility with SQL databases and makes Python access easier
    merged.columns = (
        merged.columns
            .str.strip()            # Remove leading/trailing whitespace
            .str.lower()            # All lowercase
            .str.replace(" ", "_")  # Spaces to underscores
            .str.replace("-", "_")  # Hyphens to underscores
            .str.replace("__+", "_", regex=True)  # Remove consecutive underscores
    )

    # ===========================================================================
    # DERIVED METRIC: Planting Readiness Score (0-100)
    # ===========================================================================
    # This score helps determine ideal days for outdoor planting activities.
    # Higher score = better conditions for planting.
    #
    # Components (weights chosen based on agricultural best practices):
    # 1. Soil Temperature (40%): Ideal range 60-75°F (50-80°F clipped)
    # 2. Precipitation Probability (20%): Lower rain chance = better
    # 3. Wind Speed (20%): Calm conditions (< 15 mph) preferred
    # 4. Soil Moisture (20%): Not too wet (< 0.35 preferred)
    merged["planting_readiness"] = (
        # Soil temp: normalize 50-80°F range to 0-1, then multiply by 40
        (merged["soil_temperature_0cm"].clip(50, 80) - 50) / 30 * 40 +
        # Rain: invert probability (0% rain = 20 points, 100% rain = 0 points)
        (1 - merged["precipitation_probability"] / 100) * 20 +
        # Wind: calm = 20 points, normalize by 20 mph max
        (1 - merged["wind_speed_10m"] / 20).clip(0, 1) * 20 +
        # Soil moisture: drier = better, normalize by 0.5 max
        (1 - merged["soil_moisture_0_to_1cm"].clip(0, 0.5) / 0.5) * 20
    ).clip(0, 100)  # Final score clipped to 0-100 range

    # ===========================================================================
    # DERIVED METRIC: Allergy Risk Score (0-100)
    # ===========================================================================
    # This score assesses respiratory health risk from air quality and pollen.
    # Higher score = higher risk for allergies and respiratory issues.
    #
    # Components (weights based on EPA and allergy research):
    # 1. PM2.5 (25%): Fine particles, major AQI contributor (35 μg/m³ threshold)
    # 2. Ozone (15%): Ground-level ozone, respiratory irritant (70 ppb threshold)
    # 3. Pollen (60%): Average of six allergen types (200 grains/m³ threshold)
    merged["allergy_risk"] = (
        # PM2.5: normalize by EPA "Unhealthy for Sensitive Groups" threshold (35)
        (merged["pm2_5"] / 35).clip(0, 1) * 25 +
        # Ozone: normalize by "Moderate" AQI threshold (70 ppb)
        (merged["ozone"] / 70).clip(0, 1) * 15 +
        # Pollen: average of 6 types, normalize by "High" threshold (200 grains/m³)
        (merged[[
            "grass_pollen", "ragweed_pollen", "birch_pollen",
            "alder_pollen", "mugwort_pollen", "olive_pollen"
        ]].fillna(0).mean(axis=1) / 200).clip(0, 1) * 60
    ).clip(0, 100)  # Final score clipped to 0-100 range

    # ===========================================================================
    # OPERATIONAL & HEALTH RISK FLAGS (Binary: 0 or 1)
    # ===========================================================================
    # These flags trigger alerts or filter data based on specific thresholds.
    
    # High Wind Flag: Wind speed > 15 mph (unsafe for outdoor activities)
    merged["high_wind_flag"] = (merged["wind_speed_10m"] > 15).astype(int)
    
    # Rain Expected Flag: Precipitation probability > 50% (likely rain)
    merged["rain_expected_flag"] = (merged["precipitation_probability"] > 50).astype(int)
    
    # Soil Too Wet Flag: Soil moisture > 0.35 (poor planting conditions)
    merged["soil_too_wet_flag"] = (merged["soil_moisture_0_to_1cm"] > 0.35).astype(int)
    
    # Poor Air Quality Flag: US AQI > 100 ("Unhealthy for Sensitive Groups")
    merged["poor_air_quality_flag"] = (merged["us_aqi"] > 100).astype(int)
    
    # High Pollen Flag: Average pollen > 150 grains/m³ (high allergy risk)
    merged["high_pollen_flag"] = (
        merged[[
            "grass_pollen", "ragweed_pollen", "birch_pollen",
            "alder_pollen", "mugwort_pollen", "olive_pollen"
        ]].fillna(0).mean(axis=1) > 150
    ).astype(int)
    
    # Heat Stress Flag: Temperature >= 85°F (heat advisory threshold)
    merged["heat_stress_flag"] = (merged["temperature_2m"] >= 85).astype(int)
    
    # Respiratory Risk Flag: Allergy risk score >= 60 (high respiratory risk)
    merged["respiratory_risk_flag"] = (merged["allergy_risk"] >= 60).astype(int)

    # ===========================================================================
    # COMPOSITE FLAG: Best Overall Day
    # ===========================================================================
    # This flag identifies ideal days for outdoor activities (planting, exercise, etc.)
    # Requirements: ALL of the following must be true:
    # - Planting readiness >= 65 (good conditions)
    # - Allergy risk <= 40 (low health risk)
    # - No rain expected
    # - No high winds
    # - Air quality is good (AQI <= 100)
    merged["best_overall_day_flag"] = (
        (merged["planting_readiness"] >= 65) &
        (merged["allergy_risk"] <= 40) &
        (merged["rain_expected_flag"] == 0) &
        (merged["high_wind_flag"] == 0) &
        (merged["poor_air_quality_flag"] == 0)
    ).astype(int)

    # ===========================================================================
    # DATA QUALITY DIAGNOSTICS
    # ===========================================================================
    # Log summary stats for debugging and validation
    logging.info("Merged DataFrame Summary Statistics:\n%s", merged.describe(include="all"))
    logging.info("Null Values Count:\n%s", merged.isnull().sum())
    logging.info("Duplicate Rows Count: %d", merged.duplicated().sum())

    # ===========================================================================
    # FINAL CLEANUP
    # ===========================================================================
    # Remove duplicate rows (shouldn't happen, but good practice)
    before_dupes = len(merged)
    merged = merged.drop_duplicates()
    logging.info("Duplicate rows removed: %d", before_dupes - len(merged))

    # Forward fill then backward fill to handle missing values
    # This is safe because we're working with time-series data
    merged = merged.ffill().bfill()
    
    # Ensure date column is datetime type
    merged["date"] = pd.to_datetime(merged["date"])
    
    # Coerce all non-date columns to numeric (handles any string artifacts)
    numeric_cols = merged.columns.drop("date")
    merged[numeric_cols] = merged[numeric_cols].apply(pd.to_numeric, errors="coerce")
    
    # Final sort by date and reset index
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
    
    Args:
        df: Transformed DataFrame to load
        table_name: Target table name (default: open_meteo_merged)
        
    Behavior:
        - Replaces the table on each run (idempotent ETL)
        - Raises exception on failure (caller should handle)
        
    IMPORTANT: Update the connection string with your actual PostgreSQL credentials.
    Current config assumes local PostgreSQL (localhost:5432) with default credentials.
    This will NOT work in Databricks serverless environments.
    """
    logging.info("Starting load into PostgreSQL")

    # Incremental Loading Note:
    # Forecast data changes on every API call and does not contain stable primary keys.
    # Because values shift daily and timestamps are regenerated, incremental loading
    # is not applicable. The table is overwritten each run to ensure the latest forecast.

    # Build SQLAlchemy connection engine
    # Format: postgresql+psycopg2://username:password@host:port/database
    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"
    )

    try:
        # Write DataFrame to PostgreSQL
        # if_exists="replace": Drop and recreate table (idempotent behavior)
        # index=False: Don't write DataFrame index as a column
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
        # Log error and re-raise for caller to handle
        logging.exception("PostgreSQL load failed: %s", e)
        raise


def load_to_unity_catalog(df, table_name="main.weather.hourly_merged"):
    """
    Load the final merged DataFrame into Unity Catalog.
    Alternative to PostgreSQL for Databricks-native storage.
    
    Args:
        df: Pandas DataFrame to save
        table_name: Fully qualified table name (catalog.schema.table)
        
    Behavior:
        - Converts pandas to Spark DataFrame
        - Writes to Unity Catalog using Delta Lake format
        - Overwrites table on each run (idempotent)
        
    Requirements:
        - Must be run in a Databricks environment with Unity Catalog enabled
        - Table location must exist (catalog and schema)
        - User must have CREATE TABLE permissions on the schema
    """
    logging.info("Starting load into Unity Catalog")
    
    # Incremental Loading Note:
    # Forecast data does not support incremental loading because values change
    # with each API refresh and no stable keys exist. Overwrite mode ensures the
    # table always reflects the most recent forecast.
    
    try:
        # Convert pandas DataFrame to Spark DataFrame
        # Note: 'spark' is a built-in object in Databricks notebooks
        spark_df = spark.createDataFrame(df)
        
        # Write to Unity Catalog
        # mode("overwrite"): Replace table if it exists
        # saveAsTable(): Writes to managed Delta table in Unity Catalog
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
    Full ETL pipeline orchestration: Extract → Merge → Transform → Load
    
    Args:
        load_destination: Where to save the final data
            - "postgres": Load to PostgreSQL database
            - "unity_catalog": Load to Databricks Unity Catalog
            - "skip": Don't load, just return DataFrame (testing mode)
        table_name: Target table name (optional, uses defaults if None)
    
    Returns:
        DataFrame: Transformed data (always returned, even if loaded)
        None: If extraction fails
        
    Execution Flow:
        1. Extract weather/soil data (7-day forecast)
        2. Extract air quality/pollen data (limited to 2026-06-05)
        3. Merge both datasets on timestamp (inner join)
        4. Transform: standardize columns, compute scores, create flags
        5. Load to destination (if specified)
        
    Error Handling:
        - Both extraction functions use tuple returns (df, error)
        - If either extraction fails, pipeline aborts and returns None
        - Load failures raise exceptions (caller should handle)
    """
    logging.info("===== ETL PIPELINE STARTED =====")

    # ===========================================================================
    # STEP 1: EXTRACT WEATHER DATA
    # ===========================================================================
    weather_df, error = extract_data()
    if error:
        logging.error("Pipeline aborted: %s", error)
        return None  # Stop pipeline if weather extraction fails

    # ===========================================================================
    # STEP 2: EXTRACT AIR QUALITY DATA
    # ===========================================================================
    air_df, error = extract_air_quality_pollen()
    if error:
        logging.error("Pipeline aborted: %s", error)
        return None  # Stop pipeline if air quality extraction fails

    # ===========================================================================
    # STEP 3: MERGE DATASETS
    # ===========================================================================
    # Inner join on timestamp ensures we only keep hours with both datasets
    merged = merge_weather_air_quality(weather_df, air_df)

    # ===========================================================================
    # STEP 4: TRANSFORM DATA
    # ===========================================================================
    # Standardize columns, compute scores, create flags, clean data
    merged = transform_data(merged)

    # ===========================================================================
    # STEP 5: LOAD DATA
    # ===========================================================================
    # Route to appropriate destination based on load_destination parameter
    if load_destination == "postgres":
        if table_name:
            load_to_postgres(merged, table_name)
        else:
            load_to_postgres(merged)  # Use default table name
            
    elif load_destination == "unity_catalog":
        if table_name:
            load_to_unity_catalog(merged, table_name)
        else:
            load_to_unity_catalog(merged)  # Use default table name
            
    else:
        # Skip loading (useful for testing or when caller wants to handle storage)
        logging.info("Load skipped (destination: %s)", load_destination)

    logging.info("===== ETL PIPELINE COMPLETED =====")
    
    # Always return the transformed DataFrame (even if loaded)
    # This allows caller to preview results or perform additional analysis
    return merged


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    """
    Main execution block (runs when script is called directly).
    
    This section only executes when the file is run as a script, NOT when imported.
    Useful for testing the pipeline locally or scheduling as a job task.
    
    To customize:
        - Change load_destination to "postgres" or "unity_catalog" for production
        - Set table_name parameter to control destination table name
        - Adjust logging level in setup_logging() for more/less detail
    """
    # Setup logging (must be called before any other functions)
    setup_logging()
    
    # Run the full ETL pipeline
    # Currently set to "skip" load for testing - change for production use
    result_df = run_pipeline(load_destination="skip")
    
    # Display results summary if pipeline succeeded
    if result_df is not None:
        print("\n" + "="*80)
        print("Pipeline completed successfully!")
        print(f"Final dataset: {result_df.shape[0]} rows × {result_df.shape[1]} columns")
        print("="*80)
        print("\nFirst 5 rows:")
        print(result_df.head())
    else:
        # Pipeline failed during extraction
        print("\n" + "="*80)
        print("Pipeline failed during extraction. Check logs for details.")
        print("="*80)
