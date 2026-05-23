🌱 Open‑Meteo Environmental Analytics Pipeline
Weather • Soil • Air Quality • Pollen • Feature Engineering • Decision Flags
This project builds a complete environmental decision‑support dataset using the Open‑Meteo API suite. It integrates weather, soil, air quality, and pollen data into a single engineered dataset with custom scoring models and operational flags.

The final output is a clean, analysis‑ready CSV suitable for dashboards, SQL databases, and reporting.

📁 Repository Structure
Code
/open-meteo-project
│
├── data/
│   └── merged_open_meteo_final.csv
│
├── notebooks/
│   └── open_meteo_pipeline.ipynb
│
└── README.md
notebooks/ contains the full Google Colab workflow

data/ contains the final engineered dataset

README.md documents the project, methodology, and features
🌤️ Data Sources
All data is retrieved programmatically using the Open‑Meteo API:

Weather & Soil

Temperature

Humidity

Precipitation probability

Wind speed

Soil temperature

Soil moisture

Air Quality

PM2.5, PM10

Ozone, NO₂, SO₂, CO

U.S. AQI and pollutant‑specific AQI components

Pollen

Grass

Ragweed

Birch

Alder

Mugwort

Olive

All timestamps are converted to local time and standardized for merging.

🔧 ETL Pipeline Summary
The notebook performs the following steps:

API Requests  
Pulls hourly weather, soil, air quality, and pollen data.

Cleaning & Standardization

Lowercase, underscore‑safe column names

Datetime parsing and timezone removal

Missing pollen values filled with zero for scoring

Merging  
Weather/soil + air/pollen merged on a clean datetime index.

Feature Engineering

Two composite scores

Seven operational flags

One best‑day composite flag

Final Export  
merged_open_meteo_final.csv

🌿 Composite Scores
1. Planting Readiness Score (0–100)
A weighted model estimating planting suitability using:

Soil temperature (40%)

Precipitation probability (20%)

Wind speed (20%)

Soil moisture (20%)

2. Allergy Risk Score (0–100)
A weighted model estimating allergy burden using:

PM2.5 (25%)

Ozone (15%)

Average pollen concentration (60%)

Both scores are clipped to the 0–100 range.

🚦 Operational Flags (Binary Indicators)
These flags convert environmental conditions into simple, actionable signals.

Flag	Meaning	Threshold
high_wind_flag	Unsafe wind conditions	wind_speed_10m > 15 mph
rain_expected_flag	Rain likely	precipitation_probability > 50%
soil_too_wet_flag	Soil unsuitable for planting	soil_moisture_0_to_1cm > 0.35
poor_air_quality_flag	AQI unhealthy for sensitive groups	us_aqi > 100
high_pollen_flag	High allergy burden	avg pollen > 150
heat_stress_flag	Heat caution threshold	temperature_2m ≥ 85°F
respiratory_risk_flag	Elevated respiratory burden	allergy_risk ≥ 60


⭐ Best Overall Day Flag
A composite indicator identifying optimal outdoor days:

A day is marked as best_overall_day_flag = 1 when:

Planting readiness ≥ 65

Allergy risk ≤ 40

No rain expected

No high wind

AQI ≤ 100

This flag summarizes multiple environmental factors into a single “go/no‑go” signal.

📦 Final Deliverable
merged_open_meteo_final.csv
This file contains:

Clean hourly timestamps

Weather + soil variables

Air quality + pollen variables

Composite scores

Seven operational flags

Best overall day indicator

It is fully ready for:

SQL loading

Dashboard creation

Modeling

Reporting

🔁 Reproducibility
The entire pipeline is contained in:

notebooks/open_meteo_pipeline.ipynb

Running the notebook will regenerate:

all API pulls

all transformations

all engineered features

the final CSV
