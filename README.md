Green Thumbs & Runny Noses: A Forecasting Tool for Gardeners with Allergies
Open‑Meteo Environmental Analytics Pipeline
Weather • Soil • Air Quality • Pollen • Feature Engineering • Decision Flags

This project builds a complete environmental decision‑support dataset using the Open‑Meteo API suite. It integrates weather, soil, air quality, and pollen data into a single engineered dataset with custom scoring models and operational flags.

pipelines.ipynb
Purpose:  
End‑to‑end ETL pipeline that fetches environmental data from Open‑Meteo APIs and produces a unified dataset for database loading.

Workflow:

Fetches hourly weather, soil, air quality, and pollen data for Louisville, KY

Converts timestamps to local time (America/New_York)

Normalizes JSON responses into pandas DataFrames

Merges all variables into a single hourly dataset

Engineers composite scores (planting readiness, allergy risk)

Generates boolean environmental risk flags

Exports final dataset to data/merged_open_meteo_final.csv

Pipeline Flowchart
This diagram illustrates the data extraction and transformation process used to generate the final environmental dataset.
![Environmental Conditions Pipeline](assets/environmental_conditions_flowchart.png)

Key Features:

Uses three Open‑Meteo endpoints (Weather/Soil, Air Quality, Pollen)

Ensures consistent timestamp alignment across APIs

Handles missing values and unit conversions

Produces a clean, analysis‑ready dataset for PostgreSQL loading

Usage:

Code
Run all cells in pipelines.ipynb
schema.ipynb
Purpose:  
Programmatically generates the full SQL schema (documentation + CREATE TABLE statements) and writes it to schema.sql.

Demonstrates:

Constructing SQL schema strings in Python

Embedding documentation directly into SQL files

Writing .sql files from Python

Ensuring reproducible schema generation for database creation

Usage:

Code
Run all cells in schema.ipynb
schema.sql will be created automatically
initial_load.py
Purpose:  
Creates PostgreSQL tables using schema.sql and loads the processed dataset into the fact table.

Workflow:

Connects to PostgreSQL using psycopg2

Executes schema.sql to create all tables

Loads merged_open_meteo_final.csv into pandas

Inserts rows into fact_environmental_conditions

Closes database connection cleanly

Key Features:

Uses parameterized SQL inserts

Includes error handling for database operations

Ensures reproducible table creation and loading

Usage:

Code
python initial_load.py
Learning Outcomes
Students completing this project will:

Understand multi‑API extraction using Open‑Meteo

Learn how to normalize and merge heterogeneous environmental datasets

Practice timestamp handling and timezone conversion

Build a star schema with dimension and fact tables

Generate SQL schema files programmatically

Load structured data into PostgreSQL using Python

Interpret ERDs and relational database design principles

Data Reference
Environmental Variables
The merged dataset includes:

Weather: temperature, wind speed, precipitation probability

Soil: soil temperature, soil moisture

Air Quality: PM2.5, ozone, AQI

Pollen: grass, ragweed, birch, alder, mugwort, olive

Composite Scores: planting readiness, allergy risk

Flags: high wind, rain expected, poor air quality, high pollen, etc.

Source APIs
All data is sourced from:

👉 Open‑Meteo API  
https://open-meteo.com/en/docs

Free access

No authentication required

High‑resolution hourly environmental data

Supports weather, soil, air quality, and pollen endpoints

Requirements
Install dependencies from requirements.txt:

Code
pip install -r requirements.txt
Key libraries:

requests — API calls

pandas — data transformation

openmeteo_requests — optimized Open‑Meteo client

psycopg2-binary — PostgreSQL connection

numpy — numeric operations

ERD Reference
The ERD (database/erd.png) illustrates:

dim_datetime (time dimension)

dim_environmental_factors (location dimension)

fact_environmental_conditions (central fact table)

Relationships follow a star schema with 1‑to‑many cardinality.

Usage Summary
To reproduce the full pipeline:

Run pipelines.ipynb → generates merged CSV

Run schema.ipynb → generates schema.sql

Run initial_load.py → creates tables + loads data

View ERD in database/erd.png

Query your PostgreSQL database
