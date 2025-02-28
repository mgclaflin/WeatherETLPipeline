# WeatherETLPipeline

Here’s a template for your **Weather ETL Pipeline** project README file:

---

# Weather ETL Pipeline

This project demonstrates an **ETL (Extract, Transform, Load)** pipeline that collects weather data from an API, processes it, and stores it in a **PostgreSQL** database for later analysis. The pipeline is automated to run regularly, updating the database with fresh weather data at specified intervals.

## Project Overview

The main goal of this project is to showcase **data engineering** skills, including:
- Extracting data from an API (Weather API)
- Transforming data into the desired format
- Loading the data into a **PostgreSQL** database
- Automating the ETL process with scheduling

## Features
- **Data Extraction**: Weather data is fetched from a public weather API (e.g., OpenWeatherMap).
- **Data Transformation**: Data is cleaned and transformed to fit the schema of the PostgreSQL database.
- **Data Loading**: Transformed data is inserted into a PostgreSQL database for storage.
- **Automation**: The ETL process runs regularly to keep the weather data up-to-date.

## Prerequisites

- Python 3.x
- PostgreSQL database
- Access to a weather API (e.g., OpenWeatherMap, WeatherAPI)
- Required Python libraries (listed below)

## Installation

1. **Clone the repository**:


2. **Set up a Python virtual environment**:


4. **Install dependencies**:


5. **Set up PostgreSQL database**:
   - Create a PostgreSQL database and note down the connection credentials.
   - Run the SQL script to create the required tables: schema.sql


6. **Configure the Weather API**:
   - Sign up for an API key from your chosen weather service (e.g., [OpenWeatherMap](https://openweathermap.org/)).
   - Set the API key and other configuration details in `config.py`.


## Usage

1. **Running the ETL Pipeline**:
   - To run the entire ETL pipeline manually:
     ```bash
     python src/etl_pipeline.py
     ```

2. **Automating the ETL Process**:
   - Use **cron** (Linux/macOS) or **Task Scheduler** (Windows) to run the pipeline at regular intervals (e.g., every 10 minutes).
   - Example cron job to run every minute for the next 10 minutes:
     ```bash
     * * * * * /usr/bin/python3 /path/to/etl_pipeline.py
     ```

3. **Viewing the Data**:
   - After the ETL process runs, data will be stored in the PostgreSQL database.
   - You can query the data using SQL to analyze the weather patterns.

## Dependencies

- `requests`: For making HTTP requests to the weather API.
- `psycopg2`: For PostgreSQL database interaction.
- `pandas`: For data manipulation and transformation.
- `schedule`: For scheduling the ETL process.
- `dotenv`: For loading environment variables (API keys, DB credentials).

You can install all dependencies by running:
```bash
pip install -r requirements.txt
```

# Environment Variables Setup

To run this project, you will need to configure the environment variables required for the application to connect to external services like PostgreSQL, API endpoints, etc. Below is a list of the required variables that need to be added to your `.env` file:

## `.env` File

Create a `.env` file in the root directory of the project and add the following variables:

```env
# Database Configuration
DB_NAME=your_database_name        # Name of your PostgreSQL database
DB_USER=your_database_user        # Username to access the database
DB_PASSWORD=your_database_password # Password for the database user
DB_HOST=localhost                 # Host where the PostgreSQL database is running
DB_PORT=5432                      # Port where PostgreSQL is exposed (default: 5432)

# API Configuration (for OpenWeather API)
API_KEY=your_api_key              # API key for accessing external APIs (if needed)

# Cities that you want to extract data for (deliminated with ";")
CITIES=list_of_cities (ex: Denver,CO,USA;Austin,TX,USA;Stuttgart,DE)
