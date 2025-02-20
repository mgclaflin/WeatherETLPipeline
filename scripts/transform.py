
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from scripts.logger import logger
from scripts.config import RAW_DATA_PATH, CLEAN_DATA_PATH  # Import paths


# reading the raw data from the raw_weather_data.json file
def read_raw_data():
    
    try:
        with open(RAW_DATA_PATH, 'r') as json_file:
            data = json.load(json_file)
        logger.info(f"Successfully loaded raw weather data from {RAW_DATA_PATH}")
        return data
    except FileNotFoundError:
        print(f"Error: the file '{RAW_DATA_PATH}' does not exist")
        logger.error(f"Error: the file {RAW_DATA_PATH} does not exist")
        return None
    except json.jSONDecodeError as e:
        print(f"Error: failed to decode JSON frim the file '{RAW_DATA_PATH}'")
        logger.error(f"Error: failed to decode JSON from the file {RAW_DATA_PATH}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while reading raw data: {e}")
        return None

# Function to convert Unix timestamp to local time
def convert_to_local_time(timestamp, offset):
    try:
        utc_time = datetime.utcfromtimestamp(timestamp)
        return utc_time + timedelta(seconds=offset)
    except Exception as e:
        logger.error(f"Error converting timestamp {timestamp} with offset {offset}: {e}")
        return None

# processing & cleaning the weather data & storing in a dataframe
def transform_data(weather_data):
    if not weather_data:
        logger.error("No data provided for transformation")
        return None
    
    # Initialize an empty list to store records
    data = []

    try:
        # Process each record
        for record in weather_data:
            latitude, longitude = record["lat"], record["lon"]
            timezone = record["timezone"]
            timezone_offset = record["timezone_offset"]
            city = record['City']
            
            if latitude is None or longitude is None:
                logger.warning("skipping record due to missing latitude/longitude")
                continue

            # Convert timestamps
            current_time = convert_to_local_time(record["current"]["dt"], timezone_offset)
            sunrise = convert_to_local_time(record["current"]["sunrise"], timezone_offset)
            sunset = convert_to_local_time(record["current"]["sunset"], timezone_offset)

            # Extract weather details
            temp = record["current"]["temp"]
            feels_like = record["current"]["feels_like"]
            pressure = record["current"]["pressure"]
            humidity = record["current"]["humidity"]
            dew_point = record["current"]["dew_point"]
            uvi = record["current"]["uvi"]
            clouds = record["current"]["clouds"]
            visibility = record["current"]["visibility"]
            wind_speed = record["current"]["wind_speed"]
            wind_deg = record["current"]["wind_deg"]
            wind_gust = record["current"].get("wind_gust", 0)
            weather = record["current"]["weather"][0]
            weather_id = weather["id"]
            weather_main = weather["main"]
            weather_description = weather["description"]

            # Handle alerts (if any)
            alerts = record.get("alerts", [])
            alert_messages = "; ".join([alert["event"] + ": " + alert["description"] for alert in alerts])

            # Add the record to the data list
            data.append({
                "latitude": latitude,
                "longitude": longitude,
                "timezone": timezone,
                "timezone_offset": timezone_offset,
                "city": city,
                "current_time": current_time,
                "sunrise": sunrise,
                "sunset": sunset,
                "temp_F": temp,
                "feels_like_F": feels_like,
                "humidity": humidity,
                "dew_point": dew_point,
                "uvi": uvi,
                "clouds": clouds,
                "visibility": visibility,
                "wind_speed_mph": wind_speed,
                "wind_deg": wind_deg,
                "wind_gust_mph": wind_gust,
                "weather_id": weather_id,
                "weather_main": weather_main,
                "weather_description": weather_description,
                "alerts": alert_messages
            })

        # Create a DataFrame from the data list
        df = pd.DataFrame(data)
        logger.info("Successfully transformed weather data into DataFrame")
        return df
    except KeyError as e:
        logger.error(f"Missing expected key in weather data: {e}")
        return None
    except Exception as e:
        logger.erorr(f"Unexpected error during transformation: {e}")
        return None


# writing the cleaned data to the compiled clean_weather_data.csv file
def write_to_cleaned_data(df):
    if df is None or df.empty:
        logger.error("No data available to write to CSV")
        return

    try:
        # Check if the file exists to decide whether to append or create new
        if os.path.exists(CLEAN_DATA_PATH):
            # If the file exists, load the existing data, then append new data
            existing_data = pd.read_csv(CLEAN_DATA_PATH)
            updated_data = pd.concat([existing_data, df], ignore_index=True)

            # Append to the file
            updated_data.to_csv(CLEAN_DATA_PATH, index=False)
        else:
            # If the file doesn't exist, create it and write the new data
            df.to_csv(CLEAN_DATA_PATH, index=False)

        print(f"Data saved to {CLEAN_DATA_PATH}")
        logger.info(f"Data successfully saved to {CLEAN_DATA_PATH}")
    except Exception as e:
        logger.error(f"Error writing data to CSV: {e}")


try:
    
    # execution of the transformation functions
    data = read_raw_data()
    df = transform_data(data)
    write_to_cleaned_data(df)
except Exception as e:
    logger.critical(f"Pipeline execution failed at transfrom.py script: {e}")
