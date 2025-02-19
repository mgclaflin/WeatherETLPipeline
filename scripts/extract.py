
import os
import json
import requests
from dotenv import load_dotenv
from scripts.logger import logger
from scripts.config import ENV_PATH, CITIES_CONFIG_PATH, RAW_DATA_PATH, RAW_COMPILED_PATH  # Import paths


# loading environment variables (API key)
def load_env_api():
    
    # load envrionment variables from the .env file
    load_dotenv(ENV_PATH)

    # get the API key from the .env file
    api_key = os.getenv("API_KEY")
    return api_key

# function to execute the API call
# api call to get the current weather 
def get_weather(api_key, city, lat, lon, exclude='minutely,daily,hourly', units='imperial', lang='en'):
    # Build the base URL for the OneCall API
    api_url = f"https://api.openweathermap.org/data/3.0/onecall"
    
    # Prepare the parameters for the API call
    params = {
        'lat': lat,
        'lon': lon,
        'appid': api_key,
        'units': units,  # 'imperial' for Fahrenheit, 'metric' for Celsius
        'lang': lang      # Language for the response
    }
    
    # Add the 'exclude' parameter if it's provided
    if exclude:
        params['exclude'] = exclude
    try:
        # Make the API request
        response = requests.get(api_url, params=params)

        # Check if the request was successful
        data = response.json()
        # Print or process the data
        data['City']=city
        print(data)
        logger.info(f"Successfully fetched data from {api_url}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error: {response.status_code}, {response.text}")
        logger.error(f"Error fetching data from {api_url}: {e}")
        return None

# function to run the API call based upon cities in config file
def city_weather_data_extraction():
    # Step 1: Load the config file
    with open(CITIES_CONFIG_PATH, 'r') as f:
        config_data = json.load(f)

    weather_data = []

    # Step 2: Loop through each city and use the data for API requests
    for city in config_data['cities']:
        api_key = load_env_api()
        latitude = city['latitude']
        longitude = city['longitude']
        city_name = city['name']

        weather_data.append(get_weather(api_key, city_name, latitude, longitude))
        
    return weather_data

# writing the extracted data to the raw_weather_data.json file
def write_raw_data(weather_data):

    # Check if the file exists to decide whether to append or create new
    if os.path.exists(RAW_DATA_PATH):
        # If the file exists, load the existing data, then append new data
        with open(RAW_DATA_PATH, 'r') as json_file:
            existing_data = json.load(json_file)
            existing_data.extend(weather_data)

        # Append to the file
        with open(RAW_DATA_PATH, 'w') as json_file:
            json.dump(existing_data, json_file, indent=4)
    else:
        # If the file doesn't exist, create it and write the new data
        with open(RAW_DATA_PATH, 'w') as json_file:
            json.dump(weather_data, json_file, indent=4)

    print(f"Data saved to {RAW_DATA_PATH}")
    
# writing the extracted data to the raw_compiled_data.json file
def write_compiled_raw_data(weather_data):

    # Check if the file exists to decide whether to append or create new
    if os.path.exists(RAW_COMPILED_PATH):
        # If the file exists, load the existing data, then append new data
        with open(RAW_COMPILED_PATH, 'r') as json_file:
            existing_data = json.load(json_file)
            existing_data.extend(weather_data)

        # Append to the file
        with open(RAW_COMPILED_PATH, 'w') as json_file:
            json.dump(existing_data, json_file, indent=4)
    else:
        # If the file doesn't exist, create it and write the new data
        with open(RAW_COMPILED_PATH, 'w') as json_file:
            json.dump(weather_data, json_file, indent=4)

    print(f"Data saved to {RAW_COMPILED_PATH}")

# executing the api calls & writing to the file functions
weather_data = city_weather_data_extraction()
write_raw_data(weather_data)
write_compiled_raw_data(weather_data)
