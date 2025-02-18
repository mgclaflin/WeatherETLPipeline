

import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from scripts.config import ENV_PATH, CITIES_CONFIG_PATH  # Import paths


# loading environment variables (API key)
def load_env_api():
    
    # load envrionment variables from the .env file
    load_dotenv(ENV_PATH)

    # get the API key from the .env file
    api_key = os.getenv("API_KEY")
    return api_key

# load list of cities to query the API about
def load_env_cities():
    
    # load envrionment variables from the .env file
    load_dotenv(ENV_PATH)
    
    # get the list of cities from the .env file
    cities_str = os.getenv("CITIES")
    cities = cities_str.split(";")
    return cities

# getting latitude and longitude encodings for cities (list of cities in config file)
## function for getting lat & long encoding of cities
def encoding(api_key, cities):
    
    # Geocoding API endpoint
    geocoding_url = "http://api.openweathermap.org/data/2.5/weather"

    # create dataframe to store encodings
    encodings = pd.DataFrame(columns=['name', 'latitude', 'longitude'])
    
    # Loop through the cities and get their lat, lon
    for city in cities:
        # Send GET request to the OpenWeatherMap Geocoding API
        response = requests.get(geocoding_url, params={
            'q': city,
            'appid': api_key
        })

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            lat = data['coord']['lat']
            lon = data['coord']['lon']
            print(f"City: {city} - Latitude: {lat}, Longitude: {lon}")
            new_row = pd.DataFrame({"name": [city], "latitude": [lat], "longitude": [lon]})
            encodings = pd.concat([encodings, new_row], ignore_index=True)
        else:
            print(f"Failed to get data for {city}")
            
    return encodings;

## write the city encodings to the config file
def encodings_to_config(encodings):
    
    # Convert the DataFrame to the desired dictionary format
    config_data = {
        "cities": encodings.to_dict(orient="records")  # Convert rows to list of dictionaries
    }

    # Write the dictionary to a JSON file
    with open(CITIES_CONFIG_PATH, 'w') as json_file:
        json.dump(config_data, json_file, indent=4)

    print("Data has been written to cities_config.json")

# executing the defined functions above 
api_key = load_env_api()
cities = load_env_cities()
encodings = encoding(api_key, cities)
encodings_to_config(encodings)
