
import pandas as pd
from scripts.encodings_setup import load_env_api, load_env_cities, encoding, encodings_to_config
from scripts.extract import city_weather_data_extraction, write_raw_data, write_compiled_raw_data
from scripts.transform import read_raw_data, transform_data, write_to_cleaned_data
from scripts.load import read_clean_data, env_db_connection, get_or_insert_location, get_or_insert_weather, insert_record, insert_alert, delete_clean_data, delete_raw_data

def run_pipeline():
    print("Starting the ETL process \n")
    
    print("Encoding cities \n")
    #Encoding
    api_key = load_env_api()
    cities = load_env_cities()
    encodings = encoding(api_key, cities)
    encodings_to_config(encodings)
    print("Encoding Complete \n")
    
    #Extract Data
    print("Extracting Data \n")
    weather_data = city_weather_data_extraction()
    write_raw_data(weather_data)
    print("Data Extraction Complete")
    
    
    #Transform Data
    print("Transforming Data \n")    
    data = read_raw_data()
    df = transform_data(data)
    write_to_cleaned_data(df)
    print("Transforming Data Complete \n")
    
    #Load Data
    print("Loading Data \n")
    df = read_clean_data()
    conn = env_db_connection()

    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            location_id = get_or_insert_location(cursor, row['latitude'], row['longitude'], row['city'], row['timezone'], row['timezone_offset'])
            weather_id = get_or_insert_weather(cursor, row['weather_id'], row['weather_main'], row['weather_description'])
            record_id = insert_record(cursor, location_id, weather_id, row)

            if pd.notna(row['alerts']) and row['alerts'].strip():
                insert_alert(cursor, record_id, row['alerts'])
        conn.commit()
    conn.close()
    print("data inserted successfully & connection closed")
    delete_clean_data()
    delete_raw_data()
    print("Loading Data Complete \n")
    
    print("ETL process completed successfully")
    

run_pipeline()
