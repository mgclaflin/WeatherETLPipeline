
import pandas as pd
from scripts.logger import logger
from scripts.encodings_setup import load_env_api, load_env_cities, encoding, encodings_to_config
from scripts.extract import city_weather_data_extraction, write_raw_data, write_compiled_raw_data
from scripts.transform import read_raw_data, transform_data, write_to_cleaned_data
from scripts.load import read_clean_data, env_db_connection, get_or_insert_location, get_or_insert_weather, insert_record, insert_alert, delete_clean_data, delete_raw_data

def run_pipeline():
    try:
        print("Starting the ETL process \n")
        logger.info("Starting the ETL process")

        print("Encoding cities \n")
        logger.info("Encoding cities")
        #Encoding
        api_key = load_env_api()
        cities = load_env_cities()
        encodings = encoding(api_key, cities)
        encodings_to_config(encodings)
        print("Encoding Complete \n")
        logger.info("Encoding complete")

        #Extract Data
        print("Extracting Data \n")
        logger.info("Extracting data")
        weather_data = city_weather_data_extraction()
        if weather_data is None:
            logger.error("Weather data extraction failed")
            return
        write_raw_data(weather_data)
        print("Data Extraction Complete")
        logger.info("Data extraction complete")


        #Transform Data
        print("Transforming Data \n") 
        logger.info("Transforming Data")
        data = read_raw_data()
        df = transform_data(data)
        if df is None:
            logger.error("Data transformation failed")
            return
        write_to_cleaned_data(df)
        print("Transforming Data Complete \n")
        logger.info("Data Transformation Complete")

        #Load Data
        print("Loading Data \n")
        logger.info("Loading Data")
        df = read_clean_data()
        if df is None or df.empty:
            logger.error("No clean data available for loading")
            return
        
        conn = env_db_connection()
        if conn is None:
            logger.error("Database connection failed")
            return

        try:
            with conn.cursor() as cursor:
                for _, row in df.iterrows():
                    location_id = get_or_insert_location(cursor, row['latitude'], row['longitude'], row['city'], row['timezone'], row['timezone_offset'])
                    weather_id = get_or_insert_weather(cursor, row['weather_id'], row['weather_main'], row['weather_description'])
                    record_id = insert_record(cursor, location_id, weather_id, row)

                    if pd.notna(row['alerts']) and row['alerts'].strip():
                        insert_alert(cursor, record_id, row['alerts'])
                conn.commit()
            logger.info("data inserted successfully")
        except Exception as e:
            logger.error(f"Error while inserting data: {e}")
        finally:
            conn.close()
            print("data inserted successfully & connection closed")
            logger.info("Database connection closed")
            
        delete_clean_data()
        delete_raw_data()
        print("Loading Data Complete \n")
        logger.info("Loading Data Complete")

        print("ETL process completed successfully")
        logger.info("ETL process completed successfully")
        
    except Exception as e:
        logger.critical(f"Pipeline execution failed: {e}", exc_info=True)

run_pipeline()
