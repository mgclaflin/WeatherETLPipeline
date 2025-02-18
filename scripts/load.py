
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from scripts.config import ENV_PATH, RAW_DATA_PATH, CLEAN_DATA_PATH  # Import paths


# function to read the clean_data file and return a dataframe
def read_clean_data():

    df_db = pd.read_csv(CLEAN_DATA_PATH)
    return df_db

# function to load env variables & establish database connection
def env_db_connection():
    ## load environment variables and establish database connection

    load_dotenv(ENV_PATH)

    ## get local database credentials
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")

    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT)
        print("connected to postgresql on local host")
        return conn
    except psycopg2.Error as e:
        print("connection error: {e}")

# function to insert weather data into the location database table
# Insert or get Location_ID
def get_or_insert_location(cursor, lat, lon, city, timezone, tz_offset):
    cursor.execute(
        "SELECT Location_ID FROM Locations WHERE Lat=%s AND Long=%s;",
        (lat, lon)
    )
    location = cursor.fetchone()
    if location:
        print("location is already stored in the database")
        return location[0]
    else:
        cursor.execute(
            "INSERT INTO Locations (Lat, Long, City, Timezone, Timezone_offset) VALUES (%s, %s, %s, %s, %s) RETURNING Location_ID;",
            (lat, lon, city, timezone, tz_offset)
        )
        return cursor.fetchone()[0]

# function to insert weather data into the weather database table
# Insert or get Weather_ID
def get_or_insert_weather(cursor, weather_id, main, description):
    cursor.execute(
        "SELECT Weather_ID FROM Weather where Weather_ID=%s;",
        (weather_id,)
    )
    weather = cursor.fetchone()
    if weather:
        print("weather is already stored in the database")
        return weather[0]
    else:
        cursor.execute(
            "INSERT INTO Weather (Weather_ID, Main, Description) VALUES (%s, %s, %s) RETURNING Weather_ID;",
            (weather_id, main, description)
        )
        return cursor.fetchone()[0]

# function to insert weather data into the record database table
# Insert Record

def insert_record(cursor, location_id, weather_id, row):
    cursor.execute(
    """
    INSERT INTO Records (Location_ID, Weather_ID, Local_time, Sunrise, Sunset, Temp_F, Feels_like_F, 
                        Humidity, Dew_Point, UVI, Clouds, Visibility, Wind_speed_mph, Wind_deg, Wind_gust_mph)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING Record_ID;
    """,
    (location_id, weather_id, row['current_time'], row['sunrise'], row['sunset'], row['temp_F'],
        row['feels_like_F'], row['humidity'], row['dew_point'], row['uvi'], row['clouds'], row['visibility'], 
        row['wind_speed_mph'], row['wind_deg'], row['wind_gust_mph'])
    )
    print("record has been inserted into the table")
    return cursor.fetchone()[0]

# function to insert weather data into the alert database table
# Insert Alert
def insert_alert(cursor, record_id, alert_description):
    cursor.execute(
        "INSERT INTO Alerts (Record_ID, Description) VALUES (%s, %s);",
        (record_id, alert_description)
    )
    print("alert has been inserted into the table")

# function to delete the clean_data csv file once the data has been uploaded to avoid duplication of data loading/records
def delete_clean_data():
    if os.path.exists(CLEAN_DATA_PATH):
        os.remove(CLEAN_DATA_PATH)
        print("File deleted successfully")
    else:
        print("File does not exist")
        
# function to delete the raw_data csv file once the data has been uploaded to avoid duplication of data loading/records
def delete_raw_data():
    if os.path.exists(RAW_DATA_PATH):
        os.remove(RAW_DATA_PATH)
        print("File deleted successfully")
    else:
        print("File does not exist")
        

# execution of functions above and the load process

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
