
## using this for maintaining the directories
import os

# Define the base directory explicitly
BASE_DIR = r"C:\Users\matth\OneDrive\Desktop\DataEngineering\WeatherETLPipeline"

# Log file path
LOG_Path = os.path.join(BASE_DIR,"logs","pipeline.log")

# Common file paths
CITIES_CONFIG_PATH = os.path.join(BASE_DIR,"cities_config.json")
RAW_COMPILED_PATH = os.path.join(BASE_DIR, "data", "raw_compiled_data.json")
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw_weather_data.json")
CLEAN_DATA_PATH = os.path.join(BASE_DIR, "data", "clean_weather_data.csv")
ENV_PATH = os.path.join(BASE_DIR, ".env")  # If you store your env file here
LOG_PATH = os.path.join(BASE_DIR, "logs", "pipeline.log")  # Example log file path
