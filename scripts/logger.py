
import logging
from scripts.config import LOG_PATH

# clear existing handlers to prevent duplicates
logger = logging.getLogger(__name__)
logger.handlers.clear()

# Configure Logging w/ the correct log file path
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger.info("Logger initialized successfully.")
