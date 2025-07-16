import os
import logging
import time
from .config import LOGS_PATH

# Force all log timestamps to use local time instead of UTC
logging.Formatter.converter = time.localtime

def setup_logger(log_name: str = "cityretail.etl", log_file: str = "etl.log") -> logging.Logger:
    """
    Set up a logger that writes both to a file and the console, using local timestamps.

    Args:
        log_name (str): Name of the logger. Default is "cityretail.etl".
        log_file (str): Filename for the log output. Default is "etl.log".

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Ensure log directory exists
    os.makedirs(LOGS_PATH, exist_ok=True)

    # Create the full path for the log file
    log_path = os.path.join(LOGS_PATH, log_file)

    # Get or create the logger
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)

    # Prevent multiple handlers from being added on repeat calls
    if not logger.handlers:

        # Define log format (timestamp, level, message)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

        # Create a file handler and apply formatter
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Create a console (stdout) handler and apply formatter
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
