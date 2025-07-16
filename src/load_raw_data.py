import pandas as pd
import os
from .config import RAW_DATA_PATH
from .logger import setup_logger

logger = setup_logger()

def load_raw_datasets(data_dir=RAW_DATA_PATH):
    """
    Load all raw CSV files from the given directory into a dictionary of DataFrames.

    Expected files:
        - calendar.csv
        - cities_lookup.csv
        - products.csv
        - sales.csv
        - stores.csv

    Args:
        data_dir (str): Path to the directory containing raw CSVs.

    Returns:
        dict: Dictionary where keys are table names and values are DataFrames.
    """
    file_map = {
        "calendar": "calendar.csv",
        "cities_lookup": "cities_lookup.csv",
        "products": "products.csv",
        "sales": "sales.csv",
        "stores": "stores.csv"
    }

    dataframes = {}
    for name, filename in file_map.items():
        path = os.path.join(data_dir, filename)
        try:
            df = pd.read_csv(path)
            dataframes[name] = df
            logger.info(f"Loaded '{name}' ({df.shape[0]} rows, {df.shape[1]} columns)")
        except FileNotFoundError:
            logger.error(f"ERROR: File not found -> {filename}")
        except pd.errors.ParserError:
            logger.error(f"ERROR: Failed to parse -> {filename}")
        except Exception as e:
            logger.exception(f"ERROR loading '{filename}': {e}")

    return dataframes
