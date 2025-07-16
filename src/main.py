# CityRetail ETL Orchestration
import sys
import os
from .load_raw_data import load_raw_datasets  
from .clean_data import standardize_city_names, parse_calendar_dates, save_cleaned_dataframes 
from .load_to_postgres import run_load  
from .config import CLEANED_DATA_PATH  # ensures RAW, CLEANED, LOGS paths created at import time
from .incremental_etl import run_incremental_clean_and_load_all, should_use_incremental 
from .logger import setup_logger  

# Setup logger
logger = setup_logger()

def cleaned_files_exist():
    """
    Check if all required cleaned CSV files already exist in the output directory.

    Returns:
        bool: True if all expected cleaned files exist, otherwise False.
    """
    expected_files = [
        "calendar.csv",
        "products.csv",
        "sales.csv",
        "stores.csv"
    ]
    exists = all(os.path.exists(os.path.join(CLEANED_DATA_PATH, f)) for f in expected_files)
    if exists:
        logger.info("Detected all cleaned CSV files.")
    else:
        logger.info("Missing or outdated cleaned files. Triggering preprocessing.")
    return exists

def run_etl(force_preprocessing=False, incremental=False):
    """
    Orchestrate the full or incremental ETL process:
    - If incremental: only new records are processed and loaded.
    - Otherwise: raw files are loaded, cleaned, saved, and inserted into the database.

    Args:
        force_preprocessing (bool): If True, force re-cleaning even if cleaned files exist.
        incremental (bool): If True, run in incremental mode using existing DB state.

    Returns:
        None
    """
    logger.info("Starting CityRetail ETL process...")

    if incremental:
        # Run incremental ETL mode
        print("Running in INCREMENTAL mode...")
        logger.info("Incremental mode: Processing only new rows from raw files.")
        run_incremental_clean_and_load_all()
        print("Incremental ETL process complete!")
        logger.info("Incremental ETL process complete.")
        return

    if not force_preprocessing and cleaned_files_exist():
        # Skip cleaning if files already exist and not forced
        print("Cleaned files already exist. Skipping preprocessing.")
        logger.info("Cleaned files already exist. Skipping raw extraction and cleaning.")
    else:
        logger.info("Full mode: Cleaning and loading entire dataset from scratch.")
        print("Running full preprocessing of raw files...")

        # Load raw data
        raw_data = load_raw_datasets()
        calendar = raw_data["calendar"]
        products = raw_data["products"]
        sales = raw_data["sales"]
        stores = raw_data["stores"]
        cities_lookup = raw_data["cities_lookup"]

        # Clean datasets
        print("Cleaning and transforming data...")
        stores_clean = standardize_city_names(stores, cities_lookup)
        calendar_clean = parse_calendar_dates(calendar)
        products_clean = products.copy()
        sales_clean = sales.copy()

        # Save cleaned files
        print(f"Saving cleaned data to '{CLEANED_DATA_PATH}/'")
        logger.info("Saving cleaned data to disk...")
        cleaned_data = {
            "calendar": calendar_clean,
            "products": products_clean,
            "sales": sales_clean,
            "stores": stores_clean
        }
        save_cleaned_dataframes(cleaned_data, output_dir=CLEANED_DATA_PATH)

    # Load cleaned data into PostgreSQL
    print("Loading data into PostgreSQL...")
    logger.info("Running full table load to PostgreSQL...")
    run_load()
    print("ETL process complete!")
    logger.info("Full ETL process complete.")

if __name__ == "__main__":
    try:
        # Check for optional CLI flags
        force = "--force" in sys.argv

        # Auto-detect incremental if no flag is passed
        if "--incremental" in sys.argv:
            incremental = True
        else:
            incremental = should_use_incremental()

        logger.info(f"Running in {'incremental' if incremental else 'full'} mode...")
        run_etl(force_preprocessing=force, incremental=incremental)

    except Exception as e:
        logger.exception("ETL run failed due to an unexpected error.")
        print(f"ETL run failed: {e}. See logs for details.")
        raise
