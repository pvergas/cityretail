import os
import psycopg2
import pandas as pd
import time
from .load_raw_data import load_raw_datasets
from .config import CLEANED_DATA_PATH
from .logger import setup_logger  

logger = setup_logger()

def load_csv_to_table(cursor, filepath, table_name):
    """
    Load data from a CSV file into a PostgreSQL table.
    Clears the table before inserting new data.

    Args:
        cursor (psycopg2 cursor): Database cursor.
        filepath (str): Full path to the cleaned CSV file.
        table_name (str): Target PostgreSQL table name.

    Returns:
        None
    """
    logger.info(f"Loading {table_name} from {filepath}")
    try:
        df = pd.read_csv(filepath)
        cursor.execute(f"DELETE FROM {table_name}")
        df = df.astype(object)
        records = df.values.tolist()
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.executemany(insert_query, records)
        logger.info(f"Inserted {len(records)} rows into {table_name}")
    except Exception as e:
        logger.error(f"Error loading table {table_name} from {filepath}", exc_info=True)
        raise

def wait_for_postgres(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS, retries=5, delay=2):
    """
    Retry connection to PostgreSQL database until available or fail after retries.

    Args:
        DB_HOST (str): Database host.
        DB_PORT (str): Database port.
        DB_NAME (str): Database name.
        DB_USER (str): Database user.
        DB_PASS (str): Database password.
        retries (int): Number of retry attempts. Default is 5.
        delay (int): Base delay between retries. Default is 2.

    Returns:
        None
    """
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            conn.close()
            logger.info("PostgreSQL is ready.")
            return
        except psycopg2.OperationalError as e:
            wait = delay ** attempt
            logger.warning(f"[Retry {attempt}/{retries}] PostgreSQL not ready: {e}. Retrying in {wait} seconds...")
            time.sleep(wait)
    logger.critical("PostgreSQL is not available after multiple attempts.")
    raise Exception("PostgreSQL is not available after multiple retries.")

def timed_load(filename, table_name, db_config):
    """
    Measure and log the time to load a CSV file into a PostgreSQL table.

    Args:
        filename (str): Name of the CSV file (without extension).
        table_name (str): Target table name in PostgreSQL.
        db_config (dict): Database connection parameters.

    Returns:
        None
    """
    start = time.time()
    filepath = os.path.join(CLEANED_DATA_PATH, f"{filename}.csv")
    try:
        conn = psycopg2.connect(**db_config)
        with conn:
            with conn.cursor() as cursor:
                load_csv_to_table(cursor, filepath, table_name)
        elapsed = time.time() - start
        logger.info(f"Loaded {table_name} in {elapsed:.2f} seconds")
    except Exception as e:
        logger.exception(f"Error loading {table_name}: {e}")

def run_load():
    """
    Main ETL function to load cleaned data into PostgreSQL.
    Clears FactSales first, loads dimensions, then FactSales.
    Also runs KPI SQL scripts.

    Args:
        None

    Returns:
        None
    """
    """
    Main controller to load cleaned data into PostgreSQL.
    Loads in order to maintain referential integrity:
    1. Clear FactSales (break FK)
    2. Load Dimension tables
    3. Load FactSales
    4. Execute KPI SQL scripts
    """
    logger.info("Starting full ETL data load to PostgreSQL...")

    # Get DB credentials strictly from environment variables (managed via .env for safety)
    DB_HOST = os.environ["DB_HOST"]
    DB_PORT = os.environ["DB_PORT"]
    DB_NAME = os.environ["DB_NAME"]
    DB_USER = os.environ["DB_USER"]
    DB_PASS = os.environ["DB_PASS"]

    wait_for_postgres(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)

    db_config = {
        "host": DB_HOST,
        "port": DB_PORT,
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASS
    }

    table_map = {
        "products": "DimProduct",
        "stores": "DimStore",
        "calendar": "DimDate",
        "sales": "FactSales"
    }

    start_time = time.time()
    
    # Delete only FactSales first (to break FK links)
    clear_table("factsales", db_config) 

    # Then load Dimensions (which will internally delete + insert)
    for filename in ["products", "stores", "calendar"]:
        table_name = table_map[filename]
        timed_load(filename, table_name, db_config)

    # Finally load FactSales
    timed_load("sales", table_map["sales"], db_config)

    total_time = time.time() - start_time
    logger.info(f"Total ETL load time: {total_time:.2f} seconds")

    try:
        # Run post-load SQL scripts for KPI views and indexes
        conn = psycopg2.connect(**db_config)
        with conn:
            with conn.cursor() as cursor:
                execute_sql_file(cursor, "kpi_views.sql")
                execute_sql_file(cursor, "kpi_indexes.sql")
        logger.info("KPI views and indexes created successfully.")
    except Exception as e:
        logger.exception("Error creating KPI views/indexes")

def execute_sql_file(cursor, filename):
    """
    Execute a SQL script file using the given database cursor.

    Args:
        cursor (psycopg2 cursor): Active cursor to execute SQL.
        filename (str): SQL filename in the `sql/` directory.

    Returns:
        None
    """
    sql_path = os.path.join("sql", filename)
    with open(sql_path, "r") as f:
        sql_script = f.read()
        cursor.execute(sql_script)
    logger.info(f"Executed SQL file: {filename}")

def clear_table(table_name, db_config):
    """
    Delete all rows from the specified PostgreSQL table.

    Args:
        table_name (str): Table to be cleared.
        db_config (dict): Database connection config.

    Returns:
        None
    """
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DELETE FROM {table_name}")
            conn.commit()

if __name__ == "__main__":
    run_load()
