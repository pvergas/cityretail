import os
import psycopg2
from psycopg2 import sql
import pandas as pd
import time
from .load_raw_data import load_raw_datasets
from .clean_data import standardize_city_names, parse_calendar_dates
from .config import RAW_DATA_PATH, CLEANED_DATA_PATH
from .load_to_postgres import wait_for_postgres
from .logger import setup_logger

logger = setup_logger()

def run_incremental_clean_and_load_all():
    """
    Perform incremental ETL by identifying and inserting only new records.

    Reads raw data, compares primary keys to existing DB records, appends new ones,
    and refreshes KPI views and indexes.

    Args:
        None

    Returns:
        None
    """
    """
    Run the incremental ETL process:
    - Loads only new rows from each dimension/fact table based on primary keys.
    - Applies upserts to keep the database up-to-date.
    """
    etl_start = time.time()
    logger.info("[Incremental ETL] Starting process...")

    # Get DB credentials strictly from environment variables (managed via .env for safety)
    DB_HOST = os.environ["DB_HOST"]
    DB_PORT = os.environ["DB_PORT"]
    DB_NAME = os.environ["DB_NAME"]
    DB_USER = os.environ["DB_USER"]
    DB_PASS = os.environ["DB_PASS"]

    wait_for_postgres(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    cursor = conn.cursor()

    def fetch_existing_ids(conn, table, id_column):
        """Retrieve all existing IDs from the specified table and column.
            
        Args:
            conn (psycopg2 connection): Active database connection.
            table (str): Table name to query.
            id_column (str): Name of the primary key column.

        Returns:
            set: A set of existing primary key values.
        """
        with conn.cursor() as cur:
            # Compose the SQL query safely using psycopg2.sql.Identifier.
            # This protects against:
            # - SQL injection if table/column names are passed dynamically
            # - Case-sensitivity issues in PostgreSQL (e.g., "ProductID" vs productid)
            # - Reserved keyword conflicts (e.g., Date, User)
            query = sql.SQL("SELECT {field} FROM {table};").format(
                field=sql.Identifier(id_column),
                table=sql.Identifier(table)
            )
            cur.execute(query)
            return set(row[0] for row in cur.fetchall())

    def filter_new_rows(df, id_column, existing_ids):
        """
        Filter out rows from a DataFrame that already exist in the database.

        Args:
            df (pd.DataFrame): Input DataFrame to filter.
            id_column (str): Column name of the primary key.
            existing_ids (set): Set of existing primary keys.

        Returns:
            pd.DataFrame: Filtered DataFrame containing only new rows.
        """
        filtered = df[~df[id_column].isin(existing_ids)]
        logger.debug(f"[{id_column}] Filtered {len(df) - len(filtered)} duplicate rows.")
        return filtered

    def insert_upsert(cursor, df, table, key_column):
        """
        Insert new rows or update existing ones using PostgreSQL UPSERT.

        Args:
            cursor (psycopg2 cursor): Active database cursor.
            df (pd.DataFrame): Data to be inserted or updated.
            table (str): Target table name.
            key_column (str): Primary key column for conflict resolution.

        Returns:
            None
        """
        """
        Perform an upsert (insert or update) into the target table using the key column.
        Uses ON CONFLICT DO UPDATE to maintain consistency.
        """
        # Prevent unnecessary upserts on empty DataFrames
        if df.empty:
            logger.info(f"[{table}] No new data to insert or update.")
            return

        try:
            df = df.astype(object)
            records = df.values.tolist()
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            update_cols = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col != key_column])
            query = f"""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
                ON CONFLICT ({key_column}) DO UPDATE SET {update_cols}
            """
            cursor.executemany(query, records)
            logger.info(f"[{table}] Inserted/Updated {len(records)} rows.")
        except Exception as e:
            logger.error(f"[{table}] Error during insert_upsert", exc_info=True)
            raise

    # Load all raw datasets
    raw = load_raw_datasets(data_dir=RAW_DATA_PATH)

    # Normalize all column names to lowercase to avoid KeyErrors
    for name, df in raw.items():
        df.columns = df.columns.str.lower()

    # === DimProduct ===
    start = time.time()
    products = raw["products"]
    existing_ids = fetch_existing_ids(conn, "dimproduct", "productid")
    new_products = filter_new_rows(products, "productid", existing_ids)
    if new_products.empty:
        logger.info("[DimProduct] No new rows to insert.")  # Extra clarity before skipping insert
    else:
        path = os.path.join(CLEANED_DATA_PATH, "products.csv")
        new_products.to_csv(path, mode='a', header=not os.path.exists(path), index=False)
        insert_upsert(cursor, new_products, "dimproduct", "productid")
    logger.info(f"[DimProduct] Completed in {time.time() - start:.2f} seconds")



    # === DimStore ===
    start = time.time()
    stores = standardize_city_names(raw["stores"], raw["cities_lookup"])
    existing_ids = fetch_existing_ids(conn, "dimstore", "storeid")
    new_stores = filter_new_rows(stores, "storeid", existing_ids)
    if new_stores.empty:
        logger.info("[DimStore] No new rows to insert.")  # New condition to log empty
    else:
        path = os.path.join(CLEANED_DATA_PATH, "stores.csv")
        new_stores.to_csv(path, mode='a', header=not os.path.exists(path), index=False)
        insert_upsert(cursor, new_stores, "dimstore", "storeid")
    logger.info(f"[DimStore] Completed in {time.time() - start:.2f} seconds")

    # === DimDate ===
    start = time.time()
    calendar = parse_calendar_dates(raw["calendar"])
    existing_ids = fetch_existing_ids(conn, "dimdate", "dateid")
    new_dates = filter_new_rows(calendar, "dateid", existing_ids)
    if new_dates.empty:
        logger.info("[DimDate] No new rows to insert.")
    else:
        path = os.path.join(CLEANED_DATA_PATH, "calendar.csv")
        new_dates.to_csv(path, mode='a', header=not os.path.exists(path), index=False)
        insert_upsert(cursor, new_dates, "dimdate", "dateid")
    logger.info(f"[DimDate] Completed in {time.time() - start:.2f} seconds")

    # === FactSales ===
    start = time.time()
    sales = raw["sales"]
    sales["dateid"] = sales["dateid"].astype(int)
    existing_ids = fetch_existing_ids(conn, "factsales", "salesid")
    new_sales = filter_new_rows(sales, "salesid", existing_ids)
    if new_sales.empty:
        logger.info("[FactSales] No new rows to insert.")
    else:
        path = os.path.join(CLEANED_DATA_PATH, "sales.csv")
        new_sales.to_csv(path, mode='a', header=not os.path.exists(path), index=False)
        insert_upsert(cursor, new_sales, "factsales", "salesid")
    logger.info(f"[FactSales] Completed in {time.time() - start:.2f} seconds")

    # Commit all inserts/updates
    conn.commit()

    # Refresh views and indexes
    try:
        with conn.cursor() as view_cursor:
            execute_sql_file(view_cursor, "kpi_views.sql")
            execute_sql_file(view_cursor, "kpi_indexes.sql")
        logger.info("KPI views and indexes refreshed after incremental ETL.")
    except Exception as e:
        logger.exception("Error creating KPI views/indexes after incremental load")

    cursor.close()
    conn.close()
    logger.info("[Incremental ETL] All new rows inserted and committed.")
    logger.info(f"[Incremental ETL] Finished in {time.time() - etl_start:.2f} seconds.")

def execute_sql_file(cursor, filename):
    """
    Execute an SQL script from a file.

    Args:
        cursor (psycopg2 cursor): Active database cursor.
        filename (str): Name of the SQL file in the /sql directory.

    Returns:
        None
    """
    sql_path = os.path.join("sql", filename)
    with open(sql_path, "r") as f:
        sql_script = f.read()
        cursor.execute(sql_script)

def should_use_incremental():
    """
    Detects if DimProduct already contains rows, meaning we should run incremental mode.
    Returns:
        bool: True if incremental ETL should be used.
    """
    try:
        conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"],
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASS"]
        )
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dimproduct;")
            count = cur.fetchone()[0]
        conn.close()
        return count > 0
    except Exception as e:
        logger.warning("Could not connect to DB to check for incremental mode. Defaulting to full load.", exc_info=True)
        return False

if __name__ == "__main__":
    run_incremental_clean_and_load_all()
