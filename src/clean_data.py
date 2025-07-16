import pandas as pd
import os
from .logger import setup_logger
from .config import CLEANED_DATA_PATH 
from .logger import setup_logger

logger = setup_logger()

def standardize_city_names(stores_df, lookup_df):
    """
    Merge stores with cities_lookup to replace inconsistent city names
    with standardized ones.

    Args:
        stores_df (DataFrame): The raw stores DataFrame (with lowercase columns).
        lookup_df (DataFrame): The city name mapping DataFrame (with lowercase columns).

    Returns:
        DataFrame: Cleaned version of stores with standardized city values.
    """
    stores_df = stores_df.copy()
    lookup_df = lookup_df.copy()

    # Normalize column names
    stores_df.columns = stores_df.columns.str.lower()
    lookup_df.columns = lookup_df.columns.str.lower()

    merged = stores_df.merge(
        lookup_df,
        left_on="city", right_on="rawcity",  # lowercased
        how="left"
    )

    # Replace raw city with standardized city
    merged = merged.drop(columns=["city", "rawcity"])
    merged = merged.rename(columns={"standardcity": "city"})

    # Log specific unmapped city rows if any
    unmapped = merged[merged["city"].isnull()]

    if not unmapped.empty:
        logger.warning("Some cities could not be standardized.")
        logger.warning(f"Unmapped cities: {unmapped[['storeid', 'storename', 'region']].to_dict(orient='records')}")

    return merged

def parse_calendar_dates(calendar_df):
    """
    Parse and enrich the calendar dataset with validated and derived time features.

    This function:
    - Parses the 'date' column into datetime format.
    - Logs a warning if any dates are invalid or unparsable.
    - Adds a new column 'weeknumber' using ISO calendar week numbers (1–53).
    - Adds a boolean column 'isweekend' indicating if the date falls on a Saturday or Sunday.

    Args:
        calendar_df (DataFrame): The raw calendar DataFrame containing at least a 'date' column.

    Returns:
        DataFrame: The cleaned and enriched calendar DataFrame, ready for analysis or loading.
    """
    calendar_df = calendar_df.copy()

    # Normalize column names
    calendar_df.columns = calendar_df.columns.str.lower()

    # Convert the 'date' column to datetime format (NaT for errors)
    calendar_df["date"] = pd.to_datetime(calendar_df["date"], errors="coerce")

    # Warn if any dates failed to parse
    if calendar_df["date"].isnull().any():
        logger.warning("Some calendar dates could not be parsed.")

    # Add ISO week number for grouping and time series analysis
    calendar_df["weeknumber"] = calendar_df["date"].dt.isocalendar().week

    # Mark whether each date falls on a weekend (Saturday=5, Sunday=6)
    calendar_df["isweekend"] = calendar_df["date"].dt.weekday >= 5

    return calendar_df

def save_cleaned_dataframes(df_dict, output_dir=CLEANED_DATA_PATH):
    """
    Save all cleaned DataFrames to CSV files.

    Args:
        df_dict (dict): Keyed by name, values are DataFrames.
        output_dir (str): Path to save cleaned files.
    """
    os.makedirs(output_dir, exist_ok=True)

    for name, df in df_dict.items():
        output_path = os.path.join(output_dir, f"{name}.csv")
        df.to_csv(output_path, index=False)
        logger.info(f"Saved cleaned file: {output_path}")
