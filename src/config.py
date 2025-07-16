import os

"""
Configuration module for setting up data directories.

Uses the DATA_PATH environment variable if provided, 
or defaults to the "data" folder in the project root.

Creates the following folders if they don't exist:
- data/raw/
- data/cleaned/
- data/logs/
"""

# Get project root (assumes config.py is in src/)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Base data path – can be overridden by an environment variable
#DATA_PATH = os.getenv("DATA_PATH", "data")

# Prefer environment variable, fallback to 'project-root/data'
DATA_PATH = os.getenv("DATA_PATH", os.path.join(PROJECT_ROOT, "data"))

# Subpaths
RAW_DATA_PATH = os.path.join(DATA_PATH, "raw")
CLEANED_DATA_PATH = os.path.join(DATA_PATH, "cleaned")
LOGS_PATH = os.path.join(DATA_PATH, "logs")  

# Ensure all paths exist when the app starts
for path in [RAW_DATA_PATH, CLEANED_DATA_PATH, LOGS_PATH]:
    os.makedirs(path, exist_ok=True)