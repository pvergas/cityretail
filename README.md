# CityRetail

This project extracts, cleans, transforms, and loads CityRetail sales data into a PostgreSQL database using a fully Dockerized ETL pipeline. The data is prepared for OLAP queries and Power BI dashboards.

---

## Quick Start

1. **Clone the repo**
    ```bash
    git clone https://github.com/your-username/cityretail-etl.git
    cd cityretail-etl
    ```

2. **Add raw CSVs to:** `data/raw/`
    - `calendar.csv`
    - `cities_lookup.csv`
    - `products.csv`
    - `sales.csv`
    - `stores.csv`

3. **Configure environment variables**  
   Copy the example file and add your credentials:
    ```bash
    cp .env.example .env
    ```

4. **Run the full pipeline**
    ```bash
    make all
    ```

---


## Makefile Commands

The `Makefile` simplifies management of the CityRetail ETL pipeline using Docker Compose. Below are the available commands:

```make
make all              # Run a clean full ETL pipeline (builds images, resets DB, runs ETL)
make force         # Run a full ETL even if cleaned files already exist
make incremental   # Run the ETL in incremental mode (only new/updated rows)
make clean         # Stop and remove containers, volumes, and network (project only)
make rebuild       # Rebuild Docker images from scratch (no cache)
make dangerous-reset  # WARNING: Removes all Docker containers, images, and volumes system-wide
```

---

## ETL Execution Modes

The CityRetail ETL pipeline supports three run modes:

| **Mode**        | **Command**                        | **Use When...**                                     |
|------------------|------------------------------------|-----------------------------------------------------|
| `Full`           | `make all` or `python -m src.main`   | You want to load existing cleaned data without reprocessing |
| `Force`          | `make force` or `python -m src.main --force`  | You need to reprocess all raw data from scratch     |
| `Incremental`    | `make incremental` or `python -m src.main --incremental` | You only want to clean and load new rows           |

> Tip: Use `incremental` for daily/weekly updates, `force` after changing cleaning logic, and `full` for quick re-runs with no data changes.

---

## Environment Variable Management

To follow best practices:

- Sensitive values (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `DB_USER`, `DB_PASS`) are externalized in a `.env` file.
- The `.env` file is excluded from Git via `.gitignore`.
- A `.env.example` is provided for safe template sharing.

### Example `.env.example`:

To use your own credentials, copy the example file and rename it to `.env`:

```bash
cp .env.example .env
```

Then edit `.env` and fill in your actual PostgreSQL username and password.

```env
POSTGRES_USER=your_db_user
POSTGRES_PASSWORD=your_db_password

DB_USER=your_db_user
DB_PASS=your_db_password
```

### Why this matters:
These variables are used by Docker Compose to inject secrets securely into your containers.

---


## PostgreSQL Dump

To export the full database to `lastname_dump.sql`:

```bash
docker exec -t cityretail-db-1 pg_dump -U $POSTGRES_USER -d cityretail > lastname_dump.sql
```

---

## Dockerized Architecture

- `db`: PostgreSQL container, auto-loads `star_schema.sql`
- `etl`: Waits for DB health, runs the ETL pipeline via `main.py`
- Shared volume for logs and data
- Makefile commands simplify common tasks

---

## Folder Structure

```
project-root/
├── cityretail_venv/         # Optional local virtual environment
├── data/
│   ├── raw/                 # Raw input CSVs (user-provided)
│   ├── cleaned/             # Output cleaned CSVs (ETL result)
│   └── logs/                # Log files (etl.log)
├── notebooks/
│   └── EDA.ipynb            # Optional exploratory notebook
├── sql/
│   ├── star_schema.sql      # Schema definition
│   ├── kpi_views.sql        # Business logic views
│   └── kpi_indexes.sql      # Performance indexing
├── src/
│   ├── clean_data.py
│   ├── config.py
│   ├── incremental_etl.py
│   ├── load_raw_data.py
│   ├── load_to_postgres.py
│   ├── logger.py
│   └── main.py              # ETL pipeline entrypoint
├── cityretail.pbix          # Power BI dashboard
├── vergas_dump.sql          # Exported PostgreSQL dump
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── requirements.txt
└── README.md
```

---

## Dashboard

The Power BI file Located in the project root (`cityretail.pbix`) visualizes:
- **Sales trends (monthly & weekly)** – via `v_monthly_sales`, `v_weekly_sales`
- **Weekend vs weekday behavior** – from `v_weekend_sales_comparison`
- **Top and regional performance** – using `v_sales_by_region`, `v_top_region`
- **Product-level metrics** – from `v_product_performance` (includes margin)
- **Monthly category analysis** – from `v_monthly_sales_by_category`

---

## Project Notes

- Logs are auto-saved to `data/logs/etl.log`
- All transformation steps are modular and reproducible
- EDA and logbook are included for inspection/report

---

## Virtual Environment (Optional for Local Dev)

```bash
source cityretail_venv/Scripts/activate
```
