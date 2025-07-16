# =======================================
# CityRetail ETL Makefile
# =======================================

# Main target: Auto-detects full or incremental mode
all:
	docker-compose down -v --remove-orphans
	docker-compose build
	docker-compose up -d db                                   
	docker-compose run --rm etl python -m src.main

# Force full ETL even if cleaned files exist
force:
	docker-compose run --rm etl python -m src.main --force

# Run incremental ETL manually
incremental:
	docker-compose run --rm etl python -m src.main --incremental

# Safe cleanup (only this project's containers, volumes, networks)
clean:
	docker-compose down -v --remove-orphans

# Rebuild images without cache (for development use)
rebuild:
	docker-compose build --no-cache

# Full Docker reset (dangerous — deletes ALL containers/images/volumes)
dangerous-reset:
	docker system prune -af --volumes
