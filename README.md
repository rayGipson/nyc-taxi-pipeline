# NYC Taxi Data Pipeline

Production-style ETL pipeline for NYC Yellow Cab trip data.

## Architecture

Bronze (raw parquet) → Silver (validated) → Gold (star schema)

## Quick Start
```bash
# Setup
make setup
make up

# Run pipeline
make pipeline

# Run tests
make test
```

## Tech Stack

- **Storage**: PostgreSQL 15 (star schema)
- **Processing**: Python 3.11 (pandas, pydantic)
- **Orchestration**: Docker Compose (Airflow in v2)
- **Testing**: pytest

## Project Status

**Phase 1**: Infrastructure complete  
**Phase 2**: Implementing pipeline modules (in progress)

## Design Decisions

- **Why Postgres now?** Local dev simplicity; clear Redshift migration path
- **Why star schema?** Optimized for analytical queries (BI/dashboards)
- **Why parquet staging?** Columnar format, ~70% smaller than CSV

## Directory Structure

See `docs/architecture.md` for detailed documentation.