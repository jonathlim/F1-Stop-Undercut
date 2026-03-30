# F1 Stop Undercut

Ingests Formula 1 lap data, loads it into Databricks, and transforms it using dbt Core.

## Project Overview

This pipeline pulls race lap data from the [FastF1](https://docs.fastf1.dev/) Python library for the 2025 and 2026 F1 seasons, lands it in Databricks Unity Catalog, and transforms it through a layered dbt project into analysis-ready mart tables.

## Tech Stack

- **Python 3.12** — ingestion scripts
- **FastF1** — F1 timing and telemetry data
- **Databricks (Unity Catalog)** — cloud data warehouse
- **dbt Core** — data transformations
- **SQL** — Databricks SQL dialect

## Architecture
```
FastF1 API
    ↓
ingest.py (Python)
    ↓
f1stop.raw.raw_race_results (Databricks)
    ↓
f1stop.stg.stg_race_results (dbt staging)
    ↓
f1stop.intermediate.int_laps_enriched (dbt intermediate)
    ↓
┌──────────────────────┬─────────────────────┬──────────────────────┐
mart_driver_race_pace  mart_tyre_strategy    mart_race_summary
```

## Data Pipeline

### Ingestion (`ingest.py`)
- Pulls race lap data from FastF1 API with local caching
- Incremental load — checks existing rounds before pulling, skips already loaded data
- Converts FastF1 timedelta and datetime types for Databricks compatibility
- Loads raw data as STRING into Databricks in batches of 250 rows

### dbt Transformations

**Staging (`stg_race_results`)**
- Casts all columns from STRING to proper types
- Handles NaN, None, and empty string values
- Incremental append strategy with composite unique key

**Intermediate (`int_laps_enriched`)**
- Adds derived flags: `is_green_flag_lap`, `is_pit_lap`, `is_outlap`
- Calculates `stint_lap_number` and `gap_to_fastest_lap_sec` using window functions
- Green flag logic accounts for combined track status codes

**Marts**
- `mart_driver_race_pace` — fastest lap, average pace, sector breakdowns, consistency (stddev) per driver per race
- `mart_tyre_strategy` — stint analysis, tyre degradation, compound performance per driver per race  
- `mart_race_summary` — race overview including finish position, positions gained, pit stops, and compounds used

## Project Structure
```
F1-Stop-Undercut/
├── ingest.py                  # Incremental ingestion script
├── requirements.txt
├── .gitignore
└── f1stop_dbt/                # dbt project
    ├── dbt_project.yml
    ├── packages.yml
    ├── macros/
    │   └── get_custom_schema.sql
    └── models/
        ├── staging/
        │   ├── sources.yml
        │   ├── stg_race_results.sql
        │   └── stg_race_results.yml
        ├── intermediate/
        │   ├── int_laps_enriched.sql
        │   └── int_laps_enriched.yml
        └── mart/
            ├── mart_driver_race_pace.sql
            ├── mart_driver_race_pace.yml
            ├── mart_tyre_strategy.sql
            ├── mart_tyre_strategy.yml
            ├── mart_race_summary.sql
            └── mart_race_summary.yml
```

## Setup

### Prerequisites
- Python 3.12
- Databricks workspace with Unity Catalog
- dbt Core with Databricks adapter

### Installation
```bash
# Clone the repo
git clone https://github.com/jonathlim/F1-Stop-Undercut.git
cd F1-Stop-Undercut

# Create and activate virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1  # Windows

# Install dependencies
pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file in the root directory:
```
DATABRICKS_HOST=your_host
DATABRICKS_HTTP_PATH=your_http_path
DATABRICKS_TOKEN=your_token
DATABRICKS_CATALOG=f1stop
DATABRICKS_SCHEMA=raw
```

### Running the Pipeline
```bash
# Run ingestion
python ingest.py

# Run dbt transformations
cd f1stop_dbt
dbt deps
dbt run
dbt test
```

## Data Source

Lap data sourced from the [FastF1](https://docs.fastf1.dev/) library which provides access to official F1 timing data. Currently ingesting Race sessions for the 2025 and 2026 seasons.