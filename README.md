# City Energy Observatory

A practical reference platform for collecting, standardizing, analyzing, and visualizing energy data from European cities. The repository uses the existing IESCO smart-meter extract as a reference source while defining reusable contracts for four city integrations.

## Project Shape

```text
source connectors (API, object storage, catalog upload)
        -> Airflow ingestion and validation
        -> canonical meter, asset, observation, and metadata tables
        -> PostgreSQL analytical model
        -> Superset / Streamlit dashboards
```

The source data can be annual or monthly time series and may include latitude/longitude. Connectors should preserve source provenance and publish the canonical fields used by the dashboard and warehouse schema.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements_dashboard.txt
streamlit run streamlit_app.py
```

The dashboard automatically discovers the latest `meters_*.csv` and `readings_*.csv` files in the project root. It provides:

- consumption trend and meter KPIs;
- source quality summaries;
- asset coordinates and latest observations;
- a four-city integration catalog showing source and readiness;
- a canonical data contract that can be reused by API and object-storage adapters.

## Pipeline Components

| Component | Purpose |
| --- | --- |
| `utils/city_energy.py` | Source discovery, field normalization, timestamp handling, quality flags, and KPIs |
| `airflow/city_energy_pipeline.py` | Minimal DAG template: ingest, validate/standardize, publish |
| `sql/postgres_schema.sql` | PostgreSQL contract for cities, assets, observations, provenance, and metadata |
| `silver_clean.py` | Existing detailed cleaning flow for the IESCO source extract |
| `gold_layer_pandas.py` | Existing dimensional analytics layer for Parquet and BI use |
| `streamlit_app.py` | Source-backed city observatory prototype |
| `generate_dashboard_pages.py` | Existing dashboard generation utilities |

## City Integration Contract

1. **API city:** fetch incrementally using a source cursor or observation timestamp.
2. **Object-storage city:** discover new objects, record checksums, and process idempotently.
3. **Catalog upload city:** validate an uploaded file against the canonical schema before accepting it.
4. **All cities:** normalize units to kWh/kW, timestamps to UTC, coordinates to decimal degrees, and quality values to a controlled vocabulary.
5. **Every load:** retain `city_id`, source type, source record identifier, ingestion timestamp, and metadata.

Changes to the canonical model should be discussed with the project lead before implementation because city-specific requirements may need additional metadata or dimensions.

## Running the Existing Layers

The legacy IESCO path remains available when its generated Bronze directory is present:

```bash
python datagenerator_v2.0_parallel.py
python silver_clean.py
python gold_layer_pandas.py
```

Those jobs produce `iesco_complete_data/`, `iesco_silver_data/`, and `iesco_gold_data/`. The new observatory does not require those generated directories; it can work directly from the checked-in extracts.

## PostgreSQL and Superset

Apply the warehouse contract with:

```bash
psql "$DATABASE_URL" -f sql/postgres_schema.sql
```

Expose `energy.energy_observation` and views for monthly consumption, quality rate, and asset coverage as Superset datasets. Keep dashboard logic in SQL views or reusable dataset definitions so the four city integrations share the same visual templates.

## Validation

```bash
python -m py_compile streamlit_app.py utils/city_energy.py airflow/city_energy_pipeline.py
```

For a deployed environment, add connector-level tests for schema validation, duplicate observation handling, UTC conversion, coordinate bounds, and referential integrity.

## Scope and Next Steps

This repository is a working prototype, not a claim that all four city connectors are complete. The next implementation steps are to replace the three DAG task stubs with source adapters, configure an Airflow Postgres connection, add idempotent staging tables, and publish Superset datasets from the canonical model.
