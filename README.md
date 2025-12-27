# SyStock
SyStock — Data Warehouse and ETL for Inventory & Sales Analytics

Overview
--------
SyStock is a data engineering project that implements a reproducible ETL pipeline and layered data warehouse designed to support inventory and sales analytics for retail operations. The repository contains extraction scripts, transformation logic, staging and analytical schemas, orchestration artifacts, and Docker-based development environments.

Problem Statement
-----------------
Retail and distribution teams need accurate, time-aware views of inventory levels, product movement, sales performance, and store operations. Raw operational systems are often fragmented and not optimized for analytics. SyStock consolidates heterogeneous sources into a governed, queryable data warehouse to enable reporting, BI dashboards, and downstream machine learning.

Key Features
------------
- **Layered architecture**: Bronze (raw), Silver (cleaned/enriched), and Gold (analytics-ready) data layers.
- **Modular ETL**: Extract, transform, and load components written in Python, organized for maintainability and reuse.
- **SQL-first analytics**: Warehouse schema, facts, dimensions, and views provided in the `data_warehouse/schema` folder.
- **Containerized development**: Docker and docker-compose files to run services and the warehouse locally.
- **Automated snapshots**: Utilities for capturing point-in-time snapshots of product and sales state.

Architecture & Components
------------------------
- Extraction: `extract/extract_api.py` and other extractors collect source data.
- Bronze layer: raw file ingestion and landing area implemented under `etl_pipeline/bronze_layer` and `etl_pipeline/data/bronze`.
- Silver layer: cleaning and enrichment in `etl_pipeline/silver_layer`, producing dimension and fact datasets.
- Data warehouse: schemas, views, and stored procedures under `data_warehouse/schema` (dims, facts, views, snapshot procedures).
- Orchestration: pipeline manager and flow controls are in `pipeline_manager` and the `etl_pipeline` package.
- Infrastructure: `docker-compose.yml` and `DOCKER_SETUP.md` provide containerized environment instructions.

Technologies
------------
- Python (ETL and orchestration)
- PostgreSQL or compatible data warehouse (SQL schemas and views)
- Docker and Docker Compose (development and integration)
- SQL for schema, views, and snapshot procedures
- pytest for automated tests (see `test` folder)

Repository Layout (high level)
-----------------------------
- `etl_pipeline/` — ETL code, bronze/silver pipelines, and implementation notes.
- `data_warehouse/schema/` — SQL schemas, dimensions, facts, views, and snapshot procedures.
- `extract/`, `load/` — extraction and load helpers.
- `pipeline_manager/` — orchestration flows and pipeline entry points.
- `docker-compose.yml`, `DOCKER_SETUP.md` — environment and service orchestration.
- `test/` — unit and integration tests for pipelines.

Getting Started
---------------
Prerequisites: Docker and Docker Compose installed, and Python 3.9+ for local development.

Quick start (development with Docker):

```bash
# Start services and the local development environment
docker-compose up --build -d

# Run ETL pipeline (example entrypoint — adjust as needed)
docker-compose exec <service> python -m pipeline_manager.pipeline_flow
```

See [DOCKER_SETUP.md](DOCKER_SETUP.md) for full container configuration and [START_HERE.md](START_HERE.md) for project onboarding steps.

Development
-----------
To run tests locally (from a configured Python environment):

```bash
python -m pip install -r etl_pipeline/requirements.txt
pytest -q
```

For iterative development, run pipeline modules directly or use the provided orchestration in `pipeline_manager`.

Contributing
------------
Contributions are welcome. Please follow these guidelines:
- Open an issue to discuss major changes.
- Create feature branches and submit pull requests with descriptive titles.
- Include tests for new functionality where applicable.

License
-------
This project is licensed under the terms in the repository `LICENSE` file.

Contact
-------
For questions about design, architecture, or contribution, open an issue or contact the repository maintainers.
