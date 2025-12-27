# Bronze Layer — Documentation

## Overview
The Bronze layer is responsible for ingesting and persisting raw data pulled from external sources (the public API used by this project). Raw payloads are stored as Parquet files (Snappy compression) to ensure efficient storage and fast reads during downstream processing.

This README documents the primary modules in `etl_pipeline/bronze_layer`, the expected bronze directory layout, usage examples, configuration points, and common troubleshooting notes for developers.

## Purpose and responsibilities
- Ingest raw JSON payloads from extractors and persist them as timestamped Parquet files.
- Provide safe read primitives to load the most recent raw snapshot for a given entity.
- Maintain a small retention policy to avoid excessive disk usage (cleanup of old files).
- Emit basic statistics and reports to help downstream layers (silver/gold) understand available data.

## Main modules

1. `bronze_manager.py` — BronzeLayerManager

     Responsibilities:
     - Validate and normalize incoming raw payloads (best-effort checks).
     - Persist entity data to `etl_pipeline/data/bronze/{entity}/` with timestamped filenames.
     - List available entities and read the latest Parquet snapshot for a given entity.
     - Provide basic entity-level statistics and cleanup utilities.

     Important methods (examples):

     - `__init__(base_path: Optional[Union[str, Path]] = None, retention: int = 5, log_level: str = "INFO")`
         Initialize manager. `base_path` defaults to `etl_pipeline/data/bronze`.

     - `ingest_data(data: Dict, entity_name: str) -> Optional[str]`
         Persist a single entity payload. Returns the written file path or None on failure.

     - `ingest_multiple_entities(data_dict: Dict[str, Dict]) -> Dict[str, Optional[str]]`
         Persist multiple entities in a single call (useful for extractors that return multiple endpoints).

     - `read_latest_data(entity_name: str) -> Optional[polars.DataFrame]`
         Load the most recent Parquet snapshot for an entity. Returns `None` if not present.

     - `list_entities() -> List[str]`
         Return the list of bronze entity folders that contain parquet files.

     - `get_entity_statistics(entity_name: str) -> Dict`
         Return metadata such as row count, column count and file size (MB).

     - `cleanup_old_files(entity_name: str, keep_count: int = 5) -> int`
         Remove older files for an entity, keeping the `keep_count` newest files.

2. `bronze_pipeline.py` — BronzePipeline

     Responsibilities:
     - Coordinate extraction and ingestion: call extractors, validate results, and use `BronzeLayerManager` to persist data.
     - Provide simple orchestration entrypoints used in development and CI.

     Important methods (examples):

     - `run_full_extraction() -> Dict[str, Optional[str]]`
         Trigger extraction for all configured endpoints and persist results. Returns a mapping of entity -> file path.

     - `run_single_entity_extraction(entity_name: str) -> Optional[str]`
         Extract a single endpoint and persist its snapshot.

     - `generate_report() -> Dict`
         Aggregate statistics for all entities and return a structured report.

     - `cleanup_old_data(keep_count: int = 5) -> Dict[str, int]`
         Cleanup old files across all entities.

3. `extract/extract_api.py` — Extractor utilities (referenced)

     The extractor module provides HTTP helpers to fetch API endpoints, with retries, timeout handling and structured logging. Typical helpers:

     - `get_data(url: str, retries: int = 3) -> Optional[Dict]`
     - `extract_all_endpoints() -> Dict[str, Optional[Dict]]`
     - `save_as_parquet(data: Dict, filename: str, output_dir: Optional[str] = None) -> Optional[str]`

     The pipeline uses these helpers to obtain raw payloads that are forwarded to `BronzeLayerManager.ingest_data()`.

## Expected bronze directory layout

By convention the Bronze manager persists files under `etl_pipeline/data/bronze/` with one subdirectory per entity. Example:

```
etl_pipeline/data/bronze/
    ├─ clientes/
    │   ├─ clientes_raw_20231201_120000.parquet
    │   ├─ clientes_raw_20231201_130000.parquet
    │   └─ clientes_raw_20231201_140000.parquet
    ├─ produtos/
    ├─ vendas/
    ├─ estoque/
    └─ distribucao_interna/
```

Each file is a snapshot of raw records at a specific ingestion timestamp. The manager selects the latest file when downstream consumers call `read_latest_data()`.

## Typical data flow

1. An extractor fetches JSON payload(s) from the external API.
2. The pipeline (`BronzePipeline`) receives the payload and calls `BronzeLayerManager.ingest_data()` for each entity.
3. The manager writes a timestamped Parquet file under the corresponding bronze entity folder.
4. Downstream modules (silver layer) read the latest bronze snapshot to perform cleaning and enrichment.

## Usage examples

Run the full bronze extraction pipeline (from project root):

```bash
python -m etl_pipeline.bronze_layer.bronze_pipeline
```

Programmatic usage in a Python session:

```python
from pathlib import Path
from etl_pipeline.bronze_layer.bronze_manager import BronzeLayerManager

manager = BronzeLayerManager(base_path=Path('etl_pipeline/data/bronze'))

# Ingest a single payload
file_path = manager.ingest_data(api_payload, entity_name='clientes')

# Read the latest data
df = manager.read_latest_data('clientes')

# Get stats and cleanup
stats = manager.get_entity_statistics('clientes')
removed = manager.cleanup_old_files('clientes', keep_count=5)
```

## Configuration

- `base_path` (constructor of `BronzeLayerManager`): override the default bronze directory.
- `retention` / `keep_count`: controls how many recent snapshots to keep per entity.
- Logging level can be set when initializing the manager (`log_level`) or globally via `logging.basicConfig(...)`.

Configurations for the whole repository (Docker, DB, environment variables) live at the project root: `README.md` and `DOCKER_SETUP.md`.

## Testing guidance

- Unit tests should use deterministic Parquet fixtures. Place small sample files under `test/fixtures/bronze/` and use `tmp_path` to point `BronzeLayerManager` to a temporary directory.
- Example pytest snippet:

```python
def test_read_and_cleanup(tmp_path, sample_parquet):
        manager = BronzeLayerManager(base_path=tmp_path)
        # copy sample_parquet into tmp_path/clients/...
        df = manager.read_latest_data('clientes')
        assert df is not None
        removed = manager.cleanup_old_files('clientes', keep_count=1)
        assert isinstance(removed, int)
```

## Logging and observability

All bronze modules use the standard `logging` framework. For development enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

Logs provide context about HTTP retries, file writes, and any failures during ingestion.

## Common troubleshooting

- No files found for an entity: verify the extractor returned data and that `BronzeLayerManager` wrote files to the expected `etl_pipeline/data/bronze/{entity}` folder.
- Permission or I/O errors: on Windows + OneDrive repositories, path locks may occur. If you see intermittent write failures, try moving the repo to a local path outside OneDrive.
- Schema drift: bronze stores raw payloads. If downstream transformations fail due to missing fields, inspect the bronze snapshot to decide whether to adapt the transformer or add a lightweight normalization step.
- Disk usage: adjust retention (`keep_count`) or run `cleanup_old_data()` via cron/CI to prevent uncontrolled growth.
