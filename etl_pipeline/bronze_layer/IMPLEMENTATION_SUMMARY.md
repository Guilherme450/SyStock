# Bronze Layer — Implementation Summary

## Summary of implemented components

1. `bronze_layer/bronze_manager.py`
     - Validation of raw data
     - Ingestion and Parquet storage with Snappy compression
     - Audit fields: `_ingestion_timestamp`, `_entity_name`
     - Read latest data per entity
     - Entity statistics and automatic cleanup
     - Structured logging

2. `bronze_layer/bronze_pipeline.py`
     - Integration with `extract_api.py`
     - Full and single-entity extraction
     - Reporting and cleanup routines

3. `extract/extract_api.py` (updated)
     - Retries (3 attempts) and timeout handling
     - Support for multiple endpoints and validation
     - Detailed logging and exception handling

4. `bronze_layer/__init__.py` — module exports
5. `bronze_layer/examples.py` — five practical examples
6. `bronze_layer/README.md` — detailed module docs
7. `test/test_bronze_layer.py` — unit tests covering core behavior
8. `config.py` and `.env.example` — centralized configuration

## Architecture overview

```
API
    ↓
extract_api.py (get_data, extract_all_endpoints, validate_data, save_as_parquet)
    ↓
BronzePipeline (orchestration)
    ↓
BronzeLayerManager (ingest, read, stats, cleanup)
    ↓
data/bronze/{entity}/*.parquet
```

## Usage examples

Full extraction:

```python
from bronze_layer.bronze_pipeline import BronzePipeline
pipeline = BronzePipeline()
results = pipeline.run_full_extraction()
```

Read latest data:

```python
from bronze_layer.bronze_manager import BronzeLayerManager
bronze = BronzeLayerManager()
df = bronze.read_latest_data('clients')
```

Run tests:

```bash
cd "c:\Users\guilh\OneDrive\Área de Trabalho\SyStock"
python -m pytest test/test_bronze_layer.py -v
```

## Data layout

Each Parquet file contains original API columns plus audit fields, for example:

```python
{
        'id': int,
        'name': str,
        'email': str,
        '_ingestion_timestamp': datetime,
        '_entity_name': str
}
```

## Implemented features

- Robust validation
- Parquet storage (Snappy)
- Ingestion timestamps
- Automatic retries and timeouts for API calls
- Structured logging
- Old file cleanup
- Statistics generation
- Unit tests and documentation

## Next steps (before Silver layer)

1. Run and validate extraction examples
2. Check generated files and content quality
3. Configure endpoints in `config.py` if needed
4. Add monitoring and alerts for ingestion failures

Silver layer work: cleaning, type validation, normalization, deduplication and enrichment.

## Notes

- Paths are relative to `etl_pipeline/`
- Default retention keeps N newest files per entity (configurable)
- Snappy compression is the default for Parquet

Status: Bronze Layer complete and ready for validation
