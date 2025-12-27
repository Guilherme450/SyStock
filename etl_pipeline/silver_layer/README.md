# Silver Layer — Documentation

This document describes the `silver_layer` module: its purpose, architecture, inputs/outputs, configuration, usage examples, testing guidance, and common troubleshooting notes. It is intended as a developer-facing reference to understand and operate the silver (cleaned/enriched) layer of the SyStock ETL pipeline.

## Overview

The silver layer converts raw bronze data into cleaned, enriched, and analytics-ready dimension and fact tables. Transformations are implemented using Polars DataFrames and the results are persisted as Parquet files under the project `etl_pipeline/data/silver` directory.

Key components:
- `SilverLayerManager` — orchestrates transformations, saves Parquet outputs, and provides helpers to read results and generate reports.
- `SilverPipeline` — a simple orchestrator that runs a full transformation, generates reports, and provides cleanup utilities.
- `transformations.py` — contains `DimensionTransformer` and `FactTransformer` classes implementing entity-specific logic.

## Architecture & responsibilities

- `DimensionTransformer`: transforms bronze entities into dimension tables (clientes, produtos, lojas, tempo).
- `FactTransformer`: transforms bronze entities into fact tables (vendas, estoque, distribuicoes).
- `SilverLayerManager`: composes transformers, exposes these methods:
  - `transform_dimension(dimension_name, save=True) -> polars.DataFrame`
  - `transform_fact(fact_name, save=True) -> polars.DataFrame`
  - `transform_all() -> Dict[str,int]` (runs all dims and facts, returns row counts)
  - `get_dimension(dimension_name) -> Optional[polars.DataFrame]`
  - `get_fact(fact_name) -> Optional[polars.DataFrame]`
  - `get_statistics() -> Dict` (returns row counts, column counts, file sizes)
  - `generate_report() -> str`
- `SilverPipeline`: provides `run_full_transformation()`, `generate_report()`, and `cleanup_old_data(days=30)` and a `main()` entrypoint for module execution.

## Inputs (expected bronze structure)

The transformers read the latest Parquet file present in specific bronze subdirectories. Expected layout (relative to project root):

```
etl_pipeline/data/bronze/
  ├─ clientes/           # parquet files with client records
  ├─ produtos/           # parquet files with product records
  ├─ lojas/              # store metadata
  ├─ vendas/             # sales payloads (with `items` struct/array)
  ├─ estoque/            # inventory snapshots
  ├─ distribucao_interna/# internal distributions with `items`
  └─ categorias/         # optional categories used to enrich products
```

Each transformer reads the most recent `*.parquet` file from the corresponding directory. If a required entity folder or file is missing, the transformer raises `FileNotFoundError` or logs a warning.

Field expectations (examples — check `transformations.py` for exact column names):
- Sales (`vendas`): `id`, `sale_date`, `store_id`, `client_id`, `items` (struct with `product_id`, `quantity`, `unit_price`, `total_price`).
- Products (`produtos`): `id`, `name`, `sale_price`, `cost_price`, `category_id`, etc.

## Outputs

- Parquet files are written under `etl_pipeline/data/silver/` with two subfolders:
  - `dims/` — dimension Parquet files (e.g. `dim_clientes.parquet`, `dim_produtos.parquet`, `dim_lojas.parquet`, `dim_tempo.parquet`).
  - `facts/` — fact Parquet files (e.g. `fact_vendas.parquet`, `fact_estoque.parquet`, `fact_distribuicoes.parquet`).

Files are written with Snappy compression. Transformers return Polars DataFrames which are also available in memory when running programmatically.

## Configuration

- `SilverLayerManager` accepts optional `bronze_dir` and `silver_dir` (both `pathlib.Path`) to override default locations.
- Logging level can be set via `log_level` parameter when constructing `SilverLayerManager`.
- Environment-level configuration for the whole project (database, ETL settings) is documented at repository root `README.md` and `DOCKER_SETUP.md`.

## Usage

Run the full silver-layer pipeline from the project root:

```bash
# Run as a module (uses default project paths)
python -m etl_pipeline.silver_layer.silver_pipeline
```

Run programmatically from a Python REPL or script:

```python
from pathlib import Path
from etl_pipeline.silver_layer.silver_manager import SilverLayerManager

mgr = SilverLayerManager(
    bronze_dir=Path("etl_pipeline/data/bronze"),
    silver_dir=Path("etl_pipeline/data/silver"),
    log_level="INFO",
)

# Transform a single dimension and get the DataFrame
df_dim = mgr.transform_dimension("clientes", save=True)

# Transform all entities
results = mgr.transform_all()
print(results)

# Generate a human-readable report
print(mgr.generate_report())
```

Examples when running inside the repository Docker container:

```bash
docker-compose exec etl-pipeline python -m etl_pipeline.silver_layer.silver_pipeline
```

## Testing

- Unit tests for the silver layer (if present) live in the repository `test/` folder. Run the test suite from the project root:

```bash
pip install -r etl_pipeline/requirements.txt
pytest -q
```

When authoring tests for `silver_layer`, prefer small, deterministic Parquet fixtures under `test/fixtures` and use temporary directories (e.g. `tmp_path` fixture) to validate read/write behavior.

## Troubleshooting & Notes

- Missing bronze files: Transformers raise `FileNotFoundError` if required bronze data is absent. Ensure bronze parquet files exist and are accessible.
- Schema mismatches: Transformers expect specific column names. If source schema differs, adapt the transformer functions or create a small mapping step in the bronze ingestion.
- Performance: Polars is used for efficient in-memory transformations. For very large datasets, run transformations in an environment with sufficient memory or partition processing into smaller batches.
- OneDrive / Windows paths: The project root on OneDrive can introduce path/locking issues on Windows. If you encounter I/O errors, consider moving the project to a local folder (e.g., `C:\src\SyStock`).
- Parquet compression: outputs use Snappy compression. Ensure readers support Snappy.

## Development notes and extension points

- Add new dimension or fact transformations by implementing `transform_<entity>` methods in `DimensionTransformer` or `FactTransformer`, then register the entity in `SilverLayerManager.DIMENSIONS` or `.FACTS`.
- To persist to a database instead of Parquet, replace the `save` branch in `SilverLayerManager` to write to your target (e.g., PostgreSQL via `COPY`/copy_from or using `pandas.to_sql` style flows).

## References

- Project-level docs: [README.md](../../README.md) and [DOCKER_SETUP.md](../../DOCKER_SETUP.md)
- Core modules: `silver_manager.py`, `silver_pipeline.py`, `transformations.py`

