"""
Load pipeline module.

This module defines LoadPipeline, a convenience wrapper that reads Silver
parquet files for dimensions and facts and invokes loader routines that
persist those datasets into the target database.

Typical usage:
    from pathlib import Path
    from etl_pipeline.load.load_pipeline import LoadPipeline

    project_root = Path(__file__).resolve().parent.parent.parent
    pipeline = LoadPipeline(project_root)
    pipeline.load_all()             # load all dims and facts
    pipeline.load_all_dimensions()  # load only dimensions
    pipeline.load_all_facts()       # load only facts

Notes:
- The pipeline expects silver parquet files under:
    <project_root>/etl_pipeline/data/silver/dims
    <project_root>/etl_pipeline/data/silver/facts
- Loaders are configured via DB_CONFIG from load.load_entities.
"""
from pathlib import Path
from typing import List

from load.load_entities import (
    ReadSilverParquet,
    LoadDimension,
    LoadFacts,
    DB_CONFIG,
)


class LoadPipeline:
    """Pipeline orchestrator for loading dimensions and facts into a database.

    The class prepares readers for the silver parquet sources and instantiates
    loader objects. It exposes methods to load all dimensions, all facts, or
    both.

    Args:
        project_root: Root path of the project used to locate silver data.
    """

    def __init__(self, project_root: Path):
        self.project_root = project_root

        silver_dir_dim = (
            project_root / "etl_pipeline" / "data" / "silver" / "dims"
        )
        silver_dir_fact = (
            project_root / "etl_pipeline" / "data" / "silver" / "facts"
        )

        self.reader_dims = ReadSilverParquet(silver_dir_dim)
        self.reader_facts = ReadSilverParquet(silver_dir_fact)

        self.loader_dims = LoadDimension(DB_CONFIG, self.reader_dims)
        self.loader_facts = LoadFacts(DB_CONFIG, self.reader_facts)

    def load_all_dimensions(self, selected: List[str] | None = None):
        """Load all dimension tables or a selected subset.

        Args:
            selected: Optional list of dimension keys to run (e.g. 'dim_clientes').
                      If None, all known dimensions are loaded.
        """
        mapping = {
            "dim_tempo": self.loader_dims.load_dim_tempo,
            "dim_clientes": self.loader_dims.load_dim_clientes,
            "dim_lojas": self.loader_dims.load_dim_lojas,
            "dim_produtos": self.loader_dims.load_dim_produto,
        }

        to_run = mapping.keys() if not selected else selected

        for name in to_run:
            fn = mapping.get(name)
            if not fn:
                print(f"[WARN] Dimensão desconhecida: {name}")
                continue
            try:
                print(f"[INFO] Iniciando carga da dimensão: {name}")
                fn()
            except Exception as exc:
                print(f"[ERROR] Falha ao carregar {name}: {exc}")

    def load_all_facts(self, selected: List[str] | None = None):
        """Load all fact tables or a selected subset.

        Args:
            selected: Optional list of fact keys to run (e.g. 'fact_vendas').
                      If None, all known facts are loaded.
        """
        mapping = {
            "fact_estoque": self.loader_facts.load_fact_estoque,
            "fact_vendas": self.loader_facts.load_fact_vendas,
            "fact_distribuicoes": self.loader_facts.load_fact_distribuicao,
        }

        to_run = mapping.keys() if not selected else selected

        for name in to_run:
            fn = mapping.get(name)
            if not fn:
                print(f"[WARN] Fato desconhecido: {name}")
                continue
            try:
                print(f"[INFO] Iniciando carga do fato: {name}")
                fn()
            except Exception as exc:
                print(f"[ERROR] Falha ao carregar {name}: {exc}")

    def load_all(self):
        """Load all dimensions first, then all facts."""
        self.load_all_dimensions()
        self.load_all_facts()


def _get_project_root() -> Path:
    # assume file is in etl_pipeline/load
    return Path(__file__).resolve().parent.parent.parent

if __name__ == "__main__":
    project_root = _get_project_root()
    pipeline = LoadPipeline(project_root)

    pipeline.load_all()
