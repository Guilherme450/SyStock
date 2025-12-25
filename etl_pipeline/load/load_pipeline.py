from pathlib import Path
#import argparse
#import sys
from typing import List

from load_entities import (
    ReadSilverParquet,
    LoadDimension,
    LoadFacts,
    DB_CONFIG,
)

class LoadPipeline:
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
        """Carrega todas as dimensões ou as selecionadas."""
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
        """Carrega todos os fatos ou os selecionados."""
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
        self.load_all_dimensions()
        self.load_all_facts()


def _get_project_root() -> Path:
    # assume file is in etl_pipeline/load
    return Path(__file__).resolve().parent.parent.parent


# def parse_args(argv: List[str]) -> argparse.Namespace:
#     p = argparse.ArgumentParser(description="Orquestrador de carregamento DW")
#     group = p.add_mutually_exclusive_group()
#     group.add_argument("--all", action="store_true", help="Carrega todas entidades")
#     group.add_argument("--dims", nargs="*", help="Carrega dimensões (ou lista)" )
#     group.add_argument("--facts", nargs="*", help="Carrega fatos (ou lista)")
#     return p.parse_args(argv)


# def main(argv: List[str] | None = None):
#     args = parse_args(list(argv) if argv is not None else None)

#     project_root = _get_project_root()
#     pipeline = LoadPipeline(project_root)

#     if args.all:
#         pipeline.load_all()
#         return

#     if args.dims is not None:
#         # if user passes empty list (i.e. --dims) args.dims == [] meaning all dims
#         selected = None if len(args.dims) == 0 else args.dims
#         pipeline.load_all_dimensions(selected)
#         return

#     if args.facts is not None:
#         selected = None if len(args.facts) == 0 else args.facts
#         pipeline.load_all_facts(selected)
#         return

#     print("Nenhuma ação especificada. Use --help para ver opções.")


# if __name__ == "__main__":
#     # argparse expects sys.argv[1:], so pass None to main to use sys.argv implicitly
#     load_pipe = LoadPipeline()
#     load_pipe.load_all()
