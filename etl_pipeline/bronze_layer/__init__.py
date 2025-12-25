"""
Bronze Layer - Raw Data Ingestion
Responsável por ingerir e armazenar dados brutos da API em formato Parquet.
"""

from .bronze_manager import BronzeLayerManager
from .bronze_pipeline import BronzePipeline

__all__ = ["BronzeLayerManager", "BronzePipeline"]
