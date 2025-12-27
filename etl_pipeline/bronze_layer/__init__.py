"""
Bronze Layer - Raw Data Ingestion
Responsible for ingesting and storing raw API data in Parquet format.
"""

from .bronze_manager import BronzeLayerManager

__all__ = ["BronzeLayerManager"]
