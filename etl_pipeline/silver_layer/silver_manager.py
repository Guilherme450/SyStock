"""
Silver Layer Manager - Central management for silver layer operations

This module provides the SilverLayerManager class which orchestrates
data transformations from bronze to silver layer.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import polars as pl

from transformations import DimensionTransformer, FactTransformer

logger = logging.getLogger(__name__)


class SilverLayerManager:
    """
    Central manager for silver layer operations.
    
    Handles data transformations from bronze layer to silver layer,
    managing dimensions and facts following a star schema pattern.
    """

    # Data entity names and their categories
    DIMENSIONS = {
        "clientes": "dim_clientes",
        "produtos": "dim_produtos",
        "lojas": "dim_lojas",
        "tempo": "dim_tempo",
    }

    FACTS = {
        "vendas": "fact_vendas",
        "estoque": "fact_estoque",
        "distribuicoes": "fact_distribuicoes",
    }

    def __init__(
        self,
        bronze_dir: Optional[Path] = None,
        silver_dir: Optional[Path] = None,
        log_level: str = "INFO",
    ):
        """
        Initialize Silver Layer Manager.

        Parameters
        ----------
        bronze_dir : Path, optional
            Path to bronze layer data directory. If None, uses default path.
        silver_dir : Path, optional
            Path to silver layer output directory. If None, creates silver/ subdirectory.
        log_level : str
            Logging level (DEBUG, INFO, WARNING, ERROR).
        """
        self.base_dir = Path(__file__).resolve().parent
        self.project_root = self.base_dir.parent.parent

        self.bronze_dir = bronze_dir or self.project_root / "etl_pipeline" / "data" / "bronze"
        self.silver_dir = silver_dir or self.project_root / "etl_pipeline" / "data" / "silver"

        # Create silver directory if it doesn't exist
        self.silver_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories for dimensions and facts
        self.dims_dir = self.silver_dir / "dims"
        self.facts_dir = self.silver_dir / "facts"
        self.dims_dir.mkdir(exist_ok=True)
        self.facts_dir.mkdir(exist_ok=True)

        # Initialize transformers
        self.dimension_transformer = DimensionTransformer(self.bronze_dir)
        self.fact_transformer = FactTransformer(self.bronze_dir)

        # Setup logging
        self._setup_logging(log_level)

        logger.info(f"✓ SilverLayerManager initialized")
        logger.debug(f"  Bronze dir: {self.bronze_dir}")
        logger.debug(f"  Silver dir: {self.silver_dir}")

    def _setup_logging(self, log_level: str) -> None:
        """Setup logging configuration."""
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, log_level))

    def transform_dimension(
         self,
         dimension_name: str,
         save: bool = True,
     ) -> pl.DataFrame:
         """
         Transform a specific dimension from bronze layer.

         Parameters
         ----------
         dimension_name : str
             Name of the dimension (clientes, produtos, lojas, tempo).
         save : bool
             Whether to save the transformed data to parquet.

         Returns
         -------
         pl.DataFrame
             Transformed dimension dataframe.

         Raises
         ------
         ValueError
             If dimension_name is not valid.
         """
         if dimension_name not in self.DIMENSIONS:
             raise ValueError(
                 f"Invalid dimension: {dimension_name}. "
                 f"Valid options: {list(self.DIMENSIONS.keys())}"
             )

         logger.info(f"Transforming dimension: {dimension_name}")

         try:
             # Get transformation method
             transform_method = getattr(
                 self.dimension_transformer,
                 f"transform_{dimension_name}",
                 None,
             )

             if not transform_method:
                 raise AttributeError(
                     f"Transformation method not found for {dimension_name}"
                 )

             # Execute transformation
             df = transform_method()

             if save:
                 output_path = (
                     self.dims_dir / f"{self.DIMENSIONS[dimension_name]}.parquet"
                 )
                 df.write_parquet(output_path, compression="snappy")
                 logger.info(
                     f"✓ Dimension {dimension_name} saved: {output_path} "
                     f"({len(df)} rows)"
                 )

             logger.debug(f"  Schema: {df.schema}")
             return df

         except Exception as e:
             logger.error(f"✗ Error transforming dimension {dimension_name}: {e}")
             raise

    def transform_fact(
         self,
         fact_name: str,
         save: bool = True,
     ) -> pl.DataFrame:
         """
         Transform a specific fact from bronze layer.

         Parameters
         ----------
         fact_name : str
             Name of the fact (vendas, estoque, distribuicoes).
         save : bool
             Whether to save the transformed data to parquet.

         Returns
         -------
         pl.DataFrame
             Transformed fact dataframe.

         Raises
         ------
         ValueError
             If fact_name is not valid.
         """
         if fact_name not in self.FACTS:
             raise ValueError(
                 f"Invalid fact: {fact_name}. "
                 f"Valid options: {list(self.FACTS.keys())}"
             )

         logger.info(f"Transforming fact: {fact_name}")

         try:
             # Get transformation method
             transform_method = getattr(
                 self.fact_transformer,
                 f"transform_{fact_name}",
                 None,
             )

             if not transform_method:
                 raise AttributeError(
                     f"Transformation method not found for {fact_name}"
                 )

             # Execute transformation
             df = transform_method()

             if save:
                 output_path = self.facts_dir / f"{self.FACTS[fact_name]}.parquet"
                 df.write_parquet(output_path, compression="snappy")
                 logger.info(
                     f"✓ Fact {fact_name} saved: {output_path} "
                     f"({len(df)} rows)"
                 )

             logger.debug(f"  Schema: {df.schema}")
             return df

         except Exception as e:
             logger.error(f"✗ Error transforming fact {fact_name}: {e}")
             raise

    def transform_all(self) -> Dict[str, int]:
        """
        Transform all dimensions and facts.

        Returns
        -------
        dict
            Dictionary with row counts for each transformed entity.
        """
        logger.info("Starting full transformation...")
        results = {}

        # Transform all dimensions
        logger.info("Transforming dimensions...")
        for dim_name in self.DIMENSIONS.keys():
            try:
                df = self.transform_dimension(dim_name, save=True)
                results[dim_name] = len(df)
            except Exception as e:
                logger.error(f"Failed to transform dimension {dim_name}: {e}")
                results[dim_name] = 0

        # Transform all facts
        logger.info("Transforming facts...")
        for fact_name in self.FACTS.keys():
            try:
                df = self.transform_fact(fact_name, save=True)
                results[fact_name] = len(df)
            except Exception as e:
                logger.error(f"Failed to transform fact {fact_name}: {e}")
                results[fact_name] = 0

        logger.info("✓ Full transformation completed")
        return results

    def get_dimension(self, dimension_name: str) -> Optional[pl.DataFrame]:
        """
        Load a transformed dimension from parquet.

        Parameters
        ----------
        dimension_name : str
            Name of the dimension.

        Returns
        -------
        pl.DataFrame or None
            Dimension dataframe or None if file not found.
        """
        try:
            output_path = (
                self.dims_dir / f"{self.DIMENSIONS[dimension_name]}.parquet"
            )
            if not output_path.exists():
                logger.warning(f"Dimension file not found: {output_path}")
                return None
            return pl.read_parquet(output_path)
        except Exception as e:
            logger.error(f"Error reading dimension {dimension_name}: {e}")
            raise

    def get_fact(self, fact_name: str) -> Optional[pl.DataFrame]:
        """
        Load a transformed fact from parquet.

        Parameters
        ----------
        fact_name : str
            Name of the fact.

        Returns
        -------
        pl.DataFrame or None
            Fact dataframe or None if file not found.
        """
        try:
            output_path = self.facts_dir / f"{self.FACTS[fact_name]}.parquet"
            if not output_path.exists():
                logger.warning(f"Fact file not found: {output_path}")
                return None
            return pl.read_parquet(output_path)
        except Exception as e:
            logger.error(f"Error reading fact {fact_name}: {e}")
            raise

    def get_statistics(self) -> Dict[str, Dict]:
        """
        Get statistics for all transformed entities.

        Returns
        -------
        dict
            Statistics including row counts, file sizes, etc.
        """
        stats = {}

        for dim_name, file_name in self.DIMENSIONS.items():
            try:
                path = self.dims_dir / f"{file_name}.parquet"
                if path.exists():
                    df = pl.read_parquet(path)
                    stats[dim_name] = {
                        "type": "dimension",
                        "rows": len(df),
                        "columns": len(df.columns),
                        "file_size_mb": path.stat().st_size / (1024 * 1024),
                    }
            except Exception as e:
                logger.warning(f"Could not get stats for {dim_name}: {e}")

        for fact_name, file_name in self.FACTS.items():
            try:
                path = self.facts_dir / f"{file_name}.parquet"
                if path.exists():
                    df = pl.read_parquet(path)
                    stats[fact_name] = {
                        "type": "fact",
                        "rows": len(df),
                        "columns": len(df.columns),
                        "file_size_mb": path.stat().st_size / (1024 * 1024),
                    }
            except Exception as e:
                logger.warning(f"Could not get stats for {fact_name}: {e}")

        return stats

    def generate_report(self) -> str:
        """
        Generate a detailed report of the silver layer.

        Returns
        -------
        str
            Formatted report string.
        """
        stats = self.get_statistics()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        report = f"""
╔════════════════════════════════════════════════════════════════════════╗
║                      SILVER LAYER REPORT                               ║
╚════════════════════════════════════════════════════════════════════════╝

Generated: {timestamp}
Location: {self.silver_dir}

DIMENSIONS:
"""
        total_rows = 0
        total_size = 0

        for dim_name, file_name in self.DIMENSIONS.items():
            if dim_name in stats:
                s = stats[dim_name]
                report += f"  {dim_name:15} {s['rows']:>10,} rows  {s['columns']:>3} cols  {s['file_size_mb']:>8.2f} MB\n"
                total_rows += s["rows"]
                total_size += s["file_size_mb"]

        report += f"\nFACTS:\n"
        for fact_name, file_name in self.FACTS.items():
            if fact_name in stats:
                s = stats[fact_name]
                report += f"  {fact_name:15} {s['rows']:>10,} rows  {s['columns']:>3} cols  {s['file_size_mb']:>8.2f} MB\n"
                total_rows += s["rows"]
                total_size += s["file_size_mb"]

        report += f"""
─────────────────────────────────────────────────────────────────────────
TOTAL: {len(stats)} entities | {total_rows:,} rows | {total_size:.2f} MB
"""
        return report
