"""
Silver Layer Pipeline - Orchestration of data transformations

This module provides the pipeline orchestration for transforming bronze layer
data to silver layer dimensions and facts.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from silver_layer.silver_manager import SilverLayerManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class SilverPipeline:
    """
    Silver Layer Pipeline Orchestrator.
    
    Manages the complete transformation process from bronze to silver layer,
    with error handling and reporting capabilities.
    """

    def __init__(
        self,
        bronze_dir: Optional[Path] = None,
        silver_dir: Optional[Path] = None,
    ):
        """
        Initialize the silver layer pipeline.

        Parameters
        ----------
        bronze_dir : Path, optional
            Path to bronze layer data. Defaults to project structure.
        silver_dir : Path, optional
            Path to silver layer output. Defaults to project structure.
        """
        self.manager = SilverLayerManager(
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
        )
        self.execution_results = {}

    def run_full_transformation(self) -> Dict[str, int]:
        """
        Run complete transformation of all dimensions and facts.

        Returns
        -------
        dict
            Results with row counts for each transformed entity.
        """
        logger.info("=" * 70)
        logger.info("STARTING FULL TRANSFORMATION - SILVER LAYER")
        logger.info("=" * 70)

        try:
            results = self.manager.transform_all()
            self.execution_results = results

            logger.info("=" * 70)
            logger.info("✓ FULL TRANSFORMATION COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)

            return results

        except Exception as e:
            logger.error(f"✗ Error during full transformation: {e}")
            raise

    # def run_single_dimension_extraction(self, dimension_name: str) -> int:
    #     """
    #     Run extraction for a single dimension.

    #     Parameters
    #     ----------
    #     dimension_name : str
    #         Name of the dimension to extract.

    #     Returns
    #     -------
    #     int
    #         Number of rows transformed.
    #     """
    #     logger.info(f"Transformando dimensão: {dimension_name}")

    #     try:
    #         df = self.manager.transform_dimension(dimension_name, save=True)
    #         row_count = len(df)
    #         self.execution_results[dimension_name] = row_count

    #         logger.info(f"✓ Dimensão {dimension_name} transformada: {row_count} linhas")
    #         return row_count

    #     except Exception as e:
    #         logger.error(f"✗ Erro ao transformar dimensão {dimension_name}: {e}")
    #         raise

    # def run_single_fact_extraction(self, fact_name: str) -> int:
    #     """
    #     Run extraction for a single fact.

    #     Parameters
    #     ----------
    #     fact_name : str
    #         Name of the fact to extract.

    #     Returns
    #     -------
    #     int
    #         Number of rows transformed.
    #     """
    #     logger.info(f"Transformando fato: {fact_name}")

    #     try:
    #         df = self.manager.transform_fact(fact_name, save=True)
    #         row_count = len(df)
    #         self.execution_results[fact_name] = row_count

    #         logger.info(f"✓ Fato {fact_name} transformado: {row_count} linhas")
    #         return row_count

    #     except Exception as e:
    #         logger.error(f"✗ Erro ao transformar fato {fact_name}: {e}")
    #         raise

    def generate_report(self) -> str:
        """
        Generate a detailed report of the silver layer state.

        Returns
        -------
        str
            Formatted report.
        """
        logger.info("Generating report...")
        report = self.manager.generate_report()
        logger.info(report)
        return report

    def cleanup_old_data(self, days: int = 30) -> int:
        """
        Clean up old silver layer files.

        Parameters
        ----------
        days : int
            Files older than this many days will be removed.

        Returns
        -------
        int
            Number of files removed.
        """
        logger.info(f"Cleaning files older than {days} days...")

        from datetime import timedelta
        import os

        cutoff_time = (datetime.now() - timedelta(days=days)).timestamp()
        removed_count = 0

        for directory in [self.manager.dims_dir, self.manager.facts_dir]:
            for file in directory.glob("*.parquet"):
                if file.stat().st_mtime < cutoff_time:
                    try:
                        file.unlink()
                        removed_count += 1
                        logger.debug(f"Removed: {file}")
                    except Exception as e:
                        logger.error(f"Error removing {file}: {e}")

        logger.info(f"✓ {removed_count} files removed")
        return removed_count


def main():
    """Main execution function."""
    pipeline = SilverPipeline()

    # Run full extraction
    pipeline.run_full_transformation()

    # Generate report
    pipeline.generate_report()


if __name__ == "__main__":
    main()