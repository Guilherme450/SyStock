"""Bronze Layer ingestion pipeline.

Orchestrates API extraction and ingestion into the Bronze layer.
"""

import logging
from typing import Dict, Optional, List
import sys
from icecream import ic
from pathlib import Path

# Add parent path to import local modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from extract.extract_api import extract_all_endpoints

from bronze_layer.bronze_manager import BronzeLayerManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BronzePipeline:
    """Pipeline connecting the Extract layer with the Bronze layer."""
    
    def __init__(self, base_path: Optional[str] = None):
        """Initialize the pipeline.

        Args:
            base_path: base path to store data
        """
        self.bronze_manager = BronzeLayerManager(base_path)
        logger.info("Bronze Pipeline initialized")
    
    def run_full_extraction(self) -> Dict[str, Optional[str]]:
        """
        Executa extração completa de todos os endpoints.
        
        Returns:
            Dict com resultado de cada entidade
        """
        logger.info("Starting full extraction pipeline")
        
        # Extract data from all endpoints
        extracted_data = extract_all_endpoints()

        # Ingest data into the Bronze layer
        results = self.bronze_manager.ingest_multiple_entities(extracted_data)
        
        logger.info("Full extraction pipeline completed")
        return results
    
    # def run_single_entity_extraction(self, entity_name: str) -> Optional[str]:
    #     """
    #     Executa extração de uma entidade específica.
        
    #     Args:
    #         entity_name: Nome da entidade (ex: 'clientes', 'produtos')
            
    #     Returns:
    #         str: Caminho do arquivo salvo, ou None se falhar
    #     """
    #     logger.info(f"Starting extraction for entity: {entity_name}")
        
    #     if entity_name not in ENDPOINTS:
    #         logger.error(f"Unknown entity: {entity_name}")
    #         logger.info(f"Available entities: {list(ENDPOINTS.keys())}")
    #         return None
        
    #     # Extrair dados
    #     url = f"https://systock-api.onrender.com/{ENDPOINTS[entity_name]}"
    #     data = get_data(url)
        
    #     if data is None:
    #         logger.error(f"Failed to extract data for {entity_name}")
    #         return None
        
    #     # Ingerir na bronze layer
    #     result = self.bronze_manager.ingest_data(data, entity_name)
        
    #     logger.info(f"Extraction for {entity_name} completed")
    #     return result
    
    def generate_report(self) -> Dict:
        """Generate a report for the Bronze layer.

        Returns:
            dict with statistics for all entities
        """
        logger.info("Generating bronze layer report")
        
        entities = self.bronze_manager.list_entities()
        report = {
            "total_entities": len(entities),
            "entities": {}
        }
        
        for entity in entities:
            stats = self.bronze_manager.get_entity_statistics(entity)
            report["entities"][entity] = stats
        
        logger.info(f"Report generated for {len(entities)} entities")
        return report
    
    def cleanup_old_data(self, keep_count: int = 5) -> Dict[str, int]:
        """Remove old files for all entities.

        Args:
            keep_count: number of files to keep per entity

        Returns:
            dict with number of files removed per entity
        """
        logger.info(f"Starting cleanup (keeping {keep_count} recent files per entity)")
        
        entities = self.bronze_manager.list_entities()
        cleanup_results = {}
        
        for entity in entities:
            removed = self.bronze_manager.cleanup_old_files(entity, keep_count)
            cleanup_results[entity] = removed
        
        logger.info(f"Cleanup completed. Removed {sum(cleanup_results.values())} files")
        return cleanup_results


if __name__ == '__main__':
    # Example usage
    pipeline = BronzePipeline()

    # Option 1: run full extraction
    results = pipeline.run_full_extraction()
    ic("Extraction results:", results)

    # Option 2: generate report
    # report = pipeline.generate_report()
    # print("Bronze Layer Report:", report)

    # Option 3: cleanup older files
    # cleanup_results = pipeline.cleanup_old_data(keep_count=5)
    # print("Cleanup results:", cleanup_results)

    logger.info("Bronze pipeline available for use")
