"""
Pipeline de Ingestão da Bronze Layer
Orquestra a extração de dados da API e ingestão na bronze layer.
"""

import logging
from typing import Dict, Optional, List
import sys
from icecream import ic
from pathlib import Path

# Adicionar path do parent para importar módulos
sys.path.insert(0, str(Path(__file__).parent.parent))

from extract.extract_api import extract_all_endpoints, get_data, ENDPOINTS
from bronze_manager import BronzeLayerManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BronzePipeline:
    """
    Pipeline que conecta Extract Layer com Bronze Layer.
    """
    
    def __init__(self, base_path: Optional[str] = None):
        """
        Inicializa o pipeline.
        
        Args:
            base_path: Caminho base para armazenar dados
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
        
        # Extrair dados de todos os endpoints
        extracted_data = extract_all_endpoints() # do modulo extract -> conexao direta com a fonte (API)
        
        # Ingerir dados na bronze layer
        results = self.bronze_manager.ingest_multiple_entities(extracted_data) # modulo bronze_manager 
        
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
        """
        Gera relatório da bronze layer.
        
        Returns:
            Dict com estatísticas de todas as entidades
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
        """
        Remove arquivos antigos de todas as entidades.
        
        Args:
            keep_count: Número de arquivos a manter por entidade
            
        Returns:
            Dict com número de arquivos removidos por entidade
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
    # Exemplo de uso
    pipeline = BronzePipeline()
    
    # Opção 1: Executar extração completa
    results = pipeline.run_full_extraction()
    ic("Extraction results:", results)
    
    # Opção 2: Extrair uma entidade específica
    # result = pipeline.run_single_entity_extraction('clientes')
    # print("Single entity result:", result)
    
    # Opção 3: Gerar relatório
    # report = pipeline.generate_report()
    # print("Bronze Layer Report:", report)
    
    # Opção 4: Limpar arquivos antigos
    # cleanup_results = pipeline.cleanup_old_data(keep_count=5)
    # print("Cleanup results:", cleanup_results)
    
    logger.info("Bronze pipeline available for use")
