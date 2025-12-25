import logging
from typing import Dict

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def validate_data(data: Dict) -> bool:
    """
    Valida se os dados extraídos estão no formato esperado.
    
    Args:
        data: Dados extraídos da API
        
    Returns:
        bool: True se válido
    """
    if not data:
        logger.warning("Empty data received")
        return False
    
    if not isinstance(data, dict):
        logger.error(f"Invalid data format: expected dict, got {type(data)}")
        return False
    
    logger.info(f"Data validation successful")
    return True

