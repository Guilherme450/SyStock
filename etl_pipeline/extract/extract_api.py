import os
import requests
import polars as pl
#import pyarrow
from pathlib import Path
from requests.exceptions import ConnectionError
from icecream import ic
from typing import Dict, Optional, List
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# A API funciona por paginação
API_URL = 'https://systock-api.onrender.com/'

ENDPOINTS = {
    'clientes': 'clients',
    'produtos': 'products',
    'lojas': 'stores',
    'vendas': 'sales',
    'estoque': 'stock',
    'movimentos': 'movements',
    'transportadoras': 'carriers',
    'fornecedores': 'suppliers',
    'categorias': 'categories',
    'entradas': 'entries',
    'distribuicao_interna': 'internal-distributions'
}


def get_data(url: str, retries: int = 3) -> Optional[Dict]:
    """
    Conecta com a API e retorna os dados.
    
    Args:
        url: URL da API para conectar
        retries: Número de tentativas em caso de erro
        
    Returns:
        Dict com dados da API, ou None se falhar
    """
    for attempt in range(retries):
        try:
            logger.info(f"Connecting to API: {url} (attempt {attempt + 1}/{retries})")
            
            with requests.Session() as session:
                response = session.get(url, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                logger.info(f"Successfully retrieved data from {url}")
                return data
        
        except ConnectionError as conn_err:
            logger.error(f"Connection error: {conn_err}")
            ic(conn_err)
        except requests.HTTPError as http_err:
            logger.error(f"HTTP error: {http_err}")
            ic(http_err)
        except requests.Timeout as timeout_err:
            logger.error(f"Timeout error: {timeout_err}")
            ic(timeout_err)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            ic(e)
    
    logger.error(f"Failed to retrieve data from {url} after {retries} attempts")
    return None

def extract_all_endpoints() -> Dict[str, List[Dict]]:
    """
    Extrai dados de todos os endpoints disponíveis.
    
    Returns:
        Dict com {endpoint_name: data}
    """
    extracted_data = {}
    
    for entity_name, endpoint in ENDPOINTS.items():
        url = f"{API_URL}{endpoint}"
        logger.info(f"Extracting data from endpoint: {entity_name}")
        
        data = get_data(url)
        extracted_data[entity_name] = data
    
    return extracted_data


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


def save_as_parquet(data: Dict, filename: str, output_dir: Optional[str] = None) -> Optional[str]:
     """
     Armazena dados no formato Parquet com compressão Snappy.
    
     Args:
         data: Dicionário com dados a salvar
         filename: Nome do arquivo (sem extensão)
         output_dir: Diretório de saída. Se None, usa o diretório padrão.
        
     Returns:
         str: Caminho do arquivo salvo, ou None se falhar
     """
     try:
         # Validar dados
         if not validate_data(data):
             logger.error(f"Data validation failed for {filename}")
             return None
        
         # Definir diretório de saída
         if output_dir is None:
             output_dir = Path(__file__).parent.parent / "data" / "raw"
         else:
             output_dir = Path(output_dir)
        
         output_dir.mkdir(parents=True, exist_ok=True)
        
         # Extrair dados se estiverem em estrutura nested
         if isinstance(data, dict) and 'data' in data:
             df_data = data['data']
         else:
             df_data = data
        
         # Converter para DataFrame Polars
         logger.info(f"Converting data to Polars DataFrame")
         df = pl.DataFrame(df_data)
        
         # Definir caminho do arquivo
         file_path = output_dir / f"{filename}.parquet"
        
         # Salvar como Parquet
         logger.info(f"Saving parquet file: {file_path}")
         df.write_parquet(
             file_path,
             compression='snappy',
             use_pyarrow=True
         )
        
         logger.info(f"Successfully saved {len(df)} records to {file_path}")
         return str(file_path)
    
     except Exception as e:
         logger.error(f"Error saving parquet file {filename}: {str(e)}")
         ic(e)
         return None 


if __name__ == '__main__':
    logger.info("Starting API extraction")

    # client_data = {}
    
    # data = get_data(API_URL + 'clients')

    # client_data['clients'] = data
    # if client_data:
    #      result = save_as_parquet(client_data, 'raw_clientes')
    #      ic(result)

    ic(extract_all_endpoints())
    
    logger.info("API extraction completed")