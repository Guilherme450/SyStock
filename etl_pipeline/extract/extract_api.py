import os
import requests
import polars as pl
#import pyarrow
from pathlib import Path
from requests.exceptions import ConnectionError
from icecream import ic
from typing import Dict, Optional, List
import logging

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# The API supports paginated endpoints
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
    """Connect to the API and return JSON data.

    Args:
        url: API URL to request
        retries: number of retry attempts on failure

    Returns:
        dict with API response data, or None on failure
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
    """Extract data from all configured endpoints.

    Returns:
        mapping of {endpoint_name: data}
    """
    extracted_data = {}
    
    for entity_name, endpoint in ENDPOINTS.items():
        url = f"{API_URL}{endpoint}"
        logger.info(f"Extracting data from endpoint: {entity_name}")
        
        data = get_data(url)
        extracted_data[entity_name] = data
    
    return extracted_data


def validate_data(data: Dict) -> bool:
    """Validate that extracted data matches the expected format.

    Args:
        data: data extracted from the API

    Returns:
        bool: True if valid
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
    """Save data as a Parquet file with Snappy compression.

    Args:
        data: dictionary with data to save
        filename: output filename (without extension)
        output_dir: output directory; defaults to `etl_pipeline/data/raw`

    Returns:
        str: saved file path, or None on failure
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

    # Example usage (uncomment to run locally):
    # data = get_data(API_URL + 'clients')
    # save_as_parquet({'clients': data}, 'raw_clients')

    ic(extract_all_endpoints())
    logger.info("API extraction completed")