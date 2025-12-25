"""
Configurações do ETL Pipeline
"""

from pathlib import Path
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

# Caminhos
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
LOGS_DIR = PROJECT_ROOT / "logs"

# Criar diretórios
for dir_path in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, LOGS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# API Configuration
API_URL = os.getenv('API_URL', 'https://systock-api.onrender.com/')
API_TIMEOUT = int(os.getenv('API_TIMEOUT', '30'))
API_RETRIES = int(os.getenv('API_RETRIES', '3'))

# Database Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', 'systock')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

# Parquet Configuration
PARQUET_COMPRESSION = os.getenv('PARQUET_COMPRESSION', 'snappy')
KEEP_FILES_COUNT = int(os.getenv('KEEP_FILES_COUNT', '5'))

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Entities
# ENTITIES = {
#     'clientes': 'cliente',
#     'produtos': 'produto',
#     'lojas': 'loja',
#     'vendas': 'venda',
#     'estoque': 'estoque'
# }

# Prefect Configuration
PREFECT_LOG_LEVEL = os.getenv('PREFECT_LOG_LEVEL', 'INFO')

# print(f"""
# ETL Pipeline Configuration:
# - API URL: {API_URL}
# - Data Directory: {DATA_DIR}
# - Entities: {len(ENTITIES)}
# - Keep Files: {KEEP_FILES_COUNT}
# """)
