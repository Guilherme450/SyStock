"""
Módulo de extração de dados da API
"""

from .extract_api import (
    get_data,
    extract_all_endpoints,
    validate_data,
    save_as_parquet,
    ENDPOINTS,
    API_URL
)

__all__ = [
    "get_data",
    "extract_all_endpoints",
    "validate_data",
    "save_as_parquet",
    "ENDPOINTS",
    "API_URL"
]
