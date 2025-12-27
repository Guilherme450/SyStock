"""Bronze Layer Manager

Manages ingestion and storage of raw API data into Parquet files.
"""

import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import polars as pl
from icecream import ic

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BronzeLayerManager:
    """Manager for the Bronze layer.

    Responsibilities:
    - receive raw API data
    - basic validation
    - store as Parquet with compression
    - maintain ingestion history with timestamps
    """

    def __init__(self, base_path: Optional[str] = None):
        """Initialize the Bronze layer manager.

        Args:
            base_path: base path for storing files. If None, uses the default data/bronze path.
        """
        if base_path is None:
            base_path = Path(__file__).parent.parent / "data" / "bronze"
        else:
            base_path = Path(base_path)
        
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Bronze Layer initialized at: {self.base_path}")

    def validate_raw_data(self, data: List) -> bool:
        # TODO: The extract module already provides validation; keep lightweight checks here.

        """Validate raw data received from the API.

        Args:
            data: list of dictionaries with raw records

        Returns:
            bool: True if data is valid
        """
        if not data:
            logger.warning("Empty data received")
            return False
        
        if not isinstance(data, list):
            logger.error(f"Invalid data type: {type(data)}. Expected list.")
            return False
        
        logger.info(f"Data validation passed. Records: {len(data) if isinstance(data, list) else 1}")
        return True

    def ingest_data(self, data: List, entity_name: str) -> Optional[str]:
        """Ingest raw data and store it as a Parquet file.

        Args:
            data: data payload from the API
            entity_name: entity name (e.g., 'clientes', 'produtos')

        Returns:
            str: saved file path, or None on failure
        """
        try:
            # Validar dados
            if not self.validate_raw_data(data):
                logger.error(f"Data validation failed for entity: {entity_name}")
                return None
            
            # Converter para DataFrame Polars
            logger.info(f"Converting data to Polars DataFrame for entity: {entity_name}")
            
            # Tratar dados se forem dict com lista dentro
            if isinstance(data, dict) and 'data' in data:
                df_data = data['data']
            else:
                df_data = data
            
            df = pl.DataFrame(df_data)
            
            # Adicionar colunas de auditoria
            df = df.with_columns([
                pl.lit(datetime.now()).alias("_ingestion_timestamp"),
                pl.lit(entity_name).alias("_entity_name"),
            ])
            
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{entity_name}_raw_{timestamp}.parquet"
            file_path = self.base_path / entity_name / filename
            
            # Criar diretório se não existir
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Salvar como Parquet com compressão
            logger.info(f"Saving data to: {file_path}")
            df.write_parquet(
                file_path,
                compression='snappy',
                use_pyarrow=True
            )
            
            logger.info(
                f"Successfully ingested {len(df)} records for entity '{entity_name}' "
                f"to {file_path}"
            )
            
            return str(file_path)
        
        except Exception as e:
            logger.error(f"Error ingesting data for entity '{entity_name}': {str(e)}")
            ic(e)
            return None

    def ingest_multiple_entities(
        self,
        data_dict: Dict[str, List[Dict]]
    ) -> Dict[str, Optional[str]]:
        # TODO: The extract module can already batch; this method coordinates ingestion.

        """Ingest multiple entities at once.

        Args:
            data_dict: mapping of {entity_name: data}

        Returns:
            dict with results per entity
        """
        results = {}
        
        for entity_name, data in data_dict.items():
            logger.info(f"Processing entity: {entity_name}")

            result = self.ingest_data(data, entity_name)
            results[entity_name] = result
        
        return results

    def _get_latest_file(self, entity_name: str) -> Optional[Path]:
        """Return the most recent Parquet file for an entity, or None if missing."""
        entity_path = self.base_path / entity_name
        
        if not entity_path.exists():
            logger.warning(f"No data found for entity: {entity_name}")
            return None
        
        parquet_files = list(entity_path.glob("*.parquet"))
        
        if not parquet_files:
            logger.warning(f"No parquet files found for entity: {entity_name}")
            return None
        
        latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Latest file for {entity_name}: {latest_file}")
        
        return latest_file

    def _read_latest_data(self, entity_name: str) -> Optional[pl.DataFrame]:
        """Read the latest Parquet file for an entity and return a Polars DataFrame."""
        latest_file = self._get_latest_file(entity_name)
        
        if latest_file is None:
            return None
        
        try:
            logger.info(f"Reading data from: {latest_file}")
            df = pl.read_parquet(latest_file)
            logger.info(f"Successfully read {len(df)} records from {latest_file}")
            return df
        
        except Exception as e:
            logger.error(f"Error reading parquet file {latest_file}: {str(e)}")
            return None

    def list_entities(self) -> List[str]:
        """List all entity directories in the bronze layer."""
        entities = [d.name for d in self.base_path.iterdir() if d.is_dir()]
        logger.info(f"Available entities: {entities}")
        return sorted(entities)

    def get_entity_statistics(self, entity_name: str) -> Dict:
        """Return statistics for the latest file of the specified entity."""
        df = self._read_latest_data(entity_name)
        
        if df is None:
            return {}
        
        stats = {
            "entity_name": entity_name,
            "total_records": len(df),
            "total_columns": len(df.columns),
            "columns": df.columns,
            "last_ingestion": df.select(pl.col("_ingestion_timestamp")).max()[0, 0],
            "file_size_mb": self._get_latest_file(entity_name).stat().st_size / (1024 * 1024)
        }
        
        logger.info(f"Statistics for {entity_name}: {stats}")
        return stats

    def cleanup_old_files(self, entity_name: str, keep_count: int = 5) -> int:
        """Remove old files, keeping the N most recent ones.

        Args:
            entity_name: entity folder name
            keep_count: number of recent files to keep

        Returns:
            int: number of files removed
        """
        entity_path = self.base_path / entity_name
        
        if not entity_path.exists():
            logger.warning(f"Entity path not found: {entity_path}")
            return 0
        
        parquet_files = sorted(
            entity_path.glob("*.parquet"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        
        removed_count = 0
        for file_to_remove in parquet_files[keep_count:]:
            try:
                file_to_remove.unlink()
                logger.info(f"Removed old file: {file_to_remove}")
                removed_count += 1
            except Exception as e:
                logger.error(f"Error removing file {file_to_remove}: {str(e)}")
        
        return removed_count
