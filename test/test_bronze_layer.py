"""
Testes para a Bronze Layer
"""

import unittest
from unittest.mock import Mock, patch
from pathlib import Path
import tempfile
import polars as pl
from datetime import datetime

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl_pipeline.bronze_layer.bronze_manager import BronzeLayerManager
from etl_pipeline.extract.extract_api import validate_data


class TestBronzeLayerManager(unittest.TestCase):
    """Testes para BronzeLayerManager"""
    
    def setUp(self):
        """Configuração antes de cada teste"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.bronze = BronzeLayerManager(self.temp_dir.name)
    
    def tearDown(self):
        """Limpeza após cada teste"""
        self.temp_dir.cleanup()
    
    def test_initialization(self):
        """Testa inicialização do gerenciador"""
        self.assertIsNotNone(self.bronze.base_path)
        self.assertTrue(self.bronze.base_path.exists())
    
    def test_validate_empty_data(self):
        """Testa validação com dados vazios"""
        result = self.bronze.validate_raw_data({})
        self.assertFalse(result)
    
    def test_validate_invalid_type(self):
        """Testa validação com tipo inválido"""
        result = self.bronze.validate_raw_data("invalid")
        self.assertFalse(result)
    
    def test_validate_valid_data(self):
        """Testa validação com dados válidos"""
        valid_data = {
            'data': [
                {'id': 1, 'name': 'Cliente 1'},
                {'id': 2, 'name': 'Cliente 2'}
            ]
        }
        result = self.bronze.validate_raw_data(valid_data)
        self.assertTrue(result)
    
    def test_ingest_data_success(self):
        """Testa ingestão bem-sucedida de dados"""
        data = {
            'data': [
                {'id': 1, 'name': 'Cliente 1'},
                {'id': 2, 'name': 'Cliente 2'}
            ]
        }
        
        result = self.bronze.ingest_data(data, 'clientes')
        
        self.assertIsNotNone(result)
        self.assertTrue(Path(result).exists())
        self.assertTrue(result.endswith('.parquet'))
    
    def test_ingest_data_failure_empty(self):
        """Testa ingestão com dados vazios"""
        result = self.bronze.ingest_data({}, 'clientes')
        self.assertIsNone(result)
    
    def test_ingest_multiple_entities(self):
        """Testa ingestão de múltiplas entidades"""
        data_dict = {
            'clientes': {'data': [{'id': 1, 'name': 'Cliente 1'}]},
            'produtos': {'data': [{'id': 1, 'name': 'Produto 1'}]}
        }
        
        results = self.bronze.ingest_multiple_entities(data_dict)
        
        self.assertEqual(len(results), 2)
        self.assertIsNotNone(results['clientes'])
        self.assertIsNotNone(results['produtos'])
    
    def test_get_latest_file(self):
        """Testa obtenção do arquivo mais recente"""
        data = {'data': [{'id': 1, 'name': 'Test'}]}
        self.bronze.ingest_data(data, 'clientes')
        
        latest = self.bronze.get_latest_file('clientes')
        self.assertIsNotNone(latest)
        self.assertTrue(latest.exists())
    
    def test_get_latest_file_not_exist(self):
        """Testa obtenção de arquivo quando entidade não existe"""
        latest = self.bronze.get_latest_file('inexistente')
        self.assertIsNone(latest)
    
    def test_read_latest_data(self):
        """Testa leitura de dados mais recentes"""
        data = {'data': [
            {'id': 1, 'name': 'Test 1'},
            {'id': 2, 'name': 'Test 2'}
        ]}
        self.bronze.ingest_data(data, 'clientes')
        
        df = self.bronze.read_latest_data('clientes')
        
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 2)
        self.assertIn('id', df.columns)
        self.assertIn('name', df.columns)
    
    def test_read_latest_data_not_exist(self):
        """Testa leitura quando dados não existem"""
        df = self.bronze.read_latest_data('inexistente')
        self.assertIsNone(df)
    
    def test_list_entities(self):
        """Testa listagem de entidades"""
        data = {'data': [{'id': 1}]}
        self.bronze.ingest_data(data, 'clientes')
        self.bronze.ingest_data(data, 'produtos')
        
        entities = self.bronze.list_entities()
        
        self.assertEqual(len(entities), 2)
        self.assertIn('clientes', entities)
        self.assertIn('produtos', entities)
    
    def test_get_entity_statistics(self):
        """Testa obtenção de estatísticas"""
        data = {'data': [
            {'id': 1, 'name': 'Test 1'},
            {'id': 2, 'name': 'Test 2'}
        ]}
        self.bronze.ingest_data(data, 'clientes')
        
        stats = self.bronze.get_entity_statistics('clientes')
        
        self.assertEqual(stats['entity_name'], 'clientes')
        self.assertEqual(stats['total_records'], 2)
        self.assertEqual(stats['total_columns'], 4)  # id, name, _ingestion_timestamp, _entity_name
    
    def test_cleanup_old_files(self):
        """Testa limpeza de arquivos antigos"""
        data = {'data': [{'id': 1}]}
        
        # Criar 3 arquivos
        for i in range(3):
            self.bronze.ingest_data(data, 'clientes')
        
        # Manter apenas 1
        removed = self.bronze.cleanup_old_files('clientes', keep_count=1)
        
        self.assertEqual(removed, 2)
        
        # Verificar que apenas 1 arquivo permanece
        files = list((self.bronze.base_path / 'clientes').glob('*.parquet'))
        self.assertEqual(len(files), 1)


class TestValidateData(unittest.TestCase):
    """Testes para função validate_data"""
    
    def test_validate_valid_data(self):
        """Testa validação de dados válidos"""
        data = {'data': [{'id': 1}]}
        result = validate_data(data)
        self.assertTrue(result)
    
    def test_validate_empty_data(self):
        """Testa validação de dados vazios"""
        result = validate_data({})
        self.assertFalse(result)
    
    def test_validate_invalid_type(self):
        """Testa validação com tipo inválido"""
        result = validate_data("invalid")
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
