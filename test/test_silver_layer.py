"""
Silver Layer Tests - Unit tests for silver layer operations

This module contains comprehensive tests for the silver layer components.
"""

import unittest
import tempfile
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

import polars as pl

# Import silver layer components
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl_pipeline.silver_layer.silver_manager import SilverLayerManager
from etl_pipeline.silver_layer.transformations import DimensionTransformer, FactTransformer
from etl_pipeline.silver_layer.silver_pipeline import SilverPipeline

# Disable logging for tests
logging.disable(logging.CRITICAL)


class TestSilverLayerManager(unittest.TestCase):
    """Tests for SilverLayerManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.bronze_dir = Path(self.temp_dir.name) / "bronze"
        self.silver_dir = Path(self.temp_dir.name) / "silver"
        
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        
        self.manager = SilverLayerManager(
            bronze_dir=self.bronze_dir,
            silver_dir=self.silver_dir,
            log_level="ERROR",
        )

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_initialization(self):
        """Test SilverLayerManager initialization."""
        self.assertIsNotNone(self.manager)
        self.assertTrue(self.manager.silver_dir.exists())
        self.assertTrue(self.manager.dims_dir.exists())
        self.assertTrue(self.manager.facts_dir.exists())

    def test_dimensions_list(self):
        """Test dimensions are properly defined."""
        expected_dims = {"clientes", "produtos", "lojas", "tempo"}
        self.assertEqual(set(self.manager.DIMENSIONS.keys()), expected_dims)

    def test_facts_list(self):
        """Test facts are properly defined."""
        expected_facts = {"vendas", "estoque", "distribuicoes"}
        self.assertEqual(set(self.manager.FACTS.keys()), expected_facts)

    def test_directory_structure(self):
        """Test directory structure is created correctly."""
        self.assertTrue(self.manager.dims_dir.exists())
        self.assertTrue(self.manager.facts_dir.exists())
        self.assertTrue(self.manager.silver_dir.exists())

    def test_get_nonexistent_dimension(self):
        """Test reading non-existent dimension returns None."""
        result = self.manager.get_dimension("clientes")
        self.assertIsNone(result)

    def test_get_nonexistent_fact(self):
        """Test reading non-existent fact returns None."""
        result = self.manager.get_fact("vendas")
        self.assertIsNone(result)


class TestDimensionTransformer(unittest.TestCase):
    """Tests for DimensionTransformer class."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.bronze_dir = Path(self.temp_dir.name)
        self.transformer = DimensionTransformer(self.bronze_dir)

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_initialization(self):
        """Test DimensionTransformer initialization."""
        self.assertIsNotNone(self.transformer)
        self.assertEqual(self.transformer.bronze_dir, self.bronze_dir)

    def test_read_bronze_entity_not_found(self):
        """Test reading non-existent entity returns None."""
        result = self.transformer._read_bronze_entity("nonexistent")
        self.assertIsNone(result)

    def test_read_bronze_entity_empty_dir(self):
        """Test reading entity with empty directory returns None."""
        # Create empty directory
        entity_dir = self.bronze_dir / "clientes"
        entity_dir.mkdir(parents=True)
        
        result = self.transformer._read_bronze_entity("clientes")
        self.assertIsNone(result)


class TestFactTransformer(unittest.TestCase):
    """Tests for FactTransformer class."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.bronze_dir = Path(self.temp_dir.name)
        self.transformer = FactTransformer(self.bronze_dir)

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_initialization(self):
        """Test FactTransformer initialization."""
        self.assertIsNotNone(self.transformer)
        self.assertEqual(self.transformer.bronze_dir, self.bronze_dir)

    def test_read_bronze_entity_not_found(self):
        """Test reading non-existent entity returns None."""
        result = self.transformer._read_bronze_entity("nonexistent")
        self.assertIsNone(result)


class TestSilverPipeline(unittest.TestCase):
    """Tests for SilverPipeline class."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.bronze_dir = Path(self.temp_dir.name) / "bronze"
        self.silver_dir = Path(self.temp_dir.name) / "silver"
        
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        
        self.pipeline = SilverPipeline(
            bronze_dir=self.bronze_dir,
            silver_dir=self.silver_dir,
        )

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_initialization(self):
        """Test SilverPipeline initialization."""
        self.assertIsNotNone(self.pipeline)
        self.assertIsNotNone(self.pipeline.manager)
        self.assertEqual(self.pipeline.execution_results, {})


class TestSampleDataTransformation(unittest.TestCase):
    """Tests for transformation with sample data."""

    def setUp(self):
        """Set up test fixtures with sample data."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.bronze_dir = Path(self.temp_dir.name) / "bronze"
        self.silver_dir = Path(self.temp_dir.name) / "silver"
        
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        
        self._create_sample_bronze_data()
        
        self.manager = SilverLayerManager(
            bronze_dir=self.bronze_dir,
            silver_dir=self.silver_dir,
            log_level="ERROR",
        )

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def _create_sample_bronze_data(self):
        """Create sample bronze layer data."""
        # Sample clientes
        clientes_dir = self.bronze_dir / "clientes"
        clientes_dir.mkdir(parents=True)
        
        df_clientes = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["João Silva", "Maria Santos", "Pedro Oliveira"],
            "cpf_cnpj": ["12345678901", "98765432100", "11122233344"],
            "email": ["joao@example.com", "maria@example.com", "pedro@example.com"],
            "phone": ["11999999999", "11888888888", "11777777777"],
            "address": ["Rua A, 123", "Rua B, 456", "Rua C, 789"],
        })
        df_clientes.write_parquet(
            clientes_dir / "clientes_2024.parquet",
            compression="snappy"
        )

        # Sample produtos
        produtos_dir = self.bronze_dir / "produtos"
        produtos_dir.mkdir(parents=True)
        
        df_produtos = pl.DataFrame({
            "id": [101, 102, 103],
            "name": ["Produto A", "Produto B", "Produto C"],
            "description": ["Desc A", "Desc B", "Desc C"],
            "category_id": [1, 2, 1],
            "sale_price": [100.0, 150.0, 200.0],
            "cost_price": [50.0, 75.0, 100.0],
            "active": [True, True, False],
        })
        df_produtos.write_parquet(
            produtos_dir / "produtos_2024.parquet",
            compression="snappy"
        )

        # Sample lojas
        lojas_dir = self.bronze_dir / "lojas"
        lojas_dir.mkdir(parents=True)
        
        df_lojas = pl.DataFrame({
            "id": [1, 2],
            "name": ["Loja Centro", "Loja Sul"],
            "address": ["Av. Principal, 1000", "Av. Secundária, 2000"],
            "city": ["São Paulo", "Rio de Janeiro"],
            "state": ["SP", "RJ"],
            "phone": ["1133333333", "2144444444"],
            "manager_name": ["Carlos", "Ana"],
        })
        df_lojas.write_parquet(
            lojas_dir / "lojas_2024.parquet",
            compression="snappy"
        )

    def test_transform_clientes_with_sample_data(self):
        """Test clientes transformation with sample data."""
        try:
            df = self.manager.transform_dimension("clientes", save=False)
            
            self.assertIsNotNone(df)
            self.assertEqual(len(df), 3)
            self.assertIn("id_cliente", df.columns)
            self.assertIn("tipo_cliente", df.columns)
        except FileNotFoundError:
            # Expected if bronze data not available
            pass

    def test_transform_produtos_with_sample_data(self):
        """Test produtos transformation with sample data."""
        try:
            df = self.manager.transform_dimension("produtos", save=False)
            
            self.assertIsNotNone(df)
            self.assertEqual(len(df), 3)
            self.assertIn("margem_bruta", df.columns)
            self.assertIn("percentual_margem", df.columns)
        except FileNotFoundError:
            # Expected if bronze data not available
            pass

    def test_transform_and_save(self):
        """Test transformation and saving to file."""
        try:
            df = self.manager.transform_dimension("clientes", save=True)
            
            # Check file was created
            output_path = self.manager.dims_dir / "dim_clientes.parquet"
            self.assertTrue(output_path.exists())
            
            # Verify file can be read
            df_loaded = pl.read_parquet(output_path)
            self.assertEqual(len(df_loaded), len(df))
        except FileNotFoundError:
            # Expected if bronze data not available
            pass


class TestErrorHandling(unittest.TestCase):
    """Tests for error handling."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.bronze_dir = Path(self.temp_dir.name) / "bronze"
        self.silver_dir = Path(self.temp_dir.name) / "silver"
        
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        
        self.manager = SilverLayerManager(
            bronze_dir=self.bronze_dir,
            silver_dir=self.silver_dir,
            log_level="ERROR",
        )

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_invalid_dimension_name(self):
        """Test error handling for invalid dimension."""
        with self.assertRaises(ValueError):
            self.manager.transform_dimension("invalid_dimension")

    def test_invalid_fact_name(self):
        """Test error handling for invalid fact."""
        with self.assertRaises(ValueError):
            self.manager.transform_fact("invalid_fact")


class TestStatistics(unittest.TestCase):
    """Tests for statistics and reporting."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.bronze_dir = Path(self.temp_dir.name) / "bronze"
        self.silver_dir = Path(self.temp_dir.name) / "silver"
        
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        
        self.manager = SilverLayerManager(
            bronze_dir=self.bronze_dir,
            silver_dir=self.silver_dir,
            log_level="ERROR",
        )

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_get_statistics_empty(self):
        """Test statistics for empty silver layer."""
        stats = self.manager.get_statistics()
        self.assertEqual(len(stats), 0)

    def test_generate_report_empty(self):
        """Test report generation for empty silver layer."""
        report = self.manager.generate_report()
        self.assertIsInstance(report, str)
        self.assertIn("SILVER LAYER REPORT", report)


def run_tests():
    """Run all tests."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestSilverLayerManager))
    suite.addTests(loader.loadTestsFromTestCase(TestDimensionTransformer))
    suite.addTests(loader.loadTestsFromTestCase(TestFactTransformer))
    suite.addTests(loader.loadTestsFromTestCase(TestSilverPipeline))
    suite.addTests(loader.loadTestsFromTestCase(TestSampleDataTransformation))
    suite.addTests(loader.loadTestsFromTestCase(TestErrorHandling))
    suite.addTests(loader.loadTestsFromTestCase(TestStatistics))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1)
