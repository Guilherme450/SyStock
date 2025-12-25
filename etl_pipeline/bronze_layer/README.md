# Bronze Layer - Documentação

## Visão Geral
A Bronze Layer é responsável pela ingestão e armazenamento de dados brutos extraídos da API. Os dados são armazenados em formato Parquet com compressão Snappy para eficiência de armazenamento e leitura.

## Arquivos da Bronze Layer

### 1. `bronze_manager.py`
Gerenciador central da Bronze Layer com funcionalidades:
- **Validação de dados brutos**
- **Ingestão e armazenamento em Parquet**
- **Leitura de dados mais recentes**
- **Estatísticas por entidade**
- **Limpeza de arquivos antigos**

#### Classe: `BronzeLayerManager`

**Métodos principais:**

#### `__init__(base_path: Optional[str] = None)`
Inicializa o gerenciador.
```python
bronze = BronzeLayerManager()  # Usa caminho padrão
# ou
bronze = BronzeLayerManager("/custom/path")
```

#### `ingest_data(data: Dict, entity_name: str) -> Optional[str]`
Ingere dados de uma entidade.
```python
file_path = bronze.ingest_data(api_data, 'clientes')
```

#### `ingest_multiple_entities(data_dict: Dict[str, Dict]) -> Dict[str, Optional[str]]`
Ingere múltiplas entidades.
```python
results = bronze.ingest_multiple_entities({
    'clientes': cliente_data,
    'produtos': produto_data,
})
```

#### `read_latest_data(entity_name: str) -> Optional[pl.DataFrame]`
Lê o arquivo Parquet mais recente.
```python
df = bronze.read_latest_data('clientes')
if df is not None:
    print(df)
```

#### `list_entities() -> List[str]`
Lista todas as entidades com dados.
```python
entities = bronze.list_entities()
# ['clientes', 'produtos', 'vendas']
```

#### `get_entity_statistics(entity_name: str) -> Dict`
Obtém estatísticas de uma entidade.
```python
stats = bronze.get_entity_statistics('clientes')
# {
#     'total_records': 1000,
#     'total_columns': 10,
#     'file_size_mb': 2.5,
#     ...
# }
```

#### `cleanup_old_files(entity_name: str, keep_count: int = 5) -> int`
Remove arquivos antigos mantendo N mais recentes.
```python
removed = bronze.cleanup_old_files('clientes', keep_count=5)
```

### 2. `bronze_pipeline.py`
Pipeline de orquestração que conecta Extract com Bronze Layer.

#### Classe: `BronzePipeline`

**Métodos principais:**

#### `run_full_extraction() -> Dict[str, Optional[str]]`
Extrai todos os endpoints e ingere na bronze.
```python
pipeline = BronzePipeline()
results = pipeline.run_full_extraction()
# {'clientes': '/path/to/file.parquet', ...}
```

#### `run_single_entity_extraction(entity_name: str) -> Optional[str]`
Extrai uma entidade específica.
```python
result = pipeline.run_single_entity_extraction('clientes')
```

#### `generate_report() -> Dict`
Gera relatório com estatísticas de todas as entidades.
```python
report = pipeline.generate_report()
# {
#     'total_entities': 5,
#     'entities': {
#         'clientes': {...},
#         'produtos': {...}
#     }
# }
```

#### `cleanup_old_data(keep_count: int = 5) -> Dict[str, int]`
Remove arquivos antigos de todas as entidades.
```python
cleanup_results = pipeline.cleanup_old_data(keep_count=5)
# {'clientes': 2, 'produtos': 1, ...}
```

### 3. `extract_api.py` (Atualizado)
Módulo de extração melhorado com:
- **Retry automático (3 tentativas)**
- **Timeout handling**
- **Logging detalhado**
- **Suporte para múltiplos endpoints**

#### Funções principais:

#### `get_data(url: str, retries: int = 3) -> Optional[Dict]`
```python
data = get_data('https://systock-api.onrender.com/cliente')
```

#### `extract_all_endpoints() -> Dict[str, Optional[Dict]]`
```python
all_data = extract_all_endpoints()
# {'clientes': {...}, 'produtos': {...}, ...}
```

#### `save_as_parquet(data: Dict, filename: str, output_dir: Optional[str] = None) -> Optional[str]`
```python
file_path = save_as_parquet(data, 'raw_clientes')
```

## Estrutura de Diretórios

```
etl_pipeline/
├── data/
│   └── bronze/
│       ├── clientes/
│       │   ├── clientes_raw_20231201_120000.parquet
│       │   ├── clientes_raw_20231201_130000.parquet
│       │   └── clientes_raw_20231201_140000.parquet
│       ├── produtos/
│       │   └── ...
│       ├── vendas/
│       │   └── ...
│       └── ...
├── extract/
│   └── extract_api.py
└── bronze_layer/
    ├── __init__.py
    ├── bronze_manager.py
    ├── bronze_pipeline.py
    ├── examples.py
    └── README.md
```

## Fluxo de Dados

```
API (systock-api.onrender.com)
    ↓
extract_api.py (get_data)
    ↓
BronzePipeline (run_full_extraction)
    ↓
BronzeLayerManager (ingest_data)
    ↓
Parquet Files (data/bronze/{entity}/*.parquet)
    ↓
Silver Layer (próxima etapa)
```

## Uso Típico

### Extração Completa
```python
from bronze_layer.bronze_pipeline import BronzePipeline

pipeline = BronzePipeline()
results = pipeline.run_full_extraction()

for entity, file_path in results.items():
    if file_path:
        print(f"✓ {entity}: {file_path}")
    else:
        print(f"✗ {entity}: Falha na ingestão")
```

### Leitura de Dados
```python
from bronze_layer.bronze_manager import BronzeLayerManager

bronze = BronzeLayerManager()
df = bronze.read_latest_data('clientes')
print(f"Total de registros: {len(df)}")
print(df.head())
```

### Gerar Relatório
```python
pipeline = BronzePipeline()
report = pipeline.generate_report()

for entity, stats in report['entities'].items():
    print(f"{entity}: {stats['total_records']} registros")
```

## Logging

Todos os módulos usam logging estruturado. Configure conforme necessário:

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

## Próximos Passos

1. ✅ **Bronze Layer** (Ingestão Raw) - CONCLUÍDO
2. 🔄 **Silver Layer** (Limpeza e Validação) - Próxima etapa
3. ⏳ **Gold Layer** (Camada Analítica) - Futuro
4. ⏳ **Load** (Carregamento no DB) - Futuro

## Tratamento de Erros

A Bronze Layer inclui tratamento robusto para:
- Conexões perdidas com API
- Dados inválidos ou malformados
- Erros de I/O ao salvar arquivos
- Espaço em disco insuficiente

Todos os erros são registrados com contexto completo para debugging.
