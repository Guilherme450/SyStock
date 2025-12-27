import polars as pl
import psycopg2
from pathlib import Path
from dataclasses import dataclass, field
from psycopg2.extras import execute_values
from icecream import ic

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "systock_dw",
    "user": "postgres",
    "password": "postgres"
}

@dataclass
class ReadSilverParquet:
    silver_dir: Path = field(repr=False)

    def read(self, entity_name: str) -> pl.DataFrame:
        file_path = self.silver_dir / f"{entity_name}.parquet"

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        return pl.read_parquet(file_path)


@dataclass
class BaseLoader:
    db_config: dict
    read_silver: ReadSilverParquet

    def _get_conn(self):
        return psycopg2.connect(**self.db_config)


@dataclass
class LoadDimension(BaseLoader):
    
    def load_dim_tempo(self):
        df_dim_tempo = self.read_silver.read("dim_tempo")

        if df_dim_tempo.is_empty():
            ic("dim_tempo empty — load skipped")
            return

        sql = """
                INSERT INTO analytics.dim_tempo (
                    id_tempo,
                    data_completa,
                    ano,
                    mes,
                    dia,
                    trimestre,
                    semana,
                    dia_semana,
                    eh_fim_semana
                ) 
                VALUES %s
        """

        records = list(df_dim_tempo.iter_rows())

        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, sql, records, page_size=1000)

        ic("dim_tempo load completed successfully")

    def load_dim_clientes(self):
        df_dim_cliente = self.read_silver.read("dim_clientes")

        if df_dim_cliente.is_empty():
            ic("dim_clients empty — load skipped")
            return

        sql = """
                INSERT INTO analytics.dim_cliente (
                    id_cliente_api,
                    nome_cliente,
                    cpf_cnpj,
                    email,
                    telefone,
                    endereco,
                    tipo_cliente,
                    data_carga
                ) 
                VALUES %s
                ON CONFLICT(id_cliente_api)
                DO UPDATE SET
                    nome_cliente = EXCLUDED.nome_cliente,
                    email = EXCLUDED.email,
                    telefone = EXCLUDED.telefone,
                    endereco = EXCLUDED.endereco,
                    tipo_cliente = EXCLUDED.tipo_cliente,
                    data_carga = EXCLUDED.data_carga;
        """

        records = list(df_dim_cliente.iter_rows())

        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, sql, records, page_size=1000)

        ic("dim_clients load completed successfully")

    def load_dim_lojas(self):
        df_dim_lojas = self.read_silver.read("dim_lojas")

        if df_dim_lojas.is_empty():
            ic("dim_stores empty — load skipped")
            return

        sql = """
                INSERT INTO analytics.dim_loja (
                    id_loja_api,
                    nome_loja,
                    endereco_loja,
                    data_carga
                ) 
                VALUES %s
                ON CONFLICT(id_loja_api)
                DO UPDATE SET
                    nome_loja = EXCLUDED.nome_loja,
                    endereco_loja = EXCLUDED.endereco_loja,
                    data_carga = EXCLUDED.data_carga;
        """

        records = list(df_dim_lojas.iter_rows())

        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, sql, records, page_size=1000)

        ic("dim_stores load completed successfully")

    def load_dim_produto(self):
        df_dim_produtos = self.read_silver.read("dim_produtos")

        if df_dim_produtos.is_empty():
            ic("dim_products empty — load skipped")
            return

        sql = """
                INSERT INTO analytics.dim_produto (
                    id_produto_api,
                    nome_produto,
                    descricao_produto,
                    id_categoria,
                    preco_venda,
                    custo_fornecedor,
                    ativo,
                    nome_categoria,
                    descricao_categoria
                ) 
                VALUES %s
                ON CONFLICT (id_produto_api)
                DO UPDATE SET
                    nome_produto = EXCLUDED.nome_produto,
                    descricao_produto = EXCLUDED.descricao_produto,
                    id_categoria = EXCLUDED.id_categoria,
                    preco_venda = EXCLUDED.preco_venda,
                    custo_fornecedor = EXCLUDED.custo_fornecedor,
                    ativo = EXCLUDED.ativo,
                    nome_categoria = EXCLUDED.nome_categoria,
                    descricao_categoria = EXCLUDED.descricao_categoria;
        """

        records = list(df_dim_produtos.iter_rows())

        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, sql, records, page_size=1000)

        ic("dim_products load completed successfully")


@dataclass
class LoadFacts(BaseLoader):

    def load_fact_estoque(self):
        df_fact_estoque = self.read_silver.read("fact_estoque")

        if df_fact_estoque.is_empty():
            ic("fact_inventory empty — load skipped")
            return

        sql = """
                INSERT INTO analytics.fato_estoque (
                    id_estoque_api,
                    id_tempo,
                    id_loja,
                    id_produto,
                    quantidade_inicial,
                    quantidade_final,
                    valor_estoque_inicial,
                    valor_estoque_final,
                    entradas,
                    saidas,
                    data_carga
                ) 
                VALUES %s
                ON CONFLICT(id_estoque_api)
                DO UPDATE SET
                    id_tempo = EXCLUDED.id_tempo,
                    id_loja = EXCLUDED.id_loja,
                    id_produto = EXCLUDED.id_produto,
                    quantidade_inicial = EXCLUDED.quantidade_inicial,
                    quantidade_final = EXCLUDED.quantidade_final,
                    valor_estoque_inicial = EXCLUDED.valor_estoque_inicial,
                    valor_estoque_final = EXCLUDED.valor_estoque_final,
                    entradas = EXCLUDED.entradas,
                    saidas = EXCLUDED.saidas,
                    data_carga = EXCLUDED.data_carga

                WHERE analytics.fato_estoque.data_carga < EXCLUDED.data_carga;
        """

        records = list(df_fact_estoque.iter_rows())

        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, sql, records, page_size=1000)

        ic("fact_inventory load completed successfully")

    def load_fact_vendas(self):
        df_fact_vendas = self.read_silver.read("fact_vendas")

        if df_fact_vendas.is_empty():
            ic("fact_sales empty — load skipped")
            return

        sql = """
                INSERT INTO analytics.fato_vendas (
                    id_venda_api,
                    id_tempo,
                    id_loja,
                    id_cliente,
                    id_produto,
                    quantidade,
                    valor_unitario,
                    custo_unitario,
                    valor_total,
                    custo_total,
                    lucro,
                    margem_lucro,
                    data_carga
                ) 
                VALUES %s
                ON CONFLICT(id_venda_api)
                DO UPDATE SET
                    id_tempo = EXCLUDED.id_tempo,
                    id_loja = EXCLUDED.id_loja,
                    id_cliente = EXCLUDED.id_cliente,
                    id_produto = EXCLUDED.id_produto,
                    quantidade = EXCLUDED.quantidade,
                    valor_unitario = EXCLUDED.valor_unitario,
                    custo_unitario = EXCLUDED.custo_unitario,
                    valor_total = EXCLUDED.valor_total,
                    custo_total = EXCLUDED.custo_total,
                    lucro = EXCLUDED.lucro,
                    margem_lucro = EXCLUDED.margem_lucro,
                    data_carga = EXCLUDED.data_carga
                
                WHERE analytics.fato_vendas.data_carga < EXCLUDED.data_carga;
        """

        records = list(df_fact_vendas.iter_rows())

        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, sql, records, page_size=1000)

        ic("fact_sales load completed successfully")

    def load_fact_distribuicao(self):
        df_fact_distribuicao = self.read_silver.read("fact_distribuicoes")

        if df_fact_distribuicao.is_empty():
            ic("fact_distributions empty — load skipped")
            return

        sql = """
                INSERT INTO analytics.fato_distribuicoes (
                    id_distribuicao_api,
                    id_loja_origem,
                    id_loja_destino,
                    id_tempo,
                    id_produto,
                    quantidade,
                    status,
                    data_carga
                ) 
                VALUES %s
                ON CONFLICT(id_distribuicao_api)
                DO UPDATE SET
                    id_loja_origem = EXCLUDED.id_loja_origem,
                    id_loja_destino = EXCLUDED.id_loja_destino,
                    id_tempo = EXCLUDED.id_tempo,
                    id_produto = EXCLUDED.id_produto,
                    quantidade = EXCLUDED.quantidade,
                    status = EXCLUDED.status,
                    data_carga = EXCLUDED.data_carga
                
                WHERE analytics.fato_distribuicoes.data_carga < EXCLUDED.data_carga;
        """

        records = list(df_fact_distribuicao.iter_rows())

        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, sql, records, page_size=1000)

        ic("fact_distributions load completed successfully")

if __name__ == '__main__':
    BASE_DIR = Path(__file__).resolve().parent
    project_root = BASE_DIR.parent.parent

    silver_dir_dim = project_root / "etl_pipeline" / "data" / "silver" / "dims"
    silver_dir_fact = project_root / "etl_pipeline" / "data" / "silver" / "facts"

    reader_dims = ReadSilverParquet(silver_dir_dim)
    reader_fact = ReadSilverParquet(silver_dir_fact)

    loader_dims = LoadDimension(DB_CONFIG, reader_dims)
    loader_facts = LoadFacts(DB_CONFIG, reader_fact)

    #loader_dims.load_dim_tempo()
    # loader_dims.load_dim_clientes()
    # loader_dims.load_dim_lojas()
    # loader_dims.load_dim_produto()

    # loader_facts.load_fact_estoque()
    # loader_facts.load_fact_vendas()
    # loader_facts.load_fact_distribuicao()