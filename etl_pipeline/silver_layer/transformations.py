"""
Silver Layer Transformations - Data transformation logic

This module provides transformation classes for converting bronze layer data
into standardized silver layer dimensions and facts.
"""

import logging
from pathlib import Path
from typing import Optional

import polars as pl
from datetime import datetime

logger = logging.getLogger(__name__)


TIME_COLUMNS = {
    "vendas": ["sale_date", "predicted_delivery", "delivered_at"],
    "distribuicao_interna": ["distribution_date"],
    "estoque": ["updated_at"],
    "entradas": ["entry_date"],
}


class DimensionTransformer:
    """
    Transforms bronze layer data into silver layer dimensions.

    Handles the creation of dimensional data following a star schema pattern.
    """

    def __init__(self, bronze_dir: Path):
        """
        Initialize dimension transformer.

        Parameters
        ----------
        bronze_dir : Path
            Path to bronze layer data directory.
        """
        self.bronze_dir = bronze_dir

    def _read_bronze_entity(self, entity_name: str) -> Optional[pl.DataFrame]:
        """
        Read the most recent parquet file for an entity from bronze layer.

        Parameters
        ----------
        entity_name : str
            Name of the entity (e.g., 'clientes', 'produtos').

        Returns
        -------
        pl.DataFrame or None
            The dataframe if found, None otherwise.
        """
        entity_dir = self.bronze_dir / entity_name

        if not entity_dir.exists():
            logger.warning(f"Bronze entity directory not found: {entity_dir}")
            return None

        try:
            parquet_files = list(entity_dir.glob("*.parquet"))
            
            if not parquet_files:
                logger.warning(f"No parquet files found in {entity_dir}")
                return None

            # Get the most recent file
            latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
            logger.debug(f"Reading bronze data from: {latest_file}")

            return pl.read_parquet(latest_file)
        
        except Exception as e:
            logger.error(f"Error reading bronze entity {entity_name}: {e}")
            raise
    
    def _safe_datetime(self, value):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return None
        return None
    
    def _get_global_date_range(self) -> tuple[datetime, datetime]:
        min_dates = []
        max_dates = []

        for entity, cols in TIME_COLUMNS.items():
            df = self._read_bronze_entity(entity)

            if df is None or df.is_empty():
                continue

            for col in cols:
                if col not in df.columns:
                    continue

                series = df[col].drop_nulls()

                if series.is_empty():
                    continue

                min_val = self._safe_datetime(series.min())
                max_val = self._safe_datetime(series.max())

                if min_val:
                    min_dates.append(min_val)
                if max_val:
                    max_dates.append(max_val)

        if not min_dates or not max_dates:
            logger.warning("No valid datetime found. Using fallback range.")
            return datetime(2023, 1, 1), datetime.now()

        return min(min_dates), max(max_dates)

    def transform_clientes(self) -> pl.DataFrame:
        """
        Transform clientes (clients) data to dim_cliente.

        Returns
        -------
        pl.DataFrame
            Transformed dimension with standardized client data.
        """
        logger.info("Transforming clientes dimension...")

        df = self._read_bronze_entity("clientes")
        if df is None:
            raise FileNotFoundError("Bronze clientes data not found")

        # Transform clientes to dim_cliente
        df_transformed = (
            df
            .select(
                pl.col("id").alias("id_cliente"),
                pl.col("name").alias("nome_cliente"),
                pl.col("cpf_cnpj").alias("cpf_cnpj"),
                pl.col("email").alias("email"),
                pl.col("phone").alias("telefone"),
                pl.col("address").alias("endereco"),
            )
            .with_columns([
                pl.when(pl.col("cpf_cnpj").str.len_chars() == 11)
                    .then(pl.lit("Pessoa Física"))
                    .when(pl.col("cpf_cnpj").str.len_chars() == 14)
                    .then(pl.lit("Pessoa Jurídica"))
                    .otherwise(pl.lit("Não Classificado"))
                    .alias("tipo_cliente"),
                pl.lit(datetime.now()).alias("data_carga"),
            ])
            .unique(subset=["id_cliente", "cpf_cnpj"], keep="last")
        )

        logger.info(f"✓ Clientes transformed: {len(df_transformed)} rows")
        return df_transformed

    def transform_produtos(self) -> pl.DataFrame:
        """
        Transform produtos (products) data to dim_produto.

        Returns
        -------
        pl.DataFrame
            Transformed dimension with standardized product data.
        """
        logger.info("Transforming produtos dimension...")

        df_produtos = self._read_bronze_entity("produtos")
        df_categorias = self._read_bronze_entity("categorias")

        if df_produtos is None:
            raise FileNotFoundError("Bronze produtos data not found")

        # Start with produtos
        df_transformed = df_produtos.select(
            pl.col("id").alias("id_produto"),
            pl.col("name").alias("nome_produto"),
            pl.col("description").alias("descricao_produto"),
            pl.col("category_id").alias("id_categoria"),
            pl.col("sale_price").alias("preco_venda"),
            pl.col("cost_price").alias("custo_fornecedor"),
            pl.col("active").alias("ativo"),
        )

        # Join with categories if available
        if df_categorias is not None:
            df_categorias_renamed = df_categorias.select(
                pl.col("id").alias("id_categoria"),
                pl.col("name").alias("nome_categoria"),
                pl.col("description").alias("descricao_categoria"),
            )

            df_transformed = df_transformed.join(
                df_categorias_renamed,
                on="id_categoria",
                how="left",
            )
        else:
            df_transformed = df_transformed.with_columns([
                pl.lit(None).alias("nome_categoria"),
                pl.lit(None).alias("descricao_categoria"),
            ])

        # Calculate margins
        # df_transformed = df_transformed.with_columns([
        #     (pl.col("preco_venda") - pl.col("custo_fornecedor"))
        #     .alias("margem_bruta"),
        #     (
        #         ((pl.col("preco_venda") - pl.col("custo_fornecedor"))
        #         / pl.col("preco_venda").fill_null(1)) * 100
        #     ).round(2).alias("percentual_margem"),
        #     pl.lit(datetime.now()).alias("data_carga"),
        # ]).unique(subset=["id_produto"], keep="last")

        logger.info(f"✓ Produtos transformed: {len(df_transformed)} rows")
        return df_transformed

    def transform_lojas(self) -> pl.DataFrame:
        """
        Transform lojas (stores) data to dim_loja.

        Returns
        -------
        pl.DataFrame
            Transformed dimension with standardized store data.
        """
        logger.info("Transforming lojas dimension...")

        df = self._read_bronze_entity("lojas")
        if df is None:
            raise FileNotFoundError("Bronze lojas data not found")

        # Transform lojas to dim_loja
        df_transformed = (
            df
            .select(
                pl.col("id").alias("id_loja"),
                pl.col("name").alias("nome_loja"),
                pl.col("address").alias("endereco_loja")
            )
            .with_columns([
                pl.lit(datetime.now()).alias("data_carga"),
            ])
            .unique(subset=["id_loja"], keep="last")
        )

        logger.info(f"✓ Lojas transformed: {len(df_transformed)} rows")
        return df_transformed

    def transform_tempo(self) -> pl.DataFrame:
        """
        Transform time dimension.

        Creates a time dimension based on sales data range or predefined range.

        Returns
        -------
        pl.DataFrame
            Transformed time dimension with date components.
        """

        logger.info("Transforming tempo dimension...")

        min_date, max_date = self._get_global_date_range()

        date_range = pl.datetime_range(
            start=min_date,
            end=max_date,
            interval="1d",
            eager=True,
        )

        df_tempo = (
            pl.DataFrame({"data": date_range})
            .with_columns([
                pl.col("data").cast(pl.Date).alias("data_completa"),
                pl.col("data").dt.year().alias("ano"),
                pl.col("data").dt.month().alias("mes"),
                pl.col("data").dt.day().alias("dia"),
                pl.col("data").dt.quarter().alias("trimestre"),
                pl.col("data").dt.week().alias("semana"),
                pl.col("data").dt.weekday().alias("dia_semana"),
                (pl.col("data").dt.weekday() >= 5).alias("eh_fim_semana"),
                pl.col("data").dt.strftime("%Y%m%d").cast(pl.Int32).alias("id_tempo"),
            ])
            .select([
                "id_tempo",
                "data_completa",
                "ano",
                "mes",
                "dia",
                "trimestre",
                "semana",
                "dia_semana",
                "eh_fim_semana",
            ])
        )

        logger.info(f"✓ Tempo transformed: {len(df_tempo)} rows")
        return df_tempo


class FactTransformer:
    """
    Transforms bronze layer data into silver layer facts.

    Handles the creation of factual data following a star schema pattern.
    """

    def __init__(self, bronze_dir: Path):
        """
        Initialize fact transformer.

        Parameters
        ----------
        bronze_dir : Path
            Path to bronze layer data directory.
        """
        self.bronze_dir = bronze_dir

    def _read_bronze_entity(self, entity_name: str) -> Optional[pl.DataFrame]:
        """
        Read the most recent parquet file for an entity from bronze layer.

        Parameters
        ----------
        entity_name : str
            Name of the entity.

        Returns
        -------
        pl.DataFrame or None
            The dataframe if found, None otherwise.
        """
        entity_dir = self.bronze_dir / entity_name

        if not entity_dir.exists():
            logger.warning(f"Bronze entity directory not found: {entity_dir}")
            return None

        try:
            parquet_files = list(entity_dir.glob("*.parquet"))
            if not parquet_files:
                logger.warning(f"No parquet files found in {entity_dir}")
                return None

            # Get the most recent file
            latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
            logger.debug(f"Reading bronze data from: {latest_file}")

            return pl.read_parquet(latest_file)
        except Exception as e:
            logger.error(f"Error reading bronze entity {entity_name}: {e}")
            raise
    
    def _add_id_tempo(self, df: pl.DataFrame, col_data: str) -> pl.DataFrame:
        dtype = df.schema.get(col_data)

        if dtype == pl.Utf8:
            # string → datetime → date → id_tempo
            expr = (
                pl.col(col_data)
                .str.strptime(pl.Datetime, strict=False)
                .cast(pl.Date)
            )
        else:
            # datetime/date → date
            expr = pl.col(col_data).cast(pl.Date)

        return df.with_columns(
            expr
            .dt.strftime("%Y%m%d")
            .cast(pl.Int32)
            .alias("id_tempo")
        )

    def transform_vendas(self) -> pl.DataFrame:
        # TODO: implement the following derived metrics where applicable:
        # Direct calculations:
        # total_value = quantity * unit_price
        # total_cost = quantity * cost_unit
        # profit = total_value - total_cost
        # profit_margin = profit / total_value

        """
        Transform vendas (sales) data to fact_vendas.

        Returns
        -------
        pl.DataFrame
            Transformed fact table with sales data.
        """

        logger.info("Transforming vendas fact...")

        # df_vendas = self._read_bronze_entity("vendas")
        # #df_produtos = self._read_bronze_entity("produtos")

        # if df_vendas is None:
        #     raise FileNotFoundError("Bronze vendas data not found")

        # #Start with vendas and their items
        # df_transformed = (
        #     df_vendas
        #     .select(
        #         pl.col("id").alias("id_venda"),
        #         pl.col("sale_date"),
        #         pl.col("status").alias("status_venda"),
        #         pl.col("store_id").alias("id_loja"),
        #         pl.col("client_id").alias("id_cliente"),
        #     )
        # )

        # # adiciona id_tempo
        # df_transformed = self._add_id_tempo(df_transformed, "sale_date")

        # df_transformed = df_transformed.with_columns(
        #     pl.lit(datetime.now()).alias("data_carga")
        # ).select(
        #     "id_venda",
        #     "id_tempo",
        #     "id_loja",
        #     "id_cliente",
        #     "status_venda",
        #     "data_carga",
        # )

        # logger.info(f"✓ Vendas transformed: {len(df_transformed)} rows")

        # return df_transformed

        df_vendas = self._read_bronze_entity("vendas")
        df_produtos = self._read_bronze_entity("produtos")

        if df_vendas is None:
            raise FileNotFoundError("Bronze vendas data not found")

        # explode items and extrai campos do item
        df_items = (
            df_vendas
            .select(
                pl.col("id").alias("id_venda"),
                pl.col("sale_date"),
                pl.col("store_id").alias("id_loja"),
                pl.col("client_id").alias("id_cliente"),
                pl.col("items")
            )
            .explode("items")
            .with_columns([
                pl.col("items").struct.field("product_id").alias("id_produto"),
                pl.col("items").struct.field("quantity").alias("quantidade"),
                pl.col("items").struct.field("unit_price").alias("valor_unitario"),
                pl.col("items").struct.field("total_price").alias("custo_unitario"),
            ])
            .select([
                "id_venda", "sale_date", "id_loja", "id_cliente",
                "id_produto", "quantidade", "valor_unitario", "custo_unitario"
            ])
        )

        # preenche preços vindos de dim produtos quando item não traz
        if df_produtos is not None:
            prod_cols = df_produtos.select(
                pl.col("id").alias("id_produto"),
                pl.col("sale_price").alias("sale_price_prod"),
                pl.col("cost_price").alias("cost_price_prod"),
            )
            df_items = (
                df_items
                .join(prod_cols, on="id_produto", how="left")
                .with_columns([
                    pl.coalesce([pl.col("valor_unitario"), pl.col("sale_price_prod")]).alias("valor_unitario"),
                    pl.coalesce([pl.col("custo_unitario"), pl.col("cost_price_prod")]).alias("custo_unitario"),
                ])
                .drop(["sale_price_prod", "cost_price_prod"])
            )

        # cálculos financeiros por item
        df_items = df_items.with_columns([
            (pl.col("quantidade") * pl.col("valor_unitario")).alias("valor_total"),
            (pl.col("quantidade") * pl.col("custo_unitario")).alias("custo_total"),
        ]).with_columns([
            (pl.col("valor_total") - pl.col("custo_total")).alias("lucro"),
            pl.when(pl.col("valor_total") > 0)
              .then((pl.col("valor_total") - pl.col("custo_total")) / pl.col("valor_total"))
              .otherwise(None)
              .alias("margem_lucro")
        ])

        # id_tempo e data_carga
        df_items = self._add_id_tempo(df_items, "sale_date")
        df_items = df_items.with_columns(pl.lit(datetime.now()).alias("data_carga"))

        df_final = df_items.select([
            "id_venda", "id_tempo", "id_loja", "id_cliente",
            "id_produto", "quantidade", "valor_unitario", "custo_unitario",
            "valor_total", "custo_total", "lucro", "margem_lucro", "data_carga"
        ])

        logger.info(f"✓ Vendas transformed: {len(df_final)} rows")

        return df_final

    def transform_estoque(self) -> pl.DataFrame:
        # TODO: Add metrics such as:
        # initial_quantity (int), final_quantity (int),
        # initial_value (float), final_value (float),
        # inputs (int) and outputs (int)


        """
        Transform estoque (inventory) data to fact_estoque.

        Returns
        -------
        pl.DataFrame
            Transformed fact table with inventory data.
        """
        logger.info("Transforming estoque fact...")

        # df_estoque = self._read_bronze_entity("estoque")

        # if df_estoque is None:
        #     raise FileNotFoundError("Bronze estoque data not found")

        # df_transformed = (
        #     df_estoque
        #     .select(
        #         pl.col("id").alias("id_estoque"),
        #         pl.col("store_id").alias("id_loja"),
        #         pl.col("product_id").alias("id_produto"),
        #         pl.col("updated_at"),
        #         pl.col("quantity").alias("quantidade_total"),
        #     )
        # )

        # # adiciona id_tempo
        # df_transformed = self._add_id_tempo(df_transformed, "updated_at")

        # df_transformed = df_transformed.with_columns(
        #     pl.lit(datetime.now()).alias("data_carga")
        # ).select(
        #     "id_estoque",
        #     "id_tempo",
        #     "id_loja",
        #     "id_produto",
        #     "quantidade_total",
        #     "data_carga",
        # )

        # logger.info(f"✓ Estoque transformed: {len(df_transformed)} rows")

        # return df_transformed

        df_estoque = self._read_bronze_entity("estoque")
        df_produtos = self._read_bronze_entity("produtos")

        if df_estoque is None:
            raise FileNotFoundError("Bronze estoque data not found")

        df = (
            df_estoque
            .select(
                pl.col("id").alias("id_estoque"),
                pl.col("store_id").alias("id_loja"),
                pl.col("product_id").alias("id_produto"),
                pl.col("updated_at"),
                pl.col("quantity").alias("quantidade_total"),
            )
        )

        # calcula quantidade inicial (último snapshot), final e delta por partição
        df = df.with_columns([
            pl.col("quantidade_total").shift(1).over(["id_loja", "id_produto"]).alias("quantidade_inicial"),
            pl.col("quantidade_total").alias("quantidade_final"),
        ]).with_columns([
            (pl.col("quantidade_final") - pl.col("quantidade_inicial")).alias("delta_quantidade"),
            pl.col("quantidade_inicial").fill_null(0).alias("quantidade_inicial"),  # preencher nulos
        ]).with_columns([
            pl.when(pl.col("delta_quantidade") > 0).then(pl.col("delta_quantidade")).otherwise(0).alias("entrada"),
            pl.when(pl.col("delta_quantidade") < 0).then((-pl.col("delta_quantidade"))).otherwise(0).alias("saida"),
        ])

        # junta custo do produto para calcular valores
        if df_produtos is not None:
            prod_cost = df_produtos.select(
                pl.col("id").alias("id_produto"),
                pl.col("cost_price").alias("cost_price_prod")
            )
            df = df.join(prod_cost, on="id_produto", how="left")
        else:
            df = df.with_columns(pl.lit(0.0).alias("cost_price_prod"))

        df = df.with_columns([
            (pl.col("quantidade_inicial") * pl.col("cost_price_prod")).alias("valor_inicial"),
            (pl.col("quantidade_final") * pl.col("cost_price_prod")).alias("valor_final"),
        ])

        # id_tempo e data_carga
        df = self._add_id_tempo(df, "updated_at")
        df = df.with_columns(pl.lit(datetime.now()).alias("data_carga"))

        df_final = df.select([
            "id_estoque", "id_tempo", "id_loja", "id_produto",
            "quantidade_inicial", "quantidade_final",
            "valor_inicial", "valor_final",
            "entrada", "saida", "data_carga"
        ])

        logger.info(f"✓ Estoque transformed: {len(df_final)} rows")
        
        return df_final

    def transform_distribuicoes(self) -> pl.DataFrame:
        #TODO: realizar join entre distribuição_interna e itens_distribuição_interna

        """
        Transform distribuicoes (distributions) data to fact_distribuicoes.

        Returns
        -------
        pl.DataFrame
            Transformed fact table with distribution data.
        """
        logger.info("Transforming distribuicoes fact...")

        df_distribuicoes = self._read_bronze_entity("distribuicao_interna")

        if df_distribuicoes is None:
            raise FileNotFoundError("Bronze distribuicao_interna data not found")

        df_base = (
            df_distribuicoes
            .select(
                pl.col("id").alias("id_distribuicao"),
                pl.col("from_store_id").alias("id_loja_origem"),
                pl.col("to_store_id").alias("id_loja_destino"),
                pl.col("distribution_date"),
                pl.col("status").alias("status_distribuicao")
            )
        )

        # adiciona id_tempo
        df_base = self._add_id_tempo(df_base, "distribution_date")

        df_items = (
            df_distribuicoes
            .explode("items")
            .with_columns([
                pl.col("items").struct.field("product_id").alias("id_produto"),
                pl.col("items").struct.field("quantity").alias("quantidade")
            ])
            .select(
                pl.col("id").alias("id_distribuicao"),
                "id_produto",
                "quantidade"
            )
        )

        # ==========================
        # JOIN (núcleo do fato)
        # ==========================
        df_transformed = (
            df_items
            .join(
                df_base,
                on="id_distribuicao",
                how="left"
            )
            .with_columns(
                pl.lit(datetime.now()).alias("data_carga")
            )
            .select(
                "id_distribuicao",
                "id_loja_origem",
                "id_loja_destino",
                "id_tempo",
                "id_produto",
                "quantidade",
                "status_distribuicao",
                "data_carga",
            )
        )

        logger.info(f"✓ Distribuicoes transformed: {len(df_transformed)} rows")

        return df_transformed
