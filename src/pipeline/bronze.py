"""Camada Bronze — ingestão incremental do Volume para Delta.

Princípios desta camada:
    * o dado é preservado exatamente como veio da API: todas as colunas de
      negócio são ``STRING``, nenhuma conversão de tipo acontece aqui;
    * a carga é incremental via Auto Loader — o checkpoint garante que um
      arquivo já processado não seja lido de novo;
    * cada linha carrega a procedência: nome do arquivo de origem, data de
      modificação do arquivo e timestamp de ingestão.

As duas séries compartilham o mesmo contrato de payload
(``{"data": ..., "valor": ...}``), então um único stream cobre os dois
arquivos. A identificação da série é derivada do nome do arquivo na camada
Silver.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.pipeline import config
from src.pipeline.quality import ResultadoChecagem, checar, escalar

LOGGER = logging.getLogger("pipeline.bronze")

# Schema fixo e totalmente textual: o case exige preservar o dado bruto.
# Declarar o schema (em vez de inferir) também evita que o Auto Loader mude
# de tipo entre execuções.
SCHEMA_BRUTO = StructType(
    [
        StructField("data", StringType(), True),
        StructField("valor", StringType(), True),
    ]
)


def ler_landing(
    spark: SparkSession,
    caminho_landing: str = config.VOLUME_LANDING,
    padrao_arquivo: str = "*.json",
) -> DataFrame:
    """Cria o stream do Auto Loader sobre o Volume de landing.

    Os arquivos do SGS são um array JSON formatado em várias linhas, por isso
    ``multiLine`` é obrigatório.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("pathGlobFilter", padrao_arquivo)
        .schema(SCHEMA_BRUTO)
        .load(caminho_landing)
        .select(
            F.col("data").alias("data_raw"),
            F.col("valor").alias("valor_raw"),
            F.col("_metadata.file_name").alias("arquivo_origem"),
            F.col("_metadata.file_modification_time").alias("arquivo_modificado_em"),
            F.current_timestamp().alias("ingerido_em"),
        )
    )


def ingerir(
    spark: SparkSession,
    caminho_landing: str = config.VOLUME_LANDING,
    tabela_destino: str = config.TABELA_BRONZE,
    checkpoint: str = config.CHECKPOINT_BRONZE,
) -> Dict[str, Any]:
    """Executa a ingestão incremental e devolve métricas da execução.

    O gatilho ``availableNow`` processa tudo que estiver pendente e encerra —
    é o modo adequado para um job agendado, sem stream contínuo.
    """
    linhas_antes = _contar_linhas(spark, tabela_destino)
    LOGGER.info(
        "Bronze: iniciando ingestão de %s para %s (linhas antes: %s)",
        caminho_landing,
        tabela_destino,
        linhas_antes,
    )

    consulta = (
        ler_landing(spark, caminho_landing)
        .writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint}/_commits")
        .option("mergeSchema", "false")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(tabela_destino)
    )
    consulta.awaitTermination()

    linhas_depois = _contar_linhas(spark, tabela_destino)
    metricas = {
        "tabela": tabela_destino,
        "linhas_antes": linhas_antes,
        "linhas_depois": linhas_depois,
        "linhas_ingeridas": linhas_depois - linhas_antes,
        "arquivos_no_landing": _contar_arquivos(spark, caminho_landing),
    }
    LOGGER.info("Bronze: %s linhas ingeridas nesta execução.", metricas["linhas_ingeridas"])
    return metricas


def checagens(
    spark: SparkSession,
    tabela: str = config.TABELA_BRONZE,
    caminho_landing: str = config.VOLUME_LANDING,
) -> List[ResultadoChecagem]:
    """Checagens de qualidade da camada Bronze."""
    arquivos = _contar_arquivos(spark, caminho_landing)
    total = escalar(spark, f"SELECT COUNT(*) FROM {tabela}")
    sem_procedencia = escalar(
        spark,
        f"""
        SELECT COUNT(*) FROM {tabela}
        WHERE arquivo_origem IS NULL OR ingerido_em IS NULL
        """,
    )
    arquivos_distintos = escalar(
        spark, f"SELECT COUNT(DISTINCT arquivo_origem) FROM {tabela}"
    )
    linhas_totalmente_nulas = escalar(
        spark,
        f"SELECT COUNT(*) FROM {tabela} WHERE data_raw IS NULL AND valor_raw IS NULL",
    )

    return [
        checar(
            "bronze_landing_nao_vazio",
            arquivos > 0,
            "ao menos 1 arquivo no Volume de landing",
            arquivos,
            "Falha cedo se o upload para o Volume não foi feito.",
        ),
        checar(
            "bronze_tabela_nao_vazia",
            (total or 0) > 0,
            "> 0 linhas na tabela Bronze",
            total,
        ),
        checar(
            "bronze_procedencia_completa",
            (sem_procedencia or 0) == 0,
            "0 linhas sem arquivo_origem ou ingerido_em",
            sem_procedencia,
            "Rastreabilidade é requisito da camada Bronze.",
        ),
        checar(
            "bronze_todas_as_series_presentes",
            (arquivos_distintos or 0) == len(config.MAPA_ARQUIVO_SERIE),
            f"{len(config.MAPA_ARQUIVO_SERIE)} arquivos de origem distintos",
            arquivos_distintos,
            "Detecta o caso de uma das séries não ter sido carregada.",
        ),
        checar(
            "bronze_sem_linhas_vazias",
            (linhas_totalmente_nulas or 0) == 0,
            "0 linhas com data e valor nulos",
            linhas_totalmente_nulas,
            "Indicaria leitura incorreta do JSON (multiLine desligado, p.ex.).",
        ),
    ]


def _contar_linhas(spark: SparkSession, tabela: str) -> int:
    """Conta linhas da tabela, devolvendo 0 se ela ainda não existir."""
    if not spark.catalog.tableExists(tabela):
        return 0
    return int(escalar(spark, f"SELECT COUNT(*) FROM {tabela}") or 0)


def _contar_arquivos(spark: SparkSession, caminho: str) -> int:
    """Conta os arquivos JSON presentes no Volume de landing."""
    try:
        return int(
            spark.sql(f"LIST '{caminho}'")
            .filter(F.col("name").endswith(".json"))
            .count()
        )
    except Exception as erro:  # noqa: BLE001 - caminho inexistente vira 0
        LOGGER.warning("Não foi possível listar %s: %s", caminho, erro)
        return 0
