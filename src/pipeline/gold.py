"""Camada Gold — consolidação mensal do custo do dinheiro frente à inflação.

**Grão:** uma linha por competência mensal (``ano_mes``), 60 linhas para a
janela 2020-01 a 2024-12.

Decisão central desta camada — como compor SELIC e IPCA
-------------------------------------------------------
A série 11 do SGS é a SELIC **em % ao dia**, não anualizada. Isso importa:

* ``selic_media_dia`` é a média aritmética das taxas diárias do mês. É o que o
  enunciado pede literalmente, e serve para leitura direta;
* ``selic_acum_mes`` é a taxa efetiva do mês, obtida capitalizando os dias
  úteis: ``∏(1 + taxa_dia) - 1``. É esta que entra no juro real, porque juros
  compõem — a média aritmética subestimaria o custo efetivo do dinheiro.

O juro real usa a equação de Fisher, não a subtração ingênua:

    juro_real = ((1 + selic_mês) / (1 + ipca_mês)) - 1

O produtório é calculado como ``exp(Σ ln(1 + taxa))``. Spark não tem agregação
de produto, e a forma logarítmica é numericamente mais estável. É segura aqui
porque ``1 + taxa`` é sempre positivo (nem SELIC nem IPCA chegam a -100%).

O acumulado de 12 meses capitaliza os fatores mensais numa janela móvel e só é
preenchido quando a janela tem 12 meses completos — os 11 primeiros meses da
série ficam nulos, por definição, e não por falha.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.pipeline import config
from src.pipeline.quality import ResultadoChecagem, checar, escalar

LOGGER = logging.getLogger("pipeline.gold")

# Tolerância na reconferência da identidade de Fisher (pontos percentuais).
TOLERANCIA_FISHER = 0.000001


def _fator(coluna: str) -> Column:
    """Converte uma taxa percentual em fator de capitalização (1 + taxa)."""
    return F.lit(1) + F.col(coluna) / F.lit(100)


def agregar_selic(silver: DataFrame) -> DataFrame:
    """Consolida a SELIC diária em métricas mensais."""
    return (
        silver.filter(F.col("serie_nome") == "selic")
        .groupBy("ano_mes")
        .agg(
            F.avg("valor").cast("decimal(18,8)").alias("selic_media_dia"),
            F.count("*").cast("int").alias("selic_dias_uteis"),
            F.exp(F.sum(F.log(_fator("valor")))).alias("fator_selic_mes"),
        )
    )


def agregar_ipca(silver: DataFrame) -> DataFrame:
    """Seleciona o IPCA do mês (já mensal na origem)."""
    return (
        silver.filter(F.col("serie_nome") == "ipca")
        .groupBy("ano_mes")
        .agg(
            F.sum("valor").cast("decimal(18,8)").alias("ipca_mes"),
            F.exp(F.sum(F.log(_fator("valor")))).alias("fator_ipca_mes"),
        )
    )


def construir(
    spark: SparkSession, tabela_silver: str = config.TABELA_SILVER
) -> DataFrame:
    """Monta a tabela Gold a partir da Silver."""
    silver = spark.table(tabela_silver)

    mensal = (
        agregar_selic(silver)
        .join(agregar_ipca(silver), on="ano_mes", how="full_outer")
        .withColumn(
            "competencia", F.to_date(F.concat_ws("-", F.col("ano_mes"), F.lit("01")))
        )
        .withColumn(
            "fator_juro_real_mes",
            F.col("fator_selic_mes") / F.col("fator_ipca_mes"),
        )
    )

    janela_12m = Window.orderBy("ano_mes").rowsBetween(-11, 0)
    meses_na_janela = F.count("ano_mes").over(janela_12m)

    def acumulado(coluna_fator: str) -> Column:
        """Capitaliza o fator mensal na janela móvel, em pontos percentuais."""
        return F.when(
            meses_na_janela == 12,
            (F.exp(F.sum(F.log(F.col(coluna_fator))).over(janela_12m)) - 1) * 100,
        ).otherwise(F.lit(None))

    return (
        mensal.withColumn(
            "selic_acum_mes", (F.col("fator_selic_mes") - 1) * 100
        )
        .withColumn(
            "juro_real_mes", (F.col("fator_juro_real_mes") - 1) * 100
        )
        .withColumn("selic_acum_12m", acumulado("fator_selic_mes"))
        .withColumn("ipca_acum_12m", acumulado("fator_ipca_mes"))
        .withColumn("juro_real_acum_12m", acumulado("fator_juro_real_mes"))
        .withColumn("meses_na_janela_12m", meses_na_janela.cast("int"))
        .withColumn("processado_em", F.current_timestamp())
        .select(
            "ano_mes",
            "competencia",
            F.col("selic_media_dia"),
            F.col("selic_dias_uteis"),
            F.col("selic_acum_mes").cast("decimal(18,8)").alias("selic_acum_mes"),
            F.col("ipca_mes"),
            F.col("juro_real_mes").cast("decimal(18,8)").alias("juro_real_mes"),
            F.col("selic_acum_12m").cast("decimal(18,8)").alias("selic_acum_12m"),
            F.col("ipca_acum_12m").cast("decimal(18,8)").alias("ipca_acum_12m"),
            F.col("juro_real_acum_12m")
            .cast("decimal(18,8)")
            .alias("juro_real_acum_12m"),
            "meses_na_janela_12m",
            "processado_em",
        )
        .orderBy("ano_mes")
    )


def carregar(
    spark: SparkSession,
    tabela_silver: str = config.TABELA_SILVER,
    tabela_gold: str = config.TABELA_GOLD,
) -> Dict[str, Any]:
    """Recria a Gold por completo.

    A Gold é uma agregação determinística da Silver: reconstruir é idempotente
    por definição e mais simples de auditar que um MERGE incremental. O
    histórico continua disponível pelo time travel do Delta.
    """
    gold = construir(spark, tabela_silver)
    (
        gold.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela_gold)
    )
    spark.sql(
        f"COMMENT ON TABLE {tabela_gold} IS "
        f"'Grão: uma linha por competência mensal (ano_mes). "
        f"Juro real pela equação de Fisher sobre a SELIC capitalizada no mês.'"
    )

    total = int(escalar(spark, f"SELECT COUNT(*) FROM {tabela_gold}") or 0)
    metricas = {"tabela": tabela_gold, "linhas": total}
    LOGGER.info("Gold: %s", metricas)
    return metricas


def checagens(
    spark: SparkSession, tabela_gold: str = config.TABELA_GOLD
) -> List[ResultadoChecagem]:
    """Checagens de qualidade da camada Gold."""
    total = escalar(spark, f"SELECT COUNT(*) FROM {tabela_gold}") or 0
    competencias = escalar(spark, f"SELECT COUNT(DISTINCT ano_mes) FROM {tabela_gold}")
    nulos_obrigatorios = escalar(
        spark,
        f"""
        SELECT COUNT(*) FROM {tabela_gold}
        WHERE selic_media_dia IS NULL
           OR selic_acum_mes  IS NULL
           OR ipca_mes        IS NULL
           OR juro_real_mes   IS NULL
        """,
    )
    acum_preenchidos = escalar(
        spark,
        f"SELECT COUNT(*) FROM {tabela_gold} WHERE juro_real_acum_12m IS NOT NULL",
    )
    fisher_incoerente = escalar(
        spark,
        f"""
        SELECT COUNT(*) FROM {tabela_gold}
        WHERE ABS(
                  (1 + juro_real_mes/100) * (1 + ipca_mes/100)
                - (1 + selic_acum_mes/100)
              ) > {TOLERANCIA_FISHER}
        """,
    )
    meses_sem_selic = escalar(
        spark,
        f"SELECT COUNT(*) FROM {tabela_gold} WHERE selic_dias_uteis < 15",
    )

    esperado_acum = config.QTD_MESES_ESPERADA - 11

    return [
        checar(
            "gold_grao_unico",
            total == competencias,
            "uma linha por competência (ano_mes)",
            f"{total} linhas / {competencias} competências",
            "Prova que o grão declarado no README é o grão real da tabela.",
        ),
        checar(
            "gold_serie_completa",
            total == config.QTD_MESES_ESPERADA,
            f"{config.QTD_MESES_ESPERADA} meses entre "
            f"{config.ANO_MES_INICIAL} e {config.ANO_MES_FINAL}",
            total,
        ),
        checar(
            "gold_sem_nulos_obrigatorios",
            (nulos_obrigatorios or 0) == 0,
            "0 linhas com métrica mensal nula",
            nulos_obrigatorios,
            "Mês sem SELIC ou sem IPCA indica furo vindo da Silver.",
        ),
        checar(
            "gold_acumulado_12m_consistente",
            acum_preenchidos == esperado_acum,
            f"{esperado_acum} meses com acumulado 12m preenchido "
            f"(os 11 primeiros ficam nulos por definição)",
            acum_preenchidos,
        ),
        checar(
            "gold_identidade_de_fisher",
            (fisher_incoerente or 0) == 0,
            "(1+juro_real)*(1+ipca) = (1+selic) em todas as linhas",
            fisher_incoerente,
            "Reconferência algébrica do cálculo do juro real.",
        ),
        checar(
            "gold_meses_com_selic_suficiente",
            (meses_sem_selic or 0) == 0,
            "nenhum mês com menos de 15 dias úteis de SELIC",
            meses_sem_selic,
            "Mês incompleto distorceria a capitalização mensal.",
        ),
    ]
