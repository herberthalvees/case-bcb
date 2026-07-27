"""Camada Silver — tipagem, padronização e carga idempotente.

Decisões desta camada:

* **Tabela única para as duas séries.** O contrato do SGS é idêntico nas duas
  (``data`` e ``valor``), então uma tabela com ``serie_id`` discriminando evita
  duplicar código de MERGE e de qualidade. A identificação vem do nome do
  arquivo de origem, já preservado na Bronze.
* **Chave de negócio: ``(serie_id, data_referencia)``.** É o grão natural das
  duas séries — a SELIC tem uma observação por dia útil, o IPCA uma por mês
  (sempre no dia 01). O MERGE por essa chave é o que garante idempotência de
  verdade: mesmo que alguém reenvie os arquivos para o Volume e a Bronze
  ganhe linhas repetidas, a Silver não duplica.
* **Conversão tolerante.** ``try_to_date`` e ``try_cast`` devolvem ``NULL`` em
  vez de derrubar o job com registro malformado. O registro rejeitado vira
  métrica e é a checagem de qualidade que decide se o job para.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.pipeline import config
from src.pipeline.quality import ResultadoChecagem, checar, escalar

LOGGER = logging.getLogger("pipeline.silver")

DDL_SILVER = """
CREATE TABLE IF NOT EXISTS {tabela} (
    serie_id         INT       NOT NULL COMMENT 'Código da série no SGS',
    serie_nome       STRING    NOT NULL COMMENT 'selic | ipca',
    granularidade    STRING    NOT NULL COMMENT 'diaria | mensal',
    data_referencia  DATE      NOT NULL COMMENT 'Data da observação',
    ano_mes          STRING    NOT NULL COMMENT 'Competência no formato yyyy-MM',
    valor            DECIMAL(18,8) NOT NULL COMMENT 'SELIC em % a.d.; IPCA em % a.m.',
    arquivo_origem   STRING    COMMENT 'Arquivo de onde o registro veio',
    ingerido_em      TIMESTAMP COMMENT 'Timestamp da ingestão na Bronze',
    processado_em    TIMESTAMP COMMENT 'Timestamp do processamento na Silver'
)
USING DELTA
COMMENT 'Grão: uma linha por série e data de referência. '
        'Chave de negócio: (serie_id, data_referencia).'
"""

MERGE_SILVER = """
MERGE INTO {tabela} AS destino
USING origem_silver AS origem
    ON  destino.serie_id        = origem.serie_id
    AND destino.data_referencia = origem.data_referencia
WHEN MATCHED AND origem.ingerido_em > destino.ingerido_em THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
"""


def _mapear(campo: str) -> Column:
    """Monta um CASE que traduz o arquivo de origem em metadado da série."""
    expressao = F.lit(None)
    for arquivo, metadados in config.MAPA_ARQUIVO_SERIE.items():
        expressao = F.when(
            F.col("arquivo_origem") == arquivo, F.lit(metadados[campo])
        ).otherwise(expressao)
    return expressao


def transformar(
    spark: SparkSession, tabela_bronze: str = config.TABELA_BRONZE
) -> DataFrame:
    """Lê a Bronze, tipa os campos e devolve a Silver deduplicada.

    A deduplicação interna mantém, para cada chave de negócio, a linha de
    ingestão mais recente. Sem isso, uma Bronze com o mesmo arquivo carregado
    duas vezes faria o MERGE falhar por múltiplas correspondências na origem.
    """
    bruto = spark.table(tabela_bronze)

    tipado = bruto.select(
        _mapear("serie_id").cast("int").alias("serie_id"),
        _mapear("serie_nome").alias("serie_nome"),
        _mapear("granularidade").alias("granularidade"),
        F.expr("try_to_date(data_raw, 'dd/MM/yyyy')").alias("data_referencia"),
        F.expr("try_cast(valor_raw AS DECIMAL(18,8))").alias("valor"),
        F.col("arquivo_origem"),
        F.col("ingerido_em"),
    ).filter(
        F.col("serie_id").isNotNull()
        & F.col("data_referencia").isNotNull()
        & F.col("valor").isNotNull()
    )

    janela = Window.partitionBy("serie_id", "data_referencia").orderBy(
        F.col("ingerido_em").desc()
    )

    return (
        tipado.withColumn("_ordem", F.row_number().over(janela))
        .filter(F.col("_ordem") == 1)
        .drop("_ordem")
        .withColumn("ano_mes", F.date_format("data_referencia", "yyyy-MM"))
        .withColumn("processado_em", F.current_timestamp())
        .select(
            "serie_id",
            "serie_nome",
            "granularidade",
            "data_referencia",
            "ano_mes",
            "valor",
            "arquivo_origem",
            "ingerido_em",
            "processado_em",
        )
    )


def carregar(
    spark: SparkSession,
    tabela_bronze: str = config.TABELA_BRONZE,
    tabela_silver: str = config.TABELA_SILVER,
) -> Dict[str, Any]:
    """Executa o MERGE idempotente e devolve métricas da execução."""
    spark.sql(DDL_SILVER.format(tabela=tabela_silver))

    linhas_antes = int(escalar(spark, f"SELECT COUNT(*) FROM {tabela_silver}") or 0)

    origem = transformar(spark, tabela_bronze)
    origem.createOrReplaceTempView("origem_silver")

    resultado = spark.sql(MERGE_SILVER.format(tabela=tabela_silver)).first()
    metricas_merge = resultado.asDict() if resultado is not None else {}

    linhas_depois = int(escalar(spark, f"SELECT COUNT(*) FROM {tabela_silver}") or 0)
    metricas = {
        "tabela": tabela_silver,
        "linhas_antes": linhas_antes,
        "linhas_depois": linhas_depois,
        "linhas_novas": linhas_depois - linhas_antes,
        **metricas_merge,
    }
    LOGGER.info("Silver: %s", metricas)
    return metricas


def contar_rejeitados(
    spark: SparkSession, tabela_bronze: str = config.TABELA_BRONZE
) -> int:
    """Conta registros da Bronze que não sobrevivem à tipagem."""
    return int(
        escalar(
            spark,
            f"""
            SELECT COUNT(*)
            FROM {tabela_bronze}
            WHERE try_to_date(data_raw, 'dd/MM/yyyy') IS NULL
               OR try_cast(valor_raw AS DECIMAL(18,8)) IS NULL
            """,
        )
        or 0
    )


def checagens(
    spark: SparkSession,
    tabela_silver: str = config.TABELA_SILVER,
    tabela_bronze: str = config.TABELA_BRONZE,
) -> List[ResultadoChecagem]:
    """Checagens de qualidade da camada Silver."""
    total = escalar(spark, f"SELECT COUNT(*) FROM {tabela_silver}") or 0
    chaves_distintas = escalar(
        spark,
        f"SELECT COUNT(*) FROM (SELECT DISTINCT serie_id, data_referencia "
        f"FROM {tabela_silver})",
    )
    rejeitados = contar_rejeitados(spark, tabela_bronze)
    fora_do_dominio = escalar(
        spark,
        f"""
        SELECT COUNT(*) FROM {tabela_silver}
        WHERE (serie_nome = 'selic' AND (valor < 0 OR valor > 1))
           OR (serie_nome = 'ipca'  AND (valor < -5 OR valor > 10))
        """,
    )
    fora_da_janela = escalar(
        spark,
        f"""
        SELECT COUNT(*) FROM {tabela_silver}
        WHERE data_referencia < DATE'2020-01-01'
           OR data_referencia > DATE'2024-12-31'
        """,
    )
    meses_ipca = escalar(
        spark,
        f"SELECT COUNT(DISTINCT ano_mes) FROM {tabela_silver} "
        f"WHERE serie_nome = 'ipca'",
    )
    ipca_fora_do_dia_1 = escalar(
        spark,
        f"SELECT COUNT(*) FROM {tabela_silver} "
        f"WHERE serie_nome = 'ipca' AND DAY(data_referencia) <> 1",
    )
    meses_selic_ralos = escalar(
        spark,
        f"""
        SELECT COUNT(*) FROM (
            SELECT ano_mes, COUNT(*) AS dias
            FROM {tabela_silver}
            WHERE serie_nome = 'selic'
            GROUP BY ano_mes
            HAVING COUNT(*) < 15
        )
        """,
    )

    return [
        checar(
            "silver_chave_de_negocio_unica",
            total == chaves_distintas,
            "COUNT(*) igual a COUNT(DISTINCT serie_id, data_referencia)",
            f"{total} linhas / {chaves_distintas} chaves",
            "Prova direta da idempotência do MERGE.",
        ),
        checar(
            "silver_sem_registros_rejeitados",
            rejeitados == 0,
            "0 registros da Bronze descartados na tipagem",
            rejeitados,
            "Data ou valor que não convertem indicam mudança de contrato na API.",
        ),
        checar(
            "silver_valores_no_dominio",
            (fora_do_dominio or 0) == 0,
            "SELIC entre 0 e 1 (% a.d.) e IPCA entre -5 e 10 (% a.m.)",
            fora_do_dominio,
            "Pega erro de escala, como valor anualizado entrando como diário.",
        ),
        checar(
            "silver_janela_temporal_respeitada",
            (fora_da_janela or 0) == 0,
            "todas as datas entre 2020-01-01 e 2024-12-31",
            fora_da_janela,
        ),
        checar(
            "silver_ipca_completo",
            meses_ipca == config.QTD_MESES_ESPERADA,
            f"{config.QTD_MESES_ESPERADA} competências distintas de IPCA",
            meses_ipca,
            "Mês faltando invalidaria o acumulado de 12 meses na Gold.",
        ),
        checar(
            "silver_ipca_ancorado_no_dia_1",
            (ipca_fora_do_dia_1 or 0) == 0,
            "toda observação de IPCA no dia 01 do mês",
            ipca_fora_do_dia_1,
            "Garante que a competência mensal é inequívoca.",
        ),
        checar(
            "silver_selic_com_cobertura_mensal",
            (meses_selic_ralos or 0) == 0,
            "nenhum mês com menos de 15 dias úteis de SELIC",
            meses_selic_ralos,
            "Mês incompleto distorceria a média e o acumulado do mês.",
        ),
    ]
