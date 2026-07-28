# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Evidências
# MAGIC
# MAGIC Reúne, a partir do estado atual das tabelas, as provas que sustentam as
# MAGIC afirmações do README. Nada aqui depende de print: o histórico de
# MAGIC operações fica registrado no próprio Delta e pode ser reconsultado a
# MAGIC qualquer momento por quem avaliar.

# COMMAND ----------

import os
import sys

RAIZ_REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if RAIZ_REPO not in sys.path:
    sys.path.insert(0, RAIZ_REPO)

from src.pipeline import config  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Volumetria das três camadas

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT 'bronze' AS camada, COUNT(*) AS linhas,
               COUNT(DISTINCT arquivo_origem) AS arquivos
        FROM {config.TABELA_BRONZE}
        UNION ALL
        SELECT 'silver', COUNT(*), COUNT(DISTINCT arquivo_origem)
        FROM {config.TABELA_SILVER}
        UNION ALL
        SELECT 'gold', COUNT(*), NULL
        FROM {config.TABELA_GOLD}
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Idempotência da Bronze
# MAGIC
# MAGIC Um único lote de ingestão por arquivo, mesmo após várias execuções: o
# MAGIC checkpoint do Auto Loader não reprocessa arquivo já lido.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT arquivo_origem,
               COUNT(*)                    AS linhas,
               COUNT(DISTINCT ingerido_em) AS lotes_de_ingestao
        FROM {config.TABELA_BRONZE}
        GROUP BY arquivo_origem
        ORDER BY arquivo_origem
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Idempotência da Silver — histórico do MERGE
# MAGIC
# MAGIC Esta é a evidência mais forte do case. Cada execução do pipeline grava
# MAGIC uma versão na tabela com as métricas da operação. A primeira inseriu
# MAGIC todas as linhas; as seguintes inseriram e atualizaram **zero**, mesmo
# MAGIC lendo exatamente a mesma origem.

# COMMAND ----------

historico = spark.sql(f"DESCRIBE HISTORY {config.TABELA_SILVER}")
display(
    historico.selectExpr(
        "version AS versao",
        "timestamp AS momento",
        "operation AS operacao",
        "operationMetrics.numTargetRowsInserted AS linhas_inseridas",
        "operationMetrics.numTargetRowsUpdated  AS linhas_atualizadas",
        "operationMetrics.numTargetRowsDeleted  AS linhas_deletadas",
    ).orderBy("versao")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Grão declarado é o grão real
# MAGIC
# MAGIC Silver: uma linha por `(serie_id, data_referencia)`.
# MAGIC Gold: uma linha por `ano_mes`.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT 'silver' AS tabela,
               COUNT(*)                                   AS linhas,
               COUNT(DISTINCT serie_id, data_referencia)  AS chaves_distintas
        FROM {config.TABELA_SILVER}
        UNION ALL
        SELECT 'gold', COUNT(*), COUNT(DISTINCT ano_mes)
        FROM {config.TABELA_GOLD}
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação contra fonte externa
# MAGIC
# MAGIC O acumulado de 12 meses em dezembro reproduz o índice oficial do ano.
# MAGIC Se extração, tipagem ou capitalização estivessem erradas, não fecharia.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT g.ano_mes,
               ROUND(g.ipca_acum_12m, 2)      AS ipca_12m_pipeline,
               o.ipca_oficial,
               ROUND(g.selic_acum_12m, 2)     AS selic_12m,
               ROUND(g.juro_real_acum_12m, 2) AS juro_real_12m
        FROM {config.TABELA_GOLD} g
        JOIN (
            SELECT '2020-12' AS ano_mes,  4.52 AS ipca_oficial UNION ALL
            SELECT '2021-12', 10.06 UNION ALL
            SELECT '2022-12',  5.79 UNION ALL
            SELECT '2023-12',  4.62 UNION ALL
            SELECT '2024-12',  4.83
        ) o ON o.ano_mes = g.ano_mes
        ORDER BY g.ano_mes
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. As 18 checagens de qualidade
# MAGIC
# MAGIC Cada checagem avaliada contra o estado atual das tabelas, com o valor
# MAGIC esperado e o observado. A célula seguinte reexecuta a avaliação de
# MAGIC verdade: se alguma reprovar, levanta `DataQualityError`.

# COMMAND ----------

from src.pipeline import bronze, gold, silver  # noqa: E402
from src.pipeline.quality import avaliar  # noqa: E402

por_camada = [
    ("bronze", bronze.checagens(spark)),
    ("silver", silver.checagens(spark)),
    ("gold", gold.checagens(spark)),
]

display(
    spark.createDataFrame(
        [
            {
                "camada": camada,
                "checagem": resultado.nome,
                "situacao": "PASSOU" if resultado.aprovado else "FALHOU",
                "esperado": resultado.esperado,
                "observado": str(resultado.observado),
                "por_que_existe": resultado.descricao,
            }
            for camada, resultados in por_camada
            for resultado in resultados
        ]
    )
)

# COMMAND ----------

for camada, resultados in por_camada:
    avaliar(resultados, camada=camada)

print("As 18 checagens das três camadas passaram.")
