# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Camada Silver
# MAGIC
# MAGIC Tipagem, identificação da série, padronização de datas e carga
# MAGIC idempotente via `MERGE`.
# MAGIC
# MAGIC **Grão:** uma linha por série e data de referência.
# MAGIC **Chave de negócio:** `(serie_id, data_referencia)`.
# MAGIC
# MAGIC A lógica vive em `src/pipeline/silver.py`. Este notebook só orquestra.

# COMMAND ----------

import logging
import os
import sys

RAIZ_REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if RAIZ_REPO not in sys.path:
    sys.path.insert(0, RAIZ_REPO)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    force=True,
)

from src.pipeline import config, silver  # noqa: E402
from src.pipeline.quality import avaliar  # noqa: E402

print(f"Origem : {config.TABELA_BRONZE}")
print(f"Destino: {config.TABELA_SILVER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga via MERGE

# COMMAND ----------

metricas = silver.carregar(spark)
display(spark.createDataFrame([{k: str(v) for k, v in metricas.items()}]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checagens de qualidade

# COMMAND ----------

avaliar(silver.checagens(spark), camada="silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidência de idempotência
# MAGIC
# MAGIC Se a contagem de linhas for igual à contagem de chaves de negócio
# MAGIC distintas, nenhuma reexecução duplicou registro.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT serie_nome,
               COUNT(*)                              AS linhas,
               COUNT(DISTINCT data_referencia)       AS datas_distintas,
               MIN(data_referencia)                  AS primeira_data,
               MAX(data_referencia)                  AS ultima_data,
               MAX(processado_em)                    AS ultimo_processamento
        FROM {config.TABELA_SILVER}
        GROUP BY serie_nome
        ORDER BY serie_nome
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT * FROM {config.TABELA_SILVER}
        WHERE serie_nome = 'ipca'
        ORDER BY data_referencia
        LIMIT 10
        """
    )
)
