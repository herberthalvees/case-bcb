# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Camada Gold
# MAGIC
# MAGIC Consolidação mensal: SELIC média do mês, SELIC capitalizada no mês,
# MAGIC IPCA do mês, juro real e taxas acumuladas em 12 meses.
# MAGIC
# MAGIC **Grão:** uma linha por competência mensal (`ano_mes`) — 60 linhas.
# MAGIC
# MAGIC A lógica vive em `src/pipeline/gold.py`. Este notebook só orquestra.

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

from src.pipeline import config, gold  # noqa: E402
from src.pipeline.quality import avaliar  # noqa: E402

print(f"Origem : {config.TABELA_SILVER}")
print(f"Destino: {config.TABELA_GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construção

# COMMAND ----------

metricas = gold.carregar(spark)
display(spark.createDataFrame([{k: str(v) for k, v in metricas.items()}]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checagens de qualidade

# COMMAND ----------

avaliar(gold.checagens(spark), camada="gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado
# MAGIC
# MAGIC Conferência externa: o `ipca_acum_12m` de dezembro de cada ano deve
# MAGIC reproduzir o IPCA oficial do ano — 4,52% (2020), 10,06% (2021),
# MAGIC 5,79% (2022), 4,62% (2023) e 4,83% (2024).

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT ano_mes,
               ROUND(ipca_acum_12m, 2)      AS ipca_12m,
               ROUND(selic_acum_12m, 2)     AS selic_12m,
               ROUND(juro_real_acum_12m, 2) AS juro_real_12m
        FROM {config.TABELA_GOLD}
        WHERE ano_mes LIKE '%-12'
        ORDER BY ano_mes
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT ano_mes,
               selic_dias_uteis,
               ROUND(selic_media_dia, 6)    AS selic_media_dia,
               ROUND(selic_acum_mes, 4)     AS selic_mes,
               ROUND(ipca_mes, 4)           AS ipca_mes,
               ROUND(juro_real_mes, 4)      AS juro_real_mes,
               ROUND(juro_real_acum_12m, 2) AS juro_real_12m
        FROM {config.TABELA_GOLD}
        ORDER BY ano_mes
        """
    )
)
