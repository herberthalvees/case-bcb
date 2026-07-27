# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Camada Bronze
# MAGIC
# MAGIC Ingestão incremental dos arquivos JSON do Volume de landing para uma
# MAGIC tabela Delta no Unity Catalog, preservando o dado bruto.
# MAGIC
# MAGIC A lógica vive em `src/pipeline/bronze.py`. Este notebook só orquestra.

# COMMAND ----------

import logging
import os
import sys

# Torna o pacote src importável a partir da raiz do repositório.
RAIZ_REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if RAIZ_REPO not in sys.path:
    sys.path.insert(0, RAIZ_REPO)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    force=True,
)

from src.pipeline import bronze, config  # noqa: E402
from src.pipeline.quality import avaliar  # noqa: E402

print(f"Raiz do repositório: {RAIZ_REPO}")
print(f"Landing: {config.VOLUME_LANDING}")
print(f"Tabela destino: {config.TABELA_BRONZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestão

# COMMAND ----------

metricas = bronze.ingerir(spark)
display(spark.createDataFrame([metricas]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checagens de qualidade
# MAGIC
# MAGIC Qualquer violação levanta `DataQualityError` e derruba a task do Workflow.

# COMMAND ----------

avaliar(bronze.checagens(spark), camada="bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amostra do resultado

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT arquivo_origem,
               COUNT(*)              AS linhas,
               MIN(data_raw)         AS primeira_data,
               MAX(data_raw)         AS ultima_data,
               MAX(ingerido_em)      AS ultima_ingestao
        FROM {config.TABELA_BRONZE}
        GROUP BY arquivo_origem
        ORDER BY arquivo_origem
        """
    )
)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {config.TABELA_BRONZE} LIMIT 10"))
