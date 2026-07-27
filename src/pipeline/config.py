"""Configuração central do pipeline no Databricks.

Um único lugar para nomes de catálogo, schema, tabelas e caminhos de Volume.
Nenhum outro módulo deve conter esses literais.
"""

from __future__ import annotations

CATALOGO = "workspace"
SCHEMA = "case_bcb"

VOLUME_LANDING = f"/Volumes/{CATALOGO}/{SCHEMA}/landing"
VOLUME_CHECKPOINTS = f"/Volumes/{CATALOGO}/{SCHEMA}/checkpoints"

TABELA_BRONZE = f"{CATALOGO}.{SCHEMA}.bronze_sgs_raw"
TABELA_SILVER = f"{CATALOGO}.{SCHEMA}.silver_series_bcb"
TABELA_GOLD = f"{CATALOGO}.{SCHEMA}.gold_juro_real_mensal"

CHECKPOINT_BRONZE = f"{VOLUME_CHECKPOINTS}/bronze_sgs_raw"

# Janela coberta pela extração — usada nas checagens de completude.
ANO_MES_INICIAL = "2020-01"
ANO_MES_FINAL = "2024-12"
QTD_MESES_ESPERADA = 60

# Mapeamento arquivo de origem -> série. A série é derivada do nome do
# arquivo porque o payload do SGS não carrega o código da série.
MAPA_ARQUIVO_SERIE = {
    "selic.json": {"serie_id": 11, "serie_nome": "selic", "granularidade": "diaria"},
    "ipca.json": {"serie_id": 433, "serie_nome": "ipca", "granularidade": "mensal"},
}
