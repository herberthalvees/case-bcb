"""Configuração das séries do SGS/BCB e parâmetros da extração."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"

DATA_INICIAL_PADRAO = "01/01/2020"
DATA_FINAL_PADRAO = "31/12/2024"


@dataclass(frozen=True)
class SerieConfig:
    """Metadados de uma série temporal do SGS."""

    codigo: int
    nome: str
    granularidade: str
    unidade: str
    arquivo: str

    @property
    def url(self) -> str:
        return BASE_URL.format(codigo=self.codigo)


SERIES: Dict[str, SerieConfig] = {
    "selic": SerieConfig(
        codigo=11,
        nome="selic",
        granularidade="diaria",
        unidade="% a.d.",
        arquivo="selic.json",
    ),
    "ipca": SerieConfig(
        codigo=433,
        nome="ipca",
        granularidade="mensal",
        unidade="% a.m.",
        arquivo="ipca.json",
    ),
}
