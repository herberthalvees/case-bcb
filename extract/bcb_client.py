"""Cliente HTTP para a API de séries temporais (SGS) do Banco Central.

Responsabilidades:
    * chamar o endpoint público do SGS;
    * repetir a chamada com backoff exponencial e jitter em falhas transitórias;
    * falhar de forma explícita quando a API não responde, devolve payload
      inválido ou devolve payload vazio.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Dict, List, Optional

import requests

LOGGER = logging.getLogger(__name__)

STATUS_RETENTAVEIS = frozenset({408, 425, 429, 500, 502, 503, 504})


class BCBExtractionError(RuntimeError):
    """Erro irrecuperável durante a extração de uma série do SGS."""


class BCBClient:
    """Cliente com retentativa para o endpoint de dados do SGS."""

    def __init__(
        self,
        timeout: float = 30.0,
        max_tentativas: int = 5,
        backoff_base: float = 1.5,
        backoff_max: float = 30.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        if max_tentativas < 1:
            raise ValueError("max_tentativas deve ser >= 1")
        self.timeout = timeout
        self.max_tentativas = max_tentativas
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "case-bcb-pipeline/1.0"})

    def buscar_serie(
        self,
        url: str,
        data_inicial: str,
        data_final: str,
    ) -> List[Dict[str, Any]]:
        """Baixa uma série do SGS e devolve a lista de registros brutos.

        Args:
            url: URL do endpoint da série.
            data_inicial: data inicial no formato ``dd/MM/aaaa``.
            data_final: data final no formato ``dd/MM/aaaa``.

        Returns:
            Lista de dicionários ``{"data": ..., "valor": ...}``.

        Raises:
            BCBExtractionError: se todas as tentativas falharem, se o corpo
                não for um JSON de lista ou se a lista vier vazia.
        """
        params = {
            "formato": "json",
            "dataInicial": data_inicial,
            "dataFinal": data_final,
        }
        ultimo_erro: Optional[Exception] = None

        for tentativa in range(1, self.max_tentativas + 1):
            try:
                resposta = self.session.get(url, params=params, timeout=self.timeout)
                if resposta.status_code in STATUS_RETENTAVEIS:
                    raise requests.HTTPError(
                        f"status retentável {resposta.status_code}", response=resposta
                    )
                resposta.raise_for_status()
                return self._validar_payload(resposta, url)
            except (requests.RequestException, ValueError) as erro:
                ultimo_erro = erro
                if tentativa == self.max_tentativas:
                    break
                espera = self._calcular_espera(tentativa)
                LOGGER.warning(
                    "Falha ao consultar %s (tentativa %s/%s): %s. "
                    "Nova tentativa em %.1fs.",
                    url,
                    tentativa,
                    self.max_tentativas,
                    erro,
                    espera,
                )
                time.sleep(espera)

        raise BCBExtractionError(
            f"Não foi possível extrair a série de {url} após "
            f"{self.max_tentativas} tentativas: {ultimo_erro}"
        ) from ultimo_erro

    def _calcular_espera(self, tentativa: int) -> float:
        """Backoff exponencial com jitter, limitado por ``backoff_max``."""
        espera = min(self.backoff_base**tentativa, self.backoff_max)
        return espera * (0.5 + random.random() / 2)

    @staticmethod
    def _validar_payload(
        resposta: requests.Response, url: str
    ) -> List[Dict[str, Any]]:
        """Garante que o corpo da resposta é uma lista não vazia de registros."""
        try:
            payload = resposta.json()
        except ValueError as erro:
            raise BCBExtractionError(
                f"Resposta de {url} não é um JSON válido: {erro}"
            ) from erro

        if not isinstance(payload, list):
            raise BCBExtractionError(
                f"Payload inesperado em {url}: esperava lista, "
                f"recebi {type(payload).__name__}"
            )
        if not payload:
            raise BCBExtractionError(
                f"Payload vazio em {url}: nenhum registro devolvido"
            )

        campos_obrigatorios = {"data", "valor"}
        faltantes = campos_obrigatorios - set(payload[0])
        if faltantes:
            raise BCBExtractionError(
                f"Registros de {url} sem os campos obrigatórios: {sorted(faltantes)}"
            )
        return payload
