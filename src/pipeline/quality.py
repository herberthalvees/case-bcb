"""Checagens de qualidade de dados com falha explícita.

Cada camada monta uma lista de :class:`ResultadoChecagem` e chama
:func:`avaliar`. Se qualquer checagem reprovar, :class:`DataQualityError` é
levantada — o que derruba a task e, por consequência, o Databricks Workflow.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Sequence

LOGGER = logging.getLogger("pipeline.quality")


class DataQualityError(RuntimeError):
    """Uma ou mais checagens de qualidade foram violadas."""


@dataclass(frozen=True)
class ResultadoChecagem:
    """Resultado de uma checagem individual."""

    nome: str
    aprovado: bool
    esperado: str
    observado: Any
    descricao: str = ""

    def __str__(self) -> str:
        status = "PASSOU" if self.aprovado else "FALHOU"
        return (
            f"[{status}] {self.nome} | esperado: {self.esperado} | "
            f"observado: {self.observado}"
        )


def checar(
    nome: str,
    aprovado: bool,
    esperado: str,
    observado: Any,
    descricao: str = "",
) -> ResultadoChecagem:
    """Constrói um resultado de checagem."""
    return ResultadoChecagem(
        nome=nome,
        aprovado=bool(aprovado),
        esperado=esperado,
        observado=observado,
        descricao=descricao,
    )


def avaliar(resultados: Sequence[ResultadoChecagem], camada: str) -> None:
    """Registra o resultado das checagens e falha se alguma reprovar.

    Raises:
        DataQualityError: se ao menos uma checagem tiver ``aprovado=False``.
    """
    LOGGER.info("Checagens de qualidade — camada %s", camada.upper())
    for resultado in resultados:
        registrar = LOGGER.info if resultado.aprovado else LOGGER.error
        registrar("  %s", resultado)

    reprovadas = [r for r in resultados if not r.aprovado]
    if reprovadas:
        detalhe = "; ".join(str(r) for r in reprovadas)
        raise DataQualityError(
            f"{len(reprovadas)} de {len(resultados)} checagens falharam na "
            f"camada {camada}: {detalhe}"
        )
    LOGGER.info(
        "Todas as %s checagens da camada %s passaram.", len(resultados), camada
    )


def escalar(spark, consulta: str) -> Any:
    """Executa uma consulta que devolve uma única linha e coluna."""
    linha = spark.sql(consulta).first()
    return None if linha is None else linha[0]
