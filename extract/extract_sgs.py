"""Extração local das séries SELIC (11) e IPCA (433) do SGS/BCB.

Roda fora do Databricks (o compute serverless do Free Edition não alcança
``api.bcb.gov.br``). Salva o retorno bruto, sem transformação, em
``data/raw/`` e gera um manifesto com hash e contagem para rastreabilidade.

Uso:
    python -m extract.extract_sgs
    python -m extract.extract_sgs --serie selic --data-final 31/12/2024
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

from extract.bcb_client import BCBClient, BCBExtractionError
from extract.config import (
    DATA_FINAL_PADRAO,
    DATA_INICIAL_PADRAO,
    SERIES,
    SerieConfig,
)

LOGGER = logging.getLogger("extract")

DIRETORIO_PADRAO = Path("data/raw")


def _configurar_log(verboso: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verboso else logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )


def _salvar_json(caminho: Path, conteudo: Any) -> str:
    """Grava o conteúdo em disco de forma atômica e devolve o sha256."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    texto = json.dumps(conteudo, ensure_ascii=False, indent=2)
    temporario = caminho.with_suffix(caminho.suffix + ".tmp")
    temporario.write_text(texto, encoding="utf-8")
    temporario.replace(caminho)
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()


def extrair_serie(
    serie: SerieConfig,
    diretorio_saida: Path,
    data_inicial: str,
    data_final: str,
    client: BCBClient,
) -> Dict[str, Any]:
    """Extrai uma série, grava o arquivo bruto e devolve os metadados."""
    LOGGER.info("Extraindo série %s (código %s)...", serie.nome, serie.codigo)
    registros: List[Dict[str, Any]] = client.buscar_serie(
        serie.url, data_inicial=data_inicial, data_final=data_final
    )

    destino = diretorio_saida / serie.arquivo
    sha256 = _salvar_json(destino, registros)

    metadados = {
        "serie": serie.nome,
        "codigo_serie": serie.codigo,
        "granularidade": serie.granularidade,
        "unidade": serie.unidade,
        "arquivo": serie.arquivo,
        "data_inicial": data_inicial,
        "data_final": data_final,
        "qtd_registros": len(registros),
        "primeira_data": registros[0]["data"],
        "ultima_data": registros[-1]["data"],
        "sha256": sha256,
        "extraido_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    LOGGER.info(
        "Série %s extraída: %s registros (%s a %s) -> %s",
        serie.nome,
        metadados["qtd_registros"],
        metadados["primeira_data"],
        metadados["ultima_data"],
        destino,
    )
    return metadados


def executar(argumentos: argparse.Namespace) -> int:
    """Orquestra a extração das séries solicitadas. Devolve o exit code."""
    diretorio_saida = Path(argumentos.output_dir)
    client = BCBClient(
        timeout=argumentos.timeout,
        max_tentativas=argumentos.max_tentativas,
    )
    selecionadas = (
        list(SERIES.values())
        if argumentos.serie == "todas"
        else [SERIES[argumentos.serie]]
    )

    manifesto: List[Dict[str, Any]] = []
    for serie in selecionadas:
        try:
            manifesto.append(
                extrair_serie(
                    serie=serie,
                    diretorio_saida=diretorio_saida,
                    data_inicial=argumentos.data_inicial,
                    data_final=argumentos.data_final,
                    client=client,
                )
            )
        except BCBExtractionError as erro:
            LOGGER.error("Extração da série %s falhou: %s", serie.nome, erro)
            return 1

    _salvar_json(diretorio_saida / "_manifest.json", manifesto)
    LOGGER.info(
        "Extração concluída. Manifesto em %s", diretorio_saida / "_manifest.json"
    )
    return 0


def construir_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--serie",
        choices=[*SERIES.keys(), "todas"],
        default="todas",
        help="Série a extrair (padrão: todas).",
    )
    parser.add_argument("--data-inicial", default=DATA_INICIAL_PADRAO)
    parser.add_argument("--data-final", default=DATA_FINAL_PADRAO)
    parser.add_argument("--output-dir", default=str(DIRETORIO_PADRAO))
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--max-tentativas", type=int, default=5)
    parser.add_argument("-v", "--verboso", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    argumentos = construir_parser().parse_args(argv)
    _configurar_log(argumentos.verboso)
    return executar(argumentos)


if __name__ == "__main__":
    sys.exit(main())
