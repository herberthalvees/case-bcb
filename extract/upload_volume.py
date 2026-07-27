"""Upload dos arquivos brutos para um Volume do Unity Catalog.

Automatiza o passo que a UI faz em Catalog > Volumes > Upload, usando o
Databricks SDK. A autenticação segue a cadeia padrão do SDK
(``DATABRICKS_HOST`` / ``DATABRICKS_TOKEN`` ou perfil do ``~/.databrickscfg``).

Uso:
    python -m extract.upload_volume \
        --volume /Volumes/main/case_bcb/landing \
        --input-dir data/raw
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Sequence

LOGGER = logging.getLogger("upload")


def _arquivos_para_envio(diretorio: Path, padroes: Sequence[str]) -> List[Path]:
    arquivos: List[Path] = []
    for padrao in padroes:
        arquivos.extend(sorted(diretorio.glob(padrao)))
    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado em {diretorio} "
            f"para os padrões {list(padroes)}. "
            "Rode a extração antes do upload."
        )
    return arquivos


def enviar(
    diretorio: Path,
    volume: str,
    padroes: Sequence[str],
    sobrescrever: bool,
) -> None:
    """Envia os arquivos do diretório local para o caminho do Volume."""
    from databricks.sdk import WorkspaceClient  # import tardio: dependência opcional

    client = WorkspaceClient()
    destino_base = volume.rstrip("/")

    for arquivo in _arquivos_para_envio(diretorio, padroes):
        destino = f"{destino_base}/{arquivo.name}"
        LOGGER.info("Enviando %s -> %s", arquivo, destino)
        with arquivo.open("rb") as conteudo:
            client.files.upload(destino, conteudo, overwrite=sobrescrever)
    LOGGER.info("Upload concluído em %s", destino_base)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--volume",
        required=True,
        help="Caminho do Volume, ex.: /Volumes/main/case_bcb/landing",
    )
    parser.add_argument("--input-dir", default="data/raw")
    parser.add_argument(
        "--padrao",
        nargs="+",
        default=["*.json"],
        help="Padrões glob dos arquivos a enviar (padrão: *.json).",
    )
    parser.add_argument(
        "--nao-sobrescrever",
        action="store_true",
        help="Falha se o arquivo já existir no Volume.",
    )
    argumentos = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    try:
        enviar(
            diretorio=Path(argumentos.input_dir),
            volume=argumentos.volume,
            padroes=argumentos.padrao,
            sobrescrever=not argumentos.nao_sobrescrever,
        )
    except Exception as erro:  # noqa: BLE001 - CLI: falha explícita com exit code
        LOGGER.error("Upload falhou: %s", erro)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
