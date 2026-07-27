"""Testes do cliente de extração usando um servidor HTTP local.

Simula os cenários que o script precisa tratar: falha transitória seguida de
sucesso, payload vazio, JSON inválido e indisponibilidade total da API.

Uso:
    python -m tests.test_bcb_client
"""

from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from extract.bcb_client import BCBClient, BCBExtractionError

PAYLOAD_OK = [
    {"data": "01/01/2020", "valor": "0.016137"},
    {"data": "02/01/2020", "valor": "0.016137"},
]


class _Handler(BaseHTTPRequestHandler):
    """Handler cujo comportamento é definido por ``server.cenario``."""

    def do_GET(self) -> None:  # noqa: N802 (assinatura da stdlib)
        cenario = self.server.cenario  # type: ignore[attr-defined]
        self.server.chamadas += 1  # type: ignore[attr-defined]

        if cenario == "falha_transitoria":
            if self.server.chamadas < 3:  # type: ignore[attr-defined]
                self._responder(503, "indisponivel")
                return
            self._responder(200, json.dumps(PAYLOAD_OK))
        elif cenario == "vazio":
            self._responder(200, "[]")
        elif cenario == "json_invalido":
            self._responder(200, "<html>erro</html>")
        elif cenario == "sempre_500":
            self._responder(500, "boom")
        else:
            self._responder(200, json.dumps(PAYLOAD_OK))

    def _responder(self, status: int, corpo: str) -> None:
        dados = corpo.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(dados)))
        self.end_headers()
        self.wfile.write(dados)

    def log_message(self, *args) -> None:  # silencia o log do servidor
        return


class BCBClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.servidor = HTTPServer(("127.0.0.1", 0), _Handler)
        self.servidor.cenario = "ok"
        self.servidor.chamadas = 0
        self.thread = threading.Thread(target=self.servidor.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.servidor.server_port}/dados"
        self.client = BCBClient(max_tentativas=4, backoff_base=1.0, backoff_max=0.05)

    def tearDown(self) -> None:
        self.servidor.shutdown()
        self.servidor.server_close()

    def _buscar(self):
        return self.client.buscar_serie(self.url, "01/01/2020", "31/12/2024")

    def test_sucesso(self) -> None:
        self.assertEqual(self._buscar(), PAYLOAD_OK)

    def test_retenta_e_recupera(self) -> None:
        self.servidor.cenario = "falha_transitoria"
        self.assertEqual(self._buscar(), PAYLOAD_OK)
        self.assertEqual(self.servidor.chamadas, 3)

    def test_payload_vazio_falha(self) -> None:
        self.servidor.cenario = "vazio"
        with self.assertRaises(BCBExtractionError):
            self._buscar()

    def test_json_invalido_falha(self) -> None:
        self.servidor.cenario = "json_invalido"
        with self.assertRaises(BCBExtractionError):
            self._buscar()

    def test_api_indisponivel_esgota_tentativas(self) -> None:
        self.servidor.cenario = "sempre_500"
        with self.assertRaises(BCBExtractionError):
            self._buscar()
        self.assertEqual(self.servidor.chamadas, 4)


if __name__ == "__main__":
    unittest.main(verbosity=2)
