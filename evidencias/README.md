# Evidências

Imagens referenciadas pelo README principal. Nomes fixos — o README aponta
para estes caminhos.

| Arquivo | Onde aparece | O que mostra |
|---|---|---|
| `01_workflow_sucesso.png` | README §7.1 | Workflow com as três tasks concluídas |
| `02_historico_merge_silver.png` | README §7.2 | `DESCRIBE HISTORY` da Silver: MERGE inserindo zero |
| `03_checagens_qualidade.png` | README §7.3 | As 18 checagens aprovadas |
| `04_validacao_ibge.png` | README §8 | Acumulados de 12 meses batendo com o oficial |
| `05_extracao_local.png` | README §9.1 | CLI de extração e `_manifest.json` |

A evidência de idempotência não depende destas imagens: o histórico de
operações fica gravado no próprio Delta. Ver `notebooks/99_evidencias.py` ou
`sql/99_evidencias.sql`.
