# Passo a passo — Etapa 0 (extração + carga no Volume)

Este arquivo é o guia operacional. O README final do case será escrito depois,
com arquitetura e decisões técnicas.

---

## 1. Preparar a pasta local

Descompacte o `case-bcb.zip`. A estrutura deve ficar assim:

```
case-bcb/
├── extract/
│   ├── __init__.py
│   ├── bcb_client.py
│   ├── config.py
│   ├── extract_sgs.py
│   └── upload_volume.py
├── src/pipeline/          (vazio por enquanto — bronze/silver/gold vêm depois)
├── tests/
│   └── test_bcb_client.py
├── notebooks/             (vazio por enquanto)
├── resources/             (vazio por enquanto — job/bundle)
├── data/raw/              (saída da extração; ignorada pelo git)
├── requirements.txt
└── .gitignore
```

Abra o terminal **dentro da pasta `case-bcb`**. Todos os comandos abaixo
assumem esse diretório.

---

## 2. Criar o ambiente virtual e instalar dependências

Linux / macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## 3. Rodar os testes (opcional, mas vira evidência no README)

```bash
python -m unittest discover -s tests -v
```

Esperado: `Ran 5 tests ... OK`. Os testes sobem um servidor HTTP local que
simula a API do BCB (falha 503 seguida de sucesso, payload vazio, JSON
inválido, API indisponível). Guarde esse print.

---

## 4. Rodar a extração

```bash
python -m extract.extract_sgs
```

Isso baixa as duas séries com a janela padrão (01/01/2020 a 31/12/2024) e grava
em `data/raw/`.

Variações úteis:

```bash
# só uma série
python -m extract.extract_sgs --serie selic

# outra janela (útil para backfill)
python -m extract.extract_sgs --data-inicial 01/01/2019 --data-final 31/12/2019

# log detalhado
python -m extract.extract_sgs -v
```

---

## 5. Conferir o que foi gerado

```bash
ls -lh data/raw/
cat data/raw/_manifest.json
```

Esperado em `data/raw/`:

- `selic.json` — série 11, ~1250 registros (dias úteis de 2020 a 2024)
- `ipca.json` — série 433, 60 registros (meses de 2020 a 2024)
- `_manifest.json` — metadados da extração: sha256, contagem, primeira e
  última data, timestamp UTC

Se a contagem do IPCA não for 60, algo veio errado da API — vale reexecutar.

---

## 6. Versionar no Git

```bash
git init
git add .
git commit -m "feat: script de extração das séries SGS/BCB com retentativas"
```

Crie o repositório no GitHub e faça o push:

```bash
git remote add origin https://github.com/SEU_USUARIO/case-bcb.git
git branch -M main
git push -u origin main
```

Os arquivos `.json` de `data/raw/` **não** vão para o repositório (estão no
`.gitignore`) — o `_manifest.json` também não. Quem avaliar reproduz rodando o
script. Se preferir versionar o manifesto como evidência, remova a linha
correspondente do `.gitignore`.

---

## 7. Criar o Volume no Unity Catalog (Databricks)

Na UI do Databricks:

1. Menu lateral → **Catalog**
2. Escolha o catálogo disponível no seu workspace (no Free Edition costuma ser
   `workspace`; em workspaces pagos, `main`)
3. **Create schema** → nome: `case_bcb`
4. Dentro do schema → **Create** → **Volume** → nome: `landing`, tipo *Managed*

O caminho final fica: `/Volumes/workspace/case_bcb/landing`
(ajuste o primeiro nível se seu catálogo tiver outro nome).

---

## 8. Subir os arquivos para o Volume

**Opção A — pela UI (mais simples):**
Catalog → `case_bcb` → `landing` → botão **Upload to this volume** → selecione
`selic.json` e `ipca.json`.

**Opção B — automatizado pelo script (é um dos diferenciais do case):**

Primeiro configure as credenciais. No Databricks, gere um token em
*Settings → Developer → Access tokens*, e exporte:

```bash
export DATABRICKS_HOST=https://SEU-WORKSPACE.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
```

No Windows (PowerShell), use `$env:DATABRICKS_HOST = "..."`.

Depois:

```bash
python -m extract.upload_volume --volume /Volumes/workspace/case_bcb/landing
```

Confirme na UI que os dois arquivos apareceram no Volume.

---

## 9. Conferir do lado do Databricks

Abra um notebook no workspace e rode:

```python
display(dbutils.fs.ls("/Volumes/workspace/case_bcb/landing"))
```

Se os dois arquivos aparecerem, a Etapa 0 está concluída e o pipeline
(Bronze → Silver → Gold) pode começar a ler do Volume.

---

## Próximos passos (ainda não implementados)

- `src/pipeline/bronze.py` — ingestão incremental com Auto Loader
- `src/pipeline/silver.py` — tipagem, padronização e MERGE idempotente
- `src/pipeline/gold.py` — agregação mensal, juro real e acumulado 12 meses
- `src/pipeline/quality.py` — checagens de qualidade com falha explícita
- `resources/` — definição do Workflow (JSON export ou Asset Bundle)
- `README.md` — arquitetura, decisões técnicas, grão das tabelas, reprodução
