-- Evidencias consultaveis a qualquer momento, direto do SQL Editor.
-- Equivalente ao notebook notebooks/99_evidencias.py.

-- 1. Idempotencia da Bronze: um lote de ingestao por arquivo.
SELECT arquivo_origem,
       COUNT(*)                    AS linhas,
       COUNT(DISTINCT ingerido_em) AS lotes_de_ingestao
FROM workspace.case_bcb.bronze_sgs_raw
GROUP BY arquivo_origem
ORDER BY arquivo_origem;

-- 2. Idempotencia da Silver: historico das operacoes de MERGE.
--    Olhar a coluna operationMetrics: a partir da segunda execucao,
--    numTargetRowsInserted e numTargetRowsUpdated sao zero.
DESCRIBE HISTORY workspace.case_bcb.silver_series_bcb;

-- 3. Grao declarado igual ao grao real.
SELECT 'silver' AS tabela,
       COUNT(*)                                  AS linhas,
       COUNT(DISTINCT serie_id, data_referencia) AS chaves_distintas
FROM workspace.case_bcb.silver_series_bcb
UNION ALL
SELECT 'gold', COUNT(*), COUNT(DISTINCT ano_mes)
FROM workspace.case_bcb.gold_juro_real_mensal;

-- 4. Validacao contra o indice oficial do ano.
SELECT ano_mes,
       ROUND(ipca_acum_12m, 2)      AS ipca_12m,
       ROUND(selic_acum_12m, 2)     AS selic_12m,
       ROUND(juro_real_acum_12m, 2) AS juro_real_12m
FROM workspace.case_bcb.gold_juro_real_mensal
WHERE ano_mes LIKE '%-12'
ORDER BY ano_mes;
