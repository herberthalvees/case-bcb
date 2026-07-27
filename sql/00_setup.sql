-- Setup do Unity Catalog para o case.
-- Executar uma vez, no SQL Editor do Databricks.

CREATE SCHEMA IF NOT EXISTS workspace.case_bcb
  COMMENT 'Case beAnalytic - series SELIC (11) e IPCA (433) do SGS/BCB';

-- Landing zone dos JSON brutos extraidos localmente pela CLI de extracao.
CREATE VOLUME IF NOT EXISTS workspace.case_bcb.landing
  COMMENT 'Arquivos brutos selic.json e ipca.json';

-- Checkpoints do Auto Loader. Fica em volume separado para nao ser lido
-- como dado de entrada pelo proprio Auto Loader.
CREATE VOLUME IF NOT EXISTS workspace.case_bcb.checkpoints
  COMMENT 'Checkpoints e schema location do Auto Loader';
