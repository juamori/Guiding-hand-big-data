-- =====================================================================
-- Guiding Hand | Big Data | Preparação do ambiente (Databricks Free Edition)
-- Execute UMA vez no SQL Editor (ou numa célula %sql).
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS workspace.guiding_hand
  COMMENT 'Guiding Hand (FIAP GS 2026) - pipeline de barreiras urbanas em Pinheiros/SP';

-- Volume onde os lotes da pasta chegada/ são enviados manualmente pela interface
CREATE VOLUME IF NOT EXISTS workspace.guiding_hand.chegada
  COMMENT 'Arquivos brutos enviados em lotes a partir da pasta chegada/ do Airflow';

-- Checkpoints do Auto Loader e arquivos de controle
CREATE VOLUME IF NOT EXISTS workspace.guiding_hand.controle
  COMMENT 'Checkpoints do Auto Loader e configuracao interna';

-- Saída consumida pelo MVP (JSON exportado da Ouro)
CREATE VOLUME IF NOT EXISTS workspace.guiding_hand.exportacao
  COMMENT 'Exportacao da tabela Ouro consumida pelo MVP';
