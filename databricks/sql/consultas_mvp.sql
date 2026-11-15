-- =====================================================================
-- Guiding Hand | Big Data | Camada Ouro | Consultas de negócio do MVP
-- Catálogo/schema: workspace.guiding_hand (Databricks Free Edition)
-- Cada bloco começa com "-- @consulta <id> | <pergunta>" (usado pelos testes).
-- =====================================================================

-- @consulta C1 | Quais tipos de obstáculo são mais recorrentes, e quantos a bengala não detecta?
SELECT t.descricao                       AS tipo_obstaculo,
       t.altura,
       f.sk_origem                       AS origem,
       count(*)                          AS ocorrencias,
       count(DISTINCT f.barreira_key)    AS barreiras_distintas,
       NOT t.bengala_costuma_detectar    AS invisivel_para_bengala
FROM workspace.guiding_hand.fato_ocorrencia_barreira f
JOIN workspace.guiding_hand.dim_tipo_obstaculo t ON t.sk_tipo = f.sk_tipo
GROUP BY t.descricao, t.altura, f.sk_origem, t.bengala_costuma_detectar
ORDER BY ocorrencias DESC;

-- @consulta C2 | Quais quadras (células de ~150 m) concentram mais barreiras?
SELECT l.sk_local                        AS celula_geohash7,
       l.lat_centro, l.lon_centro,
       count(DISTINCT f.barreira_key)    AS barreiras_distintas,
       count(*)                          AS ocorrencias,
       sum(CASE WHEN t.altura = 'acima_cintura' THEN 1 ELSE 0 END) AS ocorrencias_acima_cintura
FROM workspace.guiding_hand.fato_ocorrencia_barreira f
JOIN workspace.guiding_hand.dim_local l          ON l.sk_local = f.sk_local
JOIN workspace.guiding_hand.dim_tipo_obstaculo t ON t.sk_tipo  = f.sk_tipo
WHERE f.barreira_key IS NOT NULL
GROUP BY l.sk_local, l.lat_centro, l.lon_centro
ORDER BY barreiras_distintas DESC, ocorrencias DESC
LIMIT 10;

-- @consulta C3 | Quais barreiras foram identificadas em mais de uma coleta (dias diferentes)?
SELECT substr(f.barreira_key, 1, 12)              AS barreira_id,
       f.sk_tipo                                  AS tipo_obstaculo,
       f.sk_local                                 AS celula_geohash7,
       count(DISTINCT f.data_coleta)              AS coletas_distintas,
       count(*)                                   AS deteccoes,
       count(DISTINCT f.device_hash)              AS dispositivos_distintos,
       min(f.data_identificacao)                  AS primeira_vez,
       max(f.data_identificacao)                  AS ultima_vez
FROM workspace.guiding_hand.fato_ocorrencia_barreira f
WHERE f.sk_origem = 'oculos_sintetico'
GROUP BY f.barreira_key, f.sk_tipo, f.sk_local
HAVING count(DISTINCT f.data_coleta) > 1
ORDER BY coletas_distintas DESC, deteccoes DESC;

-- @consulta C4 | Como a chuva se relaciona com as detecções de barreiras?
SELECT CASE WHEN f.choveu THEN 'com chuva' WHEN f.choveu = false THEN 'sem chuva'
            ELSE 'sem dado de clima' END            AS condicao,
       count(*)                                    AS deteccoes,
       count(DISTINCT date_trunc('HOUR', f.data_hora)) AS horas_com_deteccao,
       round(count(*) / greatest(count(DISTINCT date_trunc('HOUR', f.data_hora)), 1), 2)
                                                   AS deteccoes_por_hora,
       round(avg(f.precipitacao_mm), 2)            AS precipitacao_media_mm
FROM workspace.guiding_hand.fato_ocorrencia_barreira f
WHERE f.sk_origem = 'oculos_sintetico'
GROUP BY 1
ORDER BY deteccoes DESC;

-- @consulta C5 | IMPACTO: barreiras mapeadas, invisíveis à bengala, prontas para confirmação e resolvidas pela zeladoria
SELECT origem,
       count(*)                                                         AS barreiras_mapeadas,
       sum(CASE WHEN NOT bengala_costuma_detectar THEN 1 ELSE 0 END)    AS invisiveis_para_bengala,
       round(100.0 * sum(CASE WHEN NOT bengala_costuma_detectar THEN 1 ELSE 0 END) / count(*), 1)
                                                                        AS pct_invisiveis_para_bengala,
       sum(CASE WHEN elegivel_confirmacao THEN 1 ELSE 0 END)            AS elegiveis_confirmacao_rpa,
       sum(CASE WHEN status_barreira IN ('resolvida') THEN 1 ELSE 0 END) AS resolvidas_oficialmente,
       sum(CASE WHEN status_barreira = 'nao_redetectada' THEN 1 ELSE 0 END) AS nao_redetectadas,
       round(avg(CASE WHEN status_barreira = 'resolvida'
                      THEN datediff(data_resolucao, to_date(primeira_vez)) END), 1) AS dias_medios_ate_resolucao
FROM workspace.guiding_hand.tb_mvp_mapa_barreiras
GROUP BY origem
ORDER BY barreiras_mapeadas DESC;

-- @consulta C6 | Como evolui, dia a dia, o número de barreiras identificadas e resolvidas?
WITH identificadas AS (
  SELECT data_identificacao AS data, count(DISTINCT barreira_key) AS identificadas
  FROM workspace.guiding_hand.fato_ocorrencia_barreira
  WHERE barreira_key IS NOT NULL GROUP BY data_identificacao
), resolvidas AS (
  SELECT data_resolucao AS data, count(*) AS resolvidas
  FROM workspace.guiding_hand.fato_ocorrencia_barreira
  WHERE data_resolucao IS NOT NULL GROUP BY data_resolucao
)
SELECT d.data, coalesce(i.identificadas, 0) AS identificadas, coalesce(r.resolvidas, 0) AS resolvidas
FROM workspace.guiding_hand.dim_tempo d
LEFT JOIN identificadas i ON i.data = d.data
LEFT JOIN resolvidas r    ON r.data = d.data
WHERE coalesce(i.identificadas, 0) + coalesce(r.resolvidas, 0) > 0
ORDER BY d.data;
