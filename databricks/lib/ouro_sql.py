"""SQL da camada Ouro.

As consultas são escritas como SELECTs com o prefixo ``{s}`` (ex.: ``workspace.guiding_hand.``).
No Databricks o notebook 03 executa ``CREATE OR REPLACE TABLE <s>nome AS <select>``;
nos testes locais o mesmo SELECT vira uma temp view. Assim o SQL testado é o SQL entregue.

O SQL não usa nenhuma UDF: centros de células são calculados em DataFrame na Prata
(colunas lat_celula/lon_celula) e dim_local é montada em DataFrame (gh_transform.dim_local).
"""

DIM_TEMPO = """
WITH datas AS (
  SELECT DISTINCT to_date(ts_deteccao) AS data FROM {s}prata_eventos_oculos
  UNION SELECT DISTINCT to_date(data_abertura) FROM {s}prata_sp156_solicitacoes WHERE data_abertura IS NOT NULL
  UNION SELECT DISTINCT to_date(data_conclusao) FROM {s}prata_sp156_solicitacoes WHERE data_conclusao IS NOT NULL
  UNION SELECT DISTINCT to_date(data_hora) FROM {s}prata_clima_horario
  UNION SELECT DISTINCT data_coleta FROM {s}prata_osm_infraestrutura
)
SELECT CAST(date_format(data, 'yyyyMMdd') AS INT) AS sk_data,
       data, year(data) AS ano, month(data) AS mes, day(data) AS dia,
       date_format(data, 'EEEE') AS dia_semana,
       dayofweek(data) IN (1, 7) AS fim_de_semana
FROM datas WHERE data IS NOT NULL
"""

# dim_local é construída em DataFrame (gh_transform.dim_local), sem UDF no SQL.

# Pré-requisito: aux_barreira_oculos e aux_mapa_celula_barreira, gerados por
# gh_transform.consolidar_barreiras() (resolução de entidade espacial).
DIM_BARREIRA = """
SELECT barreira_key, origem, tipo_obstaculo AS sk_tipo, lat, lon, geohash7 AS sk_local,
       n_celulas, CAST(NULL AS STRING) AS referencia_oficial
FROM {s}aux_barreira_oculos
UNION ALL
SELECT barreira_key, 'sp156', tipo_obstaculo, lat_celula, lon_celula, geohash7, 1, protocolo
FROM {s}prata_sp156_solicitacoes WHERE tipo_obstaculo IS NOT NULL
"""

FATO_OCORRENCIA = """
WITH ocorrencias AS (
  SELECT concat('oculos:', e.event_id) AS id_ocorrencia, 'oculos_sintetico' AS sk_origem,
         e.tipo_obstaculo AS sk_tipo, e.geohash7 AS sk_local, m.barreira_key, e.device_hash,
         e.confianca, e.ts_deteccao AS data_hora, 'detectada' AS status,
         to_date(e.ts_deteccao) AS data_identificacao, CAST(NULL AS DATE) AS data_resolucao, e.data_coleta
  FROM {s}prata_eventos_oculos e
  JOIN {s}aux_mapa_celula_barreira m ON m.celula_key = e.celula_key
  WHERE e.eh_barreira
  UNION ALL
  SELECT concat('sp156:', protocolo), 'sp156', tipo_obstaculo, geohash7, barreira_key, NULL,
         NULL, data_abertura, status, to_date(data_abertura),
         CASE WHEN status = 'resolvida' THEN to_date(data_conclusao) END, data_coleta
  FROM {s}prata_sp156_solicitacoes WHERE tipo_obstaculo IS NOT NULL
  UNION ALL
  SELECT concat('osm:', osm_key), 'osm', tipo_obstaculo, geohash7, NULL, NULL,
         NULL, CAST(data_coleta AS TIMESTAMP), 'mapeada', data_coleta, NULL, data_coleta
  FROM {s}prata_osm_infraestrutura WHERE tipo_obstaculo IS NOT NULL
)
SELECT o.id_ocorrencia,
       CAST(date_format(o.data_identificacao, 'yyyyMMdd') AS INT) AS sk_data,
       o.sk_local, o.sk_tipo, o.sk_origem, o.barreira_key, o.device_hash,
       o.confianca, o.data_hora, o.status, o.data_identificacao, o.data_resolucao,
       datediff(o.data_resolucao, o.data_identificacao) AS dias_ate_resolucao,
       c.precipitacao_mm, c.choveu, o.data_coleta
FROM ocorrencias o
LEFT JOIN {s}prata_clima_horario c ON date_trunc('HOUR', o.data_hora) = c.data_hora
"""

# Tabela consumida pelo MVP: uma linha por barreira, SEM qualquer dado de dispositivo.
TB_MVP_MAPA = """
WITH ref AS (SELECT max(data_identificacao) AS hoje FROM {s}fato_ocorrencia_barreira
             WHERE sk_origem = 'oculos_sintetico'),
agg AS (
  SELECT barreira_key, count(*) AS n_ocorrencias,
         count(DISTINCT device_hash) AS n_dispositivos,
         count(DISTINCT data_identificacao) AS dias_com_deteccao,
         min(data_hora) AS primeira_vez, max(data_hora) AS ultima_vez,
         round(avg(confianca), 3) AS confianca_media,
         max(status) AS status_oficial, max(data_resolucao) AS data_resolucao
  FROM {s}fato_ocorrencia_barreira WHERE barreira_key IS NOT NULL
  GROUP BY barreira_key
)
SELECT substr(b.barreira_key, 1, 12) AS barreira_id, b.origem,
       b.sk_tipo AS tipo_obstaculo, t.descricao, t.altura, t.bengala_costuma_detectar,
       b.lat, b.lon, b.sk_local AS geohash7,
       a.n_ocorrencias, a.n_dispositivos, a.dias_com_deteccao,
       a.primeira_vez, a.ultima_vez, a.confianca_media,
       CASE WHEN b.origem = 'sp156' THEN a.status_oficial
            WHEN to_date(a.ultima_vez) >= date_sub(r.hoje, 1) THEN 'ativa'
            ELSE 'nao_redetectada' END AS status_barreira,
       a.data_resolucao,
       CASE WHEN b.origem = 'sp156' THEN NULL
            ELSE (a.n_dispositivos >= 2 AND a.n_ocorrencias >= 3) END AS elegivel_confirmacao,
       CASE WHEN b.origem = 'sp156' THEN 'registro_oficial' ELSE 'pendente' END AS status_validacao,
       CASE WHEN NOT t.bengala_costuma_detectar THEN 'alto' ELSE 'normal' END AS nivel_alerta
FROM {s}dim_barreira b
JOIN agg a ON a.barreira_key = b.barreira_key
JOIN {s}dim_tipo_obstaculo t ON t.sk_tipo = b.sk_tipo
CROSS JOIN ref r
"""

TABELAS_OURO = [
    ("dim_tempo", DIM_TEMPO),
    ("dim_barreira", DIM_BARREIRA),
    ("fato_ocorrencia_barreira", FATO_OCORRENCIA),
    ("tb_mvp_mapa_barreiras", TB_MVP_MAPA),
]


def sql(texto: str, prefixo: str = "") -> str:
    return texto.replace("{s}", prefixo)


def carregar_consultas(texto: str, prefixo_remover: str | None = None) -> list[tuple[str, str, str]]:
    """Lê consultas_mvp.sql -> [(id, pergunta, sql)]. Usado pelos notebooks 04/06 e pelos testes."""
    import re
    if prefixo_remover:
        texto = texto.replace(prefixo_remover, "")
    consultas = []
    for bloco in re.split(r"^-- @consulta ", texto, flags=re.M)[1:]:
        cabecalho, corpo = bloco.split("\n", 1)
        cid, pergunta = [x.strip() for x in cabecalho.split("|", 1)]
        sql_limpo = "\n".join(l for l in corpo.splitlines() if not l.strip().startswith("--")).strip().rstrip(";")
        consultas.append((cid, pergunta, sql_limpo))
    return consultas


def montar_evidencias(spark, prefixo: str, consultas, lote: str, ambiente: str, etapas: dict | None = None) -> dict:
    """Coleta, das tabelas, tudo o que o relatório da FIAP pede (seções 5, 7 e 8)."""
    import datetime

    def linhas(sql_texto):
        return [{k: (v if isinstance(v, (int, float, str, bool, type(None))) else str(v))
                 for k, v in r.asDict().items()} for r in spark.sql(sql_texto).collect()]

    def existe(nome):
        try:
            spark.table(prefixo + nome).limit(1).collect()
            return True
        except Exception:
            return False

    ev = {"ambiente": ambiente, "lote": lote,
          "gerado_em": datetime.datetime.now(datetime.timezone.utc).isoformat(), "etapas": etapas or {}}
    if existe("controle_execucoes_bronze"):
        ev["bronze_execucoes"] = linhas(f"SELECT rotulo, linhas_antes, linhas_depois, arquivos_novos, "
                                        f"CAST(executado_em AS STRING) AS executado_em "
                                        f"FROM {prefixo}controle_execucoes_bronze ORDER BY executado_em")
    ev["bronze_por_fonte_data"] = linhas(f"SELECT fonte, CAST(data_coleta AS STRING) AS data_coleta, count(*) AS arquivos "
                                         f"FROM {prefixo}bronze_arquivos GROUP BY 1, 2 ORDER BY 2, 1")
    ev["quarentena_por_regra"] = linhas(f"SELECT regra, fonte, count(*) AS barrados FROM {prefixo}prata_quarentena "
                                        f"GROUP BY 1, 2 ORDER BY 1, 2")
    tabelas = ["prata_eventos_oculos", "prata_clima_horario", "prata_osm_infraestrutura",
               "prata_sp156_solicitacoes", "prata_olhovivo_paradas", "prata_quarentena", "dim_tempo",
               "dim_local", "dim_tipo_obstaculo", "dim_origem", "dim_barreira", "fato_ocorrencia_barreira",
               "tb_mvp_mapa_barreiras"]
    ev["contagens"] = {t: spark.table(prefixo + t).count() for t in tabelas if existe(t)}
    ev["consultas"] = [{"id": cid, "pergunta": pergunta, "sql": sql_texto, "resultado": linhas(sql_texto)[:30]}
                       for cid, pergunta, sql_texto in consultas]
    return ev
