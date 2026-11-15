# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ouro — modelo dimensional e tabela do MVP
# MAGIC **Requisitos 07 e 08.** Esquema estrela:
# MAGIC
# MAGIC - **Fato** `fato_ocorrencia_barreira` — grão: uma ocorrência de barreira (detecção dos óculos,
# MAGIC   solicitação SP156 ou elemento mapeado no OSM).
# MAGIC - **Dimensões** `dim_tempo`, `dim_local` (célula geohash 7 ≈ 150 m), `dim_tipo_obstaculo`,
# MAGIC   `dim_origem`, `dim_barreira` (entidade resolvida).
# MAGIC - **Consumo do MVP** `tb_mvp_mapa_barreiras` — uma linha por barreira, **sem nenhum dado de dispositivo**.
# MAGIC
# MAGIC A Ouro é reconstruída a cada execução (CREATE OR REPLACE) a partir da Prata: determinística e idempotente.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../lib"))
import gh_transform as gt
import ouro_sql

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema", "guiding_hand")
CAT, SCH = dbutils.widgets.get("catalogo"), dbutils.widgets.get("schema")
S = f"{CAT}.{SCH}."
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")


# COMMAND ----------

gt.dim_tipo_obstaculo(spark).write.mode("overwrite").saveAsTable(f"{S}dim_tipo_obstaculo")
gt.dim_origem(spark).write.mode("overwrite").saveAsTable(f"{S}dim_origem")

gt.dim_local(spark.table(f"{S}prata_eventos_oculos"), spark.table(f"{S}prata_sp156_solicitacoes"),
             spark.table(f"{S}prata_osm_infraestrutura")).write.mode("overwrite").saveAsTable(f"{S}dim_local")

# Resolução de entidade: junta células vizinhas do mesmo tipo (<= 20 m) numa única barreira
dim_b, mapa = gt.consolidar_barreiras(spark, spark.table(f"{S}prata_eventos_oculos"))
dim_b.write.mode("overwrite").saveAsTable(f"{S}aux_barreira_oculos")
mapa.write.mode("overwrite").saveAsTable(f"{S}aux_mapa_celula_barreira")

for nome, consulta in ouro_sql.TABELAS_OURO:
    spark.sql(f"CREATE OR REPLACE TABLE {S}{nome} AS {ouro_sql.sql(consulta, S)}")
    print(f"{nome:<28} {spark.table(S + nome).count():>6} linhas")

# COMMAND ----------

COMENTARIOS = {
    "fato_ocorrencia_barreira": "Fato: ocorrencias de barreiras. device_hash = SHA-256 com salt (LGPD); localizacao generalizada em geohash 7.",
    "tb_mvp_mapa_barreiras": "CONSUMIDA PELO MVP. Uma linha por barreira; sem dados de dispositivo; coordenada = centro da celula/agrupamento.",
    "dim_barreira": "Barreiras resolvidas (celulas do mesmo tipo a <= 20 m unificadas).",
    "dim_local": "Celulas geohash 7 (~153 m) do recorte de Pinheiros.",
}
for tabela, texto in COMENTARIOS.items():
    spark.sql(f"COMMENT ON TABLE {S}{tabela} IS '{texto}'")
spark.sql(f"ALTER TABLE {S}fato_ocorrencia_barreira ALTER COLUMN device_hash COMMENT "
          "'Identificador do dispositivo pseudonimizado (SHA-256 + salt). Nunca o ID original.'")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {S}tb_mvp_mapa_barreiras ORDER BY nivel_alerta, n_ocorrencias DESC"))
