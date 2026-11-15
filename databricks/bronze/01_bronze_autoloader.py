# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — ingestão incremental com Auto Loader
# MAGIC **Requisito 05.** Lê os arquivos enviados ao Volume `chegada` e grava em `bronze_arquivos`
# MAGIC **uma linha por arquivo, com o conteúdo bruto intacto** + metadados de origem e carga.
# MAGIC
# MAGIC - Auto Loader (`cloudFiles`) + checkpoint: cada arquivo é lido **uma única vez**.
# MAGIC - Gravação via `foreachBatch`: um arquivo com o mesmo nome e o mesmo SHA-256 já presente na tabela não é
# MAGIC   gravado de novo. Isso permite que o lote 1 tenha sido ingerido pelo notebook `bronze_lote_1` (checkpoint próprio)
# MAGIC   sem que este notebook o duplique ao processar o lote 2.
# MAGIC - `tamanho_bytes` = tamanho real do arquivo (`_metadata.file_size`).
# MAGIC - `trigger(availableNow=True)`: processa o que é novo e para (modo lote, adequado ao Free Edition).
# MAGIC - Rodar de novo sem arquivos novos **não insere nada** (idempotente).
# MAGIC - Cada execução registra contagens antes/depois em `controle_execucoes_bronze` (evidência do relatório).

# COMMAND ----------

import os, sys, uuid
sys.path.append(os.path.abspath("../lib"))
import gh_transform as gt
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema", "guiding_hand")
dbutils.widgets.text("rotulo_execucao", "lote 1 - 1a execucao")
CAT, SCH = dbutils.widgets.get("catalogo"), dbutils.widgets.get("schema")
ROTULO = dbutils.widgets.get("rotulo_execucao")
S = f"{CAT}.{SCH}."
ORIGEM = f"/Volumes/{CAT}/{SCH}/chegada"
# v2: a gravação passou a ser foreachBatch (sem duplicar). Um checkpoint de outro tipo de destino não é reaproveitado.
CHECKPOINT = f"/Volumes/{CAT}/{SCH}/controle/checkpoint_bronze_arquivos_v2"
TABELA = f"{S}bronze_arquivos"
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {TABELA} (
  conteudo_bruto STRING, arquivo_origem STRING, nome_arquivo STRING, fonte STRING, data_coleta DATE,
  tamanho_bytes BIGINT, hash_conteudo STRING, carregado_em TIMESTAMP) USING DELTA""")

def contar():
    spark.catalog.refreshTable(TABELA)
    df = spark.table(TABELA)
    return df.count(), {r["fonte"]: r["count"] for r in df.groupBy("fonte").count().collect()}

antes_total, antes_fonte = contar()
print(f"Antes: {antes_total} arquivos na Bronze -> {antes_fonte}")

# COMMAND ----------

fluxo = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "text")
         .option("wholetext", "true")          # 1 linha = 1 arquivo inteiro (bruto preservado)
         .option("pathGlobFilter", "*.json")
         .load(ORIGEM)
         .select("value", F.col("_metadata.file_path").alias("file_path"),
                 F.col("_metadata.file_size").alias("file_size")))

bronze = (gt.enriquecer_bronze(fluxo, "file_path", "file_size")
          .filter(F.col("fonte").isin(gt.FONTES))                    # ignora manifestos e arquivos fora do padrão
          .filter(~F.col("arquivo_origem").rlike(gt.PASTA_INTERNA_RE)))  # ignora _descartados, _manifestos

(bronze.writeStream
 .foreachBatch(gt.gravador_sem_duplicar(TABELA))
 .option("checkpointLocation", CHECKPOINT)
 .trigger(availableNow=True)
 .start()
 .awaitTermination())

# COMMAND ----------

depois_total, depois_fonte = contar()
novos = depois_total - antes_total
(spark.createDataFrame([(str(uuid.uuid4()), ROTULO, antes_total, depois_total, novos,
                         str(antes_fonte), str(depois_fonte))],
                       "execucao_id string, rotulo string, linhas_antes long, linhas_depois long, "
                       "arquivos_novos long, antes_por_fonte string, depois_por_fonte string")
 .withColumn("executado_em", F.current_timestamp())
 .write.mode("append").saveAsTable(f"{S}controle_execucoes_bronze"))
print(f"Depois: {depois_total} arquivos (novos nesta execução: {novos}) -> {depois_fonte}")

# COMMAND ----------

# MAGIC %md ### Evidências para o relatório (print das duas células abaixo)

# COMMAND ----------

display(spark.sql(f"""
  SELECT rotulo, linhas_antes, linhas_depois, arquivos_novos, executado_em
  FROM {S}controle_execucoes_bronze ORDER BY executado_em"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT fonte, data_coleta, nome_arquivo, tamanho_bytes, carregado_em
  FROM {TABELA} ORDER BY carregado_em, fonte"""))

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"rotulo": ROTULO, "linhas_antes": antes_total, "linhas_depois": depois_total,
                                  "arquivos_novos": novos, "por_fonte": depois_fonte}))
