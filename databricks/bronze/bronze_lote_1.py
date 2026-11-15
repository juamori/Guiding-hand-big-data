# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Lote 1 (Auto Loader)
# MAGIC Ingestão dos arquivos brutos do lote 1, que já estão no Volume `chegada`, na tabela
# MAGIC `bronze_arquivos`. É a mesma estratégia do notebook `01_bronze_autoloader` (Auto Loader + checkpoint),
# MAGIC com a mesma tabela e a mesma tabela de controle, em versão autônoma: não depende da pasta `lib/` do repositório.
# MAGIC
# MAGIC **Checkpoint exclusivo** (`controle/checkpoint_bronze_lote_1`): o histórico de outro notebook nunca impede a leitura
# MAGIC dos arquivos deste lote. A continuidade com o lote 2 é garantida pela regra "mesmo nome + mesmo SHA-256 não é
# MAGIC gravado de novo", aplicada aqui e no `01_bronze_autoloader`.
# MAGIC
# MAGIC * **Dados brutos preservados**: cada arquivo vira uma linha com o texto original completo (`conteudo_bruto`).
# MAGIC * **Origem e data de ingestão**: `arquivo_origem`, `nome_arquivo`, `fonte`, `data_coleta`, `carregado_em`, `hash_conteudo`.
# MAGIC * **Tamanho real**: `tamanho_bytes` = tamanho do arquivo em bytes (`_metadata.file_size`), conferido com os bytes UTF-8 do conteúdo.
# MAGIC * **Sem duplicação**: o checkpoint do Auto Loader não relê arquivos já processados. Além disso, um arquivo com o
# MAGIC   mesmo nome e o mesmo conteúdo já presente na tabela não é gravado de novo (por exemplo, se for reenviado em outra pasta).
# MAGIC * **Evidências**: contagens antes e depois, registro em `controle_execucoes_bronze` e conferência de integridade
# MAGIC   (o notebook para com erro se algum arquivo válido do Volume não estiver na Bronze).
# MAGIC
# MAGIC **Como usar:** *Run all*. Depois, *Run all* de novo: a 2ª execução é registrada como reexecução e deve trazer **0 arquivos novos**.

# COMMAND ----------

# Célula 1: parâmetros
import os, re, json, uuid, hashlib
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema", "guiding_hand")
dbutils.widgets.text("lote", "1")
dbutils.widgets.text("rotulo_execucao", "auto")   # "auto" = 1a execução na primeira vez, reexecução nas seguintes
dbutils.widgets.dropdown("reiniciar_checkpoint", "auto", ["auto", "nao", "sim"])

CAT, SCH = dbutils.widgets.get("catalogo"), dbutils.widgets.get("schema")
LOTE = dbutils.widgets.get("lote").strip()
S = f"{CAT}.{SCH}."
ORIGEM = f"/Volumes/{CAT}/{SCH}/chegada"
CHECKPOINT = f"/Volumes/{CAT}/{SCH}/controle/checkpoint_bronze_lote_{LOTE}"       # exclusivo deste notebook
CHECKPOINT_01 = f"/Volumes/{CAT}/{SCH}/controle/checkpoint_bronze_arquivos"        # do 01 original (só diagnóstico)
TABELA = f"{S}bronze_arquivos"
CONTROLE = f"{S}controle_execucoes_bronze"

# Mesmo padrão de nome e mesmas fontes de databricks/lib/gh_transform.py
PADRAO_ARQUIVO = r"([a-z0-9_]+)_(\d{4}-\d{2}-\d{2})\.json$"
PASTA_INTERNA_RE = r"/chegada/(.*/)?[_.][^/]*/"
FONTES = ["clima", "osm", "eventos_oculos", "sp156", "olhovivo"]
FONTES_ESPERADAS_LOTE = ["clima", "osm", "eventos_oculos", "sp156"]   # Olho Vivo ausente (sem token)

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
print("Origem:", ORIGEM, "| Tabela:", TABELA, "| Checkpoint:", CHECKPOINT)

# COMMAND ----------

# Célula 2: estrutura (idempotente; não apaga nada que já exista)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")
for vol in ("chegada", "controle", "exportacao"):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.{SCH}.{vol}")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {TABELA} (
    conteudo_bruto STRING    COMMENT 'Texto original e completo do arquivo JSON (sem alteração)',
    arquivo_origem STRING    COMMENT 'Caminho do arquivo no Volume chegada',
    nome_arquivo   STRING    COMMENT 'Nome do arquivo (<fonte>_<data>.json)',
    fonte          STRING    COMMENT 'Fonte extraída do nome do arquivo',
    data_coleta    DATE      COMMENT 'Data da coleta extraída do nome do arquivo',
    tamanho_bytes  BIGINT    COMMENT 'Tamanho real do arquivo em bytes',
    hash_conteudo  STRING    COMMENT 'SHA-256 do conteúdo bruto',
    carregado_em   TIMESTAMP COMMENT 'Data e hora da ingestão na Bronze'
  ) USING DELTA
  COMMENT 'Bronze Guiding Hand: um registro por arquivo recebido, conteúdo bruto preservado'
""")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {CONTROLE} (
    execucao_id STRING, rotulo STRING, linhas_antes BIGINT, linhas_depois BIGINT,
    arquivos_novos BIGINT, antes_por_fonte STRING, depois_por_fonte STRING, executado_em TIMESTAMP
  ) USING DELTA
  COMMENT 'Contagens de cada execução da Bronze (evidência de ingestão incremental)'
""")
print("Estrutura pronta.")

# COMMAND ----------

# Célula 3: inventário do Volume chegada (o que será lido e o que será ignorado)
def normalizar(caminho):
    return re.sub(r"^(dbfs:|file:)", "", caminho)

def caminho_ignorado(caminho):
    """Mesma regra do Spark: pastas/arquivos iniciados por '_' ou '.' (ex.: _descartados, _manifestos)."""
    partes = normalizar(caminho)[len(ORIGEM):].strip("/").split("/")
    return any(p.startswith(("_", ".")) for p in partes)

inventario = []
for raiz, pastas, arquivos in os.walk(ORIGEM):
    for nome in sorted(arquivos):
        caminho = os.path.join(raiz, nome)
        m = re.search(PADRAO_ARQUIVO, nome)
        if not nome.endswith(".json"):
            situacao = "ignorado: não é .json"
        elif caminho_ignorado(caminho):
            situacao = "ignorado: pasta interna (_ ou .)"
        elif not m or re.fullmatch(PADRAO_ARQUIVO, nome) is None:
            situacao = "ignorado: nome fora do padrão <fonte>_<AAAA-MM-DD>.json"
        elif m.group(1) not in FONTES:
            situacao = "ignorado: fonte desconhecida"
        else:
            situacao = "será lido"
        sha = None
        if situacao == "será lido":
            with open(caminho, "rb") as fh:
                sha = hashlib.sha256(fh.read()).hexdigest()
        inventario.append((normalizar(caminho), nome, m.group(1) if m else None, m.group(2) if m else None,
                           os.path.getsize(caminho), situacao, sha))

df_inv = spark.createDataFrame(inventario, "caminho string, nome_arquivo string, fonte string, data_coleta string, "
                                           "bytes long, situacao string, sha256_arquivo string")
display(df_inv.orderBy("situacao", "nome_arquivo"))

validos = [r for r in inventario if r[5] == "será lido"]
fontes_ok = sorted({r[2] for r in validos})
datas_ok = sorted({r[3] for r in validos})
print(f"Arquivos válidos: {len(validos)} | fontes: {fontes_ok} | datas de coleta: {datas_ok}")
faltando = sorted(set(FONTES_ESPERADAS_LOTE) - set(fontes_ok))
if faltando:
    print("ATENÇÃO: fontes esperadas não encontradas no Volume:", faltando)
repetidos = {}
for r in validos:
    repetidos.setdefault(r[1], []).append(r[0])
repetidos = {k: v for k, v in repetidos.items() if len(v) > 1}
if repetidos:
    print("ATENÇÃO: o mesmo nome de arquivo aparece em mais de uma pasta "
          "(conteúdo idêntico é gravado uma vez só; conteúdo diferente gera dois registros):", repetidos)
if not validos:
    raise Exception(f"Nenhum arquivo válido em {ORIGEM}. Confira se os .json do lote foram enviados/extraídos nesse Volume.")

# COMMAND ----------

# Célula 4: checkpoint — histórico e consistência com a tabela
def estado_checkpoint(caminho):
    """Lotes já registrados no checkpoint (arquivos em offsets/ e commits/)."""
    def n(sub):
        pasta = os.path.join(caminho, sub)
        return len([a for a in os.listdir(pasta) if a.isdigit()]) if os.path.isdir(pasta) else 0
    return {"existe": os.path.isdir(caminho), "lotes_iniciados": n("offsets"), "lotes_concluidos": n("commits")}

ck_01, ck_proprio = estado_checkpoint(CHECKPOINT_01), estado_checkpoint(CHECKPOINT)
print("Checkpoint do 01 original (não usado aqui):", CHECKPOINT_01, ck_01)
print("Checkpoint deste notebook:", CHECKPOINT, ck_proprio)

spark.catalog.refreshTable(TABELA)
na_tabela = {(r["nome_arquivo"], r["hash_conteudo"])
             for r in spark.table(TABELA).select("nome_arquivo", "hash_conteudo").collect()}
pendentes = [r for r in validos if (r[1], r[6]) not in na_tabela]
print(f"Arquivos válidos ainda fora da Bronze: {len(pendentes)} {[r[1] for r in pendentes]}")

MODO = dbutils.widgets.get("reiniciar_checkpoint")
# Inconsistência: o checkpoint já registrou lotes, mas há arquivos do Volume fora da tabela (ex.: tabela recriada,
# execução anterior interrompida). Sem reiniciar, o Auto Loader não releria esses arquivos. Reiniciar é seguro:
# a gravação ignora o que já está na tabela (mesmo nome + mesmo SHA-256).
reiniciar = MODO == "sim" or (MODO == "auto" and ck_proprio["lotes_iniciados"] > 0 and len(pendentes) > 0)
if reiniciar:
    try:
        dbutils.fs.rm(CHECKPOINT, True)
    except Exception:
        import shutil
        shutil.rmtree(CHECKPOINT, ignore_errors=True)
    print("CHECKPOINT REINICIADO:", CHECKPOINT, "-> os arquivos do Volume serão relidos; os já gravados não se repetem.")
CHECKPOINT_REINICIADO = reiniciar

# COMMAND ----------

# Célula 5: contagem ANTES + rótulo da execução
def contar():
    spark.catalog.refreshTable(TABELA)   # garante a leitura da versão mais recente da tabela
    df = spark.table(TABELA)
    return df.count(), {r["fonte"]: r["count"] for r in df.groupBy("fonte").count().collect()}

antes_total, antes_fonte = contar()

ROTULO = dbutils.widgets.get("rotulo_execucao").strip()
if ROTULO.lower() == "auto":
    ja_executado = spark.table(CONTROLE).filter(F.col("rotulo") == f"lote {LOTE} - 1a execucao").count() > 0
    ROTULO = f"lote {LOTE} - reexecucao" if ja_executado else f"lote {LOTE} - 1a execucao"

print(f"Execução: {ROTULO}" + (" (checkpoint reiniciado na célula 4)" if CHECKPOINT_REINICIADO else ""))
print(f"ANTES: {antes_total} arquivos na Bronze -> {antes_fonte}")

# COMMAND ----------

# Célula 6: Auto Loader (incremental, checkpoint exclusivo no Volume controle)
def gravar_lote(lote_df, lote_id):
    """Grava só o que ainda não está na tabela (mesmo nome + mesmo conteúdo = mesmo arquivo)."""
    alvo = lote_df.sparkSession.table(TABELA)
    tipo = dict(alvo.dtypes).get("tamanho_bytes", "bigint")   # respeita a tabela se ela já existir com INT
    novos = (lote_df.dropDuplicates(["nome_arquivo", "hash_conteudo"])
             .join(alvo.select("nome_arquivo", "hash_conteudo"), ["nome_arquivo", "hash_conteudo"], "left_anti")
             .withColumn("tamanho_bytes", F.col("tamanho_bytes").cast(tipo)))
    novos.write.mode("append").saveAsTable(TABELA)

fluxo = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "text")
         .option("wholetext", "true")             # 1 linha = 1 arquivo inteiro (conteúdo bruto preservado)
         .option("pathGlobFilter", "*.json")
         .load(ORIGEM)
         .select("value", F.col("_metadata.file_path").alias("caminho"),
                 F.col("_metadata.file_size").alias("tamanho_arquivo")))

caminho = F.regexp_replace("caminho", r"^(dbfs:|file:)", "")
bronze = (fluxo
          .withColumnRenamed("value", "conteudo_bruto")
          .withColumn("arquivo_origem", caminho)
          .withColumn("nome_arquivo", F.regexp_extract(caminho, r"([^/]+)$", 1))
          .withColumn("fonte", F.regexp_extract("nome_arquivo", "^" + PADRAO_ARQUIVO, 1))
          .withColumn("data_coleta", F.to_date(F.regexp_extract("nome_arquivo", "^" + PADRAO_ARQUIVO, 2)))
          .withColumn("tamanho_bytes", F.col("tamanho_arquivo").cast("long"))   # bytes reais do arquivo
          .withColumn("hash_conteudo", F.sha2("conteudo_bruto", 256))
          .withColumn("carregado_em", F.current_timestamp())
          .filter(F.col("fonte").isin(FONTES))                                   # ignora nomes fora do padrão
          .filter(~F.col("arquivo_origem").rlike(PASTA_INTERNA_RE))              # ignora _descartados, _manifestos
          .select("conteudo_bruto", "arquivo_origem", "nome_arquivo", "fonte", "data_coleta",
                  "tamanho_bytes", "hash_conteudo", "carregado_em"))

(bronze.writeStream
 .foreachBatch(gravar_lote)
 .option("checkpointLocation", CHECKPOINT)
 .trigger(availableNow=True)
 .start()
 .awaitTermination())
print("Auto Loader concluído.")

# COMMAND ----------

# Célula 7: contagem DEPOIS + registro na tabela de controle
depois_total, depois_fonte = contar()
novos = depois_total - antes_total
(spark.createDataFrame([(str(uuid.uuid4()), ROTULO, antes_total, depois_total, novos,
                         json.dumps(antes_fonte, sort_keys=True), json.dumps(depois_fonte, sort_keys=True))],
                       "execucao_id string, rotulo string, linhas_antes long, linhas_depois long, "
                       "arquivos_novos long, antes_por_fonte string, depois_por_fonte string")
 .withColumn("executado_em", F.current_timestamp())
 .write.mode("append").saveAsTable(CONTROLE))

print(f"{ROTULO}")
print(f"ANTES : {antes_total} -> {antes_fonte}")
print(f"DEPOIS: {depois_total} -> {depois_fonte}")
print(f"ARQUIVOS NOVOS NESTA EXECUÇÃO: {novos}")
if ROTULO.endswith("reexecucao") and novos != 0:
    print("ATENÇÃO: a reexecução trouxe arquivos novos (esperado 0). Algum arquivo foi enviado entre as execuções?")

# COMMAND ----------

# Célula 8: integridade — cada arquivo válido do Volume está na Bronze, idêntico e com o tamanho real?
df_arq = spark.createDataFrame([(r[0], r[1], r[6], r[4]) for r in validos],
                               "arquivo_origem string, nome_arquivo string, sha256_arquivo string, bytes_arquivo long")

spark.catalog.refreshTable(TABELA)
b = (spark.table(TABELA)
     .withColumn("bytes_conteudo_utf8", F.octet_length("conteudo_bruto"))
     .select("arquivo_origem", "nome_arquivo", "hash_conteudo", "tamanho_bytes", "bytes_conteudo_utf8", "carregado_em"))
mesmo_caminho = b.drop("nome_arquivo")
mesmo_conteudo = (b.select("nome_arquivo", F.col("hash_conteudo").alias("sha256_arquivo"),
                           F.col("arquivo_origem").alias("gravado_a_partir_de"))
                  .dropDuplicates(["nome_arquivo", "sha256_arquivo"]))

integridade = (df_arq.join(mesmo_caminho, "arquivo_origem", "left")
               .join(mesmo_conteudo, ["nome_arquivo", "sha256_arquivo"], "left")
               .withColumn("resultado",
                   F.when(F.col("hash_conteudo").isNull() & F.col("gravado_a_partir_de").isNotNull(),
                          "OK: mesmo conteúdo já gravado de outro caminho")
                    .when(F.col("hash_conteudo").isNull(), "ERRO: NÃO ESTÁ NA BRONZE")
                    .when(F.col("hash_conteudo") != F.col("sha256_arquivo"), "ERRO: conteúdo diferente do arquivo")
                    .when(F.col("tamanho_bytes") != F.col("bytes_arquivo"), "ERRO: tamanho diferente do arquivo")
                    .when(F.col("bytes_conteudo_utf8") != F.col("bytes_arquivo"), "ERRO: texto não preserva os bytes")
                    .otherwise("OK: idêntico (hash e bytes)")))
display(integridade.select("nome_arquivo", "arquivo_origem", "bytes_arquivo", "tamanho_bytes", "bytes_conteudo_utf8",
                           "resultado", "gravado_a_partir_de", "carregado_em").orderBy("nome_arquivo"))
resumo = {r["resultado"]: r["count"] for r in integridade.groupBy("resultado").count().collect()}
print(resumo)
erros = {k: v for k, v in resumo.items() if k.startswith("ERRO")}
if erros:
    raise Exception(f"Integridade da Bronze falhou: {erros}. Veja a tabela acima antes de seguir para a Prata.")

# COMMAND ----------

# Célula 9: duplicidade (todas as consultas devem voltar vazias)
print("Mesmo caminho gravado mais de uma vez:")
display(spark.sql(f"SELECT arquivo_origem, count(*) AS vezes FROM {TABELA} GROUP BY arquivo_origem HAVING count(*) > 1"))
print("Mesmo arquivo (nome + conteúdo) gravado mais de uma vez:")
display(spark.sql(f"SELECT nome_arquivo, hash_conteudo, count(*) AS vezes FROM {TABELA} "
                  f"GROUP BY nome_arquivo, hash_conteudo HAVING count(*) > 1"))

# COMMAND ----------

# MAGIC %md ### Evidências para o relatório (print das células 8, 10 e 11)

# COMMAND ----------

# Célula 10: histórico de execuções (1a execução x reexecução)
display(spark.sql(f"""
  SELECT rotulo, linhas_antes, linhas_depois, arquivos_novos, depois_por_fonte, executado_em
  FROM {CONTROLE} ORDER BY executado_em"""))

# COMMAND ----------

# Célula 11: conteúdo da Bronze (um registro por arquivo, com origem e data de ingestão)
display(spark.sql(f"""
  SELECT fonte, data_coleta, nome_arquivo, arquivo_origem, tamanho_bytes,
         left(hash_conteudo, 16) AS hash_inicio, carregado_em,
         left(conteudo_bruto, 120) AS amostra_conteudo_bruto
  FROM {TABELA} ORDER BY carregado_em, fonte"""))
