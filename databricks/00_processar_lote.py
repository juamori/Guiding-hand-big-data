# Databricks notebook source
# MAGIC %md
# MAGIC # ▶ Processar um lote (um clique)
# MAGIC Depois de enviar os arquivos do lote ao Volume `chegada`, escolha o número do lote no campo
# MAGIC acima e clique em **Run all**. Este notebook executa, em ordem:
# MAGIC
# MAGIC 1. Bronze (Auto Loader) — 1ª execução
# MAGIC 2. Bronze — **reexecução** (prova de que não duplica: deve trazer 0 arquivos novos)
# MAGIC 3. Prata — MERGE e regras de qualidade
# MAGIC 4. Prata — **reexecução** (prova de idempotência: 0 inseridas, 0 atualizadas)
# MAGIC 5. Ouro — modelo dimensional e tabela do MVP
# MAGIC 6. Exportação do JSON do MVP e de **todas as evidências** do relatório
# MAGIC
# MAGIC Ao final, baixe do Volume `exportacao` os arquivos `evidencias_lote_<N>.json` e `barreiras_mvp.json`.

# COMMAND ----------

import json
dbutils.widgets.dropdown("lote", "1", ["1", "2", "3"])
dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema", "guiding_hand")
LOTE = dbutils.widgets.get("lote")
BASE = {"catalogo": dbutils.widgets.get("catalogo"), "schema": dbutils.widgets.get("schema")}
TEMPO = 1800
etapas = {}

def rodar(nome, caminho, extra=None):
    print(f"▶ {nome} ...")
    saida = dbutils.notebook.run(caminho, TEMPO, {**BASE, **(extra or {})})
    etapas[nome] = json.loads(saida) if saida else {}
    print(f"  ✔ {nome}: {etapas[nome]}")

rodar("bronze_1", "./bronze/01_bronze_autoloader", {"rotulo_execucao": f"lote {LOTE} - 1a execucao"})
rodar("bronze_reexecucao", "./bronze/01_bronze_autoloader", {"rotulo_execucao": f"lote {LOTE} - reexecucao"})
rodar("prata_1", "./prata/02_prata")
rodar("prata_reexecucao", "./prata/02_prata")
rodar("ouro", "./ouro/03_ouro")
rodar("exportar_mvp", "./mvp/05_exportar_mvp")
rodar("evidencias", "./mvp/06_exportar_evidencias", {"lote": LOTE, "etapas_json": json.dumps(etapas)})

# COMMAND ----------

# MAGIC %md ## Verificações automáticas deste lote

# COMMAND ----------

problemas = []
if etapas["bronze_reexecucao"].get("arquivos_novos") != 0:
    problemas.append("A reexecução da Bronze trouxe arquivos novos (deveria ser 0).")
if etapas["bronze_1"].get("arquivos_novos", 0) == 0:
    problemas.append("A 1ª execução não encontrou arquivos novos: o lote foi enviado ao Volume 'chegada'?")
for m in etapas["prata_reexecucao"].get("merges", []):
    if m["inseridas"] or m["atualizadas"]:
        problemas.append(f"Reexecução da Prata alterou {m['tabela']} (deveria ser 0/0).")
print("TUDO CERTO ✔" if not problemas else "ATENÇÃO:\n- " + "\n- ".join(problemas))

# COMMAND ----------

# MAGIC %md ## Evidências para o relatório
# MAGIC Print 07 = célula abaixo · Print 08 = célula seguinte · Print 09 = última célula.

# COMMAND ----------

S = f"{BASE['catalogo']}.{BASE['schema']}."
display(spark.sql(f"SELECT rotulo, linhas_antes, linhas_depois, arquivos_novos, executado_em "
                  f"FROM {S}controle_execucoes_bronze ORDER BY executado_em"))

# COMMAND ----------

display(spark.sql(f"SELECT regra, fonte, count(*) AS registros_barrados FROM {S}prata_quarentena "
                  f"GROUP BY regra, fonte ORDER BY regra, fonte"))

# COMMAND ----------

tabelas = ["fato_ocorrencia_barreira", "dim_tempo", "dim_local", "dim_tipo_obstaculo", "dim_origem",
           "dim_barreira", "tb_mvp_mapa_barreiras"]
display(spark.createDataFrame([(t, spark.table(S + t).count()) for t in tabelas], "tabela string, linhas long"))
