# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Exportar evidências do relatório
# MAGIC Junta, a partir das tabelas, tudo o que o relatório pede: execuções da Bronze, arquivos por
# MAGIC fonte e dia, barrados por regra, contagens das camadas e o resultado das consultas C1–C6.
# MAGIC Grava `evidencias_lote_<N>.json` no Volume `exportacao` (baixe e coloque em `docs/evidencias/`).

# COMMAND ----------

import os, sys, json
sys.path.append(os.path.abspath("../lib"))
import ouro_sql

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema", "guiding_hand")
dbutils.widgets.text("lote", "1")
dbutils.widgets.text("etapas_json", "{}")
CAT, SCH = dbutils.widgets.get("catalogo"), dbutils.widgets.get("schema")
LOTE = dbutils.widgets.get("lote")
S = f"{CAT}.{SCH}."
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

consultas = ouro_sql.carregar_consultas(open(os.path.abspath("../sql/consultas_mvp.sql"), encoding="utf-8").read())
ev = ouro_sql.montar_evidencias(spark, S, consultas, LOTE, "databricks", json.loads(dbutils.widgets.get("etapas_json")))
destino = f"/Volumes/{CAT}/{SCH}/exportacao/evidencias_lote_{LOTE}.json"
with open(destino, "w", encoding="utf-8") as fh:
    json.dump(ev, fh, ensure_ascii=False, indent=1, default=str)
print(f"Evidências gravadas em {destino}")
print(json.dumps({k: ev[k] for k in ("contagens", "quarentena_por_regra")}, ensure_ascii=False, indent=1))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"arquivo": destino, "consultas": len(ev["consultas"])}))
