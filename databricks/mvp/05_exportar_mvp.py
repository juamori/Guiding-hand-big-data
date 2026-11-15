# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Exportar a tabela do MVP
# MAGIC **Requisito 08.** O MVP consome `workspace.guiding_hand.tb_mvp_mapa_barreiras`.
# MAGIC Como o Databricks Free Edition não é um backend público, a integração é feita por
# MAGIC **arquivo estruturado** (contrato em `docs/contrato_de_dados.md`): este notebook grava
# MAGIC `barreiras_mvp.json` no Volume `exportacao`. Baixe o arquivo pela interface e coloque em
# MAGIC `mvp_demo/data/` (e entregue ao time do app).

# COMMAND ----------

import json, datetime

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema", "guiding_hand")
CAT, SCH = dbutils.widgets.get("catalogo"), dbutils.widgets.get("schema")
destino = f"/Volumes/{CAT}/{SCH}/exportacao/barreiras_mvp.json"

linhas = [r.asDict() for r in spark.table(f"{CAT}.{SCH}.tb_mvp_mapa_barreiras").collect()]
pacote = {
    "contrato": "guiding-hand/tb_mvp_mapa_barreiras/v1",
    "gerado_em": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "tabela_origem": f"{CAT}.{SCH}.tb_mvp_mapa_barreiras",
    "aviso": "Barreiras de origem 'oculos_sintetico' vêm de dados SINTÉTICOS do protótipo.",
    "barreiras": linhas,
}
with open(destino, "w", encoding="utf-8") as fh:
    json.dump(pacote, fh, ensure_ascii=False, default=str, indent=1)
print(f"{len(linhas)} barreiras exportadas para {destino}")
