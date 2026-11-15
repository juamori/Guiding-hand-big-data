# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Consultas de negócio do MVP
# MAGIC Executa cada bloco de `databricks/sql/consultas_mvp.sql`, mostrando a pergunta, o SQL e o resultado.
# MAGIC Tire um print de cada resultado para a seção 8 do relatório.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../lib"))
import ouro_sql

CONSULTAS = ouro_sql.carregar_consultas(open(os.path.abspath("../sql/consultas_mvp.sql"), encoding="utf-8").read())
print([c[0] for c in CONSULTAS])

# COMMAND ----------

def executar(cid):
    _, pergunta, sql = next(c for c in CONSULTAS if c[0] == cid)
    displayHTML(f"<h3>{cid} — {pergunta}</h3><pre>{sql}</pre>")
    display(spark.sql(sql))

executar("C1")

# COMMAND ----------

executar("C2")

# COMMAND ----------

executar("C3")

# COMMAND ----------

executar("C4")

# COMMAND ----------

executar("C5")

# COMMAND ----------

executar("C6")
