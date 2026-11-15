"""Executa, célula a célula, os notebooks databricks/bronze/bronze_lote_1.py e 01_bronze_autoloader.py
num Spark local.

Substituições (só o que é exclusivo do Databricks): cloudFiles -> fonte de arquivos 'text' do Spark com
recursiveFileLookup (o Auto Loader já lista subpastas; mesma semântica de checkpoint), Delta -> Parquet,
Unity Catalog -> spark_catalog, dbutils/display falsos. Nada aqui substitui a execução no Databricks.
"""
import json
import re
import shutil
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java não encontrado", allow_module_level=True)

import pipeline_local as pl  # noqa: E402
from conftest import FIXTURES  # noqa: E402

BRONZE = Path(__file__).resolve().parents[1] / "databricks" / "bronze"
LOTE_1 = BRONZE / "bronze_lote_1.py"
NB_01 = BRONZE / "01_bronze_autoloader.py"
FONTES_LOTE_1 = ("clima", "osm", "eventos_oculos", "sp156")   # lote 1 real: sem olhovivo


class _Saida(Exception):
    pass


class _DBUtils:
    def __init__(self, valores):
        v = self.v = valores
        dbu = self

        class W:
            def text(self, nome, padrao):
                v.setdefault(nome, padrao)

            def dropdown(self, nome, padrao, opcoes):
                v.setdefault(nome, padrao)

            def get(self, nome):
                return v[nome]

        class N:
            def exit(self, valor):
                dbu.saida = json.loads(valor)
                raise _Saida()

        self.widgets, self.notebook = W(), N()   # sem .fs: o notebook usa a alternativa shutil


def _celulas(notebook: Path, raiz: Path):
    fonte = notebook.read_text(encoding="utf-8")
    fonte = (fonte.replace('.option("cloudFiles.format", "text")\n', "")
                  .replace('format("cloudFiles")', 'format("text").option("recursiveFileLookup", "true")')
                  .replace("USING DELTA", "USING PARQUET"))
    blocos = []
    for b in fonte.split("# COMMAND ----------"):
        linhas = [l for l in b.strip().splitlines() if not l.startswith("# Databricks notebook")]
        if linhas and not all(l.startswith("# MAGIC") for l in linhas):
            blocos.append("\n".join(linhas))
    controle = raiz / "controle"
    # Depois da célula de parâmetros, aponta os caminhos do Volume para a pasta temporária
    blocos[0] += (f'\nORIGEM = r"{raiz / "chegada"}"'
                  f'\nCHECKPOINT = CHECKPOINT.replace("/Volumes/spark_catalog/gh/controle", r"{controle}")'
                  f'\nCHECKPOINT_01 = r"{controle / "checkpoint_bronze_arquivos"}"\n')
    # No Spark local não há CREATE VOLUME nem catálogo de 3 níveis
    blocos[1] = re.sub(r"spark\.sql\(f\"CREATE SCHEMA.*?\n", "", blocos[1])
    blocos[1] = re.sub(r"for vol in .*?\n    spark\.sql\(.*?\n", "", blocos[1])
    return blocos


def _rodar(spark, notebook, raiz, **widgets):
    saidas = []
    dbu = _DBUtils({"catalogo": "spark_catalog", "schema": "gh", **widgets})
    ns = {"spark": spark, "dbutils": dbu, "display": lambda df: saidas.append(df.collect())}
    try:
        for bloco in _celulas(notebook, raiz):
            exec(compile(bloco, str(notebook), "exec"), ns)
    except _Saida:
        pass
    ns["_saidas"] = saidas
    return ns


@pytest.fixture()
def ambiente(tmp_path):
    spark = pl.criar_spark()
    spark.sql("DROP DATABASE IF EXISTS gh CASCADE")
    armazem = spark.conf.get("spark.sql.warehouse.dir").removeprefix("file:")
    shutil.rmtree(Path(armazem) / "gh.db", ignore_errors=True)
    spark.sql("CREATE DATABASE gh")
    lote = tmp_path / "chegada" / "lote_1"            # upload extraído numa subpasta
    lote.mkdir(parents=True)
    for fonte in FONTES_LOTE_1:
        shutil.copy(FIXTURES / fonte / f"{fonte}_2026-11-12.json", lote)
    return spark, tmp_path


def _tabela(spark, nome):
    spark.catalog.refreshTable(f"spark_catalog.gh.{nome}")
    return spark.table(f"spark_catalog.gh.{nome}")


def test_lote_1_execucao_reexecucao_e_arquivos_ignorados(ambiente):
    spark, raiz = ambiente
    chegada = raiz / "chegada"
    (chegada / "lote_1" / "sp156_2026-11-12 (1).json").write_text("{}")      # cópia renomeada pelo navegador
    (chegada / "_descartados").mkdir()
    shutil.copy(FIXTURES / "osm" / "osm_2026-11-12.json", chegada / "_descartados")
    (chegada / "reenvio").mkdir()                                              # mesmo arquivo em outra pasta
    shutil.copy(FIXTURES / "clima" / "clima_2026-11-12.json", chegada / "reenvio")

    ns = _rodar(spark, LOTE_1, raiz)
    assert ns["ROTULO"] == "lote 1 - 1a execucao"
    assert (ns["antes_total"], ns["depois_total"], ns["novos"]) == (0, 4, 4)
    assert set(ns["depois_fonte"]) == set(FONTES_LOTE_1)
    situacoes = {r[1]: r[5] for r in ns["inventario"]}
    assert situacoes["sp156_2026-11-12 (1).json"].startswith("ignorado")
    assert ns["resumo"] == {"OK: idêntico (hash e bytes)": 4, "OK: mesmo conteúdo já gravado de outro caminho": 1}
    assert ns["_saidas"][2] == [] and ns["_saidas"][3] == []   # displays: inventário, integridade, duplicidade x2
    assert ns["CHECKPOINT"].endswith("checkpoint_bronze_lote_1")

    ns = _rodar(spark, LOTE_1, raiz)
    assert ns["ROTULO"] == "lote 1 - reexecucao" and not ns["CHECKPOINT_REINICIADO"]
    assert (ns["antes_total"], ns["depois_total"], ns["novos"]) == (4, 4, 0)
    controle = _tabela(spark, "controle_execucoes_bronze").orderBy("executado_em").collect()
    assert [(r["rotulo"], r["arquivos_novos"]) for r in controle] == [
        ("lote 1 - 1a execucao", 4), ("lote 1 - reexecucao", 0)]
    bronze = _tabela(spark, "bronze_arquivos")
    assert bronze.filter("hash_conteudo IS NULL OR data_coleta IS NULL OR carregado_em IS NULL").count() == 0
    assert bronze.groupBy("nome_arquivo").count().filter("count > 1").count() == 0


def test_tamanho_bytes_e_o_tamanho_real_do_arquivo(ambiente):
    spark, raiz = ambiente
    _rodar(spark, LOTE_1, raiz)
    linhas = {r["nome_arquivo"]: r for r in _tabela(spark, "bronze_arquivos")
              .selectExpr("nome_arquivo", "tamanho_bytes", "length(conteudo_bruto) AS caracteres").collect()}
    for nome, r in linhas.items():
        assert r["tamanho_bytes"] == (raiz / "chegada" / "lote_1" / nome).stat().st_size, nome
    sp156 = linhas["sp156_2026-11-12.json"]
    assert sp156["tamanho_bytes"] > sp156["caracteres"]   # acentos: 2 bytes cada; length() contaria menos
    assert dict(_tabela(spark, "bronze_arquivos").dtypes)["tamanho_bytes"] == "bigint"


def test_checkpoint_do_01_original_nao_impede_o_lote_1(ambiente):
    """Simula o 01 original já executado (checkpoint_bronze_arquivos com os 4 arquivos registrados)."""
    spark, raiz = ambiente
    ck_01 = raiz / "controle" / "checkpoint_bronze_arquivos"
    (spark.readStream.format("text").option("recursiveFileLookup", "true").option("wholetext", "true")
     .load(str(raiz / "chegada")).writeStream.format("parquet")
     .option("checkpointLocation", str(ck_01)).trigger(availableNow=True)
     .start(str(raiz / "saida_01_original")).awaitTermination())

    ns = _rodar(spark, LOTE_1, raiz)
    assert ns["ck_01"]["existe"] and ns["ck_01"]["lotes_concluidos"] >= 1
    assert ns["novos"] == 4


def test_checkpoint_proprio_inconsistente_e_reiniciado(ambiente):
    """Checkpoint já registrou os arquivos, mas a tabela foi recriada vazia."""
    spark, raiz = ambiente
    _rodar(spark, LOTE_1, raiz)
    spark.sql("DROP TABLE spark_catalog.gh.bronze_arquivos")
    shutil.rmtree(Path(spark.conf.get("spark.sql.warehouse.dir").removeprefix("file:")) / "gh.db" / "bronze_arquivos",
                  ignore_errors=True)

    with pytest.raises(Exception, match="NÃO ESTÁ NA BRONZE"):      # sem reiniciar: 0 lidos, e o notebook avisa
        _rodar(spark, LOTE_1, raiz, reiniciar_checkpoint="nao")

    ns = _rodar(spark, LOTE_1, raiz)                                # auto: detecta e reinicia
    assert ns["CHECKPOINT_REINICIADO"] and ns["novos"] == 4
    ns = _rodar(spark, LOTE_1, raiz)                                # e a reexecução volta a dar 0
    assert not ns["CHECKPOINT_REINICIADO"] and ns["novos"] == 0


def test_continuidade_lote_2_pelo_notebook_01(ambiente):
    spark, raiz = ambiente
    _rodar(spark, LOTE_1, raiz)
    _rodar(spark, LOTE_1, raiz)
    lote2 = raiz / "chegada" / "lote_2"
    lote2.mkdir()
    for fonte in FONTES_LOTE_1:
        shutil.copy(FIXTURES / fonte / f"{fonte}_2026-11-13.json", lote2)

    r1 = _rodar(spark, NB_01, raiz, rotulo_execucao="lote 2 - 1a execucao")["dbutils"].saida
    r2 = _rodar(spark, NB_01, raiz, rotulo_execucao="lote 2 - reexecucao")["dbutils"].saida
    assert (r1["linhas_antes"], r1["linhas_depois"], r1["arquivos_novos"]) == (4, 8, 4)   # lote 1 não duplica
    assert r2["arquivos_novos"] == 0
    bronze = _tabela(spark, "bronze_arquivos")
    assert bronze.groupBy("nome_arquivo").count().filter("count > 1").count() == 0
    assert sorted(r[0] for r in bronze.select("data_coleta").distinct().collect()).__len__() == 2
