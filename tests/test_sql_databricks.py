"""Valida a SINTAXE de todo SQL executado pelos notebooks do Databricks com o parser do Spark.
Só faz parsing (sem executar): MERGE/Delta e Volumes não existem localmente."""
import re
import shutil
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java não encontrado", allow_module_level=True)

import gh_transform as gt  # noqa: E402
import ouro_sql  # noqa: E402
import pipeline_local as pl  # noqa: E402

RAIZ = Path(__file__).resolve().parents[1]
S = "workspace.guiding_hand."
APENAS_DATABRICKS = re.compile(r"^\s*(CREATE\s+VOLUME|DESCRIBE\s+HISTORY)", re.I)  # Unity Catalog / Delta


def _parse(spark, sql):
    spark._jsparkSession.sessionState().sqlParser().parsePlan(sql)


def _statements(texto):
    sem_comentarios = "\n".join(l for l in texto.splitlines() if not l.strip().startswith("--"))
    return [s.strip() for s in sem_comentarios.split(";") if s.strip()]


@pytest.fixture(scope="module")
def spark():
    return pl.criar_spark()


def test_merges_da_prata(spark):
    for tabela, chaves in [("prata_eventos_oculos", ["event_id"]), ("prata_clima_horario", ["data_hora"]),
                           ("prata_sp156_solicitacoes", ["protocolo"])]:
        _parse(spark, gt.sql_merge(S + tabela, chaves))
    _parse(spark, gt.sql_merge(S + "prata_quarentena", ["fonte", "chave_registro", "regra"], so_inserir=True))


def test_ctas_da_ouro(spark):
    for nome, consulta in ouro_sql.TABELAS_OURO:
        _parse(spark, f"CREATE OR REPLACE TABLE {S}{nome} AS {ouro_sql.sql(consulta, S)}")


def test_comandos_auxiliares_dos_notebooks(spark):
    # DESCRIBE HISTORY é sintaxe Delta (não existe no parser do Spark aberto): não validável aqui
    _parse(spark, f"COMMENT ON TABLE {S}tb_mvp_mapa_barreiras IS 'teste'")
    _parse(spark, f"ALTER TABLE {S}fato_ocorrencia_barreira ALTER COLUMN device_hash COMMENT 'x'")


@pytest.mark.parametrize("arquivo", ["databricks/00_setup.sql", "databricks/sql/contagens_bronze.sql",
                                     "databricks/sql/consultas_mvp.sql"])
def test_arquivos_sql(spark, arquivo):
    for st in _statements((RAIZ / arquivo).read_text(encoding="utf-8")):
        if not APENAS_DATABRICKS.match(st):
            _parse(spark, st)


def test_notebooks_nao_usam_api_ausente_no_serverless():
    """Serverless (Spark Connect) não tem sparkContext, RDD nem cache/persist."""
    proibidos = re.compile(r"sparkContext|\.rdd\b|\.cache\(\)|\.persist\(|sqlContext")
    for nb in (RAIZ / "databricks").rglob("*.py"):
        if nb.parent.name == "lib":
            continue
        achados = proibidos.findall(nb.read_text(encoding="utf-8"))
        assert not achados, f"{nb.name}: {achados}"
