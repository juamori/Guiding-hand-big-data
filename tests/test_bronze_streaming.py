"""Ingestão incremental da Bronze com Spark Structured Streaming REAL (fonte de arquivos +
checkpoint + trigger availableNow), simulando lote 1, reexecução, lote 2 e reexecução.

O Auto Loader (cloudFiles) é exclusivo do Databricks; aqui usamos a fonte de arquivos padrão do
Spark, que tem a mesma semântica de checkpoint (cada arquivo processado uma única vez). Valida a
lógica e as colunas da Bronze; a execução com cloudFiles é feita no notebook 01.
"""
import shutil

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java não encontrado", allow_module_level=True)

import gh_transform as gt  # noqa: E402
import pipeline_local as pl  # noqa: E402
from conftest import FIXTURES  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def _executar(spark, origem, destino, checkpoint):
    fluxo = (spark.readStream.format("text").option("wholetext", "true")
             .option("pathGlobFilter", "*.json").load(str(origem))
             .select("value", F.col("_metadata.file_path").alias("file_path")))
    bronze = gt.enriquecer_bronze(fluxo, "file_path").drop("file_path").filter(F.col("fonte").isin(gt.FONTES))
    (bronze.writeStream.format("parquet").option("checkpointLocation", str(checkpoint))
     .trigger(availableNow=True).start(str(destino)).awaitTermination())
    return spark.read.parquet(str(destino))


def test_lotes_e_reexecucoes_sem_duplicar(tmp_path):
    spark = pl.criar_spark()
    volume, destino, ck = tmp_path / "volume_chegada", tmp_path / "bronze", tmp_path / "checkpoint"
    volume.mkdir()

    def enviar_lote(data):  # upload "achatado", como na interface do Databricks
        for p in FIXTURES.rglob(f"*_{data}.json"):
            shutil.copy(p, volume / p.name)
    (volume / "manifesto_2026-11-12.json").write_text("{}")  # deve ser ignorado

    contagens = []
    enviar_lote("2026-11-12")
    contagens.append(_executar(spark, volume, destino, ck).count())   # lote 1
    contagens.append(_executar(spark, volume, destino, ck).count())   # reexecução
    enviar_lote("2026-11-13")
    contagens.append(_executar(spark, volume, destino, ck).count())   # lote 2
    df = _executar(spark, volume, destino, ck)                         # reexecução
    contagens.append(df.count())

    assert contagens == [5, 5, 10, 10], contagens
    assert df.groupBy("nome_arquivo").count().filter("count > 1").count() == 0
    assert {r["fonte"] for r in df.select("fonte").distinct().collect()} == set(gt.FONTES)
    assert df.filter("hash_conteudo IS NULL OR data_coleta IS NULL").count() == 0
