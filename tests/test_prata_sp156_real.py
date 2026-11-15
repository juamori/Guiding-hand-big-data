"""Prata SP156 com os nomes REAIS do GeoSampa, 7 camadas e datas no formato do GeoServer."""
import json
import shutil

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java não encontrado", allow_module_level=True)

import gh_transform as gt  # noqa: E402
import pipeline_local as pl  # noqa: E402
import sp156_mapeamento as mapa  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from test_perfil_sp156 import CAMADAS, _registro  # noqa: E402


def _bronze(spark, por_camada):
    pacote = {"camadas": [{"camada": c, "resposta": {"type": "FeatureCollection", "features": f}}
                          for c, f in por_camada.items()]}
    df = spark.createDataFrame([(json.dumps(pacote), "/x/sp156/sp156_2026-11-15.json")], "value string, file_path string")
    return gt.enriquecer_bronze(df, "file_path").drop("file_path")


@pytest.fixture(scope="module")
def spark():
    return pl.criar_spark()


def _com_mapeamento(monkeypatch, tmp_path, protocolo, composto=False):
    arq = tmp_path / "m.json"
    m = mapa.carregar_mapeamento()
    m["campos"]["protocolo"], m["protocolo_composto_com_camada"] = protocolo, composto
    arq.write_text(json.dumps(m))
    monkeypatch.setattr(gt, "CAMPOS_SP156", mapa.campos_sp156(arq))
    monkeypatch.setattr(gt, "PROTOCOLO_COMPOSTO_COM_CAMADA", composto)


def test_campos_reais_mapeados_e_camadas_consolidadas(spark, monkeypatch, tmp_path):
    _com_mapeamento(monkeypatch, tmp_path, "cd_identificador_original_atendimento_cidadao")
    por = {c: [_registro(c, str(i), f"9{k}{i:04d}", i, status=("FINALIZADA" if i % 2 else "EM ANDAMENTO"))
               for i in range(3)] for k, c in enumerate(CAMADAS)}
    repetida = json.loads(json.dumps(por[CAMADAS[0]][0]))
    por[CAMADAS[3]].append(repetida)  # mesma solicitação publicada em duas camadas
    bronze = _bronze(spark, por)
    validos, barrados = gt.prata_sp156(bronze)
    linhas = {r["protocolo"]: r for r in validos.collect()}
    assert len(linhas) == 21 and barrados.count() == 0  # 22 registros -> 21 solicitações (sem duplicar)
    r = linhas["900000"]
    assert r["servico"] == "Tapa-buraco" and r["tipo_obstaculo"] == "buraco"
    assert r["status"] == "aberta" and r["status_original"] == "EM ANDAMENTO"
    assert str(r["data_abertura"])[:10] == "2020-04-10" and str(r["data_conclusao"])[:10] == "2020-10-30"
    assert validos.filter(F.col("status") == "resolvida").count() == 7
    # a Bronze continua com os nomes e valores originais
    assert "tx_situacao_solicitacao" in bronze.first()["conteudo_bruto"]


def test_chave_composta_nao_funde_camadas(spark, monkeypatch, tmp_path):
    _com_mapeamento(monkeypatch, tmp_path, "cd_identificador", composto=True)
    por = {c: [_registro(c, str(i), None, i + k) for i in range(2)] for k, c in enumerate(CAMADAS)}
    validos, _ = gt.prata_sp156(_bronze(spark, por))
    assert validos.count() == 14 and validos.filter(F.col("protocolo").startswith("geoportal:")).count() == 14
