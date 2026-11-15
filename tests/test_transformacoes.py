"""Testes das transformações PySpark (Bronze/Prata/Ouro) com Spark local.

Delta Lake, Auto Loader e MERGE não rodam aqui; são validados no Databricks.
O que se testa: parsing, regras de qualidade, deduplicação, proteção de dados,
resolução de entidade e as 6 consultas do MVP sobre dados de teste.
"""
import json
from datetime import date

import pytest

import shutil  # noqa: E402

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java não encontrado: testes Spark locais pulados", allow_module_level=True)

import gh_transform as gt  # noqa: E402
import pipeline_local as pl  # noqa: E402
from conftest import FIXTURES  # noqa: E402
from guiding_hand_lib import config, sintetico  # noqa: E402


@pytest.fixture(scope="module")
def ambiente():
    spark = pl.criar_spark()
    bronze, arquivos = pl.ler_bronze(spark, FIXTURES, set())
    bronze = bronze.cache()
    prata = pl.construir_prata(spark, bronze)
    pl.construir_ouro(spark, prata)
    return spark, bronze, arquivos, prata


def test_geohash_valor_conhecido_e_centro():
    assert gt.geohash_encode(57.64911, 10.40744, 11) == "u4pruydqqvj"  # exemplo clássico
    lat, lon = gt.geohash_centro(gt.geohash_encode(-23.567, -46.702, 8))
    assert abs(lat + 23.567) < 0.0003 and abs(lon + 46.702) < 0.0003


def test_bronze_preserva_bruto_e_metadados(ambiente):
    _, bronze, arquivos, _ = ambiente
    assert bronze.count() == len(arquivos) == 10
    linha = bronze.filter("fonte = 'clima'").orderBy("data_coleta").first()
    original = open(linha["arquivo_origem"].removeprefix("file:"), encoding="utf-8").read()
    assert linha["conteudo_bruto"] == original
    assert str(linha["data_coleta"]) == "2026-11-12" and len(linha["hash_conteudo"]) == 64
    assert {r["fonte"] for r in bronze.select("fonte").distinct().collect()} == set(config.FONTES)


def test_bronze_incremental_nao_reprocessa(ambiente):
    spark, _, arquivos, _ = ambiente
    df, novos = pl.ler_bronze(spark, FIXTURES, set(arquivos))
    assert df is None and novos == []


def test_regras_de_qualidade_barram_exatamente_os_injetados(ambiente):
    _, _, _, prata = ambiente
    q = {(r["fonte"], r["regra"]): r["count"]
         for r in prata["prata_quarentena"].groupBy("fonte", "regra").count().collect()}
    # cada arquivo de eventos tem 5 inválidos injetados: 1 fora do recorte, 1 confiança 1.7,
    # 2 tipos inválidos (desconhecido e nulo), 1 abaixo do limiar -> x2 arquivos
    assert q[("eventos_oculos", "RQ2_FORA_DO_RECORTE")] == 2
    assert q[("eventos_oculos", "RQ3_TIPO_FORA_DO_DOMINIO")] == 4
    assert q[("eventos_oculos", "RQ4_CONFIANCA_INVALIDA")] == 2
    assert q[("eventos_oculos", "RQ5_CONFIANCA_ABAIXO_DO_LIMIAR")] == 2
    assert q[("clima", "RQ6_MEDIDA_FISICA_IMPOSSIVEL")] == 1
    assert q[("sp156", "RQ7_DATAS_INCOERENTES")] == 1


def test_deduplicacao_por_event_id(ambiente):
    _, _, _, prata = ambiente
    ev = prata["prata_eventos_oculos"]
    assert ev.count() == ev.select("event_id").distinct().count()
    esperado = set()
    for d in ("2026-11-12", "2026-11-13"):
        dados = json.loads((FIXTURES / "eventos_oculos" / f"eventos_oculos_{d}.json").read_text())
        esperado |= {e["event_id"] for e in dados["eventos"] if not e.get("_injetado_para_teste")
                     and e["confianca"] >= 0.5}
    assert ev.count() == len(esperado)


def test_prata_protege_dados_pessoais(ambiente):
    _, _, _, prata = ambiente
    cols = set(prata["prata_eventos_oculos"].columns)
    assert "device_id" not in cols and "lat" not in cols and "lon" not in cols
    hashes = [r["device_hash"] for r in prata["prata_eventos_oculos"].select("device_hash").limit(5).collect()]
    assert all(len(h) == 64 and not h.startswith("oculos") for h in hashes)


def test_clima_explodido_por_hora_e_dedup(ambiente):
    _, _, _, prata = ambiente
    c = prata["prata_clima_horario"]
    assert c.count() == 47  # 2 dias x 24h - 1 hora barrada (precipitação negativa)
    assert c.filter("choveu").count() == 3


def test_sp156_status_e_merge_pela_chave(ambiente):
    _, _, _, prata = ambiente
    s = {r["protocolo"]: r for r in prata["prata_sp156_solicitacoes"].collect()}
    assert set(s) == {"T-0001", "T-0002"}  # T-0003 datas incoerentes, T-0004 fora do recorte
    assert s["T-0002"]["status"] == "resolvida"  # versão mais recente (2º lote) venceu
    assert s["T-0001"]["tipo_obstaculo"] == "buraco" and s["T-0002"]["tipo_obstaculo"] == "calcada_irregular"


def test_resolucao_de_entidade_proxima_da_verdade(ambiente):
    spark, _, _, _ = ambiente
    n = spark.table("dim_barreira").filter("origem = 'oculos_sintetico'").count()
    mundo = sintetico._mundo(config.BBOX_PINHEIROS)
    dias = [date(2026, 11, 11), date(2026, 11, 12)]
    ativos = {o["id_mundo"] for o in mundo for d in dias
              if o["surgimento"] <= d and (o["remocao"] is None or d < o["remocao"])}
    # nunca mais barreiras do que obstáculos reais (+ margem pequena p/ ruído de GPS)
    assert 0.75 * len(ativos) <= n <= len(ativos) + 2, (n, len(ativos))


def test_tabela_do_mvp_sem_dado_pessoal_e_no_recorte(ambiente):
    spark, _, _, _ = ambiente
    mvp = spark.table("tb_mvp_mapa_barreiras")
    assert not {"device_hash", "device_id"} & set(mvp.columns)
    b = config.BBOX_PINHEIROS
    assert all(b.contem(r["lat"], r["lon"]) for r in mvp.select("lat", "lon").collect())


def test_fato_tem_device_apenas_como_hash(ambiente):
    spark, _, _, _ = ambiente
    f = spark.table("fato_ocorrencia_barreira")
    assert f.filter("device_hash IS NOT NULL AND length(device_hash) <> 64").count() == 0
    assert f.filter("sk_tipo IS NULL OR sk_local IS NULL").count() == 0


def test_seis_consultas_retornam_resultado(ambiente):
    spark, _, _, _ = ambiente
    r = pl.consultas(spark)
    assert set(r) == {"C1", "C2", "C3", "C4", "C5", "C6"}
    assert all(len(v) > 0 for v in r.values())
    c5 = {l["origem"]: l for l in r["C5"]}
    assert c5["sp156"]["resolvidas_oficialmente"] == 2
