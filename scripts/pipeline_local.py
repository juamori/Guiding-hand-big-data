"""Executa Bronze -> Prata -> Ouro -> consultas LOCALMENTE com PySpark (sem Delta).

Serve para: (1) testes automatizados; (2) gerar a amostra do MVP a partir de dados de
teste. NÃO substitui a execução no Databricks e NÃO produz evidência para o relatório:
Auto Loader e MERGE são emulados (lista de arquivos processados e deduplicação por chave).

Uso:  python scripts/pipeline_local.py --chegada tests/fixtures/chegada --saida saida_local
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "databricks" / "lib"))

from pyspark.sql import SparkSession  # noqa: E402

import gh_transform as gt  # noqa: E402
import ouro_sql  # noqa: E402
from pyspark import cloudpickle  # noqa: E402

cloudpickle.register_pickle_by_value(sys.modules[__name__])

SALT_TESTE = "salt-somente-para-testes-locais"


def criar_spark() -> SparkSession:
    spark = (SparkSession.builder.master("local[1]").appName("guiding-hand-local")
             .config("spark.sql.session.timeZone", "America/Sao_Paulo")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def ler_bronze(spark: SparkSession, pasta: Path, ja_processados: set[str]):
    """Emula o Auto Loader: lê só arquivos ainda não processados."""
    arquivos = sorted(str(p) for p in pasta.rglob("*.json")
                      if re.search(gt.PADRAO_ARQUIVO, p.name) and "_manifestos" not in p.parts)
    novos = [a for a in arquivos if a not in ja_processados]
    if not novos:
        return None, []
    df = spark.read.text(novos, wholetext=True).select("value", "_metadata.file_path")
    return gt.enriquecer_bronze(df, "file_path").drop("file_path"), novos


def construir_prata(spark, bronze):
    prata, quarentena = {}, []
    ev, ev_b = gt.prata_eventos(bronze, SALT_TESTE)
    prata["prata_eventos_oculos"] = ev
    quarentena.append(gt.para_quarentena(ev_b, "eventos_oculos", "event_id"))
    for nome, func, fonte, chave in [
        ("prata_clima_horario", gt.prata_clima, "clima", "data_hora"),
        ("prata_osm_infraestrutura", gt.prata_osm, "osm", "osm_key"),
        ("prata_sp156_solicitacoes", gt.prata_sp156, "sp156", "protocolo"),
        ("prata_olhovivo_paradas", gt.prata_olhovivo, "olhovivo", "codigo_parada"),
    ]:
        v, b = func(bronze)
        prata[nome] = v
        quarentena.append(gt.para_quarentena(b, fonte, chave))
    q = quarentena[0]
    for outra in quarentena[1:]:
        q = q.unionByName(outra)
    prata["prata_quarentena"] = q.dropDuplicates(["fonte", "chave_registro", "regra"])
    return prata


def construir_ouro(spark, prata):
    for nome, df in prata.items():
        df.createOrReplaceTempView(nome)
    gt.dim_tipo_obstaculo(spark).createOrReplaceTempView("dim_tipo_obstaculo")
    gt.dim_origem(spark).createOrReplaceTempView("dim_origem")
    gt.dim_local(prata["prata_eventos_oculos"], prata["prata_sp156_solicitacoes"],
                 prata["prata_osm_infraestrutura"]).cache().createOrReplaceTempView("dim_local")
    dim_b, mapa = gt.consolidar_barreiras(spark, prata["prata_eventos_oculos"])
    dim_b.createOrReplaceTempView("aux_barreira_oculos")
    mapa.createOrReplaceTempView("aux_mapa_celula_barreira")
    for nome, sql in ouro_sql.TABELAS_OURO:
        spark.sql(ouro_sql.sql(sql)).cache().createOrReplaceTempView(nome)


def consultas(spark) -> dict[str, list[dict]]:
    texto = (RAIZ / "databricks" / "sql" / "consultas_mvp.sql").read_text(encoding="utf-8")
    return {cid: [r.asDict() for r in spark.sql(q).collect()]
            for cid, _, q in ouro_sql.carregar_consultas(texto, "workspace.guiding_hand.")}


def evidencias(spark, lote="teste") -> dict:
    texto = (RAIZ / "databricks" / "sql" / "consultas_mvp.sql").read_text(encoding="utf-8")
    spark.sql("SELECT * FROM bronze_tmp").createOrReplaceTempView("bronze_arquivos")
    return ouro_sql.montar_evidencias(spark, "", ouro_sql.carregar_consultas(texto, "workspace.guiding_hand."),
                                      lote, "EXECUCAO_LOCAL_DE_TESTE (dados sinteticos e fixtures)")


def executar(chegada: Path, saida: Path | None = None) -> dict:
    spark = criar_spark()
    bronze, arquivos = ler_bronze(spark, chegada, set())
    bronze.createOrReplaceTempView("bronze_tmp")
    prata = construir_prata(spark, bronze)
    construir_ouro(spark, prata)
    resumo = {
        "arquivos_bronze": len(arquivos),
        "bronze_por_fonte": {r["fonte"]: r["count"] for r in bronze.groupBy("fonte").count().collect()},
        "prata": {n: df.count() for n, df in prata.items()},
        "quarentena_por_regra": {f"{r['fonte']}:{r['regra']}": r["count"] for r in
                                 prata["prata_quarentena"].groupBy("fonte", "regra").count().collect()},
        "ouro": {n: spark.table(n).count() for n in ["dim_local"] + [t for t, _ in ouro_sql.TABELAS_OURO]},
        "consultas": consultas(spark),
    }
    if saida:
        saida.mkdir(parents=True, exist_ok=True)
        mvp = [r.asDict() for r in spark.table("tb_mvp_mapa_barreiras").collect()]
        (saida / "barreiras_mvp.json").write_text(
            json.dumps({"origem_dos_dados": "EXECUCAO LOCAL COM DADOS DE TESTE/SINTETICOS",
                        "barreiras": mvp}, ensure_ascii=False, default=str, indent=1), encoding="utf-8")
        (saida / "evidencias_teste.json").write_text(
            json.dumps(evidencias(spark), ensure_ascii=False, default=str, indent=1), encoding="utf-8")
        (saida / "resumo.json").write_text(json.dumps(resumo, ensure_ascii=False, default=str, indent=1),
                                           encoding="utf-8")
    return resumo


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--chegada", type=Path, default=RAIZ / "tests" / "fixtures" / "chegada")
    ap.add_argument("--saida", type=Path, default=RAIZ / "saida_local")
    args = ap.parse_args()
    r = executar(args.chegada, args.saida)
    print(json.dumps({k: v for k, v in r.items() if k != "consultas"}, indent=1, default=str))
    for cid, linhas in r["consultas"].items():
        print(f"\n--- {cid}: {len(linhas)} linha(s)")
        for linha in linhas[:5]:
            print("   ", linha)
