"""DAG de coleta do Guiding Hand — Big Data Architecture & Data Integration (FIAP GS 2026).

Fluxo:  inicio >> [coletas em paralelo] >> conferir_chegada >> notificar_resultado

Requisitos atendidos:
- R02: agendado (diário, 07:00 de São Paulo), um arquivo por fonte e por coleta em chegada/<fonte>/<fonte>_<AAAA-MM-DD>.json
- R03: 5 tarefas de coleta em paralelo, tarefa de conferência, chaves em Variables,
       retries + callback de falha (notificação em arquivo e, opcionalmente, webhook)
Reexecutar a mesma data regrava os mesmos arquivos (reprodutível, sem duplicar).
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

# Airflow 3 não coloca a pasta de DAGs no sys.path em todos os modos de execução;
# garantimos que a biblioteca local seja encontrada.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pendulum  # noqa: E402
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Variable, dag, get_current_context, task

from guiding_hand_lib import coletores, config, sintetico
from guiding_hand_lib.conferencia import conferir_chegada, registrar_notificacao

PASTA_LOGS_PADRAO = "/opt/airflow/logs_pipeline"
PASTA_CHEGADA_PADRAO = "/opt/airflow/chegada"


def _var(chave: str, padrao=None, json_=False):
    return Variable.get(chave, default=padrao, deserialize_json=json_)


def _pasta_chegada() -> Path:
    return Path(_var("gh_pasta_chegada", PASTA_CHEGADA_PADRAO))


def _bbox() -> config.BBox:
    texto = _var("gh_bbox", None)
    return config.BBox.de_texto(texto) if texto else config.BBOX_PINHEIROS


def _data_coleta() -> date:
    """Data de coleta = data local (São Paulo) do instante lógico da execução."""
    ctx = get_current_context()
    instante = ctx.get("logical_date") or ctx["dag_run"].run_after
    return pendulum.instance(instante).in_timezone(config.TIMEZONE).date()


def _ao_falhar(context) -> None:
    ti = context.get("task_instance")
    registrar_notificacao(
        Path(_var("gh_pasta_logs", PASTA_LOGS_PADRAO)),
        {
            "tipo": "FALHA",
            "dag_id": ti.dag_id if ti else None,
            "task_id": ti.task_id if ti else None,
            "run_id": context.get("run_id"),
            "mensagem": (coletores.mascarar(context["exception"])[:500] if context.get("exception")
                         else f"tarefa {ti.task_id if ti else '?'} falhou - ver log no Airflow"),
        },
        _var("gh_webhook_url", None),
    )


def _executar_coleta(fonte: str, funcao) -> dict:
    data_coleta = _data_coleta()
    try:
        resultado = funcao(data_coleta)
    except (coletores.FonteNaoConfigurada, coletores.FonteIndisponivel) as exc:
        if isinstance(exc, coletores.FonteIndisponivel) and config.FONTES[fonte]["obrigatoria"]:
            raise
        movido = coletores.descartar_arquivo_anterior(fonte, _pasta_chegada(), data_coleta)
        aviso = f" Arquivo de execução anterior do mesmo dia movido para {movido}." if movido else ""
        raise AirflowSkipException(f"{fonte} pulada nesta coleta: {exc}.{aviso}") from exc
    destino = coletores.gravar(resultado, _pasta_chegada(), data_coleta)
    resumo = {"fonte": fonte, "arquivo": str(destino), "registros": resultado.registros,
              "sha256": resultado.sha256, "url": resultado.url, **resultado.detalhes}
    print(json.dumps(resumo, ensure_ascii=False))
    return resumo


@dag(
    dag_id="guiding_hand_coleta",
    description="Coleta diária das fontes do Guiding Hand (Pinheiros, SP) para chegada/",
    schedule="0 7 * * *",  # todo dia às 07:00 (America/Sao_Paulo)
    start_date=pendulum.datetime(2026, 11, 10, tz=config.TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "big-data",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=2),
        "execution_timeout": pendulum.duration(minutes=10),
        "on_failure_callback": _ao_falhar,
    },
    tags=["guiding-hand", "fiap-gs-2026", "big-data"],
)
def guiding_hand_coleta():
    inicio = EmptyOperator(task_id="inicio")

    @task
    def coletar_clima() -> dict:
        sessao = coletores.criar_sessao()
        return _executar_coleta(
            "clima",
            lambda d: coletores.coletar_clima(sessao, coletores.dia_anterior(d), _bbox(),
                                             url=_var("gh_open_meteo_url", None)),
        )

    @task
    def coletar_osm() -> dict:
        sessao = coletores.criar_sessao()
        urls = _var("gh_overpass_urls", config.OVERPASS_URLS, json_=True)
        return _executar_coleta("osm", lambda d: coletores.coletar_osm(sessao, _bbox(), urls))

    @task
    def coletar_sp156() -> dict:
        sessao = coletores.criar_sessao()
        camadas = _var("gh_sp156_camadas", [], json_=True)
        return _executar_coleta("sp156", lambda d: coletores.coletar_sp156(
            sessao, _bbox(), camadas, url=_var("gh_geosampa_wfs_url", None)))

    @task
    def coletar_olhovivo() -> dict:
        sessao = coletores.criar_sessao()
        token = _var("gh_olhovivo_token", None)  # chave de API: só na Variable
        termos = _var("gh_olhovivo_termos", config.OLHOVIVO_TERMOS, json_=True)
        return _executar_coleta("olhovivo", lambda d: coletores.coletar_olhovivo(
            sessao, token, termos, url=_var("gh_olhovivo_url", None)))

    @task
    def gerar_eventos_oculos() -> dict:
        def gerar(d: date):
            # coleta de D traz o dia D-1 completo (mesma convenção do clima)
            conteudo = sintetico.gerar_bytes(coletores.dia_anterior(d), _bbox())
            n = len(json.loads(conteudo)["eventos"])
            return coletores.ResultadoColeta("eventos_oculos", conteudo, "gerador:" + sintetico.VERSAO,
                                             n, {"sintetico": True})
        return _executar_coleta("eventos_oculos", gerar)

    @task(trigger_rule="all_done")
    def conferir() -> dict:
        manifesto = conferir_chegada(_pasta_chegada(), _data_coleta())
        for item in manifesto["itens"]:
            print(f"{item['fonte']:<15} {item['situacao']:<9} {item['bytes']:>10} bytes  {item['arquivo']}")
        if not manifesto["aprovado"]:
            raise AirflowFailException(
                "Fontes obrigatórias ausentes ou inválidas: "
                + ", ".join(manifesto["fontes_obrigatorias_com_problema"]))
        return {"aprovado": True, "data_coleta": manifesto["data_coleta"]}

    @task
    def notificar_resultado(resultado: dict) -> None:
        registrar_notificacao(
            Path(_var("gh_pasta_logs", PASTA_LOGS_PADRAO)),
            {"tipo": "SUCESSO", "mensagem": f"coleta {resultado['data_coleta']} conferida"},
            _var("gh_webhook_url", None),
        )

    coletas = [coletar_clima(), coletar_osm(), coletar_sp156(), coletar_olhovivo(), gerar_eventos_oculos()]
    resultado = conferir()
    inicio >> coletas >> resultado
    notificar_resultado(resultado)


guiding_hand_coleta()
