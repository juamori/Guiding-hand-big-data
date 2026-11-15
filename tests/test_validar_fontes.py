"""Validação de fontes de ponta a ponta contra o simulador local (APIs SIMULADAS, não reais).
Cobre: paginação completa (1234 feições com teto de 500 por resposta), classificação de camadas
(solicitações x cadastro), distância da célula do clima e o arquivo de descoberta do Airflow."""
import json
import threading
from http.server import ThreadingHTTPServer

import pytest

import validar_fontes as vf
from guiding_hand_lib import config
from simulador_fontes import Manipulador


@pytest.fixture
def simulador(monkeypatch, tmp_path):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Manipulador)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    monkeypatch.setattr(config, "OPEN_METEO_URL", base + "/v1/forecast")
    monkeypatch.setattr(config, "OVERPASS_URLS", [base + "/api/interpreter"])
    monkeypatch.setattr(config, "GEOSAMPA_WFS_URL", base + "/geoserver/geoportal/wfs")
    monkeypatch.setattr(vf, "RAIZ", tmp_path)
    monkeypatch.setattr(vf, "SAIDA_AMOSTRAS", tmp_path / "amostras")
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    yield vf
    srv.shutdown()


def test_geosampa_completo_e_so_camadas_de_solicitacao(simulador):
    s = vf.coletores.criar_sessao(total_retries=0)
    r = simulador.t_geosampa(s)
    assert r["camadas_sp156_usadas_no_airflow"] == ["geoportal:sp156_tapa_buraco"]
    assert r["camadas_infraestrutura_ignoradas"] == ["geoportal:arvore_viaria"]
    sp = next(t for t in r["testes"] if t["camada"] == "geoportal:sp156_tapa_buraco")
    assert sp["total_no_recorte_servidor"] == sp["feicoes_coletadas"] == 1234 and sp["completo"] is True
    assert sp["paginas"] == 3  # 500 + 500 + 234, apesar de pedir 1000 por página
    descoberta = json.loads((simulador.RAIZ / "airflow" / "descoberta_fontes.json").read_text())
    assert descoberta["gh_sp156_camadas"] == ["geoportal:sp156_tapa_buraco"]


def test_clima_mostra_distancia_da_grade(simulador):
    r = simulador.t_open_meteo(vf.coletores.criar_sessao(total_retries=0))
    assert r["distancia_grade_km"] is not None and "celula_dentro_do_recorte" in r


def test_coletor_do_airflow_coleta_tudo_e_recusa_cadastro(simulador):
    s = vf.coletores.criar_sessao(total_retries=0)
    r = vf.coletores.coletar_sp156(s, config.BBOX_PINHEIROS,
                                   ["geoportal:sp156_tapa_buraco", "geoportal:arvore_viaria"])
    assert r.registros == 1234 and r.detalhes["camadas_recusadas_nao_solicitacao"] == ["geoportal:arvore_viaria"]
    assert r.detalhes["camadas_incompletas"] == []
