"""Testes dos coletores com sessão HTTP falsa (sem rede)."""
import json
from datetime import date

import pytest

from guiding_hand_lib import coletores, config


class RespostaFalsa:
    def __init__(self, corpo, status=200, tipo="application/json", url="https://teste"):
        self.content = corpo.encode() if isinstance(corpo, str) else corpo
        self.text = self.content.decode()
        self.status_code, self.url = status, url
        self.headers = {"Content-Type": tipo}

    def json(self):
        return json.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise coletores.requests.HTTPError(str(self.status_code))


class SessaoFalsa:
    def __init__(self, respostas):
        self.respostas, self.chamadas = list(respostas), []

    def get(self, url, params=None, timeout=None):
        self.chamadas.append(("GET", url, params))
        return self.respostas.pop(0)

    def post(self, url, data=None, params=None, timeout=None):
        self.chamadas.append(("POST", url, params or data))
        r = self.respostas.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def test_bbox_contem_e_formatos():
    b = config.BBOX_PINHEIROS
    assert b.contem(-23.567, -46.702)
    assert not b.contem(-23.60, -46.702)
    assert b.como_overpass().startswith("-23.575,-46.71")
    assert b.como_wfs_4326().endswith("EPSG:4326")
    assert config.BBox.de_texto("-1,-2,1,2") == config.BBox(-1, -2, 1, 2)


def test_clima_usa_dia_pedido_e_centro_do_recorte():
    corpo = json.dumps({"hourly": {"time": ["2026-11-10T00:00"] * 24}})
    s = SessaoFalsa([RespostaFalsa(corpo)])
    r = coletores.coletar_clima(s, date(2026, 11, 10), config.BBOX_PINHEIROS)
    params = s.chamadas[0][2]
    assert params["start_date"] == params["end_date"] == "2026-11-10"
    assert params["timezone"] == "America/Sao_Paulo"
    assert r.registros == 24 and r.fonte == "clima"


def test_html_ou_captcha_vira_fonte_indisponivel():
    s = SessaoFalsa([RespostaFalsa("<html>Digite o codigo</html>", tipo="text/html")])
    with pytest.raises(coletores.FonteIndisponivel, match="não é JSON"):
        coletores.coletar_clima(s, date(2026, 11, 10), config.BBOX_PINHEIROS)


def test_osm_tenta_instancia_alternativa():
    ok = RespostaFalsa(json.dumps({"elements": [{"type": "node", "id": 1}]}))
    s = SessaoFalsa([coletores.requests.ConnectionError("timeout"), ok])
    r = coletores.coletar_osm(s, config.BBOX_PINHEIROS, ["https://a", "https://b"])
    assert r.registros == 1 and r.detalhes["instancia"] == "https://b"


def test_osm_todas_instancias_falham():
    s = SessaoFalsa([coletores.requests.ConnectionError("x"), coletores.requests.ConnectionError("y")])
    with pytest.raises(coletores.FonteIndisponivel, match="todas as instâncias"):
        coletores.coletar_osm(s, config.BBOX_PINHEIROS, ["https://a", "https://b"])


def test_sp156_sem_camadas_nao_configurado():
    with pytest.raises(coletores.FonteNaoConfigurada):
        coletores.coletar_sp156(SessaoFalsa([]), config.BBOX_PINHEIROS, [])


def test_sp156_empacota_camadas_e_falhas():
    fc = json.dumps({"type": "FeatureCollection", "features": [{"id": "a"}, {"id": "b"}]})
    s = SessaoFalsa([RespostaFalsa(fc), RespostaFalsa("<html>erro</html>", tipo="text/html")])
    r = coletores.coletar_sp156(s, config.BBOX_PINHEIROS, ["geoportal:a", "geoportal:b"])
    pacote = json.loads(r.conteudo)
    assert r.registros == 2 and len(pacote["camadas"]) == 1 and len(pacote["_coleta"]["falhas"]) == 1
    assert s.chamadas[0][2]["srsName"] == "EPSG:4326"


def test_olhovivo_sem_token_e_token_fora_da_url():
    with pytest.raises(coletores.FonteNaoConfigurada):
        coletores.coletar_olhovivo(SessaoFalsa([]), None, ["Pinheiros"])
    s = SessaoFalsa([RespostaFalsa("true", tipo="text/plain"),
                     RespostaFalsa(json.dumps([{"cp": 1, "py": -23.56, "px": -46.70}]))])
    r = coletores.coletar_olhovivo(s, "TOKEN-SECRETO", ["Pinheiros"])
    assert r.registros == 1
    assert "TOKEN-SECRETO" not in r.url and b"TOKEN-SECRETO" not in r.conteudo


def test_olhovivo_token_recusado():
    s = SessaoFalsa([RespostaFalsa("false", tipo="text/plain")])
    with pytest.raises(coletores.FonteIndisponivel, match="autenticação"):
        coletores.coletar_olhovivo(s, "x", ["Pinheiros"])


def test_gravar_nome_com_data_e_reexecucao_sobrescreve(tmp_path):
    r1 = coletores.ResultadoColeta("clima", b'{"hourly":1}', "u", 1)
    r2 = coletores.ResultadoColeta("clima", b'{"hourly":2}', "u", 1)
    p1 = coletores.gravar(r1, tmp_path, date(2026, 11, 12))
    p2 = coletores.gravar(r2, tmp_path, date(2026, 11, 12))
    assert p1 == p2 == tmp_path / "clima" / "clima_2026-11-12.json"
    assert p2.read_bytes() == b'{"hourly":2}'
    assert len(list((tmp_path / "clima").iterdir())) == 1  # sem .tmp esquecido, sem duplicata


def test_mascara_token_em_mensagens():
    msg = "Max retries exceeded with url: /Login/Autenticar?token=ABC123secreto&x=1 (Caused by ...)"
    assert "ABC123secreto" not in coletores.mascarar(msg)
    assert "token=***" in coletores.mascarar(msg)


def test_falha_de_conexao_no_login_nao_vaza_token():
    erro = coletores.requests.ConnectionError("url: /v2.1/Login/Autenticar?token=SEGREDO-REAL-123")
    with pytest.raises(coletores.FonteIndisponivel) as exc:
        coletores.coletar_olhovivo(SessaoFalsa([erro]), "SEGREDO-REAL-123", ["Pinheiros"])
    assert "SEGREDO-REAL-123" not in str(exc.value) and exc.value.__cause__ is None
