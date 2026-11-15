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


def _fc(*ids, props=None):
    p = {"protocolo": "x", "status": "ABERTA"} if props is None else props
    return RespostaFalsa(json.dumps({"type": "FeatureCollection",
                                     "features": [{"id": i, "properties": p} for i in ids]}))


def _hits(n):
    return RespostaFalsa(f'<wfs:FeatureCollection numberOfFeatures="{n}"/>', tipo="text/xml")


def test_sp156_pagina_ate_o_total_mesmo_com_teto_do_servidor():
    """Servidor devolve no máximo 2 por página (teto menor que o pedido): a coleta precisa seguir."""
    s = SessaoFalsa([_hits(5), _fc("a", "b"), _fc("c", "d"), _fc("e")])
    r = coletores.coletar_sp156(s, config.BBOX_PINHEIROS, ["geoportal:a"])
    camada = json.loads(r.conteudo)["camadas"][0]
    assert r.registros == 5 and [f["id"] for f in camada["resposta"]["features"]] == list("abcde")
    assert camada["paginacao"] == {**camada["paginacao"], "paginas": 3, "total_servidor": 5, "completo": True}
    assert [c[2]["startIndex"] for c in s.chamadas[1:]] == [0, 2, 4]
    assert s.chamadas[0][2]["resultType"] == "hits" and s.chamadas[1][2]["srsName"] == "EPSG:4326"
    assert r.detalhes["camadas_incompletas"] == []


def test_sp156_sem_total_segue_ate_pagina_vazia_apesar_de_teto_do_servidor():
    """Sem 'hits' e com teto de 2 no servidor: não pode parar na 1ª página 'curta'."""
    s = SessaoFalsa([RespostaFalsa("sem total", tipo="text/xml"), _fc("a", "b"), _fc("c"), _fc()])
    r = coletores.coletar_sp156(s, config.BBOX_PINHEIROS, ["geoportal:a"])
    info = json.loads(r.conteudo)["camadas"][0]["paginacao"]
    assert r.registros == 3 and info["completo"] is True and info["paginas"] == 3


def test_servidor_que_ignora_paginacao():
    sessao = lambda: SessaoFalsa([RespostaFalsa("sem total", tipo="text/xml")] + [_fc("a", "b")] * 3)
    # página cheia repetida -> certamente faltam feições
    _, info = coletores.buscar_camada_wfs(sessao(), "u", "c", config.BBOX_PINHEIROS, tamanho_pagina=2)
    assert info["paginacao_ignorada"] and info["completo"] is False
    # página curta repetida, sem total -> não dá para afirmar
    _, info = coletores.buscar_camada_wfs(sessao(), "u", "c", config.BBOX_PINHEIROS)
    assert info["completo"] is None
    r = coletores.coletar_sp156(sessao(), config.BBOX_PINHEIROS, ["geoportal:a"])
    assert r.detalhes["camadas_completude_nao_verificavel"] == ["geoportal:a"]


def test_sp156_camada_com_falha_nao_derruba_as_outras():
    s = SessaoFalsa([_hits(2), _fc("a", "b"), _hits(1), RespostaFalsa("<html>erro</html>", tipo="text/html")])
    r = coletores.coletar_sp156(s, config.BBOX_PINHEIROS, ["geoportal:a", "geoportal:b"])
    pacote = json.loads(r.conteudo)
    assert r.registros == 2 and len(pacote["camadas"]) == 1 and len(pacote["_coleta"]["falhas"]) == 1


def test_clima_registra_distancia_da_celula_da_grade():
    corpo = json.dumps({"latitude": -23.585238, "longitude": -46.659637, "hourly": {"time": ["x"] * 24}})
    r = coletores.coletar_clima(SessaoFalsa([RespostaFalsa(corpo)]), date(2026, 11, 14), config.BBOX_PINHEIROS)
    assert r.detalhes["distancia_grade_km"] == pytest.approx(4.75, abs=0.05)
    assert r.detalhes["celula_dentro_do_recorte"] is False


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


def test_sp156_recusa_camada_de_cadastro_de_infraestrutura():
    """Camadas de cadastro (ex.: árvores) não podem virar 'solicitações' no pipeline."""
    s = SessaoFalsa([_hits(2), _fc("t1", "t2", props={"especie": "ipê"}), _hits(1), _fc("s1")])
    r = coletores.coletar_sp156(s, config.BBOX_PINHEIROS, ["geoportal:arvores", "geoportal:sp156"])
    pacote = json.loads(r.conteudo)
    assert [c["camada"] for c in pacote["camadas"]] == ["geoportal:sp156"] and r.registros == 1
    assert r.detalhes["camadas_recusadas_nao_solicitacao"] == ["geoportal:arvores"]
    so_cadastro = SessaoFalsa([_hits(1), _fc("t1", props={"especie": "ipê"})])
    with pytest.raises(coletores.FonteIndisponivel, match="não é camada de solicitações"):
        coletores.coletar_sp156(so_cadastro, config.BBOX_PINHEIROS, ["geoportal:arvores"])


def test_reconhece_variacoes_de_nome_de_campo():
    assert coletores.eh_camada_de_solicitacoes([{"properties": {"NR_PROTOCOLO_SP156": 1, "DS_SITUACAO": "x"}}])
    assert not coletores.eh_camada_de_solicitacoes([{"properties": {"especie": "ipê", "altura": 5}}])


def test_validador_classifica_camadas():
    import validar_fontes as vf
    assert vf.classificar_camada("geoportal:sp156_tapa_buraco", "", []) == "solicitacao_sp156"
    assert vf.classificar_camada("geoportal:arvore", "Árvores", ["especie"]) == "infraestrutura_cadastral"
    assert vf.classificar_camada("geoportal:x", "", ["protocolo", "situacao"]) == "solicitacao_sp156"


def test_fonte_pulada_nao_deixa_arquivo_antigo_do_mesmo_dia(tmp_path):
    antigo = coletores.gravar(coletores.ResultadoColeta("sp156", b'{"camadas": []}', "u", 0), tmp_path, date(2026, 11, 16))
    movido = coletores.descartar_arquivo_anterior("sp156", tmp_path, date(2026, 11, 16))
    assert not antigo.exists() and movido.exists() and movido.parent.name == "_descartados"
    assert movido.read_bytes() == b'{"camadas": []}'  # preservado, não apagado
    assert coletores.descartar_arquivo_anterior("sp156", tmp_path, date(2026, 11, 16)) is None
