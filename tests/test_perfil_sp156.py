"""Perfil SP156 com os NOMES REAIS de campo do GeoSampa (valores sintéticos de teste)."""
import json

import perfil_sp156 as ps
import sp156_mapeamento as mapa

CAMADAS = [f"geoportal:sp156_tema_{i}" for i in range(7)]


def _registro(camada, cd, original, i, servico="Tapa-buraco", status="FINALIZADA"):
    return {"type": "Feature", "id": f"{camada}.{i}",
            # posição depende da camada e do índice: solicitações diferentes ficam em lugares diferentes
            "geometry": {"type": "Point", "coordinates": [-46.70 + i * 1e-4, -23.567 + int(camada[-1]) * 1e-3]},
            "properties": {"cd_identificador": cd, "cd_identificador_original_atendimento_cidadao": original,
                           "dc_assunto": "Pavimento", "dc_servico": servico, "dc_tema": "Via",
                           "dt_abertura": f"2020-0{4 + i % 6}-1{i % 9}Z", "dt_parecer": "2020-10-30Z",
                           "tx_situacao_solicitacao": status}}


def _arquivo(tmp_path, por_camada):
    pacote = {"_coleta": {}, "camadas": [{"camada": c, "paginacao": {"completo": True},
                                          "resposta": {"type": "FeatureCollection", "features": f}}
                                         for c, f in por_camada.items()]}
    p = tmp_path / "chegada" / "sp156" / "sp156_2026-11-15.json"
    p.parent.mkdir(parents=True)
    p.write_text(json.dumps(pacote))
    return p


def test_A_escolhe_o_identificador_unico_entre_camadas(tmp_path):
    """cd_identificador é sequência por camada (colide entre camadas); o original é único e
    reconhece a MESMA solicitação publicada em duas camadas."""
    por = {c: [_registro(c, str(i), f"9{k}{i:04d}", i) for i in range(5)] for k, c in enumerate(CAMADAS)}
    por[CAMADAS[1]].append(dict(por[CAMADAS[0]][0], id="dup"))  # mesma solicitação em outra camada
    por[CAMADAS[1]][-1]["properties"] = dict(por[CAMADAS[1]][-1]["properties"], cd_identificador="99")
    r = ps.perfil(_arquivo(tmp_path, por))
    d = r["decisao_protocolo"]
    assert d["protocolo"] == "cd_identificador_original_atendimento_cidadao" and not d["composto_com_camada"]
    cd = next(p for p in r["candidatos_protocolo"] if p["campo"] == "cd_identificador")
    assert cd["colisoes_entre_camadas"] > 0
    assert r["consolidacao"] == {"registros_nas_camadas": 36, "solicitacoes_unicas": 35,
                                 "registros_repetidos_que_a_prata_consolida": 1, "registros_sem_protocolo": 0}
    assert r["datas"]["historico"] is True and r["datas"]["abertura_mais_antiga"].startswith("2020")
    assert ("9999-99-99Z", 36) in [tuple(x) for x in r["formatos_de_data"]["data_abertura"]]
    assert {s["classificacao_prata"] for s in r["status"]} == {"resolvida"}


def test_B_original_mal_preenchido_usa_cd_identificador_global(tmp_path):
    por = {c: [_registro(c, f"{k}-{i}", None if i % 2 else f"X{k}{i}", i) for i in range(4)]
           for k, c in enumerate(CAMADAS)}
    d = ps.perfil(_arquivo(tmp_path, por))["decisao_protocolo"]
    assert d["protocolo"] == "cd_identificador" and not d["composto_com_camada"]


def test_C_colisao_nos_dois_vira_chave_composta(tmp_path):
    por = {c: [_registro(c, str(i), str(i), i + k) for i in range(4)] for k, c in enumerate(CAMADAS)}
    r = ps.perfil(_arquivo(tmp_path, por))
    assert r["decisao_protocolo"]["composto_com_camada"] is True
    assert r["consolidacao"]["solicitacoes_unicas"] == 28  # nada é fundido indevidamente


def test_aplicar_grava_decisao_e_preserva_os_demais_campos(tmp_path, monkeypatch):
    destino = tmp_path / "mapeamento_sp156.json"
    destino.write_text((mapa.ARQUIVO_MAPEAMENTO).read_text())
    monkeypatch.setattr(mapa, "ARQUIVO_MAPEAMENTO", destino)
    por = {c: [_registro(c, f"{k}-{i}", f"P{k}{i}", i) for i in range(3)] for k, c in enumerate(CAMADAS)}
    r = ps.perfil(_arquivo(tmp_path, por))
    ps.aplicar(r, tmp_path / "perfil.md")
    m = mapa.carregar_mapeamento(destino)
    assert m["campos"]["protocolo"] == r["decisao_protocolo"]["protocolo"]
    assert m["campos"]["servico"] == "dc_servico" and m["campos"]["status"] == "tx_situacao_solicitacao"
    assert m["situacao_protocolo"].startswith("DECIDIDO")
    assert mapa.campos_sp156(destino)["protocolo"][0] == r["decisao_protocolo"]["protocolo"]


def test_mapeamento_entregue_tem_servico_status_e_datas_reais():
    c = mapa.campos_sp156()
    assert c["servico"][0] == "dc_servico" and c["status"][0] == "tx_situacao_solicitacao"
    assert c["data_abertura"][0] == "dt_abertura" and c["data_conclusao"][0] == "dt_parecer"


import pytest  # noqa: E402

DECISAO_REAL = {"protocolo": "cd_identificador",
                "situacao": "DECIDIDO pelos dados reais em 2026-11-15T19:30: 100% preenchido, 0 repetidos entre camadas"}


@pytest.mark.parametrize("estado_inicial", ["pendente", "decisao_real"])
def test_recusa_aplicar_decisao_tirada_de_dados_simulados(tmp_path, monkeypatch, estado_inicial):
    """Dados simulados nunca gravam decisão, e nunca mexem numa decisão real já registrada.

    A versão anterior copiava o mapeamento do repositório e esperava protocolo=None. No Codespaces o
    repositório já tem a decisão real (cd_identificador), que a recusa corretamente PRESERVOU; a
    expectativa só valia para o arquivo de fábrica. Agora o estado inicial é montado pelo teste, nos
    dois casos, e a exigência é que o arquivo fique byte a byte igual."""
    inicial = {"campos": {"protocolo": None, "servico": "dc_servico", "status": "tx_situacao_solicitacao",
                          "data_abertura": "dt_abertura", "data_conclusao": "dt_parecer"},
               "protocolo_composto_com_camada": False, "situacao_protocolo": "PENDENTE"}
    if estado_inicial == "decisao_real":
        inicial["campos"]["protocolo"] = DECISAO_REAL["protocolo"]
        inicial["situacao_protocolo"] = DECISAO_REAL["situacao"]
    destino = tmp_path / "m.json"
    destino.write_text(json.dumps(inicial, ensure_ascii=False, indent=1))
    antes = destino.read_bytes()
    monkeypatch.setattr(mapa, "ARQUIVO_MAPEAMENTO", destino)

    # dados simulados que levariam a OUTRA escolha (o original seria único), se fossem aceitos
    por = {c: [_registro(c, f"{k}-{i}", f"P{k}{i}", i) for i in range(2)] for k, c in enumerate(CAMADAS)}
    for feats in por.values():
        for f in feats:
            f["properties"]["_origem"] = "SIMULADOR_LOCAL"
    r = ps.perfil(_arquivo(tmp_path, por))
    assert r["dados_nao_reais"] == ["SIMULADOR_LOCAL"]
    with pytest.raises(SystemExit, match="SIMULADOR") as erro:
        ps.aplicar(r, tmp_path / "p.md")
    assert destino.read_bytes() == antes                       # nada gravado
    m = mapa.carregar_mapeamento(destino)
    if estado_inicial == "pendente":
        assert m["campos"]["protocolo"] is None                 # simulado não cria decisão
    else:
        assert m["campos"]["protocolo"] == "cd_identificador"   # decisão real preservada
        assert m["situacao_protocolo"] == DECISAO_REAL["situacao"]
        assert "cd_identificador" in str(erro.value)            # a mensagem diz o que continua valendo
