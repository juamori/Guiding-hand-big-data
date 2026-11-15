"""Verificação da coleta: reprova simulador/fixtures e aponta incompatibilidades com a Prata."""
import json
import shutil
from datetime import date

import verificar_chegada as vc
from conftest import FIXTURES


def _sp156(props, n=3):
    feats = [{"type": "Feature", "id": f"x.{i}", "geometry": {"type": "Point", "coordinates": [-46.70, -23.567]},
              "properties": props} for i in range(n)]
    return {"_coleta": {"fonte": "sp156_geosampa_wfs"},
            "camadas": [{"camada": "geoportal:sp156_x", "paginacao": {"total_servidor": n, "completo": True},
                         "resposta": {"type": "FeatureCollection", "features": feats}}]}


def _chegada_real(tmp_path, sp156):
    d = date(2026, 11, 16)
    for fonte in ("clima", "osm", "eventos_oculos"):
        origem = json.loads((FIXTURES / fonte / f"{fonte}_2026-11-13.json").read_text())
        origem.pop("_fixture", None)
        origem["generator"] = "Overpass API" if fonte == "osm" else origem.get("generator")
        if fonte == "clima":
            origem["hourly"]["time"] = [f"2026-11-15T{h:02d}:00" for h in range(24)]
        p = tmp_path / fonte / f"{fonte}_{d}.json"
        p.parent.mkdir(parents=True)
        p.write_text(json.dumps(origem))
    p = tmp_path / "sp156" / f"sp156_{d}.json"
    p.parent.mkdir()
    p.write_text(json.dumps(sp156))
    return d


def test_mapeamento_lido_do_codigo_da_prata():
    campos, mapa = vc.mapeamento_prata()
    assert "status" in campos["status"] and ("buraco", "buraco") in mapa


def test_fixtures_de_teste_sao_reprovados(tmp_path):
    shutil.copytree(FIXTURES, tmp_path / "c")
    r = vc.verificar(date(2026, 11, 13), tmp_path / "c")
    assert not r["aprovada"]
    assert "conteúdo de SIMULADOR/TESTE, não é coleta real" in r["fontes"]["clima"]["problemas"]


def test_coleta_compativel_e_aprovada(tmp_path):
    d = _chegada_real(tmp_path, _sp156({"protocolo": "1", "servico": "Tapa-buraco", "status": "ABERTA",
                                        "data_abertura": "2026-11-10"}))
    r = vc.verificar(d, tmp_path)
    assert r["aprovada"], r["fontes_com_pendencia"]
    c = r["fontes"]["sp156"]["camadas"][0]
    assert c["compatibilidade_prata"]["status"] == "status" and c["feicoes_com_tipo_de_obstaculo"] == 3
    assert r["fontes"]["clima"]["dias_no_conteudo"] == ["2026-11-15"]
    assert vc.escrever(r, tmp_path / "ev").exists()


def test_campo_fora_do_mapeamento_e_apontado(tmp_path):
    d = _chegada_real(tmp_path, _sp156({"NR_PROTOCOLO": "1", "DS_SITUACAO": "ABERTA", "NM_SERVICO": "Poda"}))
    c = vc.verificar(d, tmp_path)["fontes"]["sp156"]["camadas"][0]
    assert c["compatibilidade_prata"]["protocolo"] == "NR_PROTOCOLO"
    assert "Prata não encontra o campo 'status'" in c["problemas"]
    assert "Prata não encontra o campo 'data_abertura'" in c["problemas"]


def test_arquivo_de_execucao_anterior_e_apontado(tmp_path):
    import os
    from datetime import datetime, timedelta, timezone
    d = _chegada_real(tmp_path, _sp156({"protocolo": "1", "servico": "Tapa-buraco", "status": "ABERTA",
                                        "data_abertura": "2026-11-10"}))
    antigo = tmp_path / "sp156" / f"sp156_{d}.json"
    duas_horas = (datetime.now() - timedelta(hours=2)).timestamp()
    os.utime(antigo, (duas_horas, duas_horas))
    r = vc.verificar(d, tmp_path, datetime.now(timezone.utc) - timedelta(minutes=1))
    assert not r["aprovada"] and r["fontes_com_pendencia"] == ["sp156"]
    assert "execução ANTERIOR" in r["fontes"]["sp156"]["problemas"][0]
