import json
from datetime import date

from guiding_hand_lib.conferencia import conferir_chegada, registrar_notificacao

D = date(2026, 11, 12)


def _escrever(pasta, fonte, conteudo):
    p = pasta / fonte / f"{fonte}_{D.isoformat()}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(conteudo, encoding="utf-8")


def test_aprova_quando_obrigatorias_ok_mesmo_sem_opcionais(tmp_path):
    _escrever(tmp_path, "clima", json.dumps({"hourly": {}}))
    _escrever(tmp_path, "osm", json.dumps({"elements": []}))
    _escrever(tmp_path, "eventos_oculos", json.dumps({"eventos": []}))
    m = conferir_chegada(tmp_path, D)
    assert m["aprovado"] is True
    assert (tmp_path / "_manifestos" / "manifesto_2026-11-12.json").exists()
    sit = {i["fonte"]: i["situacao"] for i in m["itens"]}
    assert sit["sp156"] == "ausente" and sit["clima"] == "ok"


def test_reprova_json_invalido_ou_estrutura_errada(tmp_path):
    _escrever(tmp_path, "clima", "{quebrado")
    _escrever(tmp_path, "osm", json.dumps({"outra_coisa": 1}))
    _escrever(tmp_path, "eventos_oculos", json.dumps({"eventos": []}))
    m = conferir_chegada(tmp_path, D)
    assert m["aprovado"] is False
    assert set(m["fontes_obrigatorias_com_problema"]) == {"clima", "osm"}


def test_notificacao_em_arquivo(tmp_path):
    registrar_notificacao(tmp_path, {"tipo": "FALHA", "mensagem": "x"})
    linha = json.loads((tmp_path / "notificacoes.jsonl").read_text().strip())
    assert linha["tipo"] == "FALHA" and "registrado_em" in linha
