from collections import Counter
from datetime import date

from guiding_hand_lib import config, sintetico


def test_deterministico_por_data():
    assert sintetico.gerar_bytes(date(2026, 11, 12)) == sintetico.gerar_bytes(date(2026, 11, 12))
    assert sintetico.gerar_bytes(date(2026, 11, 12)) != sintetico.gerar_bytes(date(2026, 11, 13))


def test_origem_sintetica_sempre_identificada():
    d = sintetico.gerar_eventos(date(2026, 11, 12))
    assert d["gerador"] == "sintetico_v1" and "SINTETICOS" in d["aviso"]
    assert all(e["origem"] == "sintetico_v1" for e in d["eventos"])


def test_ruido_intencional_presente():
    ev = sintetico.gerar_eventos(date(2026, 11, 12))["eventos"]
    injetados = Counter(e.get("_injetado_para_teste") for e in ev if e.get("_injetado_para_teste"))
    assert sum(injetados.values()) == 5
    assert any(c > 1 for c in Counter(e["event_id"] for e in ev).values())  # reenvios
    assert any(e["tipo_obstaculo"] == "pessoa" for e in ev)


def test_eventos_validos_dentro_do_recorte():
    b = config.BBOX_PINHEIROS
    ev = sintetico.gerar_eventos(date(2026, 11, 12))["eventos"]
    validos = [e for e in ev if not e.get("_injetado_para_teste")]
    assert all(b.contem(e["lat"], e["lon"]) for e in validos)
    assert all(e["ts_deteccao"].startswith("2026-11-12T") for e in validos)
