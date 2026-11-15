"""Gerador de eventos SINTÉTICOS dos óculos Guiding Hand.

Por que sintético: o protótipo físico (Maker Lab) e o modelo de visão (Applied
Computer Vision) ainda não produzem detecções em campo. O briefing da FIAP aceita
"dados simulados com geração justificada e documentada". Todo registro sai com
``origem = "sintetico_v1"`` e o arquivo traz a semente usada.

Modelo de geração (documentado em docs/fontes.md):
1. Um "mundo" fixo de obstáculos reais-plausíveis no recorte de Pinheiros, gerado
   com semente mestra — o MESMO em todos os dias, para que o mesmo obstáculo seja
   redetectado em coletas diferentes (base da deduplicação por barreira).
2. Alguns obstáculos surgem depois do início e outros são removidos (resolvidos).
3. Por dia, cada obstáculo ativo é detectado por 0..N dispositivos, com ruído de
   GPS (~6 m) e confiança variável.
4. Ruído intencional para testar a Prata: detecções de pessoas (falso alarme),
   reenvios com o mesmo event_id e uma pequena fração de registros inválidos.
"""
from __future__ import annotations

import json
import math
import random
import uuid
from datetime import date, datetime, time, timedelta
from typing import Any

from . import config

VERSAO = "sintetico_v1"
SEMENTE_MESTRA = 20261119
DATA_INICIO_MUNDO = date(2026, 11, 1)
NAMESPACE = uuid.UUID("6f1c7a52-2d0e-4c55-9a0b-8b7f6e2c9a11")
N_OBSTACULOS = 45
N_DISPOSITIVOS = 8
METROS_POR_GRAU_LAT = 111_320.0

# Pesos por tipo (mais frequentes primeiro), inspirados nas dificuldades citadas
# na visão geral do projeto (placas, galhos, carros na calçada, buracos...).
PESOS_TIPOS = {
    "placa": 9, "galho_arvore": 8, "carro_calcada": 7, "buraco": 7, "calcada_irregular": 6,
    "poste": 4, "cacamba": 3, "lixeira": 3, "entulho": 2, "orelhao": 1,
}


def _mundo(bbox: config.BBox) -> list[dict[str, Any]]:
    rnd = random.Random(SEMENTE_MESTRA)
    tipos = list(PESOS_TIPOS)
    pesos = list(PESOS_TIPOS.values())
    margem = 0.0004
    obstaculos = []
    for i in range(N_OBSTACULOS):
        tipo = rnd.choices(tipos, pesos)[0]
        surgimento = DATA_INICIO_MUNDO + timedelta(days=rnd.choice([0] * 6 + list(range(1, 15))))
        # obstáculos temporários (carro, caçamba, entulho, galho) tendem a sumir
        temporario = tipo in {"carro_calcada", "cacamba", "entulho", "galho_arvore"}
        remocao = None
        if temporario and rnd.random() < 0.6:
            remocao = surgimento + timedelta(days=rnd.randint(2, 7))
        elif rnd.random() < 0.15:
            remocao = surgimento + timedelta(days=rnd.randint(5, 12))
        obstaculos.append({
            "id_mundo": i,
            "tipo": tipo,
            "lat": rnd.uniform(bbox.sul + margem, bbox.norte - margem),
            "lon": rnd.uniform(bbox.oeste + margem, bbox.leste - margem),
            "surgimento": surgimento,
            "remocao": remocao,
        })
    return obstaculos


def _ruido(lat: float, lon: float, rnd: random.Random, desvio_m: float = 6.0) -> tuple[float, float]:
    dlat = rnd.gauss(0, desvio_m) / METROS_POR_GRAU_LAT
    dlon = rnd.gauss(0, desvio_m) / (METROS_POR_GRAU_LAT * math.cos(math.radians(lat)))
    return lat + dlat, lon + dlon


def _horario(d: date, rnd: random.Random) -> str:
    segundos = rnd.randint(6 * 3600, 22 * 3600)
    instante = datetime.combine(d, time()) + timedelta(seconds=segundos)
    return instante.strftime("%Y-%m-%dT%H:%M:%S") + "-03:00"


def gerar_eventos(data_ref: date, bbox: config.BBox = config.BBOX_PINHEIROS) -> dict[str, Any]:
    semente_dia = SEMENTE_MESTRA + data_ref.toordinal()
    rnd = random.Random(semente_dia)
    eventos: list[dict[str, Any]] = []
    seq = 0

    def novo_evento(tipo, lat, lon, confianca, dispositivo, altura=None):
        nonlocal seq
        seq += 1
        return {
            "event_id": str(uuid.uuid5(NAMESPACE, f"{data_ref.isoformat()}-{seq}")),
            "device_id": f"oculos-{dispositivo:03d}",
            "ts_deteccao": _horario(data_ref, rnd),
            "tipo_obstaculo": tipo,
            "confianca": confianca,
            "lat": lat,
            "lon": lon,
            "distancia_m": round(rnd.uniform(0.6, 3.5), 2),
            "altura": altura if altura is not None else config.TIPOS_OBSTACULO.get(tipo, "desconhecida"),
            "modelo_versao": "cv-sim-0.1",
            "origem": VERSAO,
        }

    ativos = [o for o in _mundo(bbox)
              if o["surgimento"] <= data_ref and (o["remocao"] is None or data_ref < o["remocao"])]
    for obst in ativos:
        for _ in range(rnd.choices([0, 1, 2, 3, 4], [15, 35, 28, 15, 7])[0]):
            lat, lon = _ruido(obst["lat"], obst["lon"], rnd)
            conf = round(min(0.99, max(0.2, rnd.gauss(0.80, 0.11))), 3)
            eventos.append(novo_evento(obst["tipo"], round(lat, 6), round(lon, 6), conf,
                                       rnd.randint(1, N_DISPOSITIVOS)))

    # Falsos alarmes: pessoas andando (o produto filtra; a Prata precisa saber tratar).
    for _ in range(rnd.randint(6, 12)):
        lat = rnd.uniform(bbox.sul, bbox.norte)
        lon = rnd.uniform(bbox.oeste, bbox.leste)
        eventos.append(novo_evento("pessoa", round(lat, 6), round(lon, 6),
                                   round(rnd.uniform(0.5, 0.95), 3), rnd.randint(1, N_DISPOSITIVOS)))

    # Reenvios (mesmo event_id, mesma carga) — simulam reconexão do dispositivo.
    for original in rnd.sample(eventos, k=max(1, len(eventos) // 30)):
        eventos.append(dict(original))

    # Registros inválidos intencionais (marcados para auditoria do gerador).
    invalidos = [
        ("fora_do_recorte", lambda e: e.update(lat=round(e["lat"] - 0.05, 6))),
        ("confianca_invalida", lambda e: e.update(confianca=1.7)),
        ("tipo_desconhecido", lambda e: e.update(tipo_obstaculo="ovni")),
        ("tipo_nulo", lambda e: e.update(tipo_obstaculo=None)),
        ("baixa_confianca", lambda e: e.update(confianca=0.21)),
    ]
    for motivo, mutar in invalidos:
        base = novo_evento("placa", *_ruido(*bbox.centro, rnd), 0.8, rnd.randint(1, N_DISPOSITIVOS))
        mutar(base)
        base["_injetado_para_teste"] = motivo
        eventos.append(base)

    rnd.shuffle(eventos)
    return {
        "gerador": VERSAO,
        "semente_mestra": SEMENTE_MESTRA,
        "semente_dia": semente_dia,
        "data_referencia": data_ref.isoformat(),
        "aviso": "DADOS SINTETICOS - nao representam pessoas ou deteccoes reais",
        "eventos": eventos,
    }


def gerar_bytes(data_ref: date, bbox: config.BBox = config.BBOX_PINHEIROS) -> bytes:
    return json.dumps(gerar_eventos(data_ref, bbox), ensure_ascii=False, indent=1).encode("utf-8")
