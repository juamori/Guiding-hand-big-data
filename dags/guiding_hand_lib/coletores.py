"""Coletores das fontes do Guiding Hand.

Cada coletor recebe parâmetros explícitos e uma ``requests.Session`` e devolve
``ResultadoColeta``. Não conhece Airflow — por isso pode ser testado e reutilizado
pelo script de validação de fontes. Nenhum coletor contém credenciais.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import config


_PADRAO_SEGREDO = re.compile(r"(?i)(token|apikey|api_key|key|senha|password)=([^&\s'\")]+)")


def mascarar(texto: str) -> str:
    """Remove valores de credenciais de mensagens/URLs antes de logar ou notificar."""
    return _PADRAO_SEGREDO.sub(lambda m: f"{m.group(1)}=***", str(texto))


class FonteIndisponivel(Exception):
    """A fonte respondeu, mas não é utilizável (captcha, HTML, vazio, etc.)."""


class FonteNaoConfigurada(Exception):
    """Falta configuração (token, camadas) para coletar esta fonte."""


@dataclass
class ResultadoColeta:
    fonte: str
    conteudo: bytes
    url: str
    registros: int
    detalhes: dict[str, Any] = field(default_factory=dict)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.conteudo).hexdigest()


def criar_sessao(total_retries: int = 3) -> requests.Session:
    sessao = requests.Session()
    retry = Retry(
        total=total_retries,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
    )
    sessao.mount("https://", HTTPAdapter(max_retries=retry))
    sessao.mount("http://", HTTPAdapter(max_retries=retry))
    sessao.headers["User-Agent"] = config.USER_AGENT
    return sessao


def _exigir_json(resp: requests.Response, fonte: str) -> Any:
    tipo = resp.headers.get("Content-Type", "")
    texto = resp.text.lstrip()
    if "html" in tipo.lower() or texto.startswith("<"):
        trecho = texto[:200].replace("\n", " ")
        raise FonteIndisponivel(f"{fonte}: resposta não é JSON (possível captcha/erro). Início: {trecho}")
    try:
        return resp.json()
    except ValueError as exc:
        raise FonteIndisponivel(f"{fonte}: JSON inválido: {exc}") from exc


# --------------------------------------------------------------------------- clima
def coletar_clima(sessao: requests.Session, data_ref: date, bbox: config.BBox,
                  timeout: int = 30, url: str | None = None) -> ResultadoColeta:
    """Clima horário do dia ``data_ref`` no centro do recorte (Forecast API).

    Usamos o endpoint de previsão com start_date/end_date porque o arquivo
    histórico (ERA5) só publica dados com ~5 dias de atraso.
    """
    lat, lon = bbox.centro
    params = {
        "latitude": round(lat, 4),
        "longitude": round(lon, 4),
        "hourly": ",".join(config.OPEN_METEO_HOURLY),
        "timezone": config.TIMEZONE,
        "start_date": data_ref.isoformat(),
        "end_date": data_ref.isoformat(),
    }
    resp = sessao.get(url or config.OPEN_METEO_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    dados = _exigir_json(resp, "clima")
    horas = dados.get("hourly", {}).get("time", [])
    if not horas:
        raise FonteIndisponivel("clima: resposta sem série horária")
    return ResultadoColeta("clima", resp.content, resp.url, len(horas),
                           {"data_referencia": data_ref.isoformat()})


# --------------------------------------------------------------------------- OSM
def consulta_overpass(bbox: config.BBox, timeout_s: int = 90) -> str:
    b = bbox.como_overpass()
    return f"""[out:json][timeout:{timeout_s}];
(
  node["barrier"]({b});
  node["kerb"]({b});
  node["tactile_paving"]({b});
  node["highway"="street_lamp"]({b});
  node["power"="pole"]({b});
  node["man_made"="utility_pole"]({b});
  node["amenity"="waste_basket"]({b});
  node["natural"="tree"]({b});
  node["highway"="bus_stop"]({b});
  node["highway"="crossing"]({b});
  way["footway"="sidewalk"]({b});
  way["highway"="footway"]({b});
);
out center tags;"""


def coletar_osm(sessao: requests.Session, bbox: config.BBox, urls: list[str] | None = None,
                timeout: int = 120) -> ResultadoColeta:
    erros = []
    for url in urls or config.OVERPASS_URLS:
        try:
            resp = sessao.post(url, data={"data": consulta_overpass(bbox)}, timeout=timeout)
            resp.raise_for_status()
            dados = _exigir_json(resp, "osm")
            elementos = dados.get("elements")
            if elementos is None:
                raise FonteIndisponivel(f"osm: resposta sem 'elements' ({dados.get('remark')})")
            return ResultadoColeta("osm", resp.content, url, len(elementos), {"instancia": url})
        except (requests.RequestException, FonteIndisponivel) as exc:
            erros.append(f"{url}: {exc}")
    raise FonteIndisponivel("osm: todas as instâncias falharam -> " + " | ".join(erros))


# --------------------------------------------------------------------------- SP156
def coletar_sp156(sessao: requests.Session, bbox: config.BBox, camadas: list[str],
                  timeout: int = 90, url: str | None = None) -> ResultadoColeta:
    """Solicitações SP156 georreferenciadas (GeoSampa WFS) dentro do recorte.

    ``camadas`` vem da Variable ``gh_sp156_camadas`` e é preenchida depois da
    validação de fontes (os nomes das camadas não estão documentados publicamente).
    """
    url = url or config.GEOSAMPA_WFS_URL
    if not camadas:
        raise FonteNaoConfigurada("sp156: nenhuma camada configurada em gh_sp156_camadas")
    pacote: dict[str, Any] = {
        "_coleta": {
            "fonte": "sp156_geosampa_wfs",
            "bbox": bbox.como_wfs_4326(),
            "coletado_em": datetime.now(timezone.utc).isoformat(),
        },
        "camadas": [],
    }
    total = 0
    falhas = []
    for camada in camadas:
        params = {
            "service": "WFS",
            "version": "1.1.0",
            "request": "GetFeature",
            "typeName": camada,
            "srsName": "EPSG:4326",
            "bbox": bbox.como_wfs_4326(),
            "outputFormat": "application/json",
        }
        try:
            resp = sessao.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            fc = _exigir_json(resp, f"sp156[{camada}]")
            n = len(fc.get("features", []))
            total += n
            pacote["camadas"].append({"camada": camada, "url": resp.url, "resposta": fc})
        except (requests.RequestException, FonteIndisponivel) as exc:
            falhas.append(f"{camada}: {exc}")
    if not pacote["camadas"]:
        raise FonteIndisponivel("sp156: nenhuma camada respondeu -> " + " | ".join(falhas))
    pacote["_coleta"]["falhas"] = falhas
    conteudo = json.dumps(pacote, ensure_ascii=False).encode("utf-8")
    return ResultadoColeta("sp156", conteudo, url, total,
                           {"camadas_ok": len(pacote["camadas"]), "falhas": falhas})


# --------------------------------------------------------------------------- Olho Vivo
def coletar_olhovivo(sessao: requests.Session, token: str | None, termos: list[str],
                     timeout: int = 30, url: str | None = None) -> ResultadoColeta:
    base = url or config.OLHOVIVO_URL
    if not token:
        raise FonteNaoConfigurada("olhovivo: Variable gh_olhovivo_token não definida")
    try:
        login = sessao.post(f"{base}/Login/Autenticar", params={"token": token}, timeout=timeout)
        login.raise_for_status()
    except requests.RequestException as exc:
        # a URL do login contém o token: a mensagem original nunca pode ser propagada
        raise FonteIndisponivel(f"olhovivo: falha no login ({type(exc).__name__}): {mascarar(exc)}") from None
    if login.text.strip().lower() != "true":
        raise FonteIndisponivel("olhovivo: autenticação recusada (token inválido?)")
    pacote: dict[str, Any] = {"_coleta": {"fonte": "olhovivo_parada_buscar",
                                          "coletado_em": datetime.now(timezone.utc).isoformat()},
                              "buscas": []}
    total = 0
    for termo in termos:
        resp = sessao.get(f"{base}/Parada/Buscar",
                          params={"termosBusca": termo}, timeout=timeout)
        resp.raise_for_status()
        paradas = _exigir_json(resp, "olhovivo")
        total += len(paradas) if isinstance(paradas, list) else 0
        pacote["buscas"].append({"termo": termo, "resposta": paradas})
    conteudo = json.dumps(pacote, ensure_ascii=False).encode("utf-8")
    # A URL registrada NUNCA contém o token.
    return ResultadoColeta("olhovivo", conteudo, f"{base}/Parada/Buscar", total)


# --------------------------------------------------------------------------- gravação
def nome_arquivo(fonte: str, data_coleta: date) -> str:
    return f"{fonte}_{data_coleta.isoformat()}.json"


def gravar(resultado: ResultadoColeta, pasta_chegada: Path, data_coleta: date) -> Path:
    """Grava atomicamente ``chegada/<fonte>/<fonte>_<AAAA-MM-DD>.json``.

    Reexecutar a mesma data sobrescreve o mesmo arquivo (execução reproduzível,
    sem gerar arquivos duplicados para o mesmo dia de coleta).
    """
    destino = pasta_chegada / resultado.fonte / nome_arquivo(resultado.fonte, data_coleta)
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_suffix(".json.tmp")
    temporario.write_bytes(resultado.conteudo)
    temporario.replace(destino)
    return destino


def dia_anterior(d: date) -> date:
    return d - timedelta(days=1)
