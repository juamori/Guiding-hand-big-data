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
    # A API devolve o centro da célula da grade usada (escolhida por altitude parecida, padrão
    # cell_selection=land), que pode ficar a alguns km do ponto pedido. Registramos a distância.
    grade = (dados.get("latitude"), dados.get("longitude"))
    detalhes = {"data_referencia": data_ref.isoformat(), "ponto_pedido": [params["latitude"], params["longitude"]],
                "centro_celula_grade": list(grade)}
    if None not in grade:
        detalhes["distancia_grade_km"] = round(distancia_km(lat, lon, *grade), 2)
        detalhes["celula_dentro_do_recorte"] = bbox.contem(*grade)
    return ResultadoColeta("clima", resp.content, resp.url, len(horas), detalhes)


def distancia_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distância aproximada (equiretangular), suficiente para poucos km."""
    import math
    dy = (lat2 - lat1) * 111.32
    dx = (lon2 - lon1) * 111.32 * math.cos(math.radians((lat1 + lat2) / 2))
    return math.hypot(dx, dy)


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
WFS_TAMANHO_PAGINA = 1000
WFS_MAX_PAGINAS = 50


def _total_wfs(sessao, url: str, camada: str, bbox: config.BBox, timeout: int) -> int | None:
    """Total de feições no recorte segundo o servidor (resultType=hits). None se não informado."""
    try:
        r = sessao.get(url, params={"service": "WFS", "version": "1.1.0", "request": "GetFeature",
                                    "typeName": camada, "srsName": "EPSG:4326", "bbox": bbox.como_wfs_4326(),
                                    "resultType": "hits"}, timeout=timeout)
        r.raise_for_status()
        m = re.search(r'numberOf(?:Features|Matched)="(\d+)"', r.text)
        return int(m.group(1)) if m else None
    except requests.RequestException:
        return None


def buscar_camada_wfs(sessao, url: str, camada: str, bbox: config.BBox, timeout: int = 90,
                      tamanho_pagina: int = WFS_TAMANHO_PAGINA,
                      max_paginas: int = WFS_MAX_PAGINAS) -> tuple[dict, dict]:
    """Busca TODAS as feições da camada no recorte, paginando com maxFeatures + startIndex
    (GeoServer aceita paginação também no WFS 1.1.0).

    Avança pelo número de feições realmente devolvidas (funciona mesmo se o servidor tiver um
    teto menor que a página pedida) e para quando: a página vem vazia, não traz feições novas
    (servidor ignorando startIndex) ou o total informado pelo servidor foi atingido.
    Retorna (FeatureCollection completa, informações de completude).
    """
    total_servidor = _total_wfs(sessao, url, camada, bbox, timeout)
    feicoes: list[dict] = []
    vistos: set[str] = set()
    inicio, paginas, paginacao_ignorada, ultima_url = 0, 0, False, url
    while paginas < max_paginas:
        r = sessao.get(url, params={"service": "WFS", "version": "1.1.0", "request": "GetFeature",
                                    "typeName": camada, "srsName": "EPSG:4326", "bbox": bbox.como_wfs_4326(),
                                    "outputFormat": "application/json", "maxFeatures": tamanho_pagina,
                                    "startIndex": inicio}, timeout=timeout)
        r.raise_for_status()
        ultima_url = r.url
        pagina = _exigir_json(r, f"sp156[{camada}]").get("features", [])
        paginas += 1
        novas = 0
        for f in pagina:
            chave = str(f.get("id") or json.dumps(f, sort_keys=True))
            if chave not in vistos:
                vistos.add(chave)
                feicoes.append(f)
                novas += 1
        if not pagina:
            break
        if novas == 0:
            paginacao_ignorada = True
            break
        inicio += len(pagina)
        if total_servidor is not None and len(feicoes) >= total_servidor:
            break
        # Sem total conhecido NÃO paramos numa página "curta": o servidor pode ter teto próprio
        # (ex.: 500 por resposta). Seguimos até uma página vazia.
    # completo: True = comprovado; False = faltam feições; None = não verificável
    if total_servidor is not None:
        completo = len(feicoes) >= total_servidor
    elif paginacao_ignorada:
        completo = False if paginas and len(feicoes) >= tamanho_pagina else None
    else:
        completo = paginas < max_paginas
    info = {"paginas": paginas, "total_servidor": total_servidor, "coletadas": len(feicoes),
            "completo": completo, "paginacao_ignorada": paginacao_ignorada, "url_ultima_pagina": ultima_url}
    return {"type": "FeatureCollection", "features": feicoes}, info


def coletar_sp156(sessao: requests.Session, bbox: config.BBox, camadas: list[str],
                  timeout: int = 90, url: str | None = None) -> ResultadoColeta:
    """Solicitações SP156 georreferenciadas (GeoSampa WFS) dentro do recorte, COMPLETAS (paginadas).

    ``camadas`` vem da Variable ``gh_sp156_camadas``, preenchida pela validação de fontes.
    Cada camada registra páginas, total informado pelo servidor e se a coleta ficou completa.
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
    falhas, incompletas, nao_verificadas, recusadas = [], [], [], []
    for camada in camadas:
        try:
            fc, info = buscar_camada_wfs(sessao, url, camada, bbox, timeout)
            if fc["features"] and not eh_camada_de_solicitacoes(fc["features"]):
                recusadas.append(camada)
                continue
            total += info["coletadas"]
            if info["completo"] is False:
                incompletas.append(camada)
            elif info["completo"] is None:
                nao_verificadas.append(camada)
            pacote["camadas"].append({"camada": camada, "url": info["url_ultima_pagina"], "paginacao": info,
                                      "resposta": fc})
        except (requests.RequestException, FonteIndisponivel) as exc:
            falhas.append(f"{camada}: {exc}")
    if not pacote["camadas"]:
        motivo = " | ".join(falhas + [f"{c}: não é camada de solicitações" for c in recusadas])
        raise FonteIndisponivel("sp156: nenhuma camada de solicitações utilizável -> " + motivo)
    pacote["_coleta"].update(falhas=falhas, camadas_recusadas_nao_solicitacao=recusadas,
                             camadas_incompletas=incompletas,
                             camadas_completude_nao_verificavel=nao_verificadas)
    conteudo = json.dumps(pacote, ensure_ascii=False).encode("utf-8")
    return ResultadoColeta("sp156", conteudo, url, total,
                           {"camadas_ok": len(pacote["camadas"]), "falhas": falhas,
                            "camadas_incompletas": incompletas,
                            "camadas_completude_nao_verificavel": nao_verificadas,
                            "camadas_recusadas_nao_solicitacao": recusadas})


def eh_camada_de_solicitacoes(features: list[dict]) -> bool:
    """True se as feições têm protocolo e status (solicitação SP156), não só cadastro de infraestrutura."""
    campos = {k.lower() for f in features[:50] for k in (f.get("properties") or {})}
    tem = lambda trechos: any(t in c for c in campos for t in trechos)  # aceita ds_situacao, nr_protocolo_156...
    return tem(config.TRECHOS_PROTOCOLO_SP156) and tem(config.TRECHOS_STATUS_SP156)


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


def descartar_arquivo_anterior(fonte: str, pasta_chegada: Path, data_coleta: date) -> Path | None:
    """Fonte pulada nesta execução: o arquivo do MESMO dia, de uma execução anterior, não pode
    continuar em chegada/ como se fosse desta coleta. É movido (não apagado) para chegada/_descartados/."""
    atual = pasta_chegada / fonte / nome_arquivo(fonte, data_coleta)
    if not atual.exists():
        return None
    destino = pasta_chegada / "_descartados" / (
        f"{atual.stem}__descartado_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json")
    destino.parent.mkdir(parents=True, exist_ok=True)
    atual.replace(destino)
    return destino


def dia_anterior(d: date) -> date:
    return d - timedelta(days=1)
