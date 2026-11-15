"""Configuração central do pipeline Guiding Hand.

Os valores aqui são DEFAULTS. Em produção (Airflow) cada um pode ser sobrescrito
por uma Variable de mesmo nome (prefixo ``gh_``). Nenhuma credencial fica aqui.
"""
from __future__ import annotations

from dataclasses import dataclass

TIMEZONE = "America/Sao_Paulo"


@dataclass(frozen=True)
class BBox:
    """Retângulo geográfico em graus decimais (WGS84)."""

    sul: float
    oeste: float
    norte: float
    leste: float

    def contem(self, lat: float, lon: float) -> bool:
        return self.sul <= lat <= self.norte and self.oeste <= lon <= self.leste

    @property
    def centro(self) -> tuple[float, float]:
        return ((self.sul + self.norte) / 2, (self.oeste + self.leste) / 2)

    def como_overpass(self) -> str:
        # Overpass usa (sul, oeste, norte, leste)
        return f"{self.sul},{self.oeste},{self.norte},{self.leste}"

    def como_wfs_4326(self) -> str:
        # WFS 1.1.0 com EPSG:4326 no GeoServer: minx(lon),miny(lat),maxx,maxy
        return f"{self.oeste},{self.sul},{self.leste},{self.norte},EPSG:4326"

    @classmethod
    def de_texto(cls, texto: str) -> "BBox":
        s, o, n, l = (float(x) for x in texto.split(","))
        return cls(s, o, n, l)


# Recorte piloto: ~800 m em torno da Estação Pinheiros (Linhas 4 e 9).
# Centro aproximado -23.567, -46.702 — confirmar com a validação de fontes.
BBOX_PINHEIROS = BBox(sul=-23.5750, oeste=-46.7100, norte=-23.5600, leste=-46.6940)

# Fontes. "obrigatoria" = a conferência falha se o arquivo não chegar.
FONTES = {
    "clima": {"obrigatoria": True, "descricao": "Open-Meteo (Forecast API, horário)"},
    "osm": {"obrigatoria": True, "descricao": "OpenStreetMap via Overpass API"},
    "eventos_oculos": {"obrigatoria": True, "descricao": "Eventos dos óculos (SINTÉTICOS)"},
    "sp156": {"obrigatoria": False, "descricao": "SP156 georreferenciado via GeoSampa WFS"},
    "olhovivo": {"obrigatoria": False, "descricao": "SPTrans Olho Vivo — paradas"},
}

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_HOURLY = [
    "temperature_2m",
    "precipitation",
    "rain",
    "wind_gusts_10m",
    "visibility",
    "weather_code",
]

OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

GEOSAMPA_WFS_URL = "https://wfs.geosampa.prefeitura.sp.gov.br/geoserver/geoportal/wfs"

OLHOVIVO_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
OLHOVIVO_TERMOS = ["Pinheiros", "Teodoro Sampaio", "Faria Lima", "Cardeal Arcoverde"]

# Domínio de tipos de obstáculo (contrato com Applied Computer Vision).
TIPOS_OBSTACULO = {
    "placa": "acima_cintura",
    "galho_arvore": "acima_cintura",
    "orelhao": "acima_cintura",
    "cacamba": "abaixo_cintura",
    "poste": "acima_cintura",
    "carro_calcada": "abaixo_cintura",
    "buraco": "chao",
    "calcada_irregular": "chao",
    "lixeira": "abaixo_cintura",
    "entulho": "abaixo_cintura",
    "pessoa": "movel",
}

USER_AGENT = "GuidingHand-FIAP-GS2026/1.0 (projeto academico; contato via repositorio)"
