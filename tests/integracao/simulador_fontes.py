"""Servidor HTTP local que SIMULA as 4 APIs externas, para testes de integração do DAG
quando não há internet (ex.: CI ou sandbox). As respostas seguem o formato documentado
de cada API e são marcadas como simuladas. NUNCA usar como fonte da entrega.

Uso: python tests/integracao/simulador_fontes.py 8765
"""
import json
import sys
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

AVISO = "RESPOSTA SIMULADA PARA TESTE DE INTEGRACAO - NAO E DADO REAL"


def clima(qs):
    dia = qs.get("start_date", [date.today().isoformat()])[0]
    horas = [f"{dia}T{h:02d}:00" for h in range(24)]
    precip = [1.2 if 15 <= h <= 17 else 0.0 for h in range(24)]
    return {"_aviso": AVISO, "latitude": -23.5625, "longitude": -46.6875, "timezone": "America/Sao_Paulo",
            "hourly": {"time": horas, "temperature_2m": [20.0 + h % 6 for h in range(24)],
                       "precipitation": precip, "rain": precip, "wind_gusts_10m": [15.0] * 24,
                       "visibility": [24000.0] * 24, "weather_code": [61 if p else 2 for p in precip]}}


OSM = {"_aviso": AVISO, "elements": [
    {"type": "node", "id": 1, "_origem": "SIMULADOR_LOCAL", "lat": -23.5668, "lon": -46.7015, "tags": {"highway": "street_lamp"}},
    {"type": "node", "id": 2, "lat": -23.5651, "lon": -46.6990, "tags": {"natural": "tree"}},
    {"type": "node", "id": 3, "lat": -23.5660, "lon": -46.7001, "tags": {"kerb": "lowered"}}]}

SP156 = {"type": "FeatureCollection", "_aviso": AVISO, "features": [
    {"type": "Feature", "id": "sim.1", "geometry": {"type": "Point", "coordinates": [-46.7005, -23.5662]},
     "properties": {"_origem": "SIMULADOR_LOCAL", "protocolo": "SIM-1", "servico": "Tapa-buraco", "status": "ABERTA",
                    "data_abertura": "2026-11-01T09:00:00Z"}}]}

TETO_SERVIDOR = 500
SP156_PAGINADO = [
    {"type": "Feature", "id": f"sim.{i}", "geometry": {"type": "Point",
     "coordinates": [-46.7080 + (i % 30) * 0.0004, -23.5730 + (i // 30) * 0.0004]},
     "properties": {"_origem": "SIMULADOR_LOCAL", "protocolo": f"SIM-{i}", "servico": "Tapa-buraco",
                    "status": "FINALIZADA" if i % 3 == 0 else "ABERTA",
                    "data_abertura": "2026-11-01T09:00:00Z"}} for i in range(1234)]
ARVORES = [{"type": "Feature", "id": f"arv.{i}", "geometry": {"type": "Point", "coordinates": [-46.70, -23.567]},
            "properties": {"_origem": "SIMULADOR_LOCAL", "especie": "ipe", "altura_m": 8}} for i in range(800)]

PARADAS = [{"_origem": "SIMULADOR_LOCAL", "cp": 1, "np": "PARADA SIMULADA", "ed": "R TESTE", "py": -23.5672, "px": -46.7019}]


class Manipulador(BaseHTTPRequestHandler):
    def _json(self, obj, tipo="application/json"):
        corpo = (obj if isinstance(obj, str) else json.dumps(obj)).encode()
        self.send_response(200)
        self.send_header("Content-Type", tipo)
        self.send_header("Content-Length", str(len(corpo)))
        self.end_headers()
        self.wfile.write(corpo)

    def do_GET(self):
        u = urlparse(self.path)
        qs = parse_qs(u.query)
        if u.path.endswith("/v1/forecast"):
            return self._json(clima(qs))
        if u.path.endswith("/wfs"):
            return self._wfs(qs)
        if u.path.endswith("/Parada/Buscar"):
            return self._json(PARADAS)
        self.send_error(404)

    def _wfs(self, qs):
        req = qs.get("request", [""])[0]
        if req == "GetCapabilities":
            xml = ('<wfs:WFS_Capabilities xmlns:wfs="http://www.opengis.net/wfs"><FeatureTypeList>'
                   '<FeatureType><Name>geoportal:sp156_tapa_buraco</Name><Title>SP156 tapa-buraco</Title></FeatureType>'
                   '<FeatureType><Name>geoportal:arvore_viaria</Name><Title>Arvores</Title></FeatureType>'
                   '<FeatureType><Name>geoportal:limite_distrito</Name><Title>Distritos</Title></FeatureType>'
                   '</FeatureTypeList></wfs:WFS_Capabilities>')
            return self._json(xml, "text/xml")
        camada = qs.get("typeName", [""])[0]
        feats = SP156_PAGINADO if "sp156" in camada else ARVORES
        if qs.get("resultType", [""])[0] == "hits":
            return self._json(f'<wfs:FeatureCollection numberOfFeatures="{len(feats)}"/>', "text/xml")
        inicio = int(qs.get("startIndex", ["0"])[0])
        pedido = int(qs.get("maxFeatures", ["1000000"])[0])
        tamanho = min(pedido, TETO_SERVIDOR)  # o servidor simulado tem teto de 500 por resposta
        return self._json({"type": "FeatureCollection", "_aviso": AVISO, "features": feats[inicio:inicio + tamanho]})

    def do_POST(self):
        u = urlparse(self.path)
        self.rfile.read(int(self.headers.get("Content-Length", 0) or 0))
        if u.path.endswith("/interpreter"):
            return self._json(OSM)
        if u.path.endswith("/Login/Autenticar"):
            return self._json("true", "application/json")
        self.send_error(404)

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    porta = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    ThreadingHTTPServer(("127.0.0.1", porta), Manipulador).serve_forever()
