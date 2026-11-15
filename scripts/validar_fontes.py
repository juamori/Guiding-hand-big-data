"""Valida as fontes de dados do Guiding Hand A PARTIR DO SEU COMPUTADOR (com internet).

O que faz, sem você precisar editar nada:
  1. Open-Meteo: baixa o clima de ontem no centro do recorte de Pinheiros.
  2. OpenStreetMap (Overpass): conta postes, árvores, rebaixamentos, piso tátil etc. no recorte.
  3. GeoSampa WFS: lista TODAS as camadas, encontra as do SP156 ligadas a barreiras
     (buraco, calçada, árvore, sinalização...) e testa cada uma dentro do recorte.
  4. Portal de dados abertos (CKAN): verifica se responde JSON ou se bloqueia robôs (captcha).
  5. Olho Vivo: só se a variável de ambiente OLHOVIVO_TOKEN estiver definida (nunca grava o token).

Saídas (tudo é resultado REAL desta execução):
  docs/validacao_fontes/relatorio_<data-hora>.md   -> relatório legível (me envie este arquivo)
  docs/validacao_fontes/relatorio_<data-hora>.json -> mesmo conteúdo em JSON
  amostras/<fonte>_amostra.json                    -> pequenas amostras para o repositório (R09)
  airflow/descoberta_fontes.json                   -> camadas SP156 encontradas (usado pelo Airflow)
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import traceback
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "dags"))

from guiding_hand_lib import coletores, config  # noqa: E402

PALAVRAS_SP156 = ["buraco", "pavimenta", "calcada", "calçada", "passeio", "arvore", "árvore", "poda",
                  "sinaliza", "placa", "poste", "ilumina", "lixeira", "entulho", "cacamba", "caçamba",
                  "156"]
BBOX = config.BBOX_PINHEIROS
SAIDA_DOCS = RAIZ / "docs" / "validacao_fontes"
SAIDA_AMOSTRAS = RAIZ / "amostras"


def _limitar(obj, n=5):
    """Reduz listas grandes para gerar amostras pequenas e legíveis."""
    if isinstance(obj, list):
        return [_limitar(x, n) for x in obj[:n]]
    if isinstance(obj, dict):
        return {k: _limitar(v, n) for k, v in obj.items()}
    return obj


def salvar_amostra(nome: str, dados) -> None:
    SAIDA_AMOSTRAS.mkdir(parents=True, exist_ok=True)
    (SAIDA_AMOSTRAS / f"{nome}_amostra.json").write_text(
        json.dumps({"_amostra": f"recorte real coletado em {datetime.now().isoformat(timespec='seconds')}",
                    "dados": _limitar(dados)}, ensure_ascii=False, indent=1), encoding="utf-8")


def testar(nome, funcao):
    inicio = time.time()
    try:
        r = funcao()
        r.setdefault("status", "OK")
    except Exception as exc:  # registra qualquer falha como resultado real
        r = {"status": "FALHOU", "erro": f"{type(exc).__name__}: {exc}"[:800],
             "detalhe": traceback.format_exc(limit=2)[-600:]}
    r["segundos"] = round(time.time() - inicio, 1)
    print(f"  [{r['status']}] {nome} ({r['segundos']}s)")
    return r


# ----------------------------------------------------------------------------- testes
def t_open_meteo(s):
    ontem = date.today() - timedelta(days=1)
    r = coletores.coletar_clima(s, ontem, BBOX)
    dados = json.loads(r.conteudo)
    h = dados["hourly"]
    salvar_amostra("clima", dados)
    return {"url": r.url, "data_referencia": ontem.isoformat(), "horas": r.registros,
            "variaveis": [k for k in h if k != "time"],
            "precipitacao_total_mm": round(sum(x or 0 for x in h.get("precipitation", [])), 2),
            "ponto_pedido": r.detalhes.get("ponto_pedido"),
            "centro_celula_grade": r.detalhes.get("centro_celula_grade"),
            "distancia_grade_km": r.detalhes.get("distancia_grade_km"),
            "celula_dentro_do_recorte": r.detalhes.get("celula_dentro_do_recorte"),
            "observacao": "a API devolve o centro da célula da grade do modelo; o clima é contexto regional do recorte"}


def t_overpass(s):
    r = coletores.coletar_osm(s, BBOX)
    dados = json.loads(r.conteudo)
    cont = Counter()
    for e in dados["elements"]:
        t = e.get("tags", {})
        for chave in ("barrier", "kerb", "tactile_paving", "highway", "power", "man_made",
                      "amenity", "natural", "footway"):
            if chave in t:
                cont[f"{chave}={t[chave]}"] += 1
    salvar_amostra("osm", dados)
    return {"instancia": r.detalhes.get("instancia"), "elementos": r.registros,
            "principais_tags": cont.most_common(15)}


# Uma camada só entra como "SP156" se for de SOLICITAÇÕES (zeladoria), não cadastro de infraestrutura.
SINAIS_SOLICITACAO_NOME = ("156", "solicita", "zeladoria", "reclama")


def classificar_camada(nome: str, titulo: str, campos: list[str]) -> str:
    texto = f"{nome} {titulo}".lower()
    if any(sinal in texto for sinal in SINAIS_SOLICITACAO_NOME) or \
            coletores.eh_camada_de_solicitacoes([{"properties": {c: None for c in campos}}]):
        return "solicitacao_sp156"
    return "infraestrutura_cadastral"


def t_geosampa(s):
    caps = s.get(config.GEOSAMPA_WFS_URL,
                 params={"service": "WFS", "version": "1.1.0", "request": "GetCapabilities"}, timeout=120)
    caps.raise_for_status()
    if caps.text.lstrip().startswith("<html") or "captcha" in caps.text[:2000].lower():
        raise coletores.FonteIndisponivel("GetCapabilities devolveu HTML/captcha")
    raiz = ET.fromstring(caps.content)
    camadas = []
    for ft in raiz.iter():
        if ft.tag.endswith("FeatureType"):
            nome = next((c.text for c in ft if c.tag.endswith("Name")), None)
            titulo = next((c.text for c in ft if c.tag.endswith("Title")), "") or ""
            if nome:
                camadas.append((nome, titulo))
    candidatas = [(n, t) for n, t in camadas
                  if any(p in f"{n} {t}".lower() for p in PALAVRAS_SP156)]
    testes = []
    for nome, titulo in candidatas[:25]:
        try:
            # 1) amostra pequena para conhecer os campos e classificar a camada
            r = s.get(config.GEOSAMPA_WFS_URL, params={
                "service": "WFS", "version": "1.1.0", "request": "GetFeature", "typeName": nome,
                "srsName": "EPSG:4326", "bbox": BBOX.como_wfs_4326(), "maxFeatures": 50,
                "outputFormat": "application/json"}, timeout=120)
            r.raise_for_status()
            amostra = r.json().get("features", [])
            props = sorted({k for f in amostra for k in (f.get("properties") or {})})
            tipo = classificar_camada(nome, titulo, props)
            geom = amostra[0]["geometry"]["type"] if amostra and amostra[0].get("geometry") else None
            item = {"camada": nome, "titulo": titulo, "classificacao": tipo, "geometria": geom,
                    "coordenada_exemplo": amostra[0]["geometry"]["coordinates"] if geom == "Point" else None,
                    "campos": props, "total_no_recorte_servidor": coletores._total_wfs(
                        s, config.GEOSAMPA_WFS_URL, nome, BBOX, 120)}
            # 2) só as camadas de solicitações são baixadas por completo (paginadas), como no Airflow
            if tipo == "solicitacao_sp156" and amostra:
                fc, info = coletores.buscar_camada_wfs(s, config.GEOSAMPA_WFS_URL, nome, BBOX, 120)
                item.update(feicoes_coletadas=info["coletadas"], paginas=info["paginas"],
                            completo=info["completo"], paginacao_ignorada=info["paginacao_ignorada"])
                salvar_amostra("sp156_" + re.sub(r"[^a-z0-9]+", "_", nome.lower()), fc)
            else:
                item["feicoes_na_amostra"] = len(amostra)
            testes.append(item)
        except Exception as exc:
            testes.append({"camada": nome, "titulo": titulo, "erro": str(exc)[:300]})
    sp156 = [t["camada"] for t in testes
             if t.get("classificacao") == "solicitacao_sp156" and t.get("feicoes_coletadas")]
    infra = [t["camada"] for t in testes if t.get("classificacao") == "infraestrutura_cadastral"]
    descoberta = {"gh_sp156_camadas": sp156, "infraestrutura_nao_usada_como_sp156": infra,
                  "descoberto_em": datetime.now().isoformat(timespec="seconds")}
    (RAIZ / "airflow").mkdir(exist_ok=True)
    (RAIZ / "airflow" / "descoberta_fontes.json").write_text(json.dumps(descoberta, indent=1, ensure_ascii=False),
                                                           encoding="utf-8")
    return {"total_camadas_wfs": len(camadas), "candidatas_por_palavra_chave": len(candidatas),
            "camadas_sp156_usadas_no_airflow": sp156, "camadas_infraestrutura_ignoradas": infra,
            "testes": testes,
            "status": "OK" if sp156 else "SEM_CAMADA_DE_SOLICITACOES_SP156"}


def _ano(texto) -> int | None:
    anos = [int(a) for a in re.findall(r"(?<!\d)(20[0-3]\d)(?!\d)", str(texto or ""))]
    return max(anos) if anos else None


def t_ckan(s):
    """Portal CKAN: NÃO é fonte das coletas. Serve para documentar o que existe e de quando é."""
    r = s.get("https://dados.prefeitura.sp.gov.br/api/3/action/package_show",
              params={"id": "dados-do-sp156"}, timeout=60)
    inicio = r.text.lstrip()[:300]
    if inicio.startswith("<") or "codigo" in inicio.lower():
        return {"status": "BLOQUEADO_CAPTCHA", "http": r.status_code, "inicio_resposta": inicio}
    pacote = r.json().get("result", {})
    recursos = []
    for x in pacote.get("resources", []):
        recursos.append({"nome": x.get("name"), "formato": x.get("format"), "url": x.get("url"),
                         "ano_dos_dados": _ano(x.get("name")) or _ano(x.get("url")),
                         "metadados_atualizados": x.get("last_modified") or x.get("metadata_modified")
                         or x.get("created")})
    recursos.sort(key=lambda x: (x["ano_dos_dados"] or 0, str(x["metadados_atualizados"])))
    anos = [x["ano_dos_dados"] for x in recursos if x["ano_dos_dados"]]
    mais_recente = max(anos) if anos else None
    historico = mais_recente is None or mais_recente < date.today().year - 2
    return {"http": r.status_code, "titulo": pacote.get("title"), "total_recursos": len(recursos),
            "ano_mais_recente_dos_dados": mais_recente, "recursos": recursos,
            "uso_no_projeto": ("contexto histórico apenas; NÃO coletado pelo Airflow" if historico
                               else "não coletado pelo Airflow (o SP156 atual vem do GeoSampa)"),
            "status": "OK_HISTORICO" if historico else "OK"}


def t_olhovivo(s):
    token = os.environ.get("OLHOVIVO_TOKEN")
    if not token:
        return {"status": "NAO_TESTADO", "motivo": "variável OLHOVIVO_TOKEN não definida"}
    r = coletores.coletar_olhovivo(s, token, config.OLHOVIVO_TERMOS)
    dados = json.loads(r.conteudo)
    no_recorte = [p for b in dados["buscas"] for p in (b["resposta"] or [])
                  if isinstance(p, dict) and p.get("py") and BBOX.contem(p["py"], p["px"])]
    campos = sorted({k for b in dados["buscas"] for p in (b["resposta"] or [])[:3] for k in p})
    salvar_amostra("olhovivo", dados)
    return {"paradas_retornadas": r.registros, "paradas_no_recorte": len(no_recorte), "campos": campos}


# ----------------------------------------------------------------------------- relatório
def escrever_relatorio(resultados: dict) -> Path:
    SAIDA_DOCS.mkdir(parents=True, exist_ok=True)
    carimbo = datetime.now().strftime("%Y-%m-%d_%H%M")
    (SAIDA_DOCS / f"relatorio_{carimbo}.json").write_text(
        json.dumps(resultados, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    linhas = [f"# Validação das fontes — {datetime.now():%d/%m/%Y %H:%M}", "",
              f"Recorte: {BBOX} (entorno da Estação Pinheiros)", "",
              "| Fonte | Situação | Tempo (s) |", "|---|---|---|"]
    for nome, r in resultados.items():
        linhas.append(f"| {nome} | {r['status']} | {r['segundos']} |")
    for nome, r in resultados.items():
        linhas += ["", f"## {nome}", "", "```json",
                   json.dumps({k: v for k, v in r.items() if k != "detalhe"}, ensure_ascii=False,
                              indent=1, default=str)[:6000], "```"]
    caminho = SAIDA_DOCS / f"relatorio_{carimbo}.md"
    caminho.write_text("\n".join(linhas), encoding="utf-8")
    return caminho


def main() -> int:
    print("Validando fontes do Guiding Hand (recorte Pinheiros)...")
    s = coletores.criar_sessao(total_retries=2)
    resultados = {
        "Open-Meteo (clima)": testar("Open-Meteo", lambda: t_open_meteo(s)),
        "OpenStreetMap / Overpass": testar("Overpass", lambda: t_overpass(s)),
        "GeoSampa WFS (SP156)": testar("GeoSampa", lambda: t_geosampa(s)),
        "Portal CKAN (SP156 CSV)": testar("CKAN", lambda: t_ckan(s)),
        "SPTrans Olho Vivo": testar("Olho Vivo", lambda: t_olhovivo(s)),
    }
    caminho = escrever_relatorio(resultados)
    print(f"\nRelatório: {caminho}")
    print("Envie esse arquivo .md para continuarmos.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
