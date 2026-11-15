"""Verifica os arquivos de UMA coleta em chegada/ e gera a evidência para o relatório.

Roda sozinho ao final do menu 3. Também pode ser chamado direto:
    python scripts/verificar_chegada.py                 # coleta de hoje (data de São Paulo)
    python scripts/verificar_chegada.py --data 2026-11-16

Para cada fonte: existência, tamanho, SHA-256, JSON válido, datas (nome do arquivo x conteúdo),
volume e sinais de que o dado NÃO é real (simulador ou fixture de teste).
Para o SP156: se cada camada é de solicitações, completude da paginação e compatibilidade dos
campos com o mapeamento da Prata, lido do próprio código (databricks/lib/gh_transform.py).

Saída: docs/evidencias/coleta_<data>.md e .json. Código de saída 1 se houver problema bloqueante.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "scripts"))
import ambiente_python  # noqa: E402

ambiente_python.garantir(("requests",))  # reexecuta com o Python do projeto se preciso
sys.path.insert(0, str(RAIZ / "dags"))
sys.path.insert(0, str(RAIZ / "databricks" / "lib"))
from guiding_hand_lib import coletores, config  # noqa: E402
import sp156_mapeamento as mapa  # noqa: E402

SP = ZoneInfo("America/Sao_Paulo")
MARCAS_NAO_REAIS = ("RESPOSTA SIMULADA", "SIMULADOR_LOCAL", "_fixture", "FIXTURE DE TESTE", "ESTRUTURA PRESUMIDA")


def mapeamento_prata() -> tuple[dict, list]:
    """Mesmo mapeamento usado pela Prata (módulo comum databricks/lib/sp156_mapeamento.py)."""
    return mapa.campos_sp156(), mapa.MAPA_SERVICO_SP156


def _base(caminho: Path) -> dict:
    if not caminho.exists():
        return {"existe": False}
    bruto = caminho.read_bytes()
    texto = bruto.decode("utf-8", errors="replace")
    arquivo = str(caminho.relative_to(RAIZ)) if caminho.is_relative_to(RAIZ) else str(caminho)
    item = {"existe": True, "arquivo": arquivo, "bytes": len(bruto),
            "sha256": hashlib.sha256(bruto).hexdigest(),
            "modificado_em": datetime.fromtimestamp(caminho.stat().st_mtime, SP).isoformat(timespec="seconds"),
            "marcas_nao_reais": [m for m in MARCAS_NAO_REAIS if m in texto]}
    try:
        item["_json"] = json.loads(texto)
    except ValueError as exc:
        item["erro_json"] = str(exc)
    return item


def _clima(d: dict, item: dict, data_coleta: date) -> None:
    h = d.get("hourly", {})
    horas = h.get("time", [])
    dias = sorted({t[:10] for t in horas})
    grade = (d.get("latitude"), d.get("longitude"))
    lat, lon = config.BBOX_PINHEIROS.centro
    item.update(horas=len(horas), dias_no_conteudo=dias,
                esperado=str(data_coleta - timedelta(days=1)),
                precipitacao_total_mm=round(sum(x or 0 for x in h.get("precipitation", [])), 2),
                centro_celula_grade=list(grade),
                distancia_grade_km=round(coletores.distancia_km(lat, lon, *grade), 2) if None not in grade else None)
    if dias != [item["esperado"]] or len(horas) != 24:
        item.setdefault("problemas", []).append("clima deveria ter as 24 horas do dia anterior à coleta")


def _osm(d: dict, item: dict) -> None:
    els = d.get("elements", [])
    tags = Counter(f"{k}={v}" for e in els for k, v in (e.get("tags") or {}).items()
                   if k in ("barrier", "kerb", "tactile_paving", "highway", "power", "amenity", "natural", "footway"))
    fora = [e for e in els if (e.get("lat") or (e.get("center") or {}).get("lat")) is not None and not
            config.BBOX_PINHEIROS.contem(e.get("lat") or e["center"]["lat"], e.get("lon") or e["center"]["lon"])]
    item.update(elementos=len(els), base_osm_em=(d.get("osm3s") or {}).get("timestamp_osm_base"),
                principais_tags=tags.most_common(10), fora_do_recorte=len(fora))
    if not els:
        item.setdefault("problemas", []).append("OSM sem elementos")


def _sp156(d: dict, item: dict, campos_prata: dict, mapa_servico: list) -> None:
    camadas = []
    for c in d.get("camadas", []):
        feats = c.get("resposta", {}).get("features", [])
        campos = sorted({k for f in feats for k in (f.get("properties") or {})})
        minusculos = {k.lower(): k for k in campos}
        compat = {canon: next((minusculos[n] for n in nomes if n in minusculos), None)
                  for canon, nomes in campos_prata.items()}
        props = [{k.lower(): v for k, v in (f.get("properties") or {}).items()} for f in feats]

        def valor(p, canon):
            return next((p[n] for n in campos_prata[canon] if p.get(n) not in (None, "")), None)

        servicos = Counter(str(valor(p, "servico") or c.get("camada")) for p in props)
        mapeados = sum(n for s, n in servicos.items()
                       if any(chave in s.lower() for chave, _ in mapa_servico))
        aberturas = sorted(str(valor(p, "data_abertura"))[:10] for p in props if valor(p, "data_abertura"))
        coords = [f["geometry"]["coordinates"] for f in feats
                  if (f.get("geometry") or {}).get("type") == "Point"]
        no_recorte = sum(1 for lon, lat, *_ in coords if config.BBOX_PINHEIROS.contem(lat, lon))
        registro = {
            "camada": c.get("camada"), "feicoes": len(feats), "paginacao": c.get("paginacao"),
            "eh_solicitacao": coletores.eh_camada_de_solicitacoes(feats) if feats else None,
            "campos_disponiveis": campos,
            "compatibilidade_prata": compat,
            "campos_canonicos_sem_correspondencia": [k for k, v in compat.items() if v is None],
            "status_encontrados": Counter(str(valor(p, "status")) for p in props).most_common(8),
            "servicos_encontrados": servicos.most_common(8),
            "feicoes_com_tipo_de_obstaculo": mapeados,
            "abertura_mais_antiga": aberturas[0] if aberturas else None,
            "abertura_mais_recente": aberturas[-1] if aberturas else None,
            "pontos_no_recorte": f"{no_recorte}/{len(coords)}",
        }
        problemas = []
        if registro["eh_solicitacao"] is False:
            problemas.append("não parece camada de solicitações")
        if (c.get("paginacao") or {}).get("completo") is False:
            problemas.append("paginação incompleta")
        for canon in ("protocolo", "status", "data_abertura"):
            if compat[canon] is None:
                problemas.append(f"Prata não encontra o campo '{canon}'")
        if feats and mapeados == 0:
            problemas.append("nenhum serviço mapeia para um tipo de obstáculo (ficaria fora da Ouro)")
        if problemas:
            registro["problemas"] = problemas
        camadas.append(registro)
    item["camadas"] = camadas
    item["metadados_coleta"] = d.get("_coleta", {})
    item["total_feicoes"] = sum(c["feicoes"] for c in camadas)
    if any(c.get("problemas") for c in camadas):
        item.setdefault("problemas", []).append("há camadas com pendências (ver detalhe)")


def verificar(data_coleta: date, pasta: Path = RAIZ / "chegada", iniciada_em: datetime | None = None) -> dict:
    campos_prata, mapa_servico = mapeamento_prata()
    m = mapa.carregar_mapeamento()
    resultado = {"data_coleta": data_coleta.isoformat(),
                 "mapeamento_sp156": {"arquivo": str(mapa.ARQUIVO_MAPEAMENTO.relative_to(RAIZ)),
                                      "campos": m["campos"],
                                      "protocolo_composto_com_camada": m["protocolo_composto_com_camada"],
                                      "situacao_protocolo": m.get("situacao_protocolo"),
                                      "evidencia": m.get("evidencia")},
                 "execucao_iniciada_em": iniciada_em.isoformat(timespec="seconds") if iniciada_em else None,
                 "verificado_em": datetime.now(timezone.utc).astimezone(SP).isoformat(timespec="seconds"),
                 "fontes": {}}
    for fonte, meta in config.FONTES.items():
        item = _base(pasta / fonte / coletores.nome_arquivo(fonte, data_coleta))
        item["obrigatoria"] = meta["obrigatoria"]
        d = item.pop("_json", None)
        if d is not None:
            if fonte == "clima":
                _clima(d, item, data_coleta)
            elif fonte == "osm":
                _osm(d, item)
            elif fonte == "sp156":
                _sp156(d, item, campos_prata, mapa_servico)
            elif fonte == "eventos_oculos":
                item.update(eventos=len(d.get("eventos", [])), data_referencia=d.get("data_referencia"),
                            sintetico=d.get("gerador") == "sintetico_v1")
            elif fonte == "olhovivo":
                item.update(paradas=sum(len(b.get("resposta") or []) for b in d.get("buscas", [])))
        if item["existe"] and item.get("marcas_nao_reais") and fonte != "eventos_oculos":
            item.setdefault("problemas", []).append("conteúdo de SIMULADOR/TESTE, não é coleta real")
        if item["existe"] and iniciada_em and \
                datetime.fromisoformat(item["modificado_em"]) < iniciada_em - timedelta(seconds=5):
            item.setdefault("problemas", []).append(
                f"arquivo de uma execução ANTERIOR ({item['modificado_em']}): não foi regravado nesta coleta")
        if not item["existe"] and meta["obrigatoria"]:
            item.setdefault("problemas", []).append("arquivo obrigatório ausente")
        resultado["fontes"][fonte] = item
    bloqueantes = [f for f, i in resultado["fontes"].items()
                   if i.get("problemas") and (i["obrigatoria"] or i["existe"])]
    resultado["aprovada"] = not bloqueantes
    resultado["fontes_com_pendencia"] = bloqueantes
    return resultado


def escrever(resultado: dict, destino: Path = RAIZ / "docs" / "evidencias") -> Path:
    destino.mkdir(parents=True, exist_ok=True)
    base = destino / f"coleta_{resultado['data_coleta']}"
    base.with_suffix(".json").write_text(json.dumps(resultado, ensure_ascii=False, indent=1), encoding="utf-8")
    L = [f"# Verificação da coleta de {resultado['data_coleta']}", "",
         f"Verificado em {resultado['verificado_em']}. Resultado: "
         + ("**APROVADA**" if resultado["aprovada"] else "**COM PENDÊNCIAS**: " + ", ".join(resultado["fontes_com_pendencia"])),
         "", f"Mapeamento SP156 usado: `{resultado['mapeamento_sp156']['arquivo']}` — protocolo = "
         f"`{resultado['mapeamento_sp156']['campos'].get('protocolo')}`"
         + (" (composto com a camada)" if resultado['mapeamento_sp156']['protocolo_composto_com_camada'] else "")
         + f"; {resultado['mapeamento_sp156']['situacao_protocolo']}",
         "", "| Fonte | Arquivo | Bytes | Conteúdo | Pendências |", "|---|---|---|---|---|"]
    for fonte, i in resultado["fontes"].items():
        if not i["existe"]:
            L.append(f"| {fonte} | ausente | — | — | {'; '.join(i.get('problemas', ['opcional']))} |")
            continue
        resumo = {"clima": lambda: f"{i.get('horas')} h de {i.get('dias_no_conteudo')}; grade a {i.get('distancia_grade_km')} km",
                  "osm": lambda: f"{i.get('elementos')} elementos; base OSM {i.get('base_osm_em')}",
                  "sp156": lambda: f"{i.get('total_feicoes')} feições em {len(i.get('camadas', []))} camada(s)",
                  "eventos_oculos": lambda: f"{i.get('eventos')} eventos SINTÉTICOS de {i.get('data_referencia')}",
                  "olhovivo": lambda: f"{i.get('paradas')} paradas"}[fonte]()
        L.append(f"| {fonte} | `{i['arquivo']}` | {i['bytes']} | {resumo} | {'; '.join(i.get('problemas', [])) or 'nenhuma'} |")
    sp = resultado["fontes"].get("sp156", {})
    if sp.get("camadas"):
        L += ["", "## SP156: camadas e compatibilidade com a Prata", "",
              "| Camada | Feições | Completa | Solicitação? | Campos → Prata | Sem correspondência | Abertura (mín–máx) | Pendências |",
              "|---|---|---|---|---|---|---|---|"]
        for c in sp["camadas"]:
            pag = c.get("paginacao") or {}
            mapa = ", ".join(f"{k}←{v}" for k, v in c["compatibilidade_prata"].items() if v)
            L.append(f"| {c['camada']} | {c['feicoes']} (servidor: {pag.get('total_servidor')}) | {pag.get('completo')} "
                     f"| {c['eh_solicitacao']} | {mapa} | {', '.join(c['campos_canonicos_sem_correspondencia']) or '—'} "
                     f"| {c['abertura_mais_antiga']} – {c['abertura_mais_recente']} | {'; '.join(c.get('problemas', [])) or 'nenhuma'} |")
        L += ["", "Campos disponíveis por camada:", ""]
        L += [f"- `{c['camada']}`: {', '.join(c['campos_disponiveis'])}" for c in sp["camadas"]]
    caminho = base.with_suffix(".md")
    caminho.write_text("\n".join(L) + "\n", encoding="utf-8")
    return caminho


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", help="dia de coleta AAAA-MM-DD (padrão: hoje em São Paulo)")
    ap.add_argument("--chegada", type=Path, default=RAIZ / "chegada")
    ap.add_argument("--desde", help="início da execução (ISO 8601); arquivos mais antigos são apontados")
    a = ap.parse_args()
    d = date.fromisoformat(a.data) if a.data else datetime.now(SP).date()
    r = verificar(d, a.chegada, datetime.fromisoformat(a.desde) if a.desde else None)
    print(f"Verificação: {escrever(r)}")
    print("APROVADA" if r["aprovada"] else "COM PENDÊNCIAS: " + ", ".join(r["fontes_com_pendencia"]))
    sys.exit(0 if r["aprovada"] else 1)
