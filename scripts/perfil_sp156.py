"""Perfil dos dados REAIS do SP156 já coletados (sem nenhuma chamada externa).

Lê chegada/sp156/sp156_<data>.json e mede, para cada campo identificador candidato:
preenchimento, unicidade dentro de cada camada e sobreposição entre camadas (o mesmo valor
em duas camadas é a MESMA solicitação, ou valores iguais para solicitações diferentes?).
Também mostra datas, status e serviços, sempre pelas mesmas regras da Prata.

Regra de decisão do PROTOCOLO (documentada no relatório):
  1. Elegível: preenchido em >= 99% dos registros, sem repetição dentro de uma camada e
     sem colisão entre camadas (mesmo valor com abertura, posição ou serviço diferentes).
  2. Entre elegíveis: o que reconhece mais solicitações repetidas entre camadas (consolida
     sem duplicar); empate -> o mais preenchido; empate -> ordem da lista de candidatos.
  3. Nenhum elegível, mas há um único por camada: chave composta camada + campo.
  4. Caso contrário: sem decisão (o mapeamento continua pendente).

Uso:
  python scripts/perfil_sp156.py                 # perfila a coleta mais recente
  python scripts/perfil_sp156.py --data 2026-11-15 --aplicar   # e grava a escolha no mapeamento
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "databricks" / "lib"))
import sp156_mapeamento as mapa  # noqa: E402

SP = ZoneInfo("America/Sao_Paulo")
CANDIDATOS_PREFERIDOS = ["cd_identificador", "cd_identificador_original_atendimento_cidadao"]
LIMIAR_PREENCHIMENTO = 99.0


def _vazio(v) -> bool:
    return v is None or str(v).strip() == ""


MARCAS_NAO_REAIS = ("SIMULADOR_LOCAL", "RESPOSTA SIMULADA", "FIXTURE", "ESTRUTURA PRESUMIDA")


def dados_nao_reais(arquivo: Path) -> list[str]:
    texto = arquivo.read_text(encoding="utf-8", errors="replace")
    return [m for m in MARCAS_NAO_REAIS if m in texto]


def carregar_registros(arquivo: Path) -> list[dict]:
    pacote = json.loads(arquivo.read_text(encoding="utf-8"))
    registros = []
    for c in pacote.get("camadas", []):
        for f in c.get("resposta", {}).get("features", []):
            props = {k.lower(): v for k, v in (f.get("properties") or {}).items()}
            geo = f.get("geometry") or {}
            coord = tuple(round(x, 5) for x in geo.get("coordinates", [])[:2]) if geo.get("type") == "Point" else None
            registros.append({"camada": c.get("camada"), "props": props, "coord": coord})
    return registros


def _valor(props: dict, canon: str, campos: dict):
    return next((props[n] for n in campos[canon] if not _vazio(props.get(n))), None)


def assinatura(r: dict, campos: dict) -> tuple:
    """O que identifica UMA solicitação além do id: abertura, posição e serviço."""
    return (str(_valor(r["props"], "data_abertura", campos)), r["coord"], str(_valor(r["props"], "servico", campos)))


def _formato(valores: list) -> str:
    amostra = [str(v) for v in valores[:500]]
    if not amostra:
        return "—"
    tamanhos = Counter(len(v) for v in amostra)
    numerico = all(v.isdigit() for v in amostra)
    return (f"{'numérico' if numerico else 'alfanumérico'}, tamanho "
            + ", ".join(f"{t} ({n})" for t, n in tamanhos.most_common(3)))


def perfil_candidato(registros: list[dict], campo: str, campos: dict) -> dict:
    total = len(registros)
    preenchidos = [r for r in registros if not _vazio(r["props"].get(campo))]
    por_camada: dict[str, Counter] = defaultdict(Counter)
    camadas_do_valor: dict[str, set] = defaultdict(set)
    assinaturas: dict[str, set] = defaultdict(set)
    for r in preenchidos:
        v = str(r["props"][campo]).strip()
        por_camada[r["camada"]][v] += 1
        camadas_do_valor[v].add(r["camada"])
        assinaturas[v].add(assinatura(r, campos))
    repetidos_dentro = sum(n - 1 for cont in por_camada.values() for n in cont.values() if n > 1)
    entre_camadas = [v for v, cs in camadas_do_valor.items() if len(cs) > 1]
    colisoes = [v for v in entre_camadas if len(assinaturas[v]) > 1]
    return {
        "campo": campo, "registros": total, "preenchidos": len(preenchidos),
        "preenchimento_pct": round(100 * len(preenchidos) / total, 2) if total else 0.0,
        "valores_distintos": len(camadas_do_valor),
        "repeticoes_dentro_da_mesma_camada": repetidos_dentro,
        "valores_em_mais_de_uma_camada": len(entre_camadas),
        "mesma_solicitacao_em_varias_camadas": len(entre_camadas) - len(colisoes),
        "colisoes_entre_camadas": len(colisoes),
        "formato": _formato([r["props"][campo] for r in preenchidos]),
    }


def decidir(perfis: list[dict]) -> dict:
    elegiveis = [p for p in perfis if p["preenchimento_pct"] >= LIMIAR_PREENCHIMENTO
                 and p["repeticoes_dentro_da_mesma_camada"] == 0 and p["colisoes_entre_camadas"] == 0]
    if elegiveis:
        ordem = {c: i for i, c in enumerate(CANDIDATOS_PREFERIDOS)}
        escolhido = sorted(elegiveis, key=lambda p: (-p["mesma_solicitacao_em_varias_camadas"],
                                                     -p["preenchimento_pct"], ordem.get(p["campo"], 99)))[0]
        motivo = ("único em todas as camadas, preenchido em "
                  f"{escolhido['preenchimento_pct']}% e reconhece {escolhido['mesma_solicitacao_em_varias_camadas']} "
                  "solicitações repetidas entre camadas")
        return {"protocolo": escolhido["campo"], "composto_com_camada": False, "motivo": motivo,
                "elegiveis": [p["campo"] for p in elegiveis]}
    so_por_camada = [p for p in perfis if p["preenchimento_pct"] >= LIMIAR_PREENCHIMENTO
                     and p["repeticoes_dentro_da_mesma_camada"] == 0]
    if so_por_camada:
        escolhido = max(so_por_camada, key=lambda p: p["preenchimento_pct"])
        return {"protocolo": escolhido["campo"], "composto_com_camada": True, "elegiveis": [],
                "motivo": "nenhum identificador é único entre camadas sem colisão; chave = camada + "
                          f"{escolhido['campo']} (único dentro de cada camada)"}
    return {"protocolo": None, "composto_com_camada": False, "elegiveis": [],
            "motivo": "nenhum campo atende ao preenchimento mínimo e à unicidade; decisão manual necessária"}


def consolidacao(registros: list[dict], decisao: dict) -> dict:
    campo = decisao["protocolo"]
    if not campo:
        return {}
    chaves = [(f"{r['camada']}:" if decisao["composto_com_camada"] else "") + str(r["props"].get(campo)).strip()
              for r in registros if not _vazio(r["props"].get(campo))]
    return {"registros_nas_camadas": len(registros), "solicitacoes_unicas": len(set(chaves)),
            "registros_repetidos_que_a_prata_consolida": len(chaves) - len(set(chaves)),
            "registros_sem_protocolo": len(registros) - len(chaves)}


def perfil(arquivo: Path) -> dict:
    campos = mapa.campos_sp156()
    registros = carregar_registros(arquivo)
    todos_campos = sorted({k for r in registros for k in r["props"]})
    candidatos = [c for c in CANDIDATOS_PREFERIDOS if c in todos_campos] + sorted(
        c for c in todos_campos if ("identificador" in c or "protocolo" in c) and c not in CANDIDATOS_PREFERIDOS)
    perfis = [perfil_candidato(registros, c, campos) for c in candidatos]
    decisao = decidir(perfis)
    p = [{"aberta": str(_valor(r["props"], "data_abertura", campos) or "")[:10], **r} for r in registros]
    datas = sorted(x["aberta"] for x in p if x["aberta"])
    ano_atual = datetime.now(SP).year
    camadas = Counter(r["camada"] for r in registros)
    status = Counter(str(_valor(r["props"], "status", campos)) for r in registros)
    servicos = Counter(str(_valor(r["props"], "servico", campos)) for r in registros)
    return {
        "dados_nao_reais": dados_nao_reais(arquivo),
        "arquivo": str(arquivo.relative_to(RAIZ)) if arquivo.is_relative_to(RAIZ) else str(arquivo),
        "perfilado_em": datetime.now(SP).isoformat(timespec="seconds"),
        "registros": len(registros), "camadas": dict(camadas), "campos_disponiveis": todos_campos,
        "candidatos_protocolo": perfis, "decisao_protocolo": decisao,
        "consolidacao": consolidacao(registros, decisao),
        "datas": {"abertura_mais_antiga": datas[0] if datas else None,
                  "abertura_mais_recente": datas[-1] if datas else None,
                  "por_camada": {c: [min(x["aberta"] for x in p if x["camada"] == c and x["aberta"]),
                                     max(x["aberta"] for x in p if x["camada"] == c and x["aberta"])]
                                 for c in camadas if any(x["camada"] == c and x["aberta"] for x in p)},
                  "historico": bool(datas) and int(datas[-1][:4]) < ano_atual},
        "formatos_de_data": {canon: Counter(re.sub(r"\d", "9", str(_valor(r["props"], canon, campos)))
                                            for r in registros).most_common(5)
                             for canon in ("data_abertura", "data_conclusao")},
        "status": [{"valor": v, "n": n, "classificacao_prata": mapa.classificar_status(None if v == "None" else v)}
                   for v, n in status.most_common()],
        "servicos": [{"valor": v, "n": n, "tipo_obstaculo": mapa.tipo_obstaculo(v)} for v, n in servicos.most_common()],
    }


def _situacao_atual() -> str:
    m = mapa.carregar_mapeamento()
    return (f"Continua valendo: protocolo = {m['campos'].get('protocolo')!r} "
            f"({m.get('situacao_protocolo') or 'sem decisão registrada'}).")


def aplicar(resultado: dict, evidencia: Path) -> Path:
    d = resultado["decisao_protocolo"]
    # As recusas acontecem ANTES de qualquer leitura/gravação: o arquivo fica byte a byte como estava.
    if resultado.get("dados_nao_reais"):
        raise SystemExit("Arquivo com dados de SIMULADOR/TESTE: mapeamento NÃO alterado. " + _situacao_atual())
    if not d["protocolo"]:
        raise SystemExit("Sem decisão de protocolo: mapeamento NÃO alterado. " + d["motivo"] + ". " + _situacao_atual())
    atual = mapa.carregar_mapeamento()
    atual["campos"]["protocolo"] = d["protocolo"]
    atual["protocolo_composto_com_camada"] = d["composto_com_camada"]
    atual["situacao_protocolo"] = f"DECIDIDO pelos dados reais em {resultado['perfilado_em']}: {d['motivo']}"
    atual["evidencia"] = str(evidencia.relative_to(RAIZ)) if evidencia.is_relative_to(RAIZ) else str(evidencia)
    mapa.ARQUIVO_MAPEAMENTO.write_text(json.dumps(atual, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return mapa.ARQUIVO_MAPEAMENTO


def escrever(r: dict, destino: Path) -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.with_suffix(".json").write_text(json.dumps(r, ensure_ascii=False, indent=1), encoding="utf-8")
    d, c, dt = r["decisao_protocolo"], r["consolidacao"], r["datas"]
    origem = ("**ATENÇÃO: arquivo com dados de SIMULADOR/TESTE — não use esta decisão.**"
              if r.get("dados_nao_reais") else "Dados reais coletados.")
    L = [f"# Perfil do SP156 — `{r['arquivo']}`", "", origem, "",
         f"Perfilado em {r['perfilado_em']}, sem chamadas externas. {r['registros']} registros em "
         f"{len(r['camadas'])} camadas.", "", "## Protocolo: candidatos", "",
         "| Campo | Preenchimento | Distintos | Repetidos na mesma camada | Em 2+ camadas | Mesma solicitação | Colisões | Formato |",
         "|---|---|---|---|---|---|---|---|"]
    L += [f"| `{p['campo']}` | {p['preenchimento_pct']}% ({p['preenchidos']}/{p['registros']}) | {p['valores_distintos']} "
          f"| {p['repeticoes_dentro_da_mesma_camada']} | {p['valores_em_mais_de_uma_camada']} "
          f"| {p['mesma_solicitacao_em_varias_camadas']} | {p['colisoes_entre_camadas']} | {p['formato']} |"
          for p in r["candidatos_protocolo"]]
    L += ["", f"**Decisão:** `{d['protocolo']}`" + (" (composto com a camada)" if d["composto_com_camada"] else "")
          + f" — {d['motivo']}.", ""]
    if c:
        L += ["## Consolidação das camadas", "",
              f"{c['registros_nas_camadas']} registros → **{c['solicitacoes_unicas']} solicitações únicas**; "
              f"{c['registros_repetidos_que_a_prata_consolida']} repetições entre camadas serão unificadas pelo MERGE "
              f"da Prata; {c['registros_sem_protocolo']} sem protocolo (barrados pela regra RQ1).", ""]
    L += ["## Camadas", ""] + [f"- `{k}`: {v} registros; abertura {dt['por_camada'].get(k)}" for k, v in r["camadas"].items()]
    L += ["", "## Datas", "",
          f"Aberturas de {dt['abertura_mais_antiga']} a {dt['abertura_mais_recente']}."
          + (" **Dados históricos**: não representam a situação atual das calçadas." if dt["historico"] else ""), "",
          "Formatos observados (dígitos trocados por 9): " + "; ".join(
              f"{k}: " + ", ".join(f"`{f}` ({n})" for f, n in v) for k, v in r["formatos_de_data"].items()), "",
          "## Status (regra da Prata)", "", "| Valor original | Registros | Prata |", "|---|---|---|"]
    L += [f"| {s['valor']} | {s['n']} | {s['classificacao_prata']} |" for s in r["status"]]
    L += ["", "## Serviços (tipo de obstáculo)", "", "| Serviço | Registros | Tipo |", "|---|---|---|"]
    L += [f"| {s['valor']} | {s['n']} | {s['tipo_obstaculo'] or '— (não entra na Ouro)'} |" for s in r["servicos"][:40]]
    caminho = destino.with_suffix(".md")
    caminho.write_text("\n".join(L) + "\n", encoding="utf-8")
    return caminho


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", help="dia de coleta AAAA-MM-DD (padrão: arquivo SP156 mais recente)")
    ap.add_argument("--chegada", type=Path, default=RAIZ / "chegada")
    ap.add_argument("--aplicar", action="store_true", help="grava a decisão em databricks/lib/mapeamento_sp156.json")
    a = ap.parse_args()
    if a.data:
        arq = a.chegada / "sp156" / f"sp156_{a.data}.json"
    else:
        arqs = sorted((a.chegada / "sp156").glob("sp156_*.json"))
        arq = arqs[-1] if arqs else None
    if not arq or not arq.exists():
        raise SystemExit("Arquivo SP156 não encontrado em chegada/sp156/")
    res = perfil(arq)
    data_arq = arq.stem.split("_")[-1]
    md = escrever(res, RAIZ / "docs" / "evidencias" / f"perfil_sp156_{data_arq}")
    print(f"Perfil: {md}")
    print(f"Decisão do protocolo: {res['decisao_protocolo']['protocolo']} — {res['decisao_protocolo']['motivo']}")
    if a.aplicar:
        print(f"Mapeamento atualizado: {aplicar(res, md)}")
