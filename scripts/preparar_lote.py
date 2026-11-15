"""Prepara um lote de arquivos para upload manual no Volume do Databricks (Requisito 04).

- Copia para lotes/lote_<N>/ apenas os arquivos de chegada/ que AINDA NÃO foram enviados.
- Todos os arquivos ficam numa única pasta (o Auto Loader identifica a fonte pelo nome).
- Registra o lote em docs/lotes/lote_<N>.md (evidência para o relatório) e em
  lotes/registro_lotes.json (controle de quais arquivos já foram enviados).
- Avisa se o lote não tiver um dia de coleta diferente dos lotes anteriores.

Uso:  python scripts/preparar_lote.py --lote 1
      python scripts/preparar_lote.py --lote 2 --datas 2026-11-13
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
PADRAO = re.compile(r"^([a-z0-9_]+)_(\d{4}-\d{2}-\d{2})\.json$")
FONTES = {"clima", "osm", "eventos_oculos", "sp156", "olhovivo"}


def listar_chegada(chegada: Path) -> list[tuple[Path, str, str]]:
    itens = []
    for p in sorted(chegada.rglob("*.json")):
        m = PADRAO.match(p.name)
        if m and m.group(1) in FONTES and "_manifestos" not in p.parts:
            itens.append((p, m.group(1), m.group(2)))
    return itens


def preparar(lote: int, chegada: Path, pasta_lotes: Path, pasta_docs: Path,
             datas: list[str] | None = None) -> dict:
    registro_path = pasta_lotes / "registro_lotes.json"
    registro = json.loads(registro_path.read_text(encoding="utf-8")) if registro_path.exists() else {"lotes": {}}
    if str(lote) in registro["lotes"]:
        raise SystemExit(f"O lote {lote} já foi preparado. Use o próximo número.")
    enviados = {a["arquivo"] for l in registro["lotes"].values() for a in l["arquivos"]}
    datas_anteriores = {a["data_coleta"] for l in registro["lotes"].values() for a in l["arquivos"]}

    candidatos = [(p, f, d) for p, f, d in listar_chegada(chegada)
                  if p.name not in enviados and (not datas or d in datas)]
    if not candidatos:
        raise SystemExit("Nenhum arquivo novo em chegada/ para este lote. Rode a coleta antes.")

    destino = pasta_lotes / f"lote_{lote}"
    destino.mkdir(parents=True, exist_ok=False)
    arquivos = []
    for p, fonte, data in candidatos:
        shutil.copy2(p, destino / p.name)
        conteudo = p.read_bytes()
        arquivos.append({"arquivo": p.name, "fonte": fonte, "data_coleta": data, "bytes": len(conteudo),
                         "sha256": hashlib.sha256(conteudo).hexdigest()})
    datas_lote = sorted({a["data_coleta"] for a in arquivos})
    aviso = None
    if datas_anteriores and set(datas_lote) <= datas_anteriores:
        aviso = "ATENÇÃO: este lote não contém nenhum dia de coleta novo (o Requisito 04 pede dias diferentes)."

    registro["lotes"][str(lote)] = {"preparado_em": datetime.now().isoformat(timespec="seconds"),
                                     "datas_coleta": datas_lote, "arquivos": arquivos}
    pasta_lotes.mkdir(parents=True, exist_ok=True)
    registro_path.write_text(json.dumps(registro, indent=1, ensure_ascii=False), encoding="utf-8")

    pasta_docs.mkdir(parents=True, exist_ok=True)
    linhas = [f"# Lote {lote}", "",
              f"- Preparado em: {registro['lotes'][str(lote)]['preparado_em']}",
              f"- Dias de coleta: {', '.join(datas_lote)}",
              f"- Arquivos: {len(arquivos)}",
              "- Enviado ao Volume `/Volumes/workspace/guiding_hand/chegada` em: ____/____/2026 às ____:____ (preencher)",
              "", "| Arquivo | Fonte | Dia de coleta | Bytes | SHA-256 (12) |", "|---|---|---|---|---|"]
    linhas += [f"| {a['arquivo']} | {a['fonte']} | {a['data_coleta']} | {a['bytes']} | {a['sha256'][:12]} |"
               for a in arquivos]
    if aviso:
        linhas += ["", f"> {aviso}"]
    (pasta_docs / f"lote_{lote}.md").write_text("\n".join(linhas) + "\n", encoding="utf-8")
    return {"pasta": str(destino), "arquivos": len(arquivos), "datas": datas_lote, "aviso": aviso}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--lote", type=int, required=True)
    ap.add_argument("--datas", nargs="*", help="limitar a estes dias de coleta (AAAA-MM-DD)")
    a = ap.parse_args()
    r = preparar(a.lote, RAIZ / "chegada", RAIZ / "lotes", RAIZ / "docs" / "lotes", a.datas)
    print(json.dumps(r, indent=1, ensure_ascii=False))
    sys.exit(0)
