"""Monta airflow/variables.json (NÃO versionado) a partir do modelo + descobertas + segredos.

- Base: airflow/variables.example.json
- gh_sp156_camadas: vem de airflow/descoberta_fontes.json (gerado por validar_fontes.py)
- gh_olhovivo_token / gh_webhook_url: vêm das variáveis de ambiente OLHOVIVO_TOKEN / GH_WEBHOOK_URL,
  apenas se definidas. Valores vazios são omitidos (o DAG pula a fonte opcional).
"""
import argparse
import json
import os
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
ap = argparse.ArgumentParser()
ap.add_argument("--chegada", help="pasta chegada/ (padrão: a do Docker, /opt/airflow/chegada)")
ap.add_argument("--logs", help="pasta de logs do pipeline (padrão: a do Docker)")
args = ap.parse_args()
base = json.loads((RAIZ / "airflow" / "variables.example.json").read_text(encoding="utf-8"))
if args.chegada:
    base["gh_pasta_chegada"] = args.chegada
if args.logs:
    base["gh_pasta_logs"] = args.logs
descoberta = RAIZ / "airflow" / "descoberta_fontes.json"
if descoberta.exists():
    base["gh_sp156_camadas"] = json.loads(descoberta.read_text(encoding="utf-8")).get("gh_sp156_camadas", [])
if os.environ.get("OLHOVIVO_TOKEN"):
    base["gh_olhovivo_token"] = os.environ["OLHOVIVO_TOKEN"]
if os.environ.get("GH_WEBHOOK_URL"):
    base["gh_webhook_url"] = os.environ["GH_WEBHOOK_URL"]
base = {k: v for k, v in base.items() if v not in ("", None)}
destino = RAIZ / "airflow" / "variables.json"
destino.write_text(json.dumps(base, indent=1, ensure_ascii=False), encoding="utf-8")
print(f"Variables gravadas em {destino} (arquivo local, fora do Git):")
for k, v in base.items():
    print(f"  {k} = {'***' if 'token' in k or 'webhook' in k else v}")
