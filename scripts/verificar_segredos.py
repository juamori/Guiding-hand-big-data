"""Procura credenciais esquecidas no repositório antes do commit (R09 / penalidade -1,0).

Uso: python scripts/verificar_segredos.py      (código de saída 1 se achar algo)
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
IGNORAR_DIRS = {".git", ".venv", "venv", "__pycache__", "chegada", "lotes", "saida_local",
                "logs_pipeline", "node_modules", ".pytest_cache"}
EXTENSOES = {".py", ".json", ".md", ".yml", ".yaml", ".env", ".ps1", ".bat", ".sql", ".txt", ".cfg", ".ini"}
PADROES = [
    ("token/senha com valor literal",
     re.compile(r"""(?i)(token|senha|password|secret|api[_-]?key)\s*[:=]\s*["'][A-Za-z0-9_\-]{20,}["']""")),
    ("hex de 64 caracteres em atribuição de token", re.compile(r"(?i)token[^\n]{0,20}[0-9a-f]{64}")),
    ("token pessoal Databricks", re.compile(r"dapi[0-9a-f]{32}")),
    ("chave privada", re.compile(r"-----BEGIN (RSA |EC )?PRIVATE KEY-----")),
    ("URL de webhook real", re.compile(r"https://[a-z0-9.-]*(webhook\.office\.com|discord\.com/api/webhooks|hooks\.slack\.com)/[A-Za-z0-9/_-]{10,}")),
]
PERMITIDOS = {"TOKEN-SECRETO", "COLE_AQUI", "SEU_TOKEN"}


def varrer(raiz: Path = RAIZ) -> list[str]:
    achados = []
    for p in raiz.rglob("*"):
        if not p.is_file() or set(p.relative_to(raiz).parts) & IGNORAR_DIRS:
            continue
        if p.suffix.lower() not in EXTENSOES and p.name != ".env":
            continue
        if p.name == ".env":
            achados.append(f"{p.relative_to(raiz)}: arquivo .env não deve ser versionado")
            continue
        texto = p.read_text(encoding="utf-8", errors="ignore")
        for nome, padrao in PADROES:
            for m in padrao.finditer(texto):
                if not any(x in m.group(0) for x in PERMITIDOS):
                    linha = texto[: m.start()].count("\n") + 1
                    achados.append(f"{p.relative_to(raiz)}:{linha}: {nome}")
    return achados


if __name__ == "__main__":
    r = varrer()
    print("\n".join(r) if r else "Nenhum segredo encontrado.")
    sys.exit(1 if r else 0)
