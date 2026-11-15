"""Garante que o script rode com o Python do projeto (que tem as dependências, ex.: requests).

Se um módulo necessário faltar no Python atual (ex.: chamado como "python3" no Codespaces, que
é o Python do sistema), o script é reexecutado automaticamente com o Python do ambiente do
projeto. Sem esse ambiente, para com uma mensagem clara em vez de um erro de importação.
"""
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]


def candidatos() -> list[Path]:
    lista = []
    if os.environ.get("GH_VENV"):
        lista.append(Path(os.environ["GH_VENV"]) / "bin" / "python")
    lista += [Path("/workspaces/.guiding-hand-estado/venv/bin/python"),          # Codespaces
              RAIZ / ".venv" / "bin" / "python", RAIZ / ".venv" / "Scripts" / "python.exe"]  # local / Windows
    return lista


def tem_modulos(python: Path, modulos: list[str]) -> bool:
    """O Python indicado consegue importar os módulos? (consulta real, num processo separado)."""
    codigo = ("import importlib.util, sys; "
              "sys.exit(0 if all(importlib.util.find_spec(m) for m in sys.argv[1:]) else 1)")
    try:
        return subprocess.run([str(python), "-c", codigo, *modulos], capture_output=True, timeout=60).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def garantir(modulos: tuple[str, ...] = ("requests",)) -> None:
    faltando = [m for m in modulos if importlib.util.find_spec(m) is None]
    if not faltando:
        return
    testados = []
    if os.environ.get("GH_REEXECUTADO") != "1":
        # Não compara caminhos de executável: um venv criado com "python -m venv" usa link simbólico
        # para o mesmo binário do Python do sistema. O que decide é o Python conseguir importar os módulos.
        for py in candidatos():
            if not py.exists():
                continue
            if tem_modulos(py, faltando):
                os.environ["GH_REEXECUTADO"] = "1"
                os.execv(str(py), [str(py), *sys.argv])
            testados.append(str(py))
    detalhe = f" Ambientes testados sem esses módulos: {', '.join(testados)}." if testados else ""
    raise SystemExit(f"Faltam os módulos {', '.join(faltando)} neste Python ({sys.executable}).{detalhe} "
                     "No Codespaces, rode: bash scripts/codespaces/instalar.sh")
