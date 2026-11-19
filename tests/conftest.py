import hashlib
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
for p in (RAIZ / "dags", RAIZ / "databricks" / "lib", RAIZ / "scripts", RAIZ / "tests" / "integracao"):
    sys.path.insert(0, str(p))
FIXTURES = RAIZ / "tests" / "fixtures" / "chegada"


import pytest  # noqa: E402

PROTEGIDOS = [RAIZ / "databricks" / "lib" / "mapeamento_sp156.json"]   # decisão real do protocolo SP156


def _impressao(caminhos):
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None for p in caminhos}


@pytest.fixture(autouse=True, scope="session")
def _mapeamento_e_coletas_intocados():
    """Nenhum teste pode alterar o mapeamento real nem os arquivos brutos de chegada/ do projeto."""
    chegada = RAIZ / "chegada"
    evidencias = RAIZ / "docs" / "evidencias"
    ev_antes = {p.name for p in evidencias.glob("*")} if evidencias.is_dir() else set()
    caminhos = PROTEGIDOS + (sorted(p for p in chegada.rglob("*") if p.is_file()) if chegada.is_dir() else [])
    antes = _impressao(caminhos)
    yield
    alterados = [c for c, h in _impressao(caminhos).items() if h != antes[c]]
    assert not alterados, f"Testes alteraram arquivos protegidos: {alterados}"
    novos = ({p.name for p in evidencias.glob("*")} if evidencias.is_dir() else set()) - ev_antes
    assert not novos, f"Testes criaram arquivos em docs/evidencias (evidência falsa): {sorted(novos)}"
