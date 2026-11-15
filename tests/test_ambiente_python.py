"""Erro real no Codespaces: 'python3 scripts/verificar_chegada.py' (Python do sistema, sem requests)
falhava com ModuleNotFoundError. O script agora se reexecuta com o Python do projeto."""
import json
import os
import shutil
import subprocess
import sys
import venv
from pathlib import Path

import pytest

import ambiente_python
from conftest import FIXTURES

RAIZ = Path(__file__).resolve().parents[1]


def _venv_com_requests(destino: Path, symlinks: bool) -> Path:
    """Venv que tem requests de verdade, seja qual for o Python que roda o pytest.

    (A versão anterior usava system_site_packages=True, que herda o site-packages do Python BASE, e não do
    venv que roda o pytest. No Codespaces o pytest roda no venv do projeto e o Python base não tem requests:
    o venv de teste ficava sem requests e o teste falhava.) Aqui um .pth aponta para as pastas onde estão
    requests e suas dependências no Python atual."""
    import certifi, charset_normalizer, idna, requests, urllib3  # noqa: E401
    venv.EnvBuilder(system_site_packages=False, with_pip=False, symlinks=symlinks).create(destino)
    site = next(destino.glob("lib/python3*/site-packages"))
    pastas = {str(Path(m.__file__).resolve().parent.parent) for m in (requests, urllib3, idna, charset_normalizer, certifi)}
    (site / "gh_teste_requests.pth").write_text("\n".join(sorted(pastas)) + "\n")
    py = destino / "bin" / "python"
    assert ambiente_python.tem_modulos(py, ["requests"]), "venv de teste deveria ter requests"
    return destino


@pytest.fixture(scope="module")
def venv_com_requests(tmp_path_factory):
    return _venv_com_requests(tmp_path_factory.mktemp("venv_projeto"), symlinks=False)


def _verificar(python, tmp_path, gh_venv):
    shutil.copytree(FIXTURES, tmp_path / "chegada")
    env = {**os.environ, "GH_VENV": str(gh_venv)}
    env.pop("GH_REEXECUTADO", None)
    # "-S": Python SEM site-packages -> sem requests, como o python3 do sistema no Codespaces
    return subprocess.run([str(python), "-S", str(RAIZ / "scripts" / "verificar_chegada.py"),
                           "--data", "2026-11-13", "--chegada", str(tmp_path / "chegada")],
                          capture_output=True, text=True, env=env, cwd=tmp_path)


def test_sem_requests_reexecuta_com_o_python_do_projeto(tmp_path, venv_com_requests):
    r = _verificar(sys.executable, tmp_path, venv_com_requests)
    assert "ModuleNotFoundError" not in r.stderr, r.stderr[-800:]
    assert "Verificação:" in r.stdout, r.stdout[-800:] + r.stderr[-800:]  # concluiu
    assert r.returncode == 1 and "COM PENDÊNCIAS" in r.stdout          # fixtures são reprovados como teste


def test_venv_com_link_simbolico_para_o_python_do_sistema(tmp_path):
    """Cenário real do Codespaces: 'python -m venv' cria bin/python como link para o MESMO binário do
    Python do sistema. A versão anterior comparava os executáveis resolvidos, achava que eram o mesmo
    Python e não reexecutava."""
    base = Path(getattr(sys, "_base_executable", sys.executable))
    gh_venv = _venv_com_requests(tmp_path / "venv_link", symlinks=True)
    assert (gh_venv / "bin" / "python").resolve() == base.resolve()   # mesmo binário, como no Codespaces
    r = _verificar(base, tmp_path, gh_venv)
    assert "Verificação:" in r.stdout, r.stdout[-800:] + r.stderr[-800:]


def test_pula_ambiente_sem_os_modulos_e_usa_o_seguinte(monkeypatch, tmp_path):
    sem, com = tmp_path / "sem" / "python", tmp_path / "com" / "python"
    for p in (sem, com):
        p.parent.mkdir()
        p.write_text("")
    monkeypatch.setattr(ambiente_python, "candidatos", lambda: [sem, com])
    monkeypatch.setattr(ambiente_python, "tem_modulos", lambda py, m: py == com)
    monkeypatch.setenv("GH_REEXECUTADO", "0")   # garantir() muda para "1"; o monkeypatch restaura no fim
    chamado = {}
    monkeypatch.setattr(ambiente_python.os, "execv", lambda exe, args: chamado.setdefault("exe", exe))
    with pytest.raises(SystemExit):   # execv falso retorna; na vida real o processo é substituído
        ambiente_python.garantir(("modulo_que_nao_existe_xyz",))
    assert chamado["exe"] == str(com)


def test_sem_ambiente_do_projeto_para_com_mensagem_clara(monkeypatch):
    monkeypatch.setattr(ambiente_python, "candidatos", lambda: [])
    with pytest.raises(SystemExit, match="instalar.sh"):
        ambiente_python.garantir(("modulo_que_nao_existe_xyz",))


def test_verificacao_usa_e_registra_o_mapeamento_gravado(tmp_path, monkeypatch):
    import sp156_mapeamento as mapa
    import verificar_chegada as vc
    arq = tmp_path / "mapeamento_sp156.json"
    m = mapa.carregar_mapeamento()
    m["campos"]["protocolo"] = "cd_identificador"
    m["situacao_protocolo"] = "DECIDIDO pelos dados reais (teste)"
    arq.write_text(json.dumps(m))
    monkeypatch.setattr(mapa, "ARQUIVO_MAPEAMENTO", arq)
    monkeypatch.setattr(vc, "RAIZ", tmp_path)
    assert vc.mapeamento_prata()[0]["protocolo"][0] == "cd_identificador"
    r = vc.verificar(__import__("datetime").date(2026, 11, 13), FIXTURES)
    assert r["mapeamento_sp156"]["campos"]["protocolo"] == "cd_identificador"
    assert "cd_identificador" in vc.escrever(r, tmp_path / "ev").read_text()
