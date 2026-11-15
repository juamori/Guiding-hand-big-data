"""Consistência da configuração do GitHub Codespaces (alternativa sem Docker no computador)."""
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
CS = RAIZ / "scripts" / "codespaces"
MENU = (CS / "menu.sh").read_text(encoding="utf-8")


def _devcontainer():
    texto = (RAIZ / ".devcontainer" / "devcontainer.json").read_text(encoding="utf-8")
    return json.loads(re.sub(r"^\s*//.*$", "", texto, flags=re.M))


def test_devcontainer_aponta_para_scripts_existentes():
    d = _devcontainer()
    for chave in ("postCreateCommand", "postStartCommand"):
        script = re.search(r"scripts/codespaces/\w+\.sh", d[chave]).group(0)
        assert (RAIZ / script).exists(), script
    assert 8080 in d["forwardPorts"] and 8000 in d["forwardPorts"]
    assert d["containerEnv"]["GH_ESTADO"].startswith("/workspaces/"), "estado precisa sobreviver a rebuild"


def test_menu_codespaces_tem_as_mesmas_opcoes_do_windows():
    bat = (RAIZ / "GuidingHand.bat").read_bytes().decode("ascii")
    opcoes_bat = set(re.findall(r"^echo   ([0-9A-Z])\. ", bat, flags=re.M))
    opcoes_sh = set(re.findall(r"^  ([0-9A-Z])\. ", MENU, flags=re.M))
    assert opcoes_sh == opcoes_bat, (opcoes_sh ^ opcoes_bat)
    tratadas = set(re.findall(r"(?:^|\s)([0-9A-Z])\) op", MENU))
    assert opcoes_sh - {"S"} <= tratadas, opcoes_sh - tratadas


def test_scripts_python_chamados_existem():
    for s in re.findall(r"scripts/(\w+\.py)", MENU + (CS / "iniciar.sh").read_text()):
        assert (RAIZ / "scripts" / s).exists(), s


def test_estado_e_chaves_fora_do_repositorio():
    amb = (CS / "ambiente.sh").read_text()
    assert 'GH_ESTADO="${GH_ESTADO:-/workspaces/.guiding-hand-estado}"' in amb
    assert "127.0.0.1:8080/execution/" in amb
    assert "chaves.env" in amb and "chaves.env" not in (RAIZ / ".gitignore").read_text()  # fica fora do repo


@pytest.mark.skipif(shutil.which("bash") is None, reason="sem bash")
def test_sintaxe_bash():
    for f in CS.glob("*.sh"):
        assert subprocess.run(["bash", "-n", str(f)]).returncode == 0, f.name


def test_opcao_invalida_retorna_erro_sem_executar(tmp_path):
    if shutil.which("bash") is None:
        pytest.skip("sem bash")
    r = subprocess.run(["bash", str(CS / "menu.sh"), "X"], capture_output=True, text=True,
                       env={"PATH": "/usr/bin:/bin", "GH_ESTADO": str(tmp_path)})
    assert r.returncode == 2


def test_api_server_sobe_antes_do_scheduler():
    """Falha observada em teste: scheduler antes do api-server = tarefas com Connection refused."""
    ini = (CS / "iniciar.sh").read_text()
    assert ini.index("iniciar_servico api-server") < ini.index("/api/v2/version") < ini.index("iniciar_servico scheduler")
    compose = (RAIZ / "docker-compose.yml").read_text()
    bloco = compose[compose.index("airflow-scheduler:"):compose.index("airflow-dag-processor:")]
    assert "airflow-apiserver:" in bloco and "service_healthy" in bloco


FONTES_APT = {
    "debian.sources": ("Types: deb\nURIs: http://deb.debian.org/debian\nSuites: bookworm\nComponents: main\n"
                       "Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg\n", True),
    "sources.list": ("deb http://azure.archive.ubuntu.com/ubuntu/ jammy main\n"
                     "deb http://security.ubuntu.com/ubuntu jammy-security main\n", True),
    "yarn.list": ("deb https://dl.yarnpkg.com/debian/ stable main\n", False),
    "microsoft.list": ("deb [signed-by=/usr/share/keyrings/x.gpg] https://packages.microsoft.com/repos/x jammy main\n", False),
    "misto.list": ("deb http://deb.debian.org/debian bookworm main\ndeb https://dl.yarnpkg.com/debian/ stable main\n", False),
    "vazio.list": ("", False),
}


@pytest.mark.skipif(shutil.which("bash") is None, reason="sem bash")
def test_apt_usa_so_fontes_oficiais(tmp_path):
    """Erro real no Codespaces: a fonte do Yarn da imagem (sem chave) derrubava o apt-get update."""
    texto = (CS / "apt_oficial.sh").read_text()
    funcao = "\n".join(re.findall(r"^OFICIAIS=.*$", texto, flags=re.M)) + "\n" + \
        re.search(r"^eh_oficial\(\) \{.*?^\}", texto, flags=re.M | re.S).group(0)
    (tmp_path / "f.sh").write_text(funcao)
    for nome, (conteudo, esperado) in FONTES_APT.items():
        (tmp_path / nome).write_text(conteudo)
        r = subprocess.run(["bash", "-c", f'source "{tmp_path}/f.sh"; eh_oficial "{tmp_path}/{nome}"'])
        assert (r.returncode == 0) == esperado, nome
    assert "Acquire::AllowInsecureRepositories" not in texto and "trusted=yes" not in texto
    assert "--allow-unauthenticated" not in texto
    instalar = (CS / "instalar.sh").read_text()
    assert "apt_oficial.sh" in instalar and "apt-get update" not in instalar
