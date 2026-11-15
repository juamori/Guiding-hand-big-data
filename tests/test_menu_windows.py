"""Consistência entre GuidingHand.bat, scripts/windows e a documentação.

Criado após a divergência em que o menu não listava as opções 7, 8 e G. A execução real
do .bat em cmd.exe está em tests/integracao/testar_menu_cmd.sh (requer Wine).
"""
import re
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
BAT = (RAIZ / "GuidingHand.bat").read_bytes()
TEXTO = BAT.decode("ascii")
EXIBIDAS = re.findall(r"^echo   ([0-9A-Z])\. (.+?)\r?$", TEXTO, flags=re.M)
MAPA = dict(re.findall(r'^if (?:/i )?"%op%"=="([0-9A-Z])" set "arq=([\w.]+)"', TEXTO, flags=re.M))


def test_bat_em_formato_windows():
    assert BAT.count(b"\n") == BAT.count(b"\r\n"), "todas as linhas precisam terminar em CRLF"
    assert all(b < 128 for b in BAT), "o .bat deve ser ASCII puro"
    assert 'set /p "op=' in TEXTO and 'set "op="' in TEXTO


def test_toda_opcao_exibida_tem_acao_e_vice_versa():
    exibidas = {o for o, _ in EXIBIDAS}
    assert exibidas - {"S"} == set(MAPA), (exibidas, MAPA)
    assert re.search(r'^if /i "%op%"=="S" exit /b', TEXTO, flags=re.M)


def test_toda_acao_aponta_para_script_existente_e_todos_os_scripts_estao_no_menu():
    scripts = {p.name for p in (RAIZ / "scripts" / "windows").glob("*.ps1") if not p.name.startswith("_")}
    assert set(MAPA.values()) == scripts
    for arq in MAPA.values():
        conteudo = (RAIZ / "scripts" / "windows" / arq).read_text(encoding="utf-8-sig")
        linhas = [l.strip() for l in conteudo.splitlines() if l.strip()]
        assert '. "$PSScriptRoot\\_comum.ps1"' in linhas[:2], arq  # 1ª linha (ou 2ª, após param())


def test_opcoes_novas_presentes():
    assert MAPA["7"] == "07_abrir_tela_mvp.ps1"
    assert MAPA["8"] == "08_gerar_relatorio.ps1"
    assert MAPA["G"] == "09_publicar_github.ps1"
    assert re.search(r'^if /i "%op%"=="G"', TEXTO, flags=re.M), "G deve aceitar minúscula"


def test_documentacao_so_cita_opcoes_que_existem():
    for doc in ("docs/passo_a_passo.md", "README.md"):
        texto = (RAIZ / doc).read_text(encoding="utf-8")
        citadas = set(re.findall(r"[Mm]enu \*\*([0-9A-Z])\*\*", texto)) | set(re.findall(r"^\| \*\*([0-9A-Z])\*\* \|", texto, flags=re.M))
        assert citadas, f"{doc} não cita o menu"
        assert citadas <= set(MAPA) | {"S"}, (doc, citadas - set(MAPA))


def test_readme_lista_todas_as_opcoes():
    readme = (RAIZ / "README.md").read_text(encoding="utf-8")
    listadas = set(re.findall(r"^\| \*\*([0-9A-Z])\*\* \|", readme, flags=re.M))
    assert listadas == set(MAPA) | {"S"}
