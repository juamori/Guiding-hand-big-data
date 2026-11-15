"""O gerador de relatório segue a ordem da FIAP e nunca esconde pendências."""
import json
import shutil

import pytest

pytest.importorskip("reportlab")
pypdf = pytest.importorskip("pypdf")
if shutil.which("java") is None:
    pytest.skip("Java não encontrado (evidências de teste vêm do Spark local)", allow_module_level=True)

import gerar_relatorio as gr  # noqa: E402
import pipeline_local as pl  # noqa: E402
from conftest import FIXTURES  # noqa: E402

SECOES = ["1. Integrantes", "2. O produto", "3. Diagrama", "4. Tabela de fontes", "5. Requisitos",
          "6. Prints do Airflow", "7. Contagens da Bronze", "8. Consultas SQL", "9. Conclusão"]


@pytest.fixture(scope="module")
def evidencias(tmp_path_factory):
    saida = tmp_path_factory.mktemp("local")
    pl.executar(FIXTURES, saida)
    return json.loads((saida / "evidencias_teste.json").read_text(encoding="utf-8"))


def _texto(pdf):
    return "\n".join(p.extract_text() for p in pypdf.PdfReader(str(pdf)).pages)


def test_ordem_das_secoes_e_marca_de_previa(evidencias, tmp_path):
    texto = _texto(gr.gerar(evidencias, tmp_path / "r.pdf"))
    posicoes = [texto.find(s) for s in SECOES]
    assert all(p >= 0 for p in posicoes), dict(zip(SECOES, posicoes))
    assert posicoes == sorted(posicoes)
    assert "NÃO ENTREGAR" in texto  # evidência local nunca vira relatório final
    for cid in ("C1", "C2", "C3", "C4", "C5", "C6"):
        assert f"{cid} —" in texto


def test_sem_evidencias_tudo_fica_pendente(tmp_path):
    texto = _texto(gr.gerar(None, tmp_path / "vazio.pdf"))
    assert texto.count("PENDENTE") >= 5
