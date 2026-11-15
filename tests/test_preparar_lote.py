import shutil

import pytest

import preparar_lote as pl
from conftest import FIXTURES


@pytest.fixture
def ambiente(tmp_path):
    chegada = tmp_path / "chegada"
    shutil.copytree(FIXTURES, chegada)
    (chegada / "_manifestos").mkdir()
    (chegada / "_manifestos" / "manifesto_2026-11-12.json").write_text("{}")
    return chegada, tmp_path / "lotes", tmp_path / "docs"


def test_dois_lotes_de_dias_diferentes_sem_repetir_arquivo(ambiente):
    chegada, lotes, docs = ambiente
    r1 = pl.preparar(1, chegada, lotes, docs, ["2026-11-12"])
    r2 = pl.preparar(2, chegada, lotes, docs)
    assert r1["datas"] == ["2026-11-12"] and r2["datas"] == ["2026-11-13"]
    assert r1["arquivos"] == r2["arquivos"] == 5
    nomes1 = {p.name for p in (lotes / "lote_1").iterdir()}
    nomes2 = {p.name for p in (lotes / "lote_2").iterdir()}
    assert not nomes1 & nomes2 and not any("manifesto" in n for n in nomes1 | nomes2)
    assert (docs / "lote_1.md").exists() and r2["aviso"] is None


def test_sem_arquivos_novos_e_lote_repetido(ambiente):
    chegada, lotes, docs = ambiente
    pl.preparar(1, chegada, lotes, docs)
    with pytest.raises(SystemExit, match="já foi preparado"):
        pl.preparar(1, chegada, lotes, docs)
    with pytest.raises(SystemExit, match="Nenhum arquivo novo"):
        pl.preparar(2, chegada, lotes, docs)
