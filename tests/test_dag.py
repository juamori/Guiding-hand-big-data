"""Testes estruturais do DAG (rodam no ambiente com Airflow 3 instalado)."""
import os
import sys
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
# AIRFLOW_HOME precisa ser definido ANTES do primeiro import do Airflow.
import tempfile  # noqa: E402
_HOME = Path(tempfile.gettempdir()) / "gh_pytest_airflow"
_HOME.mkdir(parents=True, exist_ok=True)
os.environ["AIRFLOW_HOME"] = str(_HOME)
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
pytest.importorskip("airflow.sdk")  # "airflow" sozinho casaria com a pasta airflow/ do projeto


@pytest.fixture(scope="module")
def dag(tmp_path_factory):
    import subprocess
    subprocess.run([sys.executable, "-m", "airflow", "db", "migrate"], check=True,
                   capture_output=True, env=os.environ.copy())
    sys.path.insert(0, str(RAIZ / "dags"))
    import importlib.util
    spec = importlib.util.spec_from_file_location("guiding_hand_coleta", RAIZ / "dags" / "guiding_hand_coleta.py")
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    from airflow.sdk import DAG
    dags = [v for v in vars(modulo).values() if isinstance(v, DAG)]
    if not dags:  # @dag registra via chamada; recupera pelo contexto
        from airflow.models.dagbag import DagBag
        bag = DagBag(dag_folder=str(RAIZ / "dags"), include_examples=False)
        assert not bag.import_errors, bag.import_errors
        return bag.get_dag("guiding_hand_coleta")
    return dags[0]


COLETAS = {"coletar_clima", "coletar_osm", "coletar_sp156", "coletar_olhovivo", "gerar_eventos_oculos"}


def test_agendado_diariamente(dag):
    assert dag.dag_id == "guiding_hand_coleta"
    assert "0 7 * * *" in str(dag.timetable.summary) or "0 7 * * *" in repr(dag.timetable)
    assert dag.max_active_runs == 1


def test_coletas_em_paralelo_entre_inicio_e_conferencia(dag):
    tarefas = {t.task_id: t for t in dag.tasks}
    assert COLETAS <= set(tarefas)
    for c in COLETAS:
        assert tarefas[c].upstream_task_ids == {"inicio"}
        assert tarefas[c].downstream_task_ids == {"conferir"}
    assert tarefas["conferir"].trigger_rule == "all_done"
    assert tarefas["notificar_resultado"].upstream_task_ids == {"conferir"}


def test_retries_e_callback_de_falha(dag):
    for t in dag.tasks:
        if t.task_id in COLETAS:
            assert t.retries == 2
            assert t.on_failure_callback
