. "$PSScriptRoot\_comum.ps1"
Titulo "Testes automatizados"
$py = Obter-PythonVenv
Instalar-Requisitos $py "requirements-dev.txt"
& $py -m pytest -q (Join-Path $Raiz "tests") "--ignore=$(Join-Path $Raiz "tests/test_dag.py")"
$codigo = $LASTEXITCODE
if (Docker-Disponivel) {
    Titulo "Validação do DAG dentro do Airflow (Docker)"
    docker compose exec -T airflow-scheduler airflow dags list-import-errors
    docker compose exec -T airflow-scheduler airflow tasks list guiding_hand_coleta
}
if ($codigo -eq 0) { Ok "Testes Python passaram" } else { Erro "Há testes falhando (veja acima)" }
