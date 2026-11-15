. "$PSScriptRoot\_comum.ps1"
Titulo "Executando a coleta agora (DAG guiding_hand_coleta)"
if (-not (Docker-Disponivel)) { Erro "Abra o Docker Desktop e rode 02_iniciar_airflow antes."; exit 1 }
$runId = "manual__" + (Get-Date -Format "yyyyMMdd_HHmmss")
Airflow-Cli dags unpause guiding_hand_coleta | Out-Null
Airflow-Cli dags trigger guiding_hand_coleta -r $runId | Out-Null
if ($LASTEXITCODE -ne 0) { Erro "Não consegui disparar o DAG. O Airflow está no ar?"; exit 1 }
Write-Host "  Execução $runId disparada. Acompanhe também em http://localhost:8080"
$estado = "queued"
for ($i = 0; $i -lt 90; $i++) {
    Start-Sleep -Seconds 10
    $json = Extrair-Json (Airflow-Cli dags list-runs guiding_hand_coleta -o json)
    try { $run = ($json | ConvertFrom-Json) | Where-Object { $_.run_id -eq $runId } } catch { $run = $null }
    if ($run) { $estado = $run.state; Write-Host "  ... estado: $estado" }
    if ($estado -in @("success", "failed")) { break }
}
$estados = Extrair-Json (Airflow-Cli tasks states-for-dag-run guiding_hand_coleta $runId -o json)
$arqEv = Join-Path $Raiz "docs\evidencias\airflow_$runId.json"
Gravar-Utf8 $arqEv $estados
Ok "Evidência da execução (estados e horários das tarefas) salva em docs\evidencias\airflow_$runId.json"
if ($estado -eq "success") { Ok "Coleta concluída com sucesso" } else { Erro "Coleta terminou como '$estado'. Abra o Airflow, clique na execução e veja a tarefa vermelha." }
Write-Host ""
Write-Host "  Arquivos em chegada\ :"
Get-ChildItem chegada -Recurse -File -Filter *.json | Where-Object { $_.FullName -notmatch "_manifestos" } |
    Sort-Object LastWriteTime | Select-Object -Last 10 | ForEach-Object { "    {0,-40} {1,10:N0} bytes" -f $_.Name, $_.Length }
