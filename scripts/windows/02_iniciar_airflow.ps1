. "$PSScriptRoot\_comum.ps1"
Titulo "Iniciando o Airflow 3 (Docker)"
if (-not (Docker-Disponivel)) { Erro "Abra o Docker Desktop, espere ficar verde e rode de novo."; exit 1 }
$py = Obter-PythonVenv
Instalar-Requisitos $py "requirements-coleta.txt"

if (-not (Test-Path .env)) {
    $fernet = & $py -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
    $jwt = & $py -c "import secrets;print(secrets.token_urlsafe(32))"
    "AIRFLOW__CORE__FERNET_KEY=$fernet`nAIRFLOW__API_AUTH__JWT_SECRET=$jwt" | Out-File -Encoding ascii .env
    Ok "Arquivo .env criado com chaves novas (fica só no seu computador)"
}
foreach ($p in @("chegada", "logs", "logs_pipeline")) { New-Item -ItemType Directory -Force $p | Out-Null }

Write-Host "  Token Olho Vivo para o Airflow (ENTER para pular):"
$token = Ler-Segredo "  Token"
if ($token) { $env:OLHOVIVO_TOKEN = $token }
& $py (Join-Path $Raiz "scripts/montar_variaveis.py")
Remove-Item Env:OLHOVIVO_TOKEN -ErrorAction SilentlyContinue

docker compose up -d
if ($LASTEXITCODE -ne 0) { Erro "Falha ao subir os containers. Veja a mensagem acima."; exit 1 }
Write-Host "  Aguardando o Airflow responder (1 a 3 minutos na primeira vez)..."
$pronto = $false
for ($i = 0; $i -lt 60; $i++) {
    try { $r = Invoke-WebRequest -UseBasicParsing http://localhost:8080/api/v2/version -TimeoutSec 5; if ($r.StatusCode -eq 200) { $pronto = $true; break } } catch { }
    Start-Sleep -Seconds 5
}
if ($pronto) {
    # reimporta as Variables a cada início (pega camadas SP156 descobertas e token novos)
    Airflow-Cli variables import /opt/airflow/config_gh/variables.json | Out-Null
    Ok "Airflow no ar: http://localhost:8080"
    Start-Process "http://localhost:8080"
} else { Erro "O Airflow não respondeu. Rode: docker compose logs airflow-apiserver" }
