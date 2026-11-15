# Funções compartilhadas pelos scripts do Guiding Hand (não execute diretamente).
# "Continue": no Windows PowerShell 5.1, "Stop" transforma qualquer saída de stderr de
# programas externos (java -version, avisos do Airflow, progresso do docker) em erro fatal.
# Os erros são tratados explicitamente via $LASTEXITCODE.
$ErrorActionPreference = "Continue"
$Raiz = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $Raiz
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

function Ok($t)    { Write-Host "  [OK] $t" -ForegroundColor Green }
function Aviso($t) { Write-Host "  [!]  $t" -ForegroundColor Yellow }
function Erro($t)  { Write-Host "  [X]  $t" -ForegroundColor Red }
function Titulo($t){ Write-Host ""; Write-Host "=== $t ===" -ForegroundColor Cyan }

function Encontrar-PythonSistema {
    foreach ($cmd in @("py -3", "python", "python3")) {
        try {
            $partes = @($cmd.Split(" "))
            $v = & $partes[0] @($partes | Select-Object -Skip 1) --version 2>&1
            if ($LASTEXITCODE -eq 0 -and "$v" -match "Python 3\.(1[0-9])") { return $cmd }
        } catch { }
    }
    return $null
}

function Obter-PythonVenv {
    $venvPy = Join-Path $Raiz ".venv\Scripts\python.exe"
    if (-not (Test-Path $venvPy)) {
        $sis = Encontrar-PythonSistema
        if (-not $sis) { Erro "Python 3.10+ não encontrado. Instale em https://www.python.org/downloads/ (marque 'Add Python to PATH')."; exit 1 }
        Write-Host "  Criando ambiente Python isolado (.venv)..."
        $partes = @($sis.Split(" "))
        & $partes[0] @($partes | Select-Object -Skip 1) -m venv (Join-Path $Raiz ".venv")
        if ($LASTEXITCODE -ne 0) { Erro "Não consegui criar o ambiente .venv"; exit 1 }
    }
    return $venvPy
}

function Instalar-Requisitos($py, $arquivo) {
    Write-Host "  Instalando dependências de $arquivo (pode levar alguns minutos na 1ª vez)..."
    & $py -m pip install --disable-pip-version-check -q --upgrade pip | Out-Null
    & $py -m pip install --disable-pip-version-check -q -r (Join-Path $Raiz $arquivo)
    if ($LASTEXITCODE -ne 0) { Erro "Falha ao instalar $arquivo"; exit 1 }
}

function Comando-Existe($nome) { return [bool](Get-Command $nome -ErrorAction SilentlyContinue) }

function Docker-Disponivel {
    if (-not (Comando-Existe "docker")) { return $false }
    docker info *> $null
    return ($LASTEXITCODE -eq 0)
}

function Ler-Segredo($pergunta) {
    if ([Console]::IsInputRedirected) {
        # execução não interativa (testes/automação): lê a linha da entrada padrão
        Write-Host "${pergunta}: " -NoNewline
        $linha = [Console]::In.ReadLine()
        Write-Host ""
        if ($null -eq $linha) { return "" }
        return $linha.Trim()
    }
    $seguro = Read-Host $pergunta -AsSecureString
    $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($seguro)
    try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr) }
    finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr) }
}

function Gravar-Utf8($caminho, $texto) {
    New-Item -ItemType Directory -Force (Split-Path $caminho) | Out-Null
    [IO.File]::WriteAllText($caminho, ($texto -join "`n"), (New-Object System.Text.UTF8Encoding($false)))
}

function Importar-Downloads {
    # Procura, na pasta Downloads, os arquivos exportados do Databricks e copia para o repositório.
    $dl = Join-Path $env:USERPROFILE "Downloads"
    if (-not (Test-Path $dl)) { return }
    $mapa = @{ "evidencias_lote_*.json" = "docs\evidencias"; "barreiras_mvp*.json" = "mvp_demo\data" }
    foreach ($padrao in $mapa.Keys) {
        Get-ChildItem $dl -Filter $padrao -File -ErrorAction SilentlyContinue | ForEach-Object {
            $nome = $_.Name -replace " \(\d+\)", ""
            if ($nome -like "barreiras_mvp*") { $nome = "barreiras_mvp.json" }
            $destino = Join-Path (Join-Path $Raiz $mapa[$padrao]) $nome
            New-Item -ItemType Directory -Force (Split-Path $destino) | Out-Null
            if (-not (Test-Path $destino) -or $_.LastWriteTime -gt (Get-Item $destino).LastWriteTime) {
                Copy-Item $_.FullName $destino -Force
                Ok "Importado de Downloads: $($_.Name) -> $($mapa[$padrao])\$nome"
            }
        }
    }
}

function Extrair-Json($saida) {
    # A CLI do Airflow pode imprimir avisos no stdout antes do JSON: guarda só o bloco JSON.
    $linhas = @($saida)
    $inicio = -1
    for ($i = 0; $i -lt $linhas.Count; $i++) { if ("$($linhas[$i])" -match '^\s*\[\s*(\{|\]|$)') { $inicio = $i; break } }
    if ($inicio -lt 0) { return $null }
    return ($linhas[$inicio..($linhas.Count - 1)] -join "`n")
}

function Airflow-Cli {
    # Executa a CLI do Airflow no container com logs silenciados (saída limpa para JSON).
    docker compose exec -T -e AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR airflow-scheduler airflow @args 2>$null
}
