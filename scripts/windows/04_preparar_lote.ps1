param([int]$Lote = 0)
. "$PSScriptRoot\_comum.ps1"
Titulo "Preparar lote para o Databricks"
if ($Lote -le 0) { $Lote = [int](Read-Host "  Número do lote (1 ou 2)") }
$py = Obter-PythonVenv
& $py (Join-Path $Raiz "scripts/preparar_lote.py") --lote $Lote
if ($LASTEXITCODE -ne 0) { exit 1 }
$pasta = Join-Path $Raiz "lotes\lote_$Lote"
Ok "Lote pronto em $pasta"
Write-Host "  Agora envie TODOS os arquivos dessa pasta para o Volume:"
Write-Host "  Databricks > Catalog > workspace > guiding_hand > Volumes > chegada > Upload to this volume"
Start-Process explorer.exe $pasta
