. "$PSScriptRoot\_comum.ps1"
Titulo "Tela do MVP"
Importar-Downloads
$py = Obter-PythonVenv
$dados = Join-Path $Raiz "mvp_demo\data\barreiras_mvp.json"
$conteudo = Get-Content $dados -Raw -Encoding UTF8
if ($conteudo -match "guiding-hand/tb_mvp_mapa_barreiras/v1") { Ok "Tela usando o export real do Databricks" }
else { Aviso "A tela está com dados de TESTE. Baixe barreiras_mvp.json do Volume 'exportacao' para a pasta Downloads e rode de novo." }
$job = Start-Job -ScriptBlock { param($p, $d) Set-Location $d; & $p -m http.server 8000 --bind 127.0.0.1 } -ArgumentList $py, (Join-Path $Raiz "mvp_demo")
Start-Sleep -Seconds 2
Start-Process "http://127.0.0.1:8000/"
Write-Host "  A tela abriu no navegador. Tire o print e salve como docs\relatorio\prints\10_tela_mvp.png"
Read-Host "  Aperte ENTER para fechar o servidor da tela"
Stop-Job $job; Remove-Job $job
