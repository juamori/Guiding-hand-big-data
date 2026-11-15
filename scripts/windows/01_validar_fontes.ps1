. "$PSScriptRoot\_comum.ps1"
Titulo "Validação das fontes de dados (Pinheiros)"
$py = Obter-PythonVenv
Instalar-Requisitos $py "requirements-coleta.txt"
Write-Host ""
Write-Host "  Se você tem o token da SPTrans Olho Vivo, cole agora (os caracteres não aparecem)."
Write-Host "  Se não tem, apenas aperte ENTER."
$token = Ler-Segredo "  Token Olho Vivo"
if ($token) { $env:OLHOVIVO_TOKEN = $token } else { Remove-Item Env:OLHOVIVO_TOKEN -ErrorAction SilentlyContinue }
& $py (Join-Path $Raiz "scripts/validar_fontes.py")
Remove-Item Env:OLHOVIVO_TOKEN -ErrorAction SilentlyContinue
$rel = Get-ChildItem docs\validacao_fontes\relatorio_*.md | Sort-Object LastWriteTime | Select-Object -Last 1
if ($rel) {
    Ok "Relatório gerado: $($rel.FullName)"
    Write-Host "  Envie esse arquivo no chat para continuarmos."
    Start-Process notepad.exe $rel.FullName
}
