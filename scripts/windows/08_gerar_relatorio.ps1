. "$PSScriptRoot\_comum.ps1"
Titulo "Gerar o relatório PDF (ordem exigida pela FIAP)"
Importar-Downloads
$py = Obter-PythonVenv
Instalar-Requisitos $py "requirements-coleta.txt"
$ints = Join-Path $Raiz "docs\relatorio\integrantes.csv"
if ((Get-Content $ints -Raw) -match "preencha") {
    Aviso "Preencha os integrantes (nome completo e RM). O Bloco de Notas vai abrir; salve e feche."
    Start-Process notepad.exe $ints -Wait
}
& $py (Join-Path $Raiz "scripts/gerar_relatorio.py")
$pdf = Join-Path $Raiz "docs\relatorio\Relatorio_Big_Data_Guiding_Hand.pdf"
if (Test-Path $pdf) { Start-Process $pdf }
Write-Host "  Itens marcados como PENDENTE em vermelho no PDF ainda precisam de evidência."
