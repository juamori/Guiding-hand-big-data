. "$PSScriptRoot\_comum.ps1"
Titulo "Verificando se há senhas ou chaves no repositório"
$py = Obter-PythonVenv
& $py (Join-Path $Raiz "scripts/verificar_segredos.py")
if ($LASTEXITCODE -eq 0) { Ok "Pode publicar no GitHub" } else { Erro "Remova os itens acima antes de publicar" }
