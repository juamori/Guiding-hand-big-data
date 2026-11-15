. "$PSScriptRoot\_comum.ps1"
Titulo "Publicar o repositório no GitHub"
if (-not (Comando-Existe "git")) { Erro "Instale o Git: https://git-scm.com/download/win"; exit 1 }
$py = Obter-PythonVenv
& $py (Join-Path $Raiz "scripts/verificar_segredos.py")
if ($LASTEXITCODE -ne 0) { Erro "Há possíveis segredos no projeto. Nada foi publicado."; exit 1 }
Write-Host "  1) No navegador, crie um repositório PÚBLICO e VAZIO em https://github.com/new"
Write-Host "     (sem README, sem .gitignore). Ex.: guiding-hand-big-data"
$url = Read-Host "  2) Cole aqui o endereço do repositório (https://github.com/usuario/nome)"
$url = $url.Trim().TrimEnd("/")
if ($url -notmatch "^https://github\.com/[^/]+/[^/]+$") { Erro "Endereço inválido."; exit 1 }
if (-not (Test-Path (Join-Path $Raiz ".git"))) { git init -b main | Out-Null }
if (-not (git config user.name)) { git config user.name (Read-Host "  Seu nome para os commits") }
if (-not (git config user.email)) { git config user.email (Read-Host "  Seu e-mail do GitHub") }
$cfg = Join-Path $Raiz "docs\relatorio\config_relatorio.json"
$json = Get-Content $cfg -Raw | ConvertFrom-Json
$json.link_repositorio = $url
Gravar-Utf8 $cfg ($json | ConvertTo-Json)
git add -A
git commit -m "Guiding Hand - Big Data (FIAP GS 2026)" | Out-Null
git remote remove origin 2>$null
git remote add origin "$url.git"
Write-Host "  Enviando... (se abrir uma janela do GitHub, faça login e autorize)"
git push -u origin main
if ($LASTEXITCODE -eq 0) { Ok "Publicado: $url"; Start-Process $url } else { Erro "O envio falhou. Veja a mensagem acima." }
