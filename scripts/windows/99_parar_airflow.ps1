. "$PSScriptRoot\_comum.ps1"
Titulo "Parando o Airflow"
if (-not (Docker-Disponivel)) { Aviso "Docker Desktop não está aberto: não há containers do Airflow rodando para parar."; exit 0 }
docker compose down
if ($LASTEXITCODE -eq 0) { Ok "Containers parados. Os arquivos em chegada\ continuam no seu computador." }
else { Erro "Não consegui parar os containers. Veja a mensagem acima." }
