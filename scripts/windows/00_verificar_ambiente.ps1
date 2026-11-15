. "$PSScriptRoot\_comum.ps1"
Titulo "Verificando o seu computador"
$tudoOk = $true
$py = Encontrar-PythonSistema
if ($py) { Ok "Python encontrado ($py)" } else { Erro "Python 3.10+ não encontrado -> https://www.python.org/downloads/ (marque 'Add Python to PATH')"; $tudoOk = $false }
if (Comando-Existe "docker") { Ok "Docker instalado" } else { Erro "Docker Desktop não encontrado -> https://www.docker.com/products/docker-desktop/"; $tudoOk = $false }
if (Comando-Existe "docker") {
    if (Docker-Disponivel) { Ok "Docker Desktop está aberto e funcionando" } else { Aviso "Docker Desktop instalado mas FECHADO: abra o Docker Desktop e espere ficar verde"; $tudoOk = $false }
}
if (Comando-Existe "git") { Ok "Git instalado" } else { Aviso "Git não encontrado (necessário só para publicar no GitHub) -> https://git-scm.com/download/win" }
if (Comando-Existe "java") { Ok "Java encontrado (testes Spark locais vão rodar)" } else { Aviso "Java não encontrado: os testes Spark locais serão pulados (não afeta a entrega)" }
$conexao = $false
try {
    $tcp = New-Object System.Net.Sockets.TcpClient
    $conexao = $tcp.ConnectAsync("api.open-meteo.com", 443).Wait(5000) -and $tcp.Connected
    $tcp.Close()
} catch { $conexao = $false }
if ($conexao) { Ok "Internet OK (api.open-meteo.com alcançável)" } else { Erro "Sem acesso à internet ou bloqueado pela rede"; $tudoOk = $false }
Write-Host ""
if ($tudoOk) { Ok "Tudo pronto." } else { Aviso "Resolva os itens marcados e rode de novo." }
