#!/usr/bin/env bash
# Variáveis de ambiente do Airflow no Codespaces. Use com:  source scripts/codespaces/ambiente.sh
# Nada aqui é segredo: as chaves internas (Fernet/JWT) são geradas uma vez e ficam em $GH_ESTADO,
# FORA do repositório. O token do Olho Vivo vem do segredo do Codespaces OLHOVIVO_TOKEN.

GH_REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export GH_REPO
export GH_ESTADO="${GH_ESTADO:-/workspaces/.guiding-hand-estado}"
export GH_VENV="${GH_VENV:-$GH_ESTADO/venv}"
export GH_PGDATA="$GH_ESTADO/postgres"
export GH_PGPORT="${GH_PGPORT:-5432}"
export GH_LOGS="$GH_ESTADO/logs_servicos"

export AIRFLOW_HOME="$GH_ESTADO/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$GH_REPO/dags"
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@127.0.0.1:${GH_PGPORT}/airflow"
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Sao_Paulo
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
# Endereço INTERNO usado pelas tarefas para falar com o Airflow (Execution API). Usamos 127.0.0.1
# para não depender da resolução de "localhost"; o api-server sobe ANTES do scheduler (iniciar.sh).
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="http://127.0.0.1:8080/execution/"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true
export AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true
export PYTHONPATH="$GH_REPO/dags"

# Endereço PÚBLICO da interface (porta 8080 encaminhada pelo Codespaces, acesso só com o seu login GitHub)
if [ -n "${CODESPACE_NAME:-}" ] && [ -n "${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN:-}" ]; then
  export GH_URL_AIRFLOW="https://${CODESPACE_NAME}-8080.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN}"
  export GH_URL_MVP="https://${CODESPACE_NAME}-8000.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN}"
else
  export GH_URL_AIRFLOW="http://localhost:8080"
  export GH_URL_MVP="http://localhost:8000"
fi
export AIRFLOW__API__BASE_URL="$GH_URL_AIRFLOW"

if [ -f "$GH_ESTADO/chaves.env" ]; then
  set -a; . "$GH_ESTADO/chaves.env"; set +a
fi
[ -d "$GH_VENV/bin" ] && export PATH="$GH_VENV/bin:$PATH"

# Para uso no terminal: mensagens coloridas
ok()    { printf '  \033[32m[OK]\033[0m %s\n' "$*"; }
aviso() { printf '  \033[33m[!]\033[0m  %s\n' "$*"; }
erro()  { printf '  \033[31m[X]\033[0m  %s\n' "$*"; }
titulo(){ printf '\n\033[36m=== %s ===\033[0m\n' "$*"; }

# CLI do Airflow com saída limpa (só o bloco JSON quando -o json)
af() { AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR airflow "$@" 2>/dev/null; }
so_json() { python3 -c '
import sys
t = sys.stdin.read()
i = min([p for p in (t.find("\n["), t.find("\n{")) if p >= 0] + ([0] if t.lstrip()[:1] in "[{" else []), default=-1)
print(t[i:].strip() if i >= 0 else "")'; }
