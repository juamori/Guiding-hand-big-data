#!/usr/bin/env bash
# Sobe PostgreSQL e Airflow 3 (api-server, scheduler e dag-processor, como no Docker Compose).
#   bash scripts/codespaces/iniciar.sh                 -> sobe tudo e espera ficar pronto
#   bash scripts/codespaces/iniciar.sh --so-banco      -> só PostgreSQL + "airflow db migrate"
#   bash scripts/codespaces/iniciar.sh --em-segundo-plano  (usado na abertura do Codespace)
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/ambiente.sh"
MODO="${1:-}"
mkdir -p "$GH_LOGS" "$GH_REPO/chegada" "$GH_REPO/logs_pipeline"

if [ ! -x "$GH_VENV/bin/airflow" ]; then
  erro "Airflow ainda não instalado. Rode: bash scripts/codespaces/instalar.sh"; exit 1
fi
if [ "$(id -u)" -eq 0 ]; then
  erro "Rode como usuário comum (no Codespaces, o padrão 'vscode'), não como root."; exit 1
fi
PGBIN="$(cat "$GH_ESTADO/pgbin" 2>/dev/null || ls -d /usr/lib/postgresql/*/bin | sort -V | tail -1)"

# ---------------------------------------------------------------- PostgreSQL
if [ ! -f "$GH_PGDATA/PG_VERSION" ]; then
  "$PGBIN/initdb" -D "$GH_PGDATA" -U postgres --auth=trust -E UTF8 >"$GH_LOGS/initdb.log" 2>&1 \
    || { erro "initdb falhou (veja $GH_LOGS/initdb.log)"; exit 1; }
fi
if ! "$PGBIN/pg_ctl" -D "$GH_PGDATA" status >/dev/null 2>&1; then
  "$PGBIN/pg_ctl" -D "$GH_PGDATA" -l "$GH_LOGS/postgres.log" -w -t 60 \
    -o "-p $GH_PGPORT -k $GH_PGDATA -c listen_addresses=127.0.0.1" start >/dev/null \
    || { erro "PostgreSQL não iniciou (veja $GH_LOGS/postgres.log)"; exit 1; }
fi
PSQL=("$PGBIN/psql" -h 127.0.0.1 -p "$GH_PGPORT" -U postgres -qtA)
if [ "$("${PSQL[@]}" -c "SELECT 1 FROM pg_roles WHERE rolname='airflow'")" != "1" ]; then
  "${PSQL[@]}" -c "CREATE USER airflow WITH PASSWORD 'airflow'" -c "CREATE DATABASE airflow OWNER airflow" >/dev/null
fi
ok "PostgreSQL ativo (127.0.0.1:$GH_PGPORT, dados em $GH_PGDATA)"

if ! af db check-migrations -t 0 >/dev/null 2>&1; then
  airflow db migrate >"$GH_LOGS/migrate.log" 2>&1 || { erro "airflow db migrate falhou (veja $GH_LOGS/migrate.log)"; exit 1; }
fi
ok "Banco do Airflow pronto"
[ "$MODO" = "--so-banco" ] && exit 0

# ---------------------------------------------------------------- Variables
python3 "$GH_REPO/scripts/montar_variaveis.py" --chegada "$GH_REPO/chegada" --logs "$GH_REPO/logs_pipeline" >/dev/null
af variables import "$GH_REPO/airflow/variables.json" >/dev/null && ok "Variables do Airflow importadas (token só se o segredo OLHOVIVO_TOKEN existir)"

# ---------------------------------------------------------------- serviços
# Cada serviço roda no próprio grupo de processos (setsid) e tem o PID gravado em $GH_ESTADO/pids.
# Não dá para achar o api-server pelo nome: ele se renomeia para "airflow api_server" e abre
# processos "gunicorn" (observado em teste).
mkdir -p "$GH_ESTADO/pids"
servico_ativo() { local pid; pid="$(cat "$GH_ESTADO/pids/$1.pid" 2>/dev/null)" && [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; }
iniciar_servico() {  # nome  comando...
  local nome="$1"; shift
  if servico_ativo "$nome"; then ok "$nome já estava rodando"; return; fi
  setsid nohup "$@" >"$GH_LOGS/$nome.log" 2>&1 < /dev/null &
  echo $! > "$GH_ESTADO/pids/$nome.pid"
  ok "$nome iniciado (log: $GH_LOGS/$nome.log)"
}
# Ordem importa: as tarefas falam com o api-server (Execution API). Se o scheduler subir antes de o
# api-server responder, a execução agendada pendente é despachada e falha com "Connection refused"
# (observado em teste; o retry recupera, mas a 1ª tentativa fica vermelha no Grid).
iniciar_servico api-server airflow api-server --port 8080 --workers 2
for _ in $(seq 1 60); do
  curl -sf -m 3 http://127.0.0.1:8080/api/v2/version >/dev/null && break
  sleep 3
done
curl -sf -m 3 http://127.0.0.1:8080/api/v2/version >/dev/null \
  || { erro "O api-server não respondeu em 3 minutos. Veja $GH_LOGS/api-server.log"; exit 1; }
iniciar_servico scheduler airflow scheduler
iniciar_servico dag-processor airflow dag-processor
ok "Airflow 3 no ar: $GH_URL_AIRFLOW  (aba PORTS > porta 8080)"
