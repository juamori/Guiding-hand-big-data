#!/usr/bin/env bash
# Para o Airflow e o PostgreSQL (os dados e o histórico das execuções continuam guardados).
source "$(dirname "${BASH_SOURCE[0]}")/ambiente.sh"
for nome in mvp dag-processor scheduler api-server; do
  pidf="$GH_ESTADO/pids/$nome.pid"
  pid="$(cat "$pidf" 2>/dev/null)"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid"   # grupo inteiro (inclui workers gunicorn)
    for _ in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -0 "$pid" 2>/dev/null && kill -KILL -- "-$pid" 2>/dev/null
    ok "$nome parado"
  fi
  rm -f "$pidf"
done
PGBIN="$(cat "$GH_ESTADO/pgbin" 2>/dev/null || true)"
[ -n "$PGBIN" ] && "$PGBIN/pg_ctl" -D "$GH_PGDATA" status >/dev/null 2>&1 && "$PGBIN/pg_ctl" -D "$GH_PGDATA" -m fast stop >/dev/null && ok "PostgreSQL parado"
exit 0
