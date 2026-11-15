#!/usr/bin/env bash
# Instalação (roda sozinha quando o Codespace é criado ou reconstruído).
# PostgreSQL 15 (apt) + Airflow 3.0.6 (pip, com as constraints oficiais) + dependências dos scripts.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/ambiente.sh"
SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
mkdir -p "$GH_ESTADO" "$GH_LOGS"

titulo "1/4 PostgreSQL"
if ! ls /usr/lib/postgresql/*/bin/initdb >/dev/null 2>&1; then
  $SUDO apt-get update -qq
  $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql >/dev/null
fi
PGBIN="$(ls -d /usr/lib/postgresql/*/bin | sort -V | tail -1)"
PGVER="$("$PGBIN/postgres" --version | grep -oE '[0-9]+' | head -1)"
[ "$PGVER" -ge 13 ] || { erro "PostgreSQL $PGVER é antigo para o Airflow 3 (mínimo 13). Reconstrua o Codespace com a configuração do repositório."; exit 1; }
echo "$PGBIN" > "$GH_ESTADO/pgbin"
ok "PostgreSQL $PGVER instalado"

titulo "2/4 Ambiente Python e Airflow 3.0.6 (3 a 6 minutos)"
# Airflow 3.0.6 tem constraints oficiais para Python 3.9 a 3.12: escolhe explicitamente um compatível.
PYBASE=""
for cand in python3.12 python3.11 python3.10 python3; do
  if command -v "$cand" >/dev/null && "$cand" -c 'import sys; sys.exit(0 if (3,9) <= sys.version_info[:2] <= (3,12) else 1)'; then
    PYBASE="$(command -v "$cand")"; break
  fi
done
[ -n "$PYBASE" ] || { erro "Nenhum Python entre 3.9 e 3.12 encontrado (Airflow 3.0.6). Reconstrua o Codespace com a configuração do repositório."; exit 1; }
PYV="$("$PYBASE" -c 'import sys;print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"
if [ -x "$GH_VENV/bin/python" ] && ! "$GH_VENV/bin/python" -c 'import sys; sys.exit(0 if sys.version_info[:2] <= (3,12) else 1)'; then
  rm -rf "$GH_VENV"   # venv criado com Python incompatível
fi
[ -x "$GH_VENV/bin/python" ] || "$PYBASE" -m venv "$GH_VENV"
ok "Python $PYV ($PYBASE)"
if ! "$GH_VENV/bin/python" -c "import airflow, psycopg2; assert airflow.__version__ == '3.0.6'" 2>/dev/null; then
  "$GH_VENV/bin/pip" install -q --upgrade pip
  "$GH_VENV/bin/pip" install -q "apache-airflow[postgres]==3.0.6" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-${PYV}.txt"
fi
"$GH_VENV/bin/pip" install -q -r "$GH_REPO/requirements-coleta.txt" pytest pypdf
ok "Airflow $("$GH_VENV/bin/python" -c 'import airflow;print(airflow.__version__)') instalado em $GH_VENV"

titulo "3/4 Chaves internas do Airflow (fora do repositório)"
if [ ! -f "$GH_ESTADO/chaves.env" ]; then
  "$GH_VENV/bin/python" - > "$GH_ESTADO/chaves.env" <<'PY'
import base64, os, secrets
print("AIRFLOW__CORE__FERNET_KEY=" + base64.urlsafe_b64encode(os.urandom(32)).decode())
print("AIRFLOW__API_AUTH__JWT_SECRET=" + secrets.token_urlsafe(32))
PY
  chmod 600 "$GH_ESTADO/chaves.env"
fi
ok "chaves em $GH_ESTADO/chaves.env"

titulo "4/4 Banco do Airflow"
bash "$GH_REPO/scripts/codespaces/iniciar.sh" --so-banco
ok "Instalação concluída. O Airflow sobe sozinho a cada abertura do Codespace."
