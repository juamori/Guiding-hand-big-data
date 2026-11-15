#!/usr/bin/env bash
# PRIMEIRA VEZ: importa o ZIP do projeto para o repositório aberto no Codespace e publica no GitHub.
# No terminal do Codespace, com o ZIP arrastado para a raiz do repositório:
#   unzip -q "$(ls -t guiding-hand-big-data*.zip | head -1)" && bash guiding-hand-big-data/scripts/codespaces/importar_zip.sh
set -euo pipefail
ORIGEM="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DESTINO="$(git -C "$(dirname "$ORIGEM")" rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$DESTINO" ] || { echo "  [X] Rode dentro da pasta do repositório (aberto no Codespace)."; exit 1; }
[ "$ORIGEM" != "$DESTINO" ] || { echo "  [OK] O projeto já está na raiz do repositório."; exit 0; }

echo "  Copiando o projeto para $DESTINO ..."
cp -a "$ORIGEM/." "$DESTINO/"
rm -rf "$ORIGEM"; rm -f "$DESTINO"/guiding-hand-big-data*.zip   # aceita "guiding-hand-big-data (1).zip"
cd "$DESTINO"
python3 scripts/verificar_segredos.py || { echo "  [X] Possível segredo encontrado: nada foi publicado."; exit 1; }
git add -A
git -c user.name="${GIT_AUTHOR_NAME:-$(git config user.name || echo 'Guiding Hand')}" commit -qm "Guiding Hand - Big Data (FIAP GS 2026)" || true
git push -q && echo "  [OK] Projeto publicado em $(git remote get-url origin)"
cat <<'MSG'

  PRÓXIMO PASSO (uma vez só): ativar a configuração do projeto neste Codespace
    Ctrl+Shift+P  ->  digite "Rebuild Container"  ->  "Codespaces: Rebuild Container"  ->  Rebuild
  A reconstrução instala PostgreSQL e Airflow 3 sozinha (5 a 10 minutos).
  Depois, no terminal:  bash scripts/codespaces/menu.sh
MSG
