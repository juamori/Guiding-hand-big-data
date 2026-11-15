#!/usr/bin/env bash
# Instala pacotes usando SOMENTE as fontes APT oficiais do sistema (Debian ou Ubuntu).
#   sudo bash scripts/codespaces/apt_oficial.sh postgresql
#
# Por quê: as imagens do Codespaces trazem fontes de terceiros (ex.: Yarn, em
# /etc/apt/sources.list.d/yarn.list) que o projeto não usa. Se a chave de uma delas
# não estiver disponível, "apt-get update" falha por inteiro ("is not signed").
# Aqui o apt consulta apenas as fontes oficiais, com a verificação GPG normal:
# nada é desabilitado e nenhuma fonte de terceiros é alterada ou removida.
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "  [X]  Rode com sudo."; exit 1; }
[ $# -gt 0 ] || { echo "uso: sudo bash $0 pacote..."; exit 2; }

OFICIAIS='(deb\.debian\.org|security\.debian\.org|archive\.ubuntu\.com|security\.ubuntu\.com|ports\.ubuntu\.com)'

# Um arquivo de fontes é oficial se TODAS as URLs dele são de espelhos oficiais.
eh_oficial() {
  local urls
  urls="$(grep -E '^\s*(deb(-src)?\s|URIs:)' "$1" | grep -oE 'https?://[^ ]+' || true)"
  [ -n "$urls" ] && ! grep -qvE "$OFICIAIS" <<< "$urls"
}

DIR="$(mktemp -d)"
trap 'rm -rf "$DIR"' EXIT
LISTA_PRINCIPAL="$DIR/sources.list"
: > "$LISTA_PRINCIPAL"
if [ -f /etc/apt/sources.list ] && eh_oficial /etc/apt/sources.list; then
  cp /etc/apt/sources.list "$LISTA_PRINCIPAL"
fi
mkdir -p "$DIR/parts"
usadas=() ignoradas=()
for f in /etc/apt/sources.list.d/*.list /etc/apt/sources.list.d/*.sources; do
  [ -f "$f" ] || continue
  if eh_oficial "$f"; then cp "$f" "$DIR/parts/"; usadas+=("$(basename "$f")")
  else ignoradas+=("$(basename "$f")"); fi
done
[ -s "$LISTA_PRINCIPAL" ] && usadas=("sources.list" "${usadas[@]}")
[ ${#usadas[@]} -gt 0 ] || { echo "  [X]  Nenhuma fonte oficial Debian/Ubuntu encontrada."; exit 1; }
echo "  Fontes oficiais usadas: ${usadas[*]}"
[ ${#ignoradas[@]} -gt 0 ] && echo "  Fontes de terceiros ignoradas (não alteradas): ${ignoradas[*]}"

OPCOES=(-o "Dir::Etc::sourcelist=$LISTA_PRINCIPAL" -o "Dir::Etc::sourceparts=$DIR/parts")
apt-get "${OPCOES[@]}" update -qq
DEBIAN_FRONTEND=noninteractive apt-get "${OPCOES[@]}" install -y -qq --no-install-recommends "$@" >/dev/null
echo "  [OK] Instalado: $*"
