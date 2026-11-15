#!/usr/bin/env bash
# Executa o GuidingHand.bat num cmd.exe real (Wine) e registra qual script cada opção chama.
# Um "powershell.bat" falso, colocado só na cópia de teste, registra a chamada em vez de executá-la.
# Uso (Linux com wine64): bash tests/integracao/testar_menu_cmd.sh
set -u
RAIZ="$(cd "$(dirname "$0")/../.." && pwd)"
TMP="$(mktemp -d)"; cp "$RAIZ/GuidingHand.bat" "$TMP/"
printf '@echo off\r\necho %%* >> "%%~dp0chamadas.txt"\r\n' > "$TMP/powershell.bat"
WINE="${WINE:-/usr/lib/wine/wine64}"; export WINEDEBUG=-all
cd "$TMP"
for op in 0 1 2 3 4 5 6 7 8 G g 9 X S s ""; do
  rm -f chamadas.txt; printf '%s\r\n' "$op" > entrada.txt
  timeout 8 "$WINE" cmd /c GuidingHand.bat < entrada.txt > /dev/null 2>&1; codigo=$?
  chamada=$( [ -f chamadas.txt ] && tr -d '\r' < chamadas.txt | head -1 | grep -o 'scripts\\windows\\[^"]*' || echo "(nenhuma)")
  printf "opcao %-4s -> %-40s %s\n" "'$op'" "$chamada" "$([ $codigo = 124 ] && echo '(voltou ao menu)' || echo '(encerrou)')"
done
rm -rf "$TMP"
