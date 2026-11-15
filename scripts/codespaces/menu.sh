#!/usr/bin/env bash
# Menu do Guiding Hand no GitHub Codespaces (mesmos números do GuidingHand.bat do Windows).
#   bash scripts/codespaces/menu.sh        -> menu interativo
#   bash scripts/codespaces/menu.sh 3      -> executa direto a opção 3
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/ambiente.sh"
cd "$GH_REPO" || exit 1
PY="$GH_VENV/bin/python"; [ -x "$PY" ] || PY=python3

airflow_no_ar() { curl -sf -m 3 http://127.0.0.1:8080/api/v2/version >/dev/null; }

op0_verificar() {
  titulo "Verificando o ambiente"
  [ "${GH_CODESPACE_CONFIG:-}" = "1" ] && ok "Configuração do Guiding Hand ativa (.devcontainer)" \
    || aviso "Codespace sem a configuração do projeto: Ctrl+Shift+P > 'Codespaces: Rebuild Container'"
  [ -x "$GH_VENV/bin/airflow" ] && ok "Airflow $("$PY" -c 'import airflow;print(airflow.__version__)') instalado" \
    || erro "Airflow não instalado: bash scripts/codespaces/instalar.sh"
  airflow_no_ar && ok "Airflow respondendo: $GH_URL_AIRFLOW" || aviso "Airflow parado: use a opção 2"
  [ -n "${OLHOVIVO_TOKEN:-}" ] && ok "Segredo OLHOVIVO_TOKEN disponível (valor não exibido)" \
    || aviso "Sem segredo OLHOVIVO_TOKEN: a fonte Olho Vivo (opcional) será pulada"
  if curl -s -o /dev/null -m 10 https://api.open-meteo.com/; then ok "Internet OK"; else erro "Sem acesso à internet"; fi
  ok "Data e hora em São Paulo: $(TZ=America/Sao_Paulo date '+%d/%m/%Y %H:%M')"
}

op1_validar() {
  titulo "Validação REAL das fontes (Pinheiros)"
  "$PY" scripts/validar_fontes.py
  local rel; rel="$(ls -t docs/validacao_fontes/relatorio_*.md 2>/dev/null | head -1)"
  [ -n "$rel" ] && { ok "Relatório: $rel"; [ -n "${CODESPACES:-}" ] && code "$rel" 2>/dev/null; }
  if [ -f airflow/descoberta_fontes.json ] && airflow_no_ar; then
    python3 scripts/montar_variaveis.py --chegada "$GH_REPO/chegada" --logs "$GH_REPO/logs_pipeline" >/dev/null
    af variables import airflow/variables.json >/dev/null && ok "Camadas SP156 descobertas enviadas às Variables do Airflow"
  fi
}

op2_iniciar() { titulo "Iniciando o Airflow 3"; bash scripts/codespaces/iniciar.sh; }

op3_coletar() {
  titulo "Executando a coleta agora (DAG guiding_hand_coleta)"
  airflow_no_ar || { bash scripts/codespaces/iniciar.sh || return 1; }
  local run estado="queued"
  run="manual__$(TZ=America/Sao_Paulo date +%Y%m%d_%H%M%S)"
  af dags unpause guiding_hand_coleta >/dev/null
  af dags trigger guiding_hand_coleta -r "$run" >/dev/null || { erro "Não consegui disparar o DAG"; return 1; }
  echo "  Execução $run disparada. Acompanhe em $GH_URL_AIRFLOW"
  for _ in $(seq 1 90); do
    sleep 10
    estado="$(af dags list-runs guiding_hand_coleta -o json | so_json | python3 -c "
import json,sys
try: print(next(r['state'] for r in json.load(sys.stdin) if r['run_id']=='$run'))
except Exception: print('?')")"
    echo "  ... estado: $estado"
    [ "$estado" = success ] || [ "$estado" = failed ] && break
  done
  mkdir -p docs/evidencias
  af tasks states-for-dag-run guiding_hand_coleta "$run" -o json | so_json > "docs/evidencias/airflow_$run.json"
  python3 -c "import json;json.load(open('docs/evidencias/airflow_$run.json'))" 2>/dev/null \
    && ok "Evidência salva: docs/evidencias/airflow_$run.json" || erro "Não consegui gravar a evidência da execução"
  [ "$estado" = success ] && ok "Coleta concluída com sucesso" || erro "Coleta terminou como '$estado': abra o Airflow e veja a tarefa vermelha"
  echo "  Arquivos em chegada/:"
  find chegada -name '*.json' -not -path '*_manifestos*' -printf '    %-42f %10s bytes  %TY-%Tm-%Td %TH:%TM\n' | sort | tail -12
}

op4_lote() {
  titulo "Preparar lote para o Databricks"
  local n="${1:-}"; [ -z "$n" ] && read -rp "  Número do lote (1 ou 2): " n
  "$PY" scripts/preparar_lote.py --lote "$n" || return 1
  (cd lotes && rm -f "lote_$n.zip" && zip -qj "lote_$n.zip" "lote_$n"/*.json)
  ok "Lote pronto: lotes/lote_$n/ e lotes/lote_$n.zip"
  echo "  1) No painel de arquivos à esquerda, clique com o botão direito em lotes/lote_$n.zip > Download"
  echo "  2) No Windows, clique com o botão direito no zip > Extrair Tudo"
  echo "  3) Databricks > Catalog > workspace > guiding_hand > Volumes > chegada > Upload to this volume"
  echo "     (selecione TODOS os .json extraídos)"
}

op5_testes() {
  titulo "Testes automatizados"
  "$PY" -m pytest -q tests -p no:cacheprovider "--ignore=tests/test_transformacoes.py" \
    "--ignore=tests/test_bronze_streaming.py" "--ignore=tests/test_sql_databricks.py" "--ignore=tests/test_relatorio.py"
  echo "  (os testes Spark exigem Java e rodam no ambiente de desenvolvimento; ver docs/STATUS.md)"
  airflow_no_ar && { af dags list-import-errors; af tasks list guiding_hand_coleta; }
}

op6_segredos() { titulo "Verificando segredos"; python3 scripts/verificar_segredos.py && ok "Nada sensível no repositório"; }

op7_mvp() {
  titulo "Tela do MVP"
  grep -q "guiding-hand/tb_mvp_mapa_barreiras/v1" mvp_demo/data/barreiras_mvp.json \
    && ok "Tela usando o export real do Databricks" \
    || aviso "Tela com dados de TESTE: arraste o barreiras_mvp.json do Databricks para mvp_demo/data/"
  local pidf="$GH_ESTADO/pids/mvp.pid" pid
  mkdir -p "$GH_ESTADO/pids"
  pid="$(cat "$pidf" 2>/dev/null)"; [ -n "$pid" ] && kill -- "-$pid" 2>/dev/null
  setsid nohup python3 -m http.server 8000 --bind 127.0.0.1 --directory "$GH_REPO/mvp_demo" \
    >/dev/null 2>&1 < /dev/null &
  echo $! > "$pidf"; disown
  for _ in 1 2 3 4 5; do curl -sf -m 2 http://127.0.0.1:8000/ >/dev/null && break; sleep 1; done
  curl -sf -m 2 http://127.0.0.1:8000/ >/dev/null \
    && ok "Tela no ar: $GH_URL_MVP  (aba PORTS > porta 8000). Print: docs/relatorio/prints/10_tela_mvp.png" \
    || erro "O servidor da tela não respondeu"
}

op8_relatorio() {
  titulo "Relatório PDF (ordem da FIAP)"
  if grep -q "preencha" docs/relatorio/integrantes.csv; then
    aviso "Preencha docs/relatorio/integrantes.csv (nome completo e RM) e rode de novo."
    [ -n "${CODESPACES:-}" ] && code docs/relatorio/integrantes.csv 2>/dev/null
  fi
  local url; url="$(git remote get-url origin 2>/dev/null | sed -E 's#\.git$##; s#^git@github.com:#https://github.com/#')"
  if [ -n "$url" ]; then
    python3 - "$url" <<'PY'
import json, sys
p = "docs/relatorio/config_relatorio.json"
c = json.load(open(p, encoding="utf-8-sig")); c["link_repositorio"] = sys.argv[1]
c["ambiente_airflow"] = ("GitHub Codespaces (Linux, sem Docker), Airflow 3.0.6 com PostgreSQL, LocalExecutor, "
                         "api-server, scheduler e dag-processor; o agendamento dispara com o ambiente ligado (catchup=False)")
json.dump(c, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
PY
  fi
  "$PY" scripts/gerar_relatorio.py
  ok "PDF: docs/relatorio/Relatorio_Big_Data_Guiding_Hand.pdf (botão direito > Download). Itens em vermelho = pendentes."
}

opG_publicar() {
  titulo "Publicar no GitHub"
  python3 scripts/verificar_segredos.py || { erro "Possível segredo encontrado: nada foi publicado."; return 1; }
  git add -A
  if git diff --cached --quiet; then ok "Nada novo para publicar"; return 0; fi
  git commit -qm "Guiding Hand - Big Data: atualização $(TZ=America/Sao_Paulo date '+%d/%m %H:%M')"
  git push -q && ok "Publicado: $(git remote get-url origin)" || erro "O envio falhou (veja acima)"
}

op9_parar() { titulo "Parando o Airflow"; bash scripts/codespaces/parar.sh; }

executar() {
  case "${1^^}" in
    0) op0_verificar ;; 1) op1_validar ;; 2) op2_iniciar ;; 3) op3_coletar ;; 4) op4_lote "${2:-}" ;;
    5) op5_testes ;; 6) op6_segredos ;; 7) op7_mvp ;; 8) op8_relatorio ;; G) opG_publicar ;;
    9) op9_parar ;; S) exit 0 ;;
    *) return 2 ;;
  esac
}

if [ $# -gt 0 ]; then
  executar "$@"; codigo=$?
  [ "$codigo" -eq 2 ] && aviso "Opção inválida: $1"
  exit "$codigo"
fi
while true; do
  cat <<'MENU'

==========================================================
  GUIDING HAND - Big Data (FIAP GS 2026) - Codespaces
==========================================================
  0. Verificar o ambiente
  1. Validar as fontes de dados (gera relatorio)
  2. Iniciar o Airflow
  3. Executar a coleta agora
  4. Preparar lote para o Databricks
  5. Rodar os testes automatizados
  6. Verificar segredos antes de publicar
  7. Abrir a tela do MVP
  8. Gerar o relatorio PDF
  G. Publicar no GitHub
  9. Parar o Airflow
  S. Sair
==========================================================
MENU
  read -rp "Escolha: " op || exit 0
  [ -z "$op" ] && continue
  executar "$op"; codigo=$?
  [ "$codigo" -eq 2 ] && aviso "Opção inválida: $op"
done
