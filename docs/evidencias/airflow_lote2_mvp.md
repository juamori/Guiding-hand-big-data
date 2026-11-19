# Evidências do Airflow, do lote 2 e do MVP (16 a 18/11/2026)

Transcrição dos prints guardados em [`prints_airflow_lote2_mvp/`](prints_airflow_lote2_mvp/). Nada aqui foi
calculado fora dos prints.

## Airflow (Codespaces) — prints 01, 02 e 04

- **DAG `guiding_hand_coleta`:**
  - agendamento `0 7 * * *`, tags `fiap-gs-2026`, `guiding-hand` e `big-data`;
  - última execução em 16/11/2026 07:00 com sucesso; próxima em 17/11/2026 07:00.
- **Três execuções** (overview): 15/11 07:00 (agendada), 15/11 07:14:18 (manual) e 16/11 07:00 (agendada).
  - Painel das últimas 24 h: **0 tarefas com falha** e **0 execuções com falha**.
  - `coletar_olhovivo` foi **pulada** nas três (sem token).
  - `coletar_sp156` foi pulada na primeira e concluída nas outras duas.
  - As demais tarefas terminaram em sucesso.
- **Graph:** `inicio` → `gerar_eventos_oculos`, `coletar_clima`, `coletar_olhovivo`, `coletar_osm`, `coletar_sp156`
  (em paralelo) → `conferir` → `notificar_resultado`.
- **Variables:** `gh_sp156_camadas`, `gh_pasta_logs`, `gh_pasta_chegada`, `gh_overpass_urls`, `gh_olhovivo_termos`
  e `gh_bbox` (-23.5750, -46.7100, -23.5600, -46.6940), todas com "Is Encrypted = true". Não há `gh_olhovivo_token`,
  o que é coerente com o Olho Vivo pulado.

## Testes no Codespace — print 03

`python -m pytest tests/ -vv`: **68 passaram, 6 pulados**, `EXIT_CODE=0`. Os pulados são os testes que precisam de
PySpark, que não está instalado no Codespace. Os testes do DAG passaram com o Airflow do Codespace.

## Lote 2 — prints 05 e 06

`scripts/preparar_lote.py --lote 2 --datas 2026-11-16` → `lotes/lote_2`, **4 arquivos**, data 2026-11-16, sem aviso.

`controle_execucoes_bronze` (consulta de 18/11):

| Rótulo | Antes | Depois | Novos | Executado em (UTC) |
|---|---|---|---|---|
| lote 1 - 1a execucao | 0 | 4 | 4 | 15/11 23:57:05 |
| lote 1 - reexecucao | 4 | 4 | 0 | 15/11 23:58:43 |
| lote 1 - reexecucao | 4 | 4 | 0 | 16/11 00:03:47 |
| lote 2 - 1a execucao | 4 | 8 | 4 | 17/11 03:43:27 |

Depois do lote 2: clima 2, eventos_oculos 2, osm 2 e sp156 2 arquivos (a coluna `depois_por_fonte` aparece cortada
no print depois do terceiro valor).

**Não há linha "lote 2 - reexecucao".** O R05 já está demonstrado pela reexecução do lote 1, mas rodar o
`bronze_lote_1` (widget lote = 2) mais uma vez completa a evidência do lote 2.

## Prata — quarentena (print 07)

`SELECT * FROM prata_quarentena`: **15 registros, todos com `data_coleta` = 2026-11-15**.

| Regra | eventos_oculos | osm | sp156 | Total |
|---|---|---|---|---|
| RQ2_FORA_DO_RECORTE | 1 | 4 | 5 | 10 |
| RQ3_TIPO_FORA_DO_DOMINIO | 2 | — | — | 2 |
| RQ4_CONFIANCA_INVALIDA | 1 | — | — | 1 |
| RQ5_CONFIANCA_ABAIXO_DO_LIMIAR | 2 | — | — | 2 |
| **Total** | 6 | 4 | 5 | **15** |

**Atualização de 19/11:** confirmado. A Prata ainda não tinha processado o lote 2 quando este print foi feito
(`prata_eventos_oculos` só tinha 15/11). Ela foi reprocessada em seguida: 145 eventos lidos, 11 barrados, 4 duplicados
e 130 únicos. Ver [`lotes_1_e_2_consultas.md`](lotes_1_e_2_consultas.md).

Análise original: o gerador sintético injeta registros inválidos novos todos os dias, com `event_id` diferentes. Se a
Prata tivesse processado a coleta de 16/11, a quarentena teria linhas com `data_coleta` = 2026-11-16. A ausência delas
indica que **a Prata provavelmente não foi reexecutada depois do lote 2**. Isso precisa ser confirmado (ver
`docs/revisao_final.md`).

## Ouro e MVP — prints 08 e 09 (estado de 18/11, antes do reprocessamento do lote 2)

- **`tb_mvp_mapa_barreiras` (amostra de 20 linhas):** colunas `barreira_id`, `origem`, `tipo_obstaculo`, `descricao`,
  `altura`, `bengala_costuma_detectar`, `lat`, `lon`, `geohash7`, `n_ocorrencias`, `n_dispositivos`, `dias_com_deteccao`
  e `primeira_vez`. Nenhuma coluna de identificação de dispositivo.
- **Tela do MVP:** "194 barreiras · gerado em 2026-11-18T23:50:39 · dados exportados da tabela Ouro
  `tb_mvp_mapa_barreiras` do Databricks". Indicadores:

  | Indicador | Valor | Origem do número |
  |---|---|---|
  | Barreiras mapeadas | 194 | C5: 168 + 26 |
  | Na altura da cabeça | 13 | C5: invisíveis 6 + 7 |
  | Prontas para confirmação | 6 | C5 |
  | Resolvidas pela zeladoria | 108 | C5 |

  Os indicadores são **iguais** aos da C5 do lote 1. **A tela final, de 19/11, mostra 204 barreiras** (`docs/relatorio/prints/10_tela_mvp.png`). A lista mostra barreiras dos óculos com "última vez" em 14/11 e
  uma em **15/11** (galho de árvore baixo, quadra `6gyccm1`). A data de 15/11 sugere que há eventos da coleta de 16/11
  na Ouro, o que contradiz a quarentena. Os dois sinais precisam ser conferidos com as consultas da revisão final.
- As barreiras do SP156 aparecem como "zeladoria: aberta" ou "resolvida", com a data de 2020. A tela não diz
  explicitamente que esses registros são históricos.
