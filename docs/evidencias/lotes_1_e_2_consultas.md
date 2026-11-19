# Consultas com os lotes 1 e 2 (19/11/2026)

Prints em [`prints_lote2_final/`](prints_lote2_final/). O caminho da pasta pessoal do workspace foi ocultado nos
prints 02 e 06. Os números abaixo foram transcritos dos prints.

## Sequência observada

1. **Print 01 — antes do reprocessamento.** `prata_eventos_oculos` só tinha `data_coleta` = 2026-11-15 (59 linhas).
   Isso confirmou que a Prata ainda não tinha processado o lote 2.
2. **Reprocessamento:** Prata e Ouro foram executadas de novo e, depois, o `04_consultas_mvp` (19/11, 10h07).
   As consultas abaixo mostram dados das duas coletas: a C3 passou a ter 24 linhas, todas com `coletas_distintas` = 2.
3. **Print 06 — `06_exportar_evidencias` com lote = 2.** Gravou `/Volumes/workspace/guiding_hand/exportacao/evidencias_lote_2.json`,
   com 6 consultas. Esse arquivo é a fonte das contagens finais do relatório PDF.

## Resultados (lotes 1 e 2)

| Consulta | Lote 1 (15/11) | Lotes 1 e 2 (19/11) |
|---|---|---|
| C1: buraco (óculos) | 13 ocorrências, 9 barreiras | 30 ocorrências, 10 barreiras |
| C1: linhas | 15 | 16 |
| C2: célula líder | `6gyccht`, 9 barreiras | `6gyccht`, 9 barreiras |
| C3: barreiras em mais de uma coleta | 0 linhas (um só dia) | **24 linhas** (ex.: buraco `6gycchy`, 2 coletas, 6 detecções) |
| C4: sem chuva | 28 detecções, 10 h, 2,8/h | 70 detecções, 19 h, 3,68/h |
| C4: com chuva | 23 detecções, 5 h, 4,6/h | 46 detecções, 11 h, 4,18/h |
| C5: SP156 | 168 mapeadas, 6 invisíveis (3,6%) | 168 mapeadas, 6 invisíveis (3,6%) |
| C5: óculos (sintético) | 26 mapeadas, 7 invisíveis (26,9%) | **36** mapeadas, **11** invisíveis (30,6%) |
| C6: dias com movimento | 101 | 102 |

Total de barreiras depois do lote 2: 168 + 36 = **204**.

- **SP156 sem barreira nova:** o arquivo de 16/11 tem o mesmo tamanho do de 15/11, mas outro SHA-256
  (`docs/lotes/lote_2.md`). Os protocolos são os mesmos, e o MERGE por `protocolo` não cria linha nova.
- **Óculos:** a coleta de 16/11 trouxe novas detecções. Parte delas reforça barreiras já vistas (C3) e parte cria
  barreiras novas (de 26 para 36).
- **C4:** continua sem significado causal, porque o gerador sintético não usa o clima.

## Tela do MVP

Resolvido em 19/11: o `05_exportar_mvp` foi executado depois do reprocessamento. O `mvp_demo/data/barreiras_mvp.json`
tem **204 barreiras** (168 SP156 + 36 óculos), e a tela (`docs/relatorio/prints/10_tela_mvp.png`) mostra:
- 204 mapeadas;
- 17 na altura da cabeça;
- 22 prontas para confirmação;
- 108 resolvidas pela zeladoria.

## Contagens finais (`evidencias_lote_2.json`, exportado em 19/11/2026 13:16 UTC)

| Tabela | Linhas |
|---|---|
| prata_eventos_oculos | 130 |
| prata_clima_horario | 48 |
| prata_osm_infraestrutura | 569 |
| prata_sp156_solicitacoes | 168 |
| prata_olhovivo_paradas | 0 |
| prata_quarentena | 20 |
| dim_tempo | 114 |
| dim_local | 123 |
| dim_tipo_obstaculo | 11 |
| dim_origem | 3 |
| dim_barreira | 204 |
| fato_ocorrencia_barreira | 365 |
| tb_mvp_mapa_barreiras | 204 |

**Conferências:**
- **Fato:** 365 = 116 detecções dos óculos (C4: 70 + 46) + 168 SP156 (C1: 149 + 9 + 4 + 4 + 2) + 81 OSM (C1: 80 + 1).
- **Barreiras:** 204 = 168 + 36 (C5) = linhas do `barreiras_mvp.json`.
- **Tela do MVP:** 17 invisíveis (6 + 11), 22 elegíveis e 108 resolvidas, iguais à C5 e ao JSON.
- **Quarentena:** 11 eventos barrados (RQ2 2 + RQ3 4 + RQ4 2 + RQ5 3); 145 lidos − 11 barrados − 4 duplicados = 130.

**Sobre o campo `"etapas": {}` do JSON:**
- Esse campo só é preenchido quando o `06_exportar_evidencias` é chamado pelo orquestrador `00_processar_lote`, que
  repassa a saída de cada notebook.
- Neste trabalho, os notebooks foram executados um a um, então o campo ficou vazio, como deveria.
- O gerador do PDF não usa esse campo. As contagens da Bronze vêm de `bronze_execucoes`, lida da tabela
  `controle_execucoes_bronze`.
- Nada foi preenchido à mão.
