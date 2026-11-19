# Execução no Databricks — lote 1 (coleta de 15/11/2026)

> Estado do **lote 1**. Os resultados finais, com os dois lotes, estão em [`lotes_1_e_2_consultas.md`](lotes_1_e_2_consultas.md).

Resultados **observados nos prints** da execução no Databricks Free Edition (serverless), recebidos em 16/11/2026.
Os números abaixo foram transcritos dos prints guardados em [`prints_databricks_lote1/`](prints_databricks_lote1/).
Nada aqui foi calculado ou estimado fora do Databricks.

> Nos prints 07 e 08, o caminho da pasta pessoal do Databricks (que contém o e-mail da conta) foi **ocultado**
> antes de entrar no repositório.

## 1. Arquivos no Volume `/Volumes/workspace/guiding_hand/chegada` — print 01

| Arquivo | Tamanho (interface) |
|---|---|
| `clima_2026-11-15.json` | 1,62 KB |
| `eventos_oculos_2026-11-15.json` | 23,58 KB |
| `osm_2026-11-15.json` | 126,32 KB |
| `sp156_2026-11-15.json` | 116,42 KB |
| `lote_1.zip` | 29,66 KB (não é lido: o Auto Loader só aceita `*.json`) |

Olho Vivo: **sem arquivo** (não há token configurado).

## 2. Bronze — notebook `bronze_lote_1` (Auto Loader) — prints 02 e 03

| Momento | Antes | Depois | Arquivos novos | Por fonte depois |
|---|---|---|---|---|
| Lote 1 — reexecução | 4 | 4 | **0** | clima 1, eventos_oculos 1, osm 1, sp156 1 |
| Lote 1 — 1ª execução | não mostrado no print | 4 (inferido do "antes" da reexecução) | — | — |

Duplicidade (célula 9): "mesmo caminho gravado mais de uma vez" → **0 linhas**; "mesmo arquivo (nome + conteúdo)
gravado mais de uma vez" → **0 linhas**. Executado em 15/11/2026, 21h03.

**Pendente:** print da célula 10 (`controle_execucoes_bronze`, com as duas linhas: 1ª execução e reexecução) e
da célula 8 (integridade SHA-256 e bytes).

## 3. Prata — notebook `02_prata` — prints 04, 05 e 06

Arquivos lidos da Bronze: **4**.

| Tabela | Inseridas | Atualizadas | Total |
|---|---|---|---|
| `prata_eventos_oculos` | 59 | 0 | 59 |
| `prata_clima_horario` | 24 | 0 | 24 |
| `prata_osm_infraestrutura` | 569 | 0 | 569 |
| `prata_sp156_solicitacoes` | 168 | 0 | 168 |
| `prata_olhovivo_paradas` | 0 | 0 | 0 |
| `prata_quarentena` | 15 | 0 | 15 |

Interpretação: 1ª execução da Prata (tudo inserido, nada atualizado).

**Pendente:** barrados **por regra** (célula "regra, fonte, registros_barrados") e a 2ª execução da Prata
(esperado: 0 inseridas e 0 atualizadas em todas as tabelas, prova do MERGE idempotente).

## 4. Ouro — notebook `03_ouro` — print 07

| Tabela | Linhas |
|---|---|
| `dim_tempo` | 114 |
| `dim_barreira` | 194 |
| `fato_ocorrencia_barreira` | 300 |
| `tb_mvp_mapa_barreiras` | 194 |

`dim_local`, `dim_tipo_obstaculo`, `dim_origem`, `aux_barreira_oculos` e `aux_mapa_celula_barreira` também são
gravadas pela mesma célula, mas a contagem delas não aparece no print.

## 5. Consultas — notebook `04_consultas_mvp` — prints 08 a 14 (16/11/2026, 10h25)

O arquivo `consultas_mvp.sql` carregou 6 consultas: C1, C2, C3, C4, C5, C6.

**C1 — tipos mais recorrentes e invisíveis à bengala** (15 linhas, todas visíveis no print)

| Tipo | Altura | Origem | Ocorrências | Barreiras distintas | Invisível p/ bengala |
|---|---|---|---|---|---|
| Buraco | chão | sp156 | 149 | 149 | não |
| Galho de árvore baixo | acima da cintura | osm | 80 | 0 | sim |
| Buraco | chão | oculos_sintetico | 13 | 9 | não |
| Calçada irregular ou quebrada | chão | oculos_sintetico | 10 | 5 | não |
| Entulho ou material de obra | abaixo da cintura | sp156 | 9 | 9 | não |
| Galho de árvore baixo | acima da cintura | oculos_sintetico | 8 | 3 | sim |
| Placa ou letreiro | acima da cintura | oculos_sintetico | 7 | 3 | sim |
| Poste | acima da cintura | oculos_sintetico | 5 | 2 | não |
| Placa ou letreiro | acima da cintura | sp156 | 4 | 4 | sim |
| Lixeira | abaixo da cintura | sp156 | 4 | 4 | não |
| Orelhão | acima da cintura | oculos_sintetico | 3 | 1 | sim |
| Carro sobre a calçada | abaixo da cintura | oculos_sintetico | 3 | 2 | não |
| Galho de árvore baixo | acima da cintura | sp156 | 2 | 2 | sim |
| Entulho ou material de obra | abaixo da cintura | oculos_sintetico | 2 | 1 | não |
| Lixeira | abaixo da cintura | osm | 1 | 0 | não |

**C2 — células (~150 m) com mais barreiras** (10 linhas): lidera `6gyccht` (-23,57323; -46,70631) com 9 barreiras
distintas e 9 ocorrências; seguem `6gycckb`, `6gyccqj` e `6gyccqc` com 8. Nas 10 células, `ocorrencias_acima_cintura` = 0.

**C3 — barreiras em mais de uma coleta:** **0 linhas.** Esperado com um único dia de coleta: a consulta exige
`count(DISTINCT data_coleta) > 1`, o que só é possível depois do lote 2.

**C4 — chuva × detecções** (apenas detecções sintéticas)

| Condição | Detecções | Horas com detecção | Detecções/hora | Precipitação média (mm) |
|---|---|---|---|---|
| sem chuva | 28 | 10 | 2,8 | 0 |
| com chuva | 23 | 5 | 4,6 | 0,25 |

**C5 — impacto** (`tb_mvp_mapa_barreiras`)

| Origem | Mapeadas | Invisíveis p/ bengala | % invisíveis | Elegíveis à confirmação | Resolvidas oficialmente | Não redetectadas |
|---|---|---|---|---|---|---|
| sp156 | 168 | 6 | 3,6 | 0 | 108 | 0 |
| oculos_sintetico | 26 | 7 | 26,9 | 6 | 0 | 0 |

A coluna `dias_medios_ate_resolucao` está cortada no print (valor não legível).

**C6 — evolução diária** (101 linhas; o print mostra as 15 primeiras, de 06/04/2020 a 30/04/2020). As datas de
2020 vêm do SP156 (série histórica do GeoSampa).

## 6. Conferência cruzada dos números (feita sobre os prints)

| Verificação | Cálculo | Resultado |
|---|---|---|
| Fato = soma de C1 | 149+80+13+10+9+8+7+5+4+4+3+3+2+2+1 | 300 = `fato_ocorrencia_barreira` ✔ |
| SP156 no fato = Prata | 149+9+4+4+2 | 168 = `prata_sp156_solicitacoes` ✔ |
| Barreiras SP156 + óculos = `dim_barreira` | C5: 168 + 26 | 194 = `dim_barreira` = `tb_mvp_mapa_barreiras` ✔ |
| Barreiras dos óculos em C1 | 9+5+3+3+2+1+2+1 | 26 = C5 ✔ |
| Invisíveis dos óculos | galho 3 + placa 3 + orelhão 1 | 7 = C5 ✔ |
| Invisíveis do SP156 | placa 4 + galho 2 | 6 = C5 ✔ |
| Detecções dos óculos no fato | 13+10+8+7+5+3+3+2 | 51 = C4 (28 + 23) ✔ |
| Eventos na Prata × no fato | 59 − 51 | 8 eventos fora do fato. O código só leva ao fato tipos de barreira (`eh_barreira`); detecções de "pessoa" ficam fora. **Não verificado** se as 8 são todas "pessoa" |
| OSM na Prata × no fato | 569 × 81 | O fato só recebe elementos OSM com tipo de obstáculo (poste, lixeira, árvore); os demais são infraestrutura (calçada, travessia, parada) |

## 7. Observações para a interpretação

- **C4 não mede efeito da chuva.** O clima é real (Open-Meteo, dia 14/11), mas as detecções são sintéticas e o gerador
  `sintetico_v1` **não usa o clima**. A diferença 2,8 × 4,6 detecções/hora é coincidência da amostra, não relação causal.
- **SP156 é histórico (abril a outubro de 2020).** As 108 "resolvidas" e as 168 barreiras do SP156 em C5 representam
  registros de 2020, não a situação atual das calçadas. A tabela do MVP ainda não marca essas barreiras como históricas.
- **OSM entra no fato sem `barreira_key`** (barreiras distintas = 0 em C1): é infraestrutura mapeada, não barreira confirmada.

## 8. Conferência com a verificação da coleta real (`docs/evidencias/coleta_2026-11-15.md`, publicada no repositório)

A verificação da coleta de 15/11 (resultado **APROVADA**, feita no Codespace) foi comparada com a Prata e a quarentena:

| Fonte | Na coleta | Na Prata | Na quarentena | Conferência |
|---|---|---|---|---|
| clima | 24 h de 14/11 (grade a 4,75 km) | 24 | 0 | 24 = 24 ✔ |
| osm | 573 elementos | 569 | 4 (RQ2) | 569 + 4 = 573 ✔ |
| sp156 | 173 feições em 7 camadas (2020-04-06 a 2020-10-01) | 168 | 5 (RQ2) | 168 + 5 = 173 ✔ |
| eventos_oculos | 67 eventos sintéticos de 14/11 | 59 | 6 | 59 + 6 = 65; os 2 restantes são compatíveis com os **reenvios** de `event_id` que o gerador injeta e a Prata deduplica (**não verificado** linha a linha) |

O lote 2 (`docs/lotes/lote_2.md`) tem `osm_2026-11-16.json` e `sp156_2026-11-16.json` com o **mesmo tamanho** dos
arquivos de 15/11 (129.356 e 119.216 bytes), mas outro SHA-256. As chaves (`osm_key` e `protocolo`) são as mesmas,
então o MERGE não cria linhas novas de OSM e SP156. O lote 2 acrescenta eventos dos óculos e clima. Resultados
finais: [`lotes_1_e_2_consultas.md`](lotes_1_e_2_consultas.md).
