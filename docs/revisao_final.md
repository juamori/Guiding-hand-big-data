# Revisão final — Guiding Hand (Big Data)

Revisão técnica de 16/11/2026, atualizada em 18 e 19/11 com as evidências do Airflow, do lote 2 e do MVP. Feita sobre o código do repositório, os testes automatizados e os 14 prints da execução
no Databricks recebidos nesse dia. **Nenhum código foi alterado nesta etapa.**

**Material analisado:** DAG e biblioteca de coleta, notebooks e SQL do Databricks, scripts, testes e documentação.
Também foram analisados os prints do Volume, da Bronze, da Prata, da Ouro e das consultas C1 a C6, e os testes
reexecutados em 16/11.

**Material não disponível nesta revisão** (vive no Codespaces ou no Databricks):
- interface do Airflow;
- relatórios de validação das fontes (`docs/validacao_fontes/`);
- verificação da coleta real (`docs/evidencias/coleta_2026-11-15.md`);
- perfil do SP156 (`perfil_sp156_2026-11-15.md`);
- arquivos exportados `evidencias_lote_1.json` e `barreiras_mvp.json`;
- repositório publicado no GitHub.

---

## Auditoria final (19/11/2026, 17h)

**Dados integrados:**
- `docs/evidencias/evidencias_lote_2.json` (ambiente `databricks`, exportado em 19/11 às 13:16 UTC);
- `mvp_demo/data/barreiras_mvp.json` (204 barreiras: 168 SP156 + 36 óculos; 204 IDs únicos; nenhuma coluna de dispositivo).

**Conferências que fecharam:**
- **Fato:** 365 = 116 + 168 + 81.
- **Barreiras:** 204 = `dim_barreira` = `tb_mvp_mapa_barreiras` = linhas do JSON.
- **Tela:** 204 / 17 / 22 / 108, igual à C5 e ao JSON.
- **Quarentena:** 20 = 15 do lote 1 + 5 eventos de 16/11.
- **Eventos dos óculos:** 145 − 11 − 4 = 130.

**Campo `etapas` vazio:** o gerador do PDF não usa esse campo. Ele só é preenchido quando o `00_processar_lote`
chama o `06_exportar_evidencias`. Foi mantido como veio.

**Testes executados:**

| Conjunto | Ambiente | Resultado |
|---|---|---|
| Suíte completa (`pytest tests`) | Python 3.13, PySpark 4.0.1, Java 21 | 94 aprovados, 1 ignorado (`test_dag.py`, sem Airflow nesse Python), 0 falhas |
| `tests/test_dag.py` | Airflow 3.0.6, Python 3.12 | 3 aprovados |
| Importação da DAG (DagBag) | Airflow 3.0.6 | `guiding_hand_coleta`, sem erros de importação |
| Tela do MVP | Node + jsdom | OK |

No Codespace (sem PySpark), a mesma suíte dá 68 aprovados e 6 ignorados: são os arquivos de teste que exigem Spark.

**PDF:** `docs/entrega/Guiding_Hand_Global_Solution_2026.pdf`, 12 páginas, sem "PRÉVIA", sem PENDENTE e sem
placeholders. Revisado página por página.

## 1. Inventário de tecnologias (confirmadas no código)

| Tecnologia | Onde está | Evidência de uso real |
|---|---|---|
| Python 3.12 | `dags/`, `scripts/`, `databricks/lib/` | Testes; coleta de 15/11 |
| Apache Airflow 3.0.6 (TaskFlow `airflow.sdk`) | `dags/guiding_hand_coleta.py` | 3 testes do DAG; prints da interface **pendentes** |
| PostgreSQL + LocalExecutor | `scripts/codespaces/`, `docker-compose.yml` | Ensaio registrado em `docs/STATUS.md` |
| GitHub Codespaces | `.devcontainer/`, `scripts/codespaces/` | Uso relatado pela usuária (menus 1, 3 e 5) |
| Databricks Free Edition (serverless) | `databricks/` | Prints de 15 e 16/11 |
| Unity Catalog e Volumes | `databricks/00_setup.sql` | Print 01 (Volume `chegada`) |
| Auto Loader (`cloudFiles`) | `bronze/bronze_lote_1.py`, `bronze/01_bronze_autoloader.py` | Prints 02 e 03 |
| Delta Lake (MERGE, `DESCRIBE HISTORY`) | `prata/02_prata.py`, `lib/gh_transform.py` | Print 06 |
| PySpark e Spark SQL | `lib/gh_transform.py`, `lib/ouro_sql.py` | Prints 06 a 14 |
| pytest | `tests/` | 94 + 3 testes passaram em 16/11 (seção 6) |
| HTML/JS (tela do MVP) | `mvp_demo/index.html` | Só com JSON de demonstração |
| **COPY INTO** | não usado | — |
| **MongoDB** | não usado (decisão em `docs/arquitetura.md`) | — |

## 2. Inventário de arquivos relevantes

| Arquivo | Função |
|---|---|
| `dags/guiding_hand_coleta.py` | DAG: 5 coletas paralelas, `conferir`, `notificar_resultado` |
| `dags/guiding_hand_lib/coletores.py` | Chamadas às APIs, paginação WFS, máscara de segredos, gravação em `chegada/` |
| `dags/guiding_hand_lib/conferencia.py` | Conferência dos arquivos do dia e notificações |
| `dags/guiding_hand_lib/config.py` | Recorte, fontes obrigatórias, URLs padrão |
| `dags/guiding_hand_lib/sintetico.py` | Gerador `sintetico_v1` (eventos dos óculos) |
| `databricks/00_setup.sql` | Schema `workspace.guiding_hand` e Volumes |
| `databricks/bronze/bronze_lote_1.py` | Bronze autônoma usada no lote 1 |
| `databricks/bronze/01_bronze_autoloader.py` | Bronze do orquestrador (lotes seguintes) |
| `databricks/prata/02_prata.py` | Regras, quarentena, MERGE |
| `databricks/ouro/03_ouro.py` | Dimensões, resolução de entidade, fato, tabela do MVP |
| `databricks/ouro/04_consultas_mvp.py` | Executa C1 a C6 |
| `databricks/mvp/05_exportar_mvp.py` | Exporta `barreiras_mvp.json` |
| `databricks/mvp/06_exportar_evidencias.py` | Exporta `evidencias_lote_<N>.json` |
| `databricks/00_processar_lote.py` | Orquestrador (Bronze ×2, Prata ×2, Ouro, exportações, checagens) |
| `databricks/lib/gh_transform.py` | Transformações da Bronze, Prata e dimensões |
| `databricks/lib/ouro_sql.py` | SQL das tabelas da Ouro e coleta de evidências |
| `databricks/lib/sp156_mapeamento.py` + `mapeamento_sp156.json` | Campos reais do SP156 (protocolo decidido pelos dados) |
| `databricks/sql/consultas_mvp.sql` | Consultas C1 a C6 |
| `databricks/sql/contagens_bronze.sql` | Contagens e prova de não duplicação da Bronze |
| `scripts/validar_fontes.py` | Validação das fontes reais no recorte |
| `scripts/verificar_chegada.py` | Verificação do conteúdo de uma coleta |
| `scripts/perfil_sp156.py` | Perfil dos dados reais do SP156 e decisão do protocolo |
| `scripts/preparar_lote.py` | Monta `lotes/lote_N.zip` |
| `scripts/verificar_segredos.py` | Varredura de segredos antes de publicar |
| `scripts/gerar_relatorio.py` | PDF na ordem da FIAP |
| `mvp_demo/index.html` | Tela de referência do MVP |

## 3. Matriz de conformidade (19/11/2026)

Situação: **CONCLUÍDO E COMPROVADO**, **CONCLUÍDO, MAS SEM EVIDÊNCIA**, **PENDENTE** ou **PRECISA DE VALIDAÇÃO EXTERNA**.

| Requisito | Situação real | Evidência encontrada | Pendência |
|---|---|---|---|
| R01 Fontes (≥ 2, ≥ 1 API real) | CONCLUÍDO E COMPROVADO | `docs/evidencias/coleta_2026-11-15.md` (APROVADA); `docs/validacao_fontes/`; coletores no código | — |
| R02 DAG agendada, um arquivo por fonte e dia | CONCLUÍDO E COMPROVADO | `dags/guiding_hand_coleta.py`; prints 01 e 02; `docs/lotes/lote_*.md` (arquivos de 15 e 16/11 com SHA-256) | — |
| R03 Paralelismo, conferência, Variables, falhas | CONCLUÍDO E COMPROVADO | Código; 3 testes do DAG; print 03 (Variables) | — |
| R04 Dois lotes de dias diferentes, upload manual | CONCLUÍDO E COMPROVADO | Prints 05 (lote 1) e 06 (Volume com os arquivos de 15 e 16/11); `lote_*.md`; tabela de controle (4 → 8) | — |
| R05 Bronze incremental, sem duplicação, contagens | CONCLUÍDO E COMPROVADO | Print 07 (controle: 0 → 4, 4 → 4, 4 → 4, 4 → 8); prints de duplicidade vazios | Reexecução do lote 2 não registrada (opcional) |
| R06 Prata com ≥ 3 regras e barrados | CONCLUÍDO E COMPROVADO | Print 08 (15 barrados por regra, lote 1); `evidencias_lote_2.json` (20 barrados após os dois lotes: RQ2 11, RQ3 4, RQ4 2, RQ5 3) | — |
| R07 Fato + ≥ 2 dimensões; 5 consultas, 1 de impacto | CONCLUÍDO E COMPROVADO | Print 09 (lote 1); prints C1 a C6 com os dois lotes (`prints_lote2_final/`); `evidencias_lote_2.json` | — |
| R08 LGPD, tabela Ouro consumida, tela | CONCLUÍDO E COMPROVADO | Código e teste (tabela sem dado de dispositivo); `barreiras_mvp.json` (204); print 10 (tela com 204) | — |
| R09 Repositório público com README, sem segredos | PRECISA DE VALIDAÇÃO EXTERNA | Scanner de segredos limpo; nenhum e-mail ou caminho pessoal nos arquivos; README com links verificados | Publicar esta versão no GitHub (push ainda não feito) |
| Relatório PDF | CONCLUÍDO E COMPROVADO quando gerado com o JSON do Databricks e os 10 prints | `docs/entrega/Guiding_Hand_Global_Solution_2026.pdf` | Entrega no portal (ação da equipe) |

O que depende de quem:
- **Validável pelo código e pelos testes:** DAG, transformações, SQL, LGPD da tabela do MVP, tela.
- **Validável pelos arquivos:** coleta real, lotes, `evidencias_lote_2.json`, JSON do MVP.
- **Depende do Databricks:** contagens da Bronze, Prata e Ouro, e as consultas (prints e JSON exportado).
- **Depende do Airflow:** execuções agendadas (prints).
- **Depende da equipe:** publicação no GitHub e envio do PDF no portal FIAP.

## 4. Prints do relatório (`docs/relatorio/prints/`)

| Arquivo | Conteúdo | Origem |
|---|---|---|
| `01_airflow_graph.png` | Graph do DAG | Airflow, 16/11 |
| `02_airflow_grid.png` | Overview com 3 execuções em 15 e 16/11 | Airflow, 16/11 |
| `03_airflow_variables.png` | Admin › Variables | Airflow, 18/11 |
| `04_pasta_chegada.png` | Sem print. O PDF usa o inventário real de `chegada/` registrado por `preparar_lote.py` em `docs/lotes/` | — |
| `05_upload_lote1.png` | Volume `chegada` com o lote 1 | Databricks, 16/11 |
| `06_upload_lote2.png` | Volume `chegada` com os 8 arquivos de 15 e 16/11 | Databricks, 19/11 |
| `07_bronze_contagens.png` | `controle_execucoes_bronze` com os lotes 1 e 2 | Databricks, 18/11 |
| `08_prata_quarentena.png` | Quarentena por regra (lote 1) | Databricks, 18/11 |
| `09_ouro_tabelas.png` | Tabelas da Ouro (lote 1; caminho pessoal ocultado) | Databricks, 15/11 |
| `10_tela_mvp.png` | Tela do MVP com 204 barreiras | Codespace, 19/11 |

## 5. Inconsistências, riscos e requisitos não comprovados

| # | Gravidade | Achado | Consequência | Ação sugerida |
|---|---|---|---|---|
| 1 | **Alta** | Os notebooks executados no Databricks usam `sys.path.insert(0, "/Workspace/Users/<e-mail>/lib")` e o mesmo caminho para `consultas_mvp.sql` (prints 07 e 08) | Se esses notebooks forem exportados para o repositório, o e-mail vai para um repositório público (R09 e LGPD). Os prints já foram salvos aqui com o e-mail ocultado | Não exportar os notebooks do workspace para o Git; manter no repositório a versão com `../lib` |
| 2 | Resolvido (18/11) | `tests/test_ambiente_python.py` gravava `docs/evidencias/coleta_2026-11-13.*` (dados de simulador) dentro do repositório | — | `verificar_chegada.py` ganhou `--saida`; o teste grava numa pasta temporária, e a proteção da suíte falha se algum teste criar arquivo em `docs/evidencias/`. Apagar os arquivos já publicados |
| 3 | Resolvido (19/11) | Lote 2 (16/11) ingerido na Bronze (4 → 8) e processado na Prata e na Ouro; C3 com 24 barreiras em 2 coletas; print do Volume com os dois lotes | — | — |
| 4 | Média | O notebook `01_bronze_autoloader` e o `00_processar_lote` **nunca rodaram no Databricks**. Eles usam caminhos relativos (`../lib`, `./bronze/...`), que só funcionam com o repositório clonado como Git folder | O lote 2 pode falhar na importação, se o workspace não tiver a mesma estrutura de pastas | Processar o lote 2 a partir de um Git folder, ou ajustar os caminhos só no workspace (sem levar o e-mail para o Git) |
| 5 | Resolvido em parte (18/11) | SP156 é histórico (2020) | A tela do MVP agora marca "registro histórico (2020)" em cada solicitação com mais de 1 ano e explica isso no indicador de resolvidas e na legenda. A tabela Ouro continua sem uma coluna própria para isso | Declarar no relatório |
| 6 | Média | C4 cruza clima real com detecções sintéticas que não dependem do clima | A diferença 2,8 × 4,6 detecções/hora não tem significado causal | Apresentar C4 como demonstração de integração, não como achado |
| 7 | Resolvido | Barrados por regra: print da quarentena de 18/11 (RQ2 10, RQ3 2, RQ4 1, RQ5 2) | — | Reexecução da Prata (0/0) continua sem print (opcional) |
| 8 | Resolvido (19/11) | Tela do MVP com o JSON final exportado do Databricks (204 barreiras: 168 SP156 + 36 óculos) | — | — |
| 9 | Baixa | O prompt de revisão fala em "cinco consultas"; o projeto tem seis (C1 a C6) | Nenhuma: a FIAP pede no mínimo 5 | Manter as 6; C5 é a de impacto |
| 10 | Baixa | O requisito cita `chegada/<fonte>_<data>.json`; o DAG grava em `chegada/<fonte>/<fonte>_<data>.json` | O nome do arquivo segue o padrão; só muda a subpasta | Mencionar no relatório |
| 11 | Resolvido | `docs/regras_qualidade.md`: coluna "Barrados (Databricks)" preenchida em 18/11 | — | — |
| 12 | Baixa | Prata: 59 eventos, mas 51 entram no fato. Pelo código, só tipos de barreira entram (falsos alarmes de "pessoa" ficam fora) | Coerente com o código, mas a composição das 8 linhas não foi verificada | Conferir com `SELECT tipo_obstaculo, count(*) FROM prata_eventos_oculos GROUP BY 1` |
| 13 | Baixa | `docs/STATUS.md` e `docs/fontes.md` diziam "nada executado no Databricks" e "coleta oficial pendente" | Desatualizado em relação aos prints | Atualizado nesta revisão (só documentação) |
| 14 | Informativo | Olho Vivo implementado, mas sem token: 0 registros | Não pode ser apresentado como fonte usada | Já tratado no README |
| 15 | Resolvido | Integrantes preenchidos em 18/11 (`docs/relatorio/integrantes.csv` e README) | — | — |
| 16 | Resolvido (19/11) | A Prata não tinha processado o lote 2 (print de `prata_eventos_oculos`: só 15/11) | — | Prata, Ouro, consultas e MVP reprocessados: 130 eventos únicos, 365 ocorrências, 204 barreiras; `evidencias_lote_2.json` exportado |
| 17 | Baixa | O lote 2 tem só a 1ª execução na tabela de controle | O R05 já está coberto pelo lote 1, mas a evidência do lote 2 fica incompleta | Rodar o `bronze_lote_1` com lote = 2 mais uma vez (esperado 8 → 8, 0 novos) |

## 6. Revisão cruzada (documentação × código × SQL × testes × prints)

| Afirmação | Código | SQL | Testes | Prints | Conclusão |
|---|---|---|---|---|---|
| Bronze com Auto Loader, `availableNow`, checkpoint | `bronze_lote_1.py`, `01_bronze_autoloader.py` | — | `test_bronze_lote_1`, `test_bronze_streaming` (Spark local) | 02, 03 | Coerente |
| Reexecução da Bronze não duplica | `foreachBatch` + checkpoint | `contagens_bronze.sql` | sim | 02 (0 novos), 03 (0 duplicados) | Coerente |
| 7 regras de qualidade | `gh_transform.py` (RQ1 a RQ7) | — | `test_transformacoes` | Total 15; por regra não mostrado | Parcial |
| MERGE idempotente | `sql_merge` com `hash_registro` | — | `test_sql_databricks`, `test_transformacoes` | Só a 1ª execução | Parcial |
| Fato + 5 dimensões | `03_ouro.py`, `ouro_sql.py` | — | `test_sql_databricks` | 07 (4 tabelas) | Coerente; 3 dimensões sem contagem no print |
| Fato = 300 | — | C1 | — | Soma de C1 = 300 | Coerente |
| 194 barreiras | `dim_barreira` = óculos + SP156 | C5 | — | 168 + 26 = 194 | Coerente |
| MVP sem dado pessoal | `TB_MVP_MAPA` sem `device_hash` | — | `test_sql_databricks` | — | Coerente (código e teste) |
| Protocolo SP156 = `cd_identificador` | `mapeamento_sp156.json` do Codespace | — | `test_perfil_sp156` | 168 válidos, 149 buracos com chave distinta | Coerente com o relato; arquivo do perfil não recebido |
| Clima D-1 | `dia_anterior()` no DAG | — | `test_coletores` | 24 horas na Prata | Coerente |

**Testes reexecutados em 16/11/2026** (fora do Databricks):
- Python 3.13 + PySpark 4.0.1 + Java 21: **94 passaram, 1 pulado** (o DAG, sem Airflow nesse Python).
- Airflow 3.0.6 + Python 3.12: **3 passaram** (`test_dag.py`).

## 7. Ações que dependem da equipe

1. Publicar no GitHub a versão final (pacote `Guiding_Hand_Entrega_Final_2026.zip`).
2. Enviar o PDF `docs/entrega/Guiding_Hand_Global_Solution_2026.pdf` no portal da FIAP até 19/11/2026, 23h55.
3. Autorizar a exclusão de `docs/evidencias/coleta_2026-11-13.md/.json`, artefato de teste que não é evidência real.
