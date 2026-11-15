# Situação do projeto — o que foi verificado e como

Atualizado em 15/11/2026 (adaptação para o GitHub Codespaces). Nada aqui é declarado concluído sem evidência executada.

> **Situação das fontes e do Databricks em 15/11:** nenhuma fonte externa foi validada com dados reais e nada foi executado no
> Databricks ainda. Isso só acontece no seu Codespace e na sua conta Databricks (seção 4).

## 0. Rodada de 15/11 — execução no GitHub Codespaces (sem Docker no computador)

### Executado e confirmado (ensaio fiel ao Codespaces)

Ambiente do ensaio: Ubuntu 24.04, usuário comum `vscode` (como no Codespaces), pasta `/workspaces`, repositório remoto
local com só o README (como um repositório recém-criado no GitHub) e as mesmas variáveis que o Codespaces define
(`CODESPACE_NAME`, `GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN`). Foram executados, na ordem do guia `docs/codespaces.md`:

| Passo | Resultado |
|---|---|
| Importar o ZIP (`importar_zip.sh`) | Projeto copiado para a raiz, ZIP removido, verificação de segredos OK, **94 arquivos publicados** (inclui `.devcontainer/` e fixtures de teste) |
| Instalação (`postCreateCommand` → `instalar.sh`) | PostgreSQL 16 (o Codespaces terá o 15 da imagem Debian bookworm), Python 3.12, **Airflow 3.0.6 com constraints oficiais**, chaves Fernet/JWT em `/workspaces/.guiding-hand-estado/chaves.env` (permissão 600, fora do repositório), banco migrado |
| Abertura (`postStartCommand` → `iniciar.sh`) | PostgreSQL, api-server, scheduler e dag-processor no ar; a 2ª chamada reconhece os serviços ativos e não duplica |
| **Agendamento** | Ao subir, o scheduler criou sozinho `scheduled__2026-11-15T10:00:00+00:00` (07:00 de São Paulo) e a execução terminou **success**: 5 coletas iniciadas dentro de 250 ms (paralelas), `conferir` e `notificar_resultado` OK, 5 arquivos `<fonte>_2026-11-15.json` em `chegada/` |
| Menu 3 (coleta manual) | **success**; evidência `docs/evidencias/airflow_manual__20261115_154128.json` gravada em JSON válido |
| Menus 0, 4, 6, 7, 8, G, 5, 9, 1 | 0 mostra o estado real (inclusive "sem internet" no ensaio); 4 gerou `lotes/lote_1.zip` com os 5 arquivos do dia; 6 sem segredos; 7 serve a tela (porta 8000); 8 gera o PDF com o ambiente do Airflow e o link do repositório; G publica (sem dados coletados, lotes, logs, chaves nem PDF); 5: 32 testes passaram no venv do Codespaces; 9 para tudo (**0 processos restantes**); 1 registrou corretamente a falha de acesso das fontes |
| Desligar e religar | Histórico preservado (2 execuções), sem duplicar a execução agendada do dia |
| Interface pelo endereço público | Login (`/auth/token/login`) redireciona para `https://<codespace>-8080.app.github.dev`, não para `localhost` (`AIRFLOW__API__BASE_URL`) |

Nesse ensaio as URLs das fontes apontaram para o **simulador local** (`tests/integracao/simulador_fontes.py`), porque
o proxy deste ambiente bloqueia as APIs. Isso valida o **Airflow e o DAG**, não as fontes.

### Executado com erro — corrigido e revalidado

| Problema encontrado no ensaio | Correção |
|---|---|
| `python3` era 3.13; o Airflow 3.0.6 não tem constraints para 3.13 (instalação parou com erro claro) | `instalar.sh` escolhe explicitamente Python 3.9–3.12 e recria um venv incompatível |
| A execução agendada era despachada antes de o api-server responder: tarefas falhavam com *Connection refused* e só passavam no retry (1ª tentativa vermelha no Grid). A causa também explica a falha vista em 12/11, antes atribuída a IPv6 | api-server sobe primeiro e o scheduler só depois do *health check*; no `docker-compose.yml`, o scheduler passou a depender do api-server **saudável**. Revalidado: a execução agendada passou na 1ª tentativa |
| O api-server se renomeia para `airflow api_server` e abre processos `gunicorn`: `parar.sh` não o encontrava e `iniciar.sh` tentava subir um 2º | Controle por arquivo de PID e grupo de processos |
| Menu 7 ficava preso esperando o servidor da tela | Servidor em grupo próprio, com PID e `disown` |
| O PDF de prévia ("NÃO ENTREGAR") ia para o GitHub | `docs/relatorio/*.pdf` ignorado (o relatório é entregue pelo Teams) |
| Opção inválida era ignorada em silêncio | Aviso "Opção inválida" |

### Não executado por falta de acesso

| Item | Motivo |
|---|---|
| As 5 fontes reais | O proxy de saída deste ambiente devolve **HTTP 403** (bloqueio por política) para api.open-meteo.com, overpass-api.de, wfs.geosampa.prefeitura.sp.gov.br, dados.prefeitura.sp.gov.br e api.olhovivo.sptrans.com.br. Não há tentativa de contornar o bloqueio |
| Codespaces real na sua conta (imagem, encaminhamento de porta, segredos, *Rebuild Container*) | Exige a sua conta GitHub |
| Databricks (Auto Loader, Delta MERGE, Volumes) | Exige a sua conta |

Arquivos **sem alteração** desde a verificação de 12/11 (evidências daquela rodada continuam válidas): DAG e biblioteca de
coleta, notebooks e SQL do Databricks, tela do MVP, `GuidingHand.bat` e scripts PowerShell.


## 1. Executado e confirmado

| Verificação | Ambiente | Resultado |
|---|---|---|
| Suíte pytest (coleta, gerador, conferência, Spark Bronze/Prata/Ouro, lotes, segredos, SQL, relatório, consistência do menu) | Python 3.12, PySpark 4.0.1, Java 21 | 51 passaram |
| Estrutura do DAG (agendamento, paralelismo, retries, callback) | Airflow 3.0.6 | 3 passaram |
| DAG em stack equivalente ao Docker Compose (PostgreSQL + LocalExecutor + api-server + scheduler + dag-processor) | Airflow 3.0.6 | 3 execuções `success`: a agendada das 07:00 (disparada sozinha, com falha recuperada por retry) e 2 manuais; na última, as 5 coletas começaram dentro de 105 ms (paralelismo real) |
| Bronze incremental com streaming real do Spark (checkpoint + availableNow) | PySpark 4.0.1 | lote 1 = 5 · reexecução = 5 · lote 2 = 10 · reexecução = 10; zero duplicados |
| Sintaxe de todo SQL dos notebooks (MERGE, CREATE TABLE AS, COMMENT, ALTER, arquivos .sql) | parser do Spark | válida (exceto sintaxe exclusiva do Databricks, listada no item 3) |
| Notebooks sem APIs ausentes no serverless (sparkContext, RDD, cache) | análise estática | nenhum uso |
| Scripts do Windows | PowerShell 7.4.6 | 12 sem erro de sintaxe; funções comuns executadas; leitura do estado e da evidência do Airflow testada com saída real da CLI (inclusive poluída por avisos); importação da pasta Downloads testada |
| Menu `GuidingHand.bat` executado em cmd.exe real | Wine 9.0 (`tests/integracao/testar_menu_cmd.sh`) | 16 entradas testadas: as 12 entradas válidas (0 a 9, G e g) chamam o script correto; S e s encerram; X e ENTER vazio voltam ao menu |
| As 12 opções executadas de verdade (não só despachadas) | PowerShell 7.4.6, ambiente `.venv` como no Windows | 0, 1, 4, 5, 6, 7, 8 completas; 2, 3, 9 param com orientação clara sem Docker; G executa tudo até o `git push` (que exige o seu login) |
| docker-compose.yml | Docker Compose v2.30.3 (`config`) | válido |
| Tela do MVP | Node + jsdom | renderiza 36 barreiras, alerta primeiro, filtro e leitor de tela OK |
| Relatório PDF | reportlab | 9 seções na ordem da FIAP; prévia de teste marcada "NÃO ENTREGAR"; pendências em vermelho |
| Segredos no repositório | scanner próprio | nenhum |

## 2. Executado com erro — corrigido e revalidado

| Problema encontrado | Correção |
|---|---|
| Token do Olho Vivo aparecia na mensagem de erro (URL de login) e iria para o log de notificações e o webhook | Mascaramento de credenciais em toda mensagem; teste automatizado |
| Windows PowerShell 5.1 + `ErrorActionPreference=Stop` derrubava scripts com qualquer saída em stderr (ex.: "Java não encontrado" com Java instalado) | `Continue` + checagem explícita de `$LASTEXITCODE`; detecção por `Get-Command` |
| CLI do Airflow imprime avisos no stdout: o acompanhamento da coleta falhava e a evidência saía corrompida | Logs da CLI silenciados + extração só do bloco JSON |
| SQL da Ouro usava UDF Python temporária em CREATE TABLE AS SELECT (risco no Unity Catalog) | Centros de células calculados em DataFrame; SQL sem UDF |
| Título da C5 cortado e chave de 64 caracteres na C3 | Corrigidos |
| (rodadas anteriores) biblioteca fora do sys.path no Airflow 3, data de coleta em UTC, UDF falhando nos workers, clima desalinhado, deduplicação inflando o mapa, critério do alerta "alto" | Corrigidos e cobertos por testes |

### Correção do menu (2ª revisão de 12/11) — encontrada pela usuária

| Problema | Causa | Correção e evidência |
|---|---|---|
| `GuidingHand.bat` sem as opções 7, 8 e G | A edição anterior procurou quebras `\r\n`, mas o Python converte para `\n` ao ler; a substituição falhou em silêncio e não havia teste do menu | Arquivo reescrito por inteiro (41 linhas CRLF, ASCII); teste automatizado `tests/test_menu_windows.py` compara menu × scripts × README × passo a passo; execução real em cmd.exe |

Ao executar cada opção de verdade, foram encontrados e corrigidos também:

| Opção | Problema | Correção |
|---|---|---|
| todas | Prompt `set /p op="..."` e ENTER vazio repetindo a escolha anterior | Forma `set /p "op=..."` e `set "op="` antes da pergunta |
| 0 | Sem Docker, mostrava "não encontrado" e "instalado mas FECHADO" ao mesmo tempo; `Test-NetConnection` pode demorar ~20 s | Mensagem única; teste de internet por TCP com limite de 5 s |
| 1, 2 | Leitura do token encerrava o PowerShell quando a entrada não era um teclado | Leitura de linha comum só nesse caso; no Windows interativo continua oculta |
| 5 | `--ignore` do teste do DAG não era aplicado (erro introduzido na revisão anterior) e a pasta `airflow/` do projeto era confundida com o pacote Airflow | Argumento calculado corretamente; teste do DAG pula só quando `airflow.sdk` não existe |
| 7 | Dizia "export real do Databricks" para qualquer arquivo sem a marca de amostra | Exige a assinatura `guiding-hand/tb_mvp_mapa_barreiras/v1`, gravada só pelo notebook 05 |
| 9 | Sem Docker, erro bruto e depois "Containers parados" | Aviso claro; sucesso só se `docker compose down` funcionar |
| G | `.gitignore` com `chegada/` excluía também `tests/fixtures/chegada/` (testes não reproduzíveis no GitHub) e `docs/lotes/` | Regras ancoradas na raiz (`/chegada/`, `/lotes/`...); fixtures e registros de lotes agora são versionados |
| todas | Caminhos `scripts\x.py` passados ao Python | Montados com `Join-Path` (funciona igual no Windows) |

## 3. Não executado por falta de acesso

| Item | Motivo | Mitigação |
|---|---|---|
| Fontes externas com dados reais | O ambiente de desenvolvimento não resolve DNS dessas APIs; o Open-Meteo bloqueia acesso automatizado pela ferramenta web | `scripts/validar_fontes.py` (menu 1) testa tudo do seu computador e gera relatório |
| Containers Docker | Sem Docker e sem acesso ao Docker Hub | Mesma configuração validada em stack nativo equivalente |
| Auto Loader (`cloudFiles`), Delta MERGE, Volumes, `DESCRIBE HISTORY`, `CREATE VOLUME` | Exclusivos do Databricks | Lógica testada com equivalentes; o notebook `00_processar_lote` verifica idempotência sozinho e avisa |
| Partes exclusivas do Windows nos scripts (Docker Desktop, Bloco de Notas, Explorer, abrir navegador, login do GitHub) | Sem Windows, sem Docker, sem sua conta | Todo o resto de cada opção foi executado no PowerShell 7 e o menu em cmd.exe real |
| Token Olho Vivo real | Credencial pessoal | Fonte opcional: o DAG pula sem quebrar |

## 4. Exige ação sua

Caminho principal (GitHub Codespaces, guia `docs/codespaces.md`):

1. Criar o repositório público com README e abrir o Codespace; arrastar o ZIP e colar **um comando**; fazer o *Rebuild Container* uma vez.
2. *(Opcional)* cadastrar o segredo `OLHOVIVO_TOKEN` no GitHub.
3. Menu **1** — primeira validação **real** das fontes; enviar o relatório no chat.
4. Abrir o Codespace em **dois dias diferentes** (a coleta agendada roda sozinha) e usar o menu **3** e o **4**; baixar o ZIP do lote, extrair no Windows e enviar ao Volume pela interface do Databricks (regra da FIAP).
5. No Databricks: conta gratuita, Git folder, `00_setup.sql`, **Run all** no `00_processar_lote` (lote 1 e lote 2).
6. Tirar os 10 prints, informar nome e RM, menu **8** e **G**, entregar o PDF no Teams.

Não cadastre forma de pagamento: a cota gratuita do Codespaces (120 core-hours e 15 GB por mês) cobre a entrega com folga.

## Saída real da última rodada (15/11)

```
## pytest (Python 3.13.18 + PySpark 4.0.1 + Java 21)
58 passed in 62.63s (0:01:02)
## pytest DAG (Airflow 3.0.6 + Python 3.12.3, venv criado por scripts/codespaces/instalar.sh)
3 passed in 3.33s
## shellcheck (scripts/codespaces)
sem avisos
## docker compose v2.30.3
config OK
## segredos
Nenhum segredo encontrado.
## pyflakes
sem avisos
```

## Saída real da rodada de 12/11

```
## pytest (Python 3.12 + PySpark 4.0.1 + Java 21)
51 passed in 105.69s (0:01:45)
## pytest DAG (Airflow 3.0.6)
3 passed in 3.86s
## menu executado em cmd.exe real (Wine 9.0)
opcao '0'  -> scripts\windows\00_verificar_ambiente.ps1 (encerrou)
opcao '1'  -> scripts\windows\01_validar_fontes.ps1    (encerrou)
opcao '2'  -> scripts\windows\02_iniciar_airflow.ps1   (encerrou)
opcao '3'  -> scripts\windows\03_executar_coleta.ps1   (encerrou)
opcao '4'  -> scripts\windows\04_preparar_lote.ps1     (encerrou)
opcao '5'  -> scripts\windows\05_rodar_testes.ps1      (encerrou)
opcao '6'  -> scripts\windows\06_verificar_segredos.ps1 (encerrou)
opcao '7'  -> scripts\windows\07_abrir_tela_mvp.ps1    (encerrou)
opcao '8'  -> scripts\windows\08_gerar_relatorio.ps1   (encerrou)
opcao 'G'  -> scripts\windows\09_publicar_github.ps1   (encerrou)
opcao 'g'  -> scripts\windows\09_publicar_github.ps1   (encerrou)
opcao '9'  -> scripts\windows\99_parar_airflow.ps1     (encerrou)
opcao 'X'  -> (nenhuma)                                (voltou ao menu)
opcao 'S'  -> (nenhuma)                                (encerrou)
opcao 's'  -> (nenhuma)                                (encerrou)
opcao ''   -> (nenhuma)                                (voltou ao menu)
## tela do MVP (Node + jsdom)
TELA OK
## segredos
Nenhum segredo encontrado.
## PowerShell 7.4.6 (parser)
12 scripts, erros de sintaxe: 0
## docker compose v2.30.3
config OK
## pyflakes
sem avisos
```
