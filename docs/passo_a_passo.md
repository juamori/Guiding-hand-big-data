# Passo a passo (o que depende de você)

**Caminho principal: GitHub Codespaces** (nada instalado no Windows, sem Docker nem WSL). Siga o guia
[`docs/codespaces.md`](codespaces.md): primeira vez, roteiro do dia 1 e do dia 2, cota gratuita e cuidados.
O menu do Codespaces (`bash scripts/codespaces/menu.sh`) usa **os mesmos números** descritos abaixo.

As etapas do **Databricks** (C e D) são iguais nos dois caminhos.

---

**Alternativa: Windows com Docker Desktop.** Dois cliques em `GuidingHand.bat`. Cada etapa diz qual opção escolher no menu e o que você deve ver.
Prazo: **19/11 às 23h55**. Os dois lotes precisam ser de **dias de coleta diferentes**.

## Etapa A — Preparar e validar (hoje, ≈ 20 min)

| Menu | O que acontece | O que você deve ver |
|---|---|---|
| **0** | Confere Python, Docker, Git e internet | Itens em verde; o que estiver em vermelho vem com o link para instalar |
| **1** | Testa as 5 fontes de verdade em Pinheiros (token Olho Vivo é opcional: ENTER pula) | O Bloco de Notas abre `docs/validacao_fontes/relatorio_....md` — **envie esse arquivo no chat** |

## Etapa B — Coleta do dia 1

Abra o **Docker Desktop** e espere ficar verde.

| Menu | O que acontece | O que você deve ver |
|---|---|---|
| **2** | Sobe o Airflow (1ª vez: alguns minutos) | O navegador abre `http://localhost:8080` com o DAG `guiding_hand_coleta` |
| **3** | Executa a coleta e salva a evidência da execução | "Coleta concluída com sucesso" e a lista de arquivos em `chegada\` |

Prints (salve em `docs\relatorio\prints\` com os nomes de `docs/relatorio/prints/LEIA-ME.md`):
`01_airflow_graph.png` (aba Graph), `03_airflow_variables.png` (Admin › Variables), `04_pasta_chegada.png`.

## Etapa C — Databricks, lote 1

1. Crie a conta gratuita no **Databricks Free Edition**.
2. Menu **G** publica o repositório no GitHub (ele verifica segredos antes). No Databricks: *Workspace → Create → Git folder* → cole o link.
3. Abra `databricks/00_setup.sql` no SQL Editor → **Run** (cria schema e Volumes).
4. Menu **4**, lote **1** → a pasta `lotes\lote_1` abre sozinha.
5. Databricks: *Catalog → workspace → guiding_hand → Volumes → chegada → Upload to this volume* → selecione **todos** os arquivos da pasta. Print `05_upload_lote1.png`.
6. Abra `databricks/00_processar_lote`, escolha **lote = 1**, clique em **Run all**. Ao final deve aparecer **TUDO CERTO ✔**.

## Etapa D — Dia 2 (outro dia), lote 2

1. Docker aberto → menu **2** (se o Airflow estiver parado) → menu **3**. Print `02_airflow_grid.png` (aba Grid mostrando os dois dias).
2. Menu **4**, lote **2** → envie os arquivos ao mesmo Volume. Print `06_upload_lote2.png`.
3. `00_processar_lote` com **lote = 2** → **Run all** → **TUDO CERTO ✔**. Prints `07`, `08` e `09` das três últimas células desse notebook.
4. No Volume `exportacao`, baixe `evidencias_lote_2.json` e `barreiras_mvp.json` (ficam na pasta Downloads; não precisa mover).
5. Menu **7** abre a tela do MVP com os dados reais → print `10_tela_mvp.png`.

## Etapa E — Relatório e entrega

1. Menu **8**: pede os integrantes (nome completo e RM) e gera `docs\relatorio\Relatorio_Big_Data_Guiding_Hand.pdf`. Itens em vermelho = o que ainda falta.
2. Menu **G** de novo para publicar a versão final.
3. Envie o PDF no Teams da disciplina.

### Se algo der errado
Copie a mensagem (tarefa vermelha no Airflow → *Logs*; ou a célula com erro no Databricks) e envie no chat.
