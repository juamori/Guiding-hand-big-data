# Guiding Hand no GitHub Codespaces (sem Docker no seu computador)

O Codespaces é um computador Linux do GitHub que abre **no navegador**. Nada é instalado no seu Windows.
O Airflow 3 roda lá com a mesma arquitetura do Docker Compose (PostgreSQL + LocalExecutor + api-server,
scheduler e dag-processor separados), e as coletas acessam as APIs reais pela internet do GitHub.

## Cota gratuita e cuidados (leia antes)

- A conta gratuita do GitHub inclui **120 core-hours e 15 GB por mês** de Codespaces. Este projeto usa uma
  máquina de 2 núcleos: **cada hora aberta consome 2 core-hours** (60 horas no mês). Estimativa da entrega: 10 a 15 core-hours.
- **Não cadastre forma de pagamento.** Sem pagamento cadastrado, ao acabar a cota o uso é bloqueado, nunca cobrado.
- O Codespace **desliga sozinho após 30 minutos sem uso** (dá para mudar em github.com > Settings > Codespaces >
  Default idle timeout, até 240 min). Ao reabrir, o Airflow sobe sozinho e o histórico continua lá.
- O agendamento diário (07:00) só dispara com o Codespace ligado. Como o DAG usa `catchup=False`, ao abrir o
  Codespace num dia o scheduler executa sozinho a coleta agendada daquele dia. Isso está declarado no relatório.
- Depois da correção da FIAP, apague o Codespace (github.com/codespaces > ... > Delete) para não gastar armazenamento.
  O repositório continua público.

## Primeira vez (≈ 20 minutos, a maior parte esperando)

1. Em **github.com/new**: nome `guiding-hand-big-data`, **Public**, marque **Add a README file** → *Create repository*.
2. No repositório: botão verde **Code** → aba **Codespaces** → **Create codespace on main**. Abre o VS Code no navegador.
3. Arraste o arquivo `guiding-hand-big-data.zip` do seu computador para o painel de arquivos à esquerda (na raiz).
4. No **Terminal** (parte de baixo; se não aparecer: menu ☰ > Terminal > New Terminal), cole e aperte ENTER:
   ```
   unzip -q "$(ls -t guiding-hand-big-data*.zip | head -1)" && bash guiding-hand-big-data/scripts/codespaces/importar_zip.sh
   ```
   Deve aparecer **[OK] Projeto publicado**. (Funciona mesmo se o navegador salvou o arquivo como `guiding-hand-big-data (1).zip`.)
5. **Ctrl+Shift+P** → digite `Rebuild Container` → **Codespaces: Rebuild Container** → **Rebuild**.
   A janela recarrega e instala PostgreSQL e Airflow sozinha (5 a 10 minutos). Este guia abre de novo no final.
6. *(Opcional, fonte Olho Vivo)* github.com > Settings > Codespaces > **New secret**: nome `OLHOVIVO_TOKEN`,
   valor = seu token da SPTrans, repositório `guiding-hand-big-data`. Depois feche e reabra o Codespace.

## Menu

No terminal: `bash scripts/codespaces/menu.sh` — mesmos números do `GuidingHand.bat` do Windows.

| Opção | O que faz | O que você deve ver |
|---|---|---|
| **0** | Verifica o ambiente | Itens em verde |
| **1** | Valida as fontes **de verdade** no recorte de Pinheiros | Relatório em `docs/validacao_fontes/` — **envie no chat** |
| **2** | Inicia o Airflow (já sobe sozinho ao abrir) | Link da interface; também na aba **PORTS** > porta 8080 > ícone do globo |
| **3** | Executa a coleta agora e salva a evidência | "Coleta concluída com sucesso" e os arquivos de `chegada/` |
| **4** | Prepara o lote para o Databricks | `lotes/lote_N.zip` para baixar |
| **5** | Testes automatizados | Testes passando |
| **6** | Verifica segredos | "Nada sensível no repositório" |
| **7** | Abre a tela do MVP | Link na aba PORTS > porta 8000 |
| **8** | Gera o relatório PDF | `docs/relatorio/Relatorio_Big_Data_Guiding_Hand.pdf` |
| **G** | Publica no GitHub | "Publicado" |
| **9** | Para o Airflow | — |

## Roteiro da entrega

**Dia 1** (hoje ou amanhã)
1. Abra o Codespace (github.com/codespaces). Espere ~1 minuto: o Airflow sobe e executa a coleta agendada do dia.
2. Menu **1** (só na primeira vez) → envie o relatório de validação no chat.
3. Menu **3** → coleta manual com evidência. Abra a interface (porta 8080) e tire os prints:
   `01_airflow_graph.png` (aba Graph), `03_airflow_variables.png` (Admin › Variables), `04_pasta_chegada.png` (painel de arquivos com `chegada/` aberta).
4. Menu **4**, lote **1** → botão direito em `lotes/lote_1.zip` → **Download** → no Windows, botão direito → **Extrair Tudo**.
5. Databricks (Etapa C de `docs/passo_a_passo.md`): Git folder do repositório, `00_setup.sql`, upload dos `.json` no Volume `chegada`
   (print `05_upload_lote1.png`), notebook `00_processar_lote` com **lote = 1** → **Run all** → **TUDO CERTO ✔**.

**Dia 2** (outro dia)
1. Abra o Codespace → coleta agendada do dia roda sozinha → menu **3**. Print `02_airflow_grid.png` (aba Grid com os dois dias).
2. Menu **4**, lote **2** → download → extrair → upload (print `06_upload_lote2.png`) → `00_processar_lote` com **lote = 2**.
   Prints `07`, `08` e `09` das três últimas células.
3. Do Volume `exportacao`, baixe `evidencias_lote_2.json` e `barreiras_mvp.json` e **arraste para o Codespace**:
   `docs/evidencias/` e `mvp_demo/data/`, respectivamente.
4. Menu **7** → print `10_tela_mvp.png`. Arraste os prints para `docs/relatorio/prints/`.
5. Preencha `docs/relatorio/integrantes.csv` → menu **8** → baixe o PDF → menu **G** → envie o PDF no Teams.
