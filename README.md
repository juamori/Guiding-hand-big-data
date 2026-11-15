# Guiding Hand — Big Data Architecture & Data Integration

**FIAP Global Solution 2026 · CitizenTech · Engenharia de Software 4º ano**

O Guiding Hand é um par de óculos inteligentes que avisa pessoas cegas ou com baixa visão sobre obstáculos que a bengala não alcança. Cada obstáculo detectado alimenta um **mapa de barreiras** que orienta a zeladoria das calçadas.

Este repositório é o componente **Dados**: coleta orquestrada no **Airflow 3**, lakehouse **Bronze/Prata/Ouro** no **Databricks** (Auto Loader, Delta, MERGE) e a tabela que o MVP exibe. Piloto: entorno da **Estação Pinheiros, São Paulo**.

## Integrantes

| Nome completo | RM |
|---|---|
| _(preencher)_ | _(preencher)_ |

## Ficha técnica

| Item | Conteúdo |
|---|---|
| Cidadão e território | Pessoas cegas ou com baixa visão que andam a pé em Pinheiros (SP) |
| Problema | A bengala não detecta obstáculos acima da cintura e calçadas têm barreiras recorrentes sem registro |
| Perguntas que o MVP responde | Quais obstáculos são mais comuns? Onde se concentram? Quais persistem entre coletas? A chuva muda o quadro? Quantas barreiras a bengala não detecta, quantas estão prontas para confirmação e quantas a zeladoria resolveu? Como isso evolui? |
| Fontes | Open-Meteo, OpenStreetMap (Overpass), SP156 via GeoSampa, SPTrans Olho Vivo (opcional), eventos dos óculos **sintéticos** |
| Tabela consumida pelo MVP | `workspace.guiding_hand.tb_mvp_mapa_barreiras` |
| ODS | 10 (redução das desigualdades), 11 (cidades sustentáveis), 9 (infraestrutura) |

## Arquitetura

```
Fontes ─► Airflow 3 (07:00, 5 coletas em paralelo ► conferir ► notificar) ─► chegada/
      ─► upload manual em 2 lotes ─► Volume ─► Bronze (Auto Loader) ─► Prata (regras + MERGE)
      ─► Ouro (fato + 5 dimensões) ─► tb_mvp_mapa_barreiras ─► tela do MVP
```

Diagrama completo e decisões: [`docs/arquitetura.md`](docs/arquitetura.md).

## Estrutura

```
dags/                      DAG do Airflow + biblioteca de coleta (guiding_hand_lib/)
databricks/00_setup.sql    schema e Volumes
databricks/bronze|prata|ouro|mvp/   notebooks (formato fonte do Databricks)
databricks/lib/            transformações PySpark e SQL da Ouro (usadas pelos notebooks e testes)
databricks/sql/            as 6 consultas de negócio e as contagens da Bronze
mvp_demo/                  tela de referência do MVP (lê o JSON exportado da Ouro)
amostras/                  pequenas amostras de cada fonte
scripts/                   validação de fontes, lotes, segredos, pipeline local
scripts/windows/ + GuidingHand.bat   automação no Windows com Docker (menu)
scripts/codespaces/ + .devcontainer/  automação no GitHub Codespaces, sem Docker (mesmo menu)
tests/                     testes automatizados (pytest) e teste da tela (Node, opcional)
docs/                      arquitetura, fontes, contrato de dados, qualidade, LGPD, modelo, lotes
```

## Como reproduzir

**GitHub Codespaces (recomendado, sem instalar nada):** abra um Codespace neste repositório; a configuração
em `.devcontainer/` instala PostgreSQL 15 e Airflow 3.0.6 e sobe o Airflow sozinha a cada abertura.
Menu: `bash scripts/codespaces/menu.sh`. Guia completo: [`docs/codespaces.md`](docs/codespaces.md).

**Windows com Docker Desktop (alternativa):** dois cliques em `GuidingHand.bat` e siga [`docs/passo_a_passo.md`](docs/passo_a_passo.md).
As opções abaixo valem para os dois menus.

| Opção | O que faz | Script |
|---|---|---|
| **0** | Verifica se o computador está pronto (Python, Docker, Git, Java, internet) | `scripts/windows/00_verificar_ambiente.ps1` |
| **1** | Valida as fontes de dados de verdade e gera relatório | `scripts/windows/01_validar_fontes.ps1` |
| **2** | Inicia o Airflow 3 no Docker | `scripts/windows/02_iniciar_airflow.ps1` |
| **3** | Executa a coleta agora e salva a evidência da execução | `scripts/windows/03_executar_coleta.ps1` |
| **4** | Prepara o lote para o Databricks | `scripts/windows/04_preparar_lote.ps1` |
| **5** | Roda os testes automatizados | `scripts/windows/05_rodar_testes.ps1` |
| **6** | Verifica segredos antes de publicar | `scripts/windows/06_verificar_segredos.ps1` |
| **7** | Abre a tela do MVP | `scripts/windows/07_abrir_tela_mvp.ps1` |
| **8** | Gera o relatório PDF na ordem da FIAP | `scripts/windows/08_gerar_relatorio.ps1` |
| **G** | Publica no GitHub | `scripts/windows/09_publicar_github.ps1` |
| **9** | Para o Airflow | `scripts/windows/99_parar_airflow.ps1` |
| **S** | Sai do menu | — |

**Linha de comando (qualquer sistema):**

```bash
python -m venv .venv && . .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
python scripts/validar_fontes.py                       # testa as fontes no recorte
cp .env.example .env                                   # preencha as duas chaves
python scripts/montar_variaveis.py                     # gera airflow/variables.json (fora do Git)
docker compose up -d                                   # Airflow em http://localhost:8080
docker compose exec airflow-scheduler airflow dags trigger guiding_hand_coleta
python scripts/preparar_lote.py --lote 1               # arquivos novos -> lotes/lote_1
```

No Databricks: Git folder com este repositório → `00_setup.sql` → upload de `lotes/lote_1` no Volume `chegada` → notebook `00_processar_lote` (lote 1). Repita com o lote 2 de outro dia. O orquestrador roda Bronze, reexecução, Prata, reexecução, Ouro, exporta o JSON do MVP e as evidências, e verifica sozinho a idempotência.

Relatório: `python scripts/gerar_relatorio.py` monta o PDF na ordem da FIAP a partir de `docs/evidencias/`, `docs/relatorio/prints/` e `docs/relatorio/integrantes.csv`; o que faltar aparece como PENDENTE.

## Testes

```bash
pytest -q tests --ignore=tests/test_dag.py   # coletores, gerador, conferência, Spark, lotes, segredos
pytest -q tests/test_dag.py                  # requer Airflow 3 instalado (ou rode dentro do container)
python scripts/pipeline_local.py             # Bronze->Ouro local com dados de teste
python tests/integracao/simulador_fontes.py  # APIs simuladas para teste de integração do DAG sem internet
```

A situação de cada verificação (o que foi testado, onde, e o que depende do Databricks ou do seu computador) está em [`docs/STATUS.md`](docs/STATUS.md).

## Segurança e LGPD

Sem chaves no código (Airflow Variables; `.env` e `airflow/variables.json` ignorados pelo Git), dispositivo pseudonimizado com SHA-256 + salt, localização generalizada, nenhuma imagem armazenada e tabela do MVP sem dado pessoal. Detalhes em [`docs/lgpd.md`](docs/lgpd.md).

## Dados sintéticos

Os eventos dos óculos são gerados por `dags/guiding_hand_lib/sintetico.py` porque o protótipo ainda não produz detecções de campo. Método em [`docs/fontes.md`](docs/fontes.md). Todo registro carrega `origem = sintetico_v1`.
