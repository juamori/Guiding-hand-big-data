# Evidências reais

- `airflow_<execução>.json`: estados e horários das tarefas, gravado automaticamente pelo menu 3.
- `evidencias_lote_<N>.json`: exportado pelo notebook `databricks/00_processar_lote` (Volume `exportacao`).
  Baixe para a pasta Downloads: os menus 7 e 8 copiam para cá sozinhos.

O gerador de relatório (menu 8) lê esta pasta. Nada aqui é escrito à mão.

## Conteúdo desta pasta (19/11/2026)

| Arquivo ou pasta | O que é |
|---|---|
| `coleta_2026-11-15.md/.json` | Verificação da coleta real de 15/11 (APROVADA) |
| `perfil_sp156_2026-11-15.md/.json` | Perfil dos dados reais do SP156 e escolha do protocolo |
| `airflow_manual__20261115_191410.json` | Estados das tarefas de uma execução manual do Airflow |
| `evidencias_lote_2.json` | Contagens e consultas exportadas do Databricks depois dos dois lotes (`06_exportar_evidencias`) |
| `databricks_lote_1.md`, `airflow_lote2_mvp.md`, `lotes_1_e_2_consultas.md` | Transcrição dos números observados nos prints |
| `prints_databricks_lote1/` | Prints do lote 1 (Bronze, Prata, consultas). Caminho pessoal do workspace ocultado |
| `prints_airflow_lote2_mvp/` | Testes no Codespace, preparo do lote 2, amostra da tabela do MVP, tela de 18/11 (estado do lote 1) |
| `prints_lote2_final/` | Consultas com os dois lotes e exportação das evidências (19/11) |
| `coleta_2026-11-13.md/.json` | **Não é evidência real.** Arquivos criados por um teste automatizado antigo, com dados de simulador. A exclusão aguarda autorização |

Os prints usados no relatório PDF ficam em `docs/relatorio/prints/`.
