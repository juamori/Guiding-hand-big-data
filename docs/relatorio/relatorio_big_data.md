# Guiding Hand — Relatório de Big Data Architecture & Data Integration

> MODELO. Estrutura na ordem exigida pela FIAP. Campos `⟦...⟧` serão preenchidos com evidências reais
> (prints e números do Airflow e do Databricks). Nenhum resultado aqui deve ser inventado.

## 1. Integrantes

| Nome completo | RM |
|---|---|
| ⟦nome⟧ | ⟦RM⟧ |

## 2. O produto em uma frase e as perguntas que o MVP responde

**Óculos de baixo custo que avisam pessoas cegas ou com baixa visão sobre obstáculos acima da cintura e transformam cada detecção num mapa de barreiras para a zeladoria de Pinheiros.**

1. Quais tipos de obstáculo são mais recorrentes, e quantos a bengala não detecta? (C1)
2. Quais quadras concentram mais barreiras? (C2)
3. Quais barreiras aparecem em mais de uma coleta? (C3)
4. Como a chuva se relaciona com as detecções? (C4)
5. **Impacto:** quantas barreiras foram mapeadas, quantas são invisíveis à bengala, quantas estão prontas para confirmação e quantas a zeladoria resolveu? (C5)
6. Como evoluem barreiras identificadas e resolvidas? (C6)

## 3. Diagrama do fluxo de dados

⟦Diagrama de `docs/arquitetura.md` exportado como imagem⟧

## 4. Tabela de fontes

⟦Tabela de `docs/fontes.md` com a coluna de validação preenchida pelo relatório real⟧

## 5. Requisitos 01 a 09

| Req. | O que foi feito | Evidência |
|---|---|---|
| 01 Fontes | 4 fontes externas reais + eventos sintéticos documentados | ⟦print do relatório de validação⟧ |
| 02 DAG | `guiding_hand_coleta`, diário 07:00, um arquivo por fonte e coleta com data no nome | ⟦print da pasta chegada/⟧ |
| 03 Paralelismo e segurança | 5 coletas em paralelo, tarefa `conferir`, chaves em Variables, retries e callback de falha | ⟦print Graph + Admin › Variables⟧ |
| 04 Dois lotes | Lote 1 (⟦data⟧) e lote 2 (⟦data⟧) enviados pela interface | ⟦prints dos uploads⟧ + `docs/lotes/` |
| 05 Bronze | Auto Loader `availableNow` + checkpoint; reexecução não duplica | ⟦print `controle_execucoes_bronze`⟧ |
| 06 Prata | Schema explícito, nulos, normalização, dedup, MERGE por chave, 7 regras | ⟦print quarentena por regra⟧ |
| 07 Ouro e SQL | Fato + 5 dimensões, 6 consultas (C5 de impacto) | seção 8 |
| 08 LGPD e MVP | device_hash, localização generalizada, sem imagens; MVP lê `tb_mvp_mapa_barreiras` | ⟦print da tela do MVP⟧ |
| 09 Repositório | README, DAGs, notebooks, SQL, amostras, sem segredos | ⟦link⟧ |

## 6. Prints do Airflow

⟦Graph view — execução concluída⟧
⟦Grid view — execuções de dois dias⟧

## 7. Contagens da Bronze

| Momento | Arquivos antes | Arquivos depois | Novos |
|---|---|---|---|
| Lote 1 — 1ª execução | ⟦⟧ | ⟦⟧ | ⟦⟧ |
| Lote 1 — reexecução | ⟦⟧ | ⟦⟧ | 0 |
| Lote 2 — 1ª execução | ⟦⟧ | ⟦⟧ | ⟦⟧ |
| Lote 2 — reexecução | ⟦⟧ | ⟦⟧ | 0 |

## 8. Consultas SQL

Para cada consulta C1–C6: pergunta, SQL (`databricks/sql/consultas_mvp.sql`), ⟦print do resultado⟧ e interpretação.

## 9. Conclusão técnica e link do repositório

⟦conclusão após os resultados reais⟧ · Repositório: ⟦link público⟧
