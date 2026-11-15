# Arquitetura de dados — Guiding Hand (Big Data)

## Fluxo da fonte ao MVP

```mermaid
flowchart LR
  subgraph Fontes
    A1[Open-Meteo<br/>API JSON]
    A2[OpenStreetMap<br/>Overpass API]
    A3[SP156 / GeoSampa<br/>WFS GeoJSON]
    A4[SPTrans Olho Vivo<br/>API + token]
    A5[Eventos dos óculos<br/>gerador SINTÉTICO]
  end
  subgraph Airflow 3 — diário 07:00
    B0((inicio)) --> B1[coletar_clima] & B2[coletar_osm] & B3[coletar_sp156] & B4[coletar_olhovivo] & B5[gerar_eventos_oculos]
    B1 & B2 & B3 & B4 & B5 --> B6[conferir]
    B6 --> B7[notificar_resultado]
  end
  A1 --> B1
  A2 --> B2
  A3 --> B3
  A4 --> B4
  A5 --> B5
  B6 --> C[(pasta chegada/<br/>fonte_AAAA-MM-DD.json)]
  C -- upload manual<br/>lote 1 e lote 2 --> D[(Volume<br/>workspace.guiding_hand.chegada)]
  D -- Auto Loader --> E[Bronze<br/>bronze_arquivos]
  E -- schema, regras, MERGE --> F[Prata<br/>5 tabelas + quarentena]
  F -- modelo estrela --> G[Ouro<br/>fato + 5 dimensões]
  G --> H[tb_mvp_mapa_barreiras]
  H -- export JSON --> I[Tela do MVP<br/>mapa de barreiras]
```

O Databricks Free Edition não se conecta ao Airflow. A passagem de `chegada/` para o Volume é **manual pela interface**, em lotes (Requisito 04). `scripts/preparar_lote.py` separa só os arquivos ainda não enviados e registra cada lote em `docs/lotes/`.

## Decisões técnicas

| Decisão | Motivo |
|---|---|
| Recorte de ~800 m em torno da Estação Pinheiros (bbox em Variable) | Bairro piloto definido pela equipe; expansível sem mudar código |
| Coleta de D traz o dia D-1 completo | O arquivo histórico do Open-Meteo tem ~5 dias de atraso; usamos a Forecast API para o dia anterior e alinhamos os eventos ao mesmo dia |
| Bronze com uma linha por arquivo (`text` + `wholetext`) | Preserva o bruto byte a byte, schema estável para 5 fontes diferentes, upload "achatado" em uma pasta |
| Schema explícito na Prata (`from_json`) | Tipos corretos garantidos; campos desconhecidos não quebram o pipeline |
| MERGE com `hash_registro` | Só atualiza o que mudou: reprocessar sem novidade gera 0 inserções e 0 atualizações |
| Identidade da barreira resolvida na Ouro (células ≤ 20 m unificadas) | O ruído de GPS espalha o mesmo obstáculo por células vizinhas; sem isso o mapa inflava ~60% nos testes |
| Ouro reconstruída a cada execução | Determinística, idempotente, simples de auditar |
| Integração com o MVP por arquivo JSON exportado | O Free Edition não é backend público; contrato de arquivo é explícito e versionado |
| Eventos dos óculos sintéticos | O protótipo físico e o modelo de visão ainda não geram dados de campo; origem marcada em todos os registros |
| MongoDB não utilizado | Não há JSON aninhado que exija upsert/aggregation antes do lakehouse; o Delta resolve. Não vale ponto extra |
| SQL da Ouro sem UDF | Centros de células calculados em DataFrame na Prata; evita restrições do Unity Catalog a funções temporárias em CREATE TABLE AS SELECT |
| Credenciais mascaradas em toda mensagem de erro | A URL de login do Olho Vivo contém o token; erros e notificações passam por `coletores.mascarar` |
| URLs das fontes sobrescrevíveis por Variable (`gh_open_meteo_url`, `gh_geosampa_wfs_url`, `gh_olhovivo_url`, `gh_overpass_urls`) | Troca de endpoint sem mudar código e testes de integração com APIs simuladas |
| Execução do Airflow no GitHub Codespaces (Linux no navegador), Docker Compose mantido como alternativa | O Airflow não roda nativamente no Windows (só via WSL2 ou containers). O Codespaces executa a MESMA arquitetura do Compose (PostgreSQL + LocalExecutor + api-server, scheduler e dag-processor), com coletas reais, sem instalar nada no computador. Estado e histórico do Airflow ficam em `/workspaces/.guiding-hand-estado` (sobrevive a desligar e reconstruir). Limitação: o agendamento só dispara com o Codespace ligado; com `catchup=False`, a coleta agendada do dia roda ao abrir |
