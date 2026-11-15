# Fontes de dados

> Situação de validação: a coluna "Validação" é atualizada com o relatório de `scripts/validar_fontes.py` executado com internet.
> Enquanto não houver relatório real em `docs/validacao_fontes/`, nenhuma fonte externa deve ser considerada testada.

| Fonte | Origem | Formato | Frequência de coleta | Atualização na origem | Finalidade no Guiding Hand | Validação |
|---|---|---|---|---|---|---|
| Clima | Open-Meteo Forecast API (`api.open-meteo.com/v1/forecast`), sem chave | JSON (listas horárias paralelas) | Diária (dia anterior completo) | A cada hora | Contextualizar detecções: chuva → poças/piso liso, rajadas → galhos. **Contexto regional**: um ponto de grade representa todo o recorte (ver limitação abaixo) | conectividade OK em 15/11 (relatório real); coleta oficial pendente |
| Infraestrutura de calçada | OpenStreetMap via Overpass API, licença ODbL | JSON | Diária | Contínua (colaborativa) | Postes, árvores, lixeiras, rebaixamentos, piso tátil, faixas e pontos de ônibus no recorte | conectividade OK em 15/11; coleta oficial pendente |
| Zeladoria SP156 | Prefeitura de SP — 7 camadas SP156 georreferenciadas do GeoSampa (WFS) | GeoJSON | Diária | **Na coleta real de 15/11, as solicitações vão de abril a outubro de 2020**: série histórica, sem atualização recente no GeoSampa | Barreiras reais (buraco, calçada, poda, sinalização) com status oficial e data de conclusão → "resolvidas". **Só camadas de solicitações**; cadastros de infraestrutura são excluídos. Coleta **paginada e completa** | conectividade OK em 15/11; camadas a reconfirmar com o validador corrigido |
| Paradas de ônibus (opcional) | SPTrans Olho Vivo API v2.1, token gratuito | JSON | Diária | Cadastro estável | Destinos de rota e pontos de calçada congestionada | depende de token |
| Eventos dos óculos | Gerador `sintetico_v1` (este repositório) | JSON | Diária | — | Simular as detecções que Applied Computer Vision produzirá | testado (sintético) |
| *Portal CKAN — Dados do SP156* | dados.prefeitura.sp.gov.br | CSV | **Não coletado** | Recursos de 2012/2013 (metadados atualizados em 2022), segundo o relatório de 15/11 | **Somente contexto histórico** no relatório; não entra no Airflow nem nas camadas | conectividade OK em 15/11 |

## Como os eventos sintéticos são gerados (`dags/guiding_hand_lib/sintetico.py`)

1. **Mundo fixo** de 45 obstáculos no recorte, criado com semente mestra `20261119` — o mesmo em todos os dias, para que o mesmo obstáculo seja redetectado em coletas diferentes.
2. Pesos por tipo inspirados nas dificuldades citadas na visão geral do projeto (placas, galhos, carros na calçada, buracos...).
3. Obstáculos temporários (carro, caçamba, entulho, galho) tendem a desaparecer em 2–7 dias, simulando remoção/resolução.
4. Por dia, cada obstáculo ativo é detectado por 0 a 4 dispositivos (8 óculos simulados), com ruído de GPS gaussiano (~6 m) e confiança ~N(0,80; 0,11).
5. **Ruído intencional** para exercitar a Prata: 6–12 detecções de pessoas (falso alarme), ~3% de reenvios com o mesmo `event_id`, e 5 registros inválidos marcados com `_injetado_para_teste`.
6. Semente do dia = semente mestra + data: **reexecutar a mesma data gera exatamente o mesmo arquivo**.

Todo registro tem `origem = "sintetico_v1"`; o arquivo traz `aviso: DADOS SINTETICOS`; na Ouro a origem aparece como `oculos_sintetico`.


## Revisão de 15/11 — três pontos do relatório real de validação

**1. Portal CKAN (SP156 em CSV).** Os recursos encontrados são de 2012/2013 (metadados atualizados em 2022): não
representam a situação atual das calçadas. Decisão: o CKAN **não é fonte das coletas**. Fica citado apenas como contexto
histórico (existência de séries antigas do SP156). O SP156 atual vem do GeoSampa, que mostra as solicitações
georreferenciadas recentes. O validador passou a listar o ano de cada recurso e a marcar a fonte como `OK_HISTORICO`.

**2. GeoSampa: camadas com exatamente 500 feições.** Duas causas, ambas tratadas:
- O **validador** pedia no máximo 500 feições por camada (`maxFeatures=500`): o número 500 era um corte do próprio
  teste, não o total. O coletor do Airflow não tinha esse corte, mas também **não paginava**: se o servidor tiver teto
  próprio por resposta, a coleta oficial ficaria incompleta sem aviso.
- Agora o coletor e o validador usam a mesma busca **paginada** (`maxFeatures` + `startIndex`, aceitos pelo GeoServer
  também no WFS 1.1.0). Ela compara com o total informado pelo servidor (`resultType=hits`), avança pelo número de feições
  realmente devolvidas (funciona com teto do servidor) e registra em cada coleta: páginas, total do servidor, total
  coletado e `completo` (true, false ou não verificável). Testado com servidor simulado de teto 500: 1.234 de 1.234 feições.
- Achado adicional: a descoberta de camadas usava palavras genéricas (árvore, poste, calçada) e podia enviar ao
  Airflow **camadas de cadastro de infraestrutura** como se fossem solicitações SP156. Agora só entram camadas de
  solicitações (nome com "156"/"solicita"/"zeladoria", ou campos de protocolo e status). O coletor recusa e registra
  qualquer camada sem esses campos.

**3. Open-Meteo: célula da grade fora do recorte.** A API devolve o **centro da célula da grade do modelo** usada no
cálculo, não o ponto pedido; por padrão (`cell_selection=land`) a célula é escolhida por altitude semelhante. Para
o centro do recorte (-23,5675; -46,7020), a célula devolvida foi (-23,585238; -46,659637): **cerca de 4,75 km** a
sudeste, fora do bairro. Consequências e documentação:
- O clima é tratado como **contexto regional** do recorte inteiro (chuva e vento variam pouco em poucos km), não como
  medição por quadra. A consulta C4 compara horas com e sem chuva no recorte, o que é coerente com essa escala.
- Cada coleta registra `ponto_pedido`, `centro_celula_grade`, `distancia_grade_km` e `celula_dentro_do_recorte`; a
  Prata preserva `lat_grade`/`lon_grade`. A limitação vai para o relatório (seção 4).


## SP156: campos reais, protocolo e caráter histórico (15/11, após a 1ª coleta real)

**Campos reais das 7 camadas e mapeamento na Prata** (`databricks/lib/mapeamento_sp156.json`, módulo comum
`databricks/lib/sp156_mapeamento.py` usado pela Prata, pela verificação da coleta e pelo perfilador):

| Prata | Campo do GeoSampa | Observação |
|---|---|---|
| protocolo | `cd_identificador` **ou** `cd_identificador_original_atendimento_cidadao` | Escolhido pelos dados reais (preenchimento, unicidade, colisões entre camadas) por `scripts/perfil_sp156.py`; evidência em `docs/evidencias/perfil_sp156_<data>.md` |
| servico | `dc_servico` | `dc_assunto` e `dc_tema` ficam preservados na Bronze |
| status | `tx_situacao_solicitacao` | Classificado em resolvida / cancelada / aberta; os valores reais e a classificação de cada um aparecem no perfil |
| data_abertura | `dt_abertura` | Formato de data do GeoServer (`2020-04-15Z`) passou a ser reconhecido |
| data_conclusao | `dt_parecer` | Data do parecer da Prefeitura; só vira "data de resolução" quando o status é resolvida |

**Bronze:** guarda o arquivo bruto inteiro; todos os nomes e valores originais (inclusive os dois identificadores,
`dc_assunto` e `dc_tema`) permanecem disponíveis. A Prata só escolhe e renomeia.

**Consolidação das 7 camadas:** a Prata junta todas as camadas e aplica o MERGE pelo protocolo. Se a mesma solicitação
aparece em mais de uma camada, vira uma linha só. O perfil mede quantas repetições existem; se nenhum identificador
for único entre camadas, a chave passa a ser `camada:identificador` (nada é fundido indevidamente).

**Dados históricos.** As solicitações coletadas são de **abril a outubro de 2020**. Elas mostram onde a zeladoria já
registrou barreiras em Pinheiros (série histórica), **não a situação atual das calçadas**. Consequências:
- no relatório e nas consultas, o SP156 deve ser apresentado como "registro histórico 2020";
- "aberta" no SP156 significa "aberta em 2020 segundo o GeoSampa", não "barreira ativa hoje";
- a evolução diária (C6) mistura 2020 (SP156) e 2026 (óculos): interpretar por origem.
- **Pendente de decisão antes da Ouro:** marcar no MVP as barreiras do SP156 como históricas, para que uma solicitação
  de 2020 não apareça na tela como barreira atual.
