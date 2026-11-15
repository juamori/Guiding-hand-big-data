# Fontes de dados

> Situação de validação: a coluna "Validação" é atualizada com o relatório de `scripts/validar_fontes.py` executado com internet.
> Enquanto não houver relatório real em `docs/validacao_fontes/`, nenhuma fonte externa deve ser considerada testada.

| Fonte | Origem | Formato | Frequência de coleta | Atualização na origem | Finalidade no Guiding Hand | Validação |
|---|---|---|---|---|---|---|
| Clima | Open-Meteo Forecast API (`api.open-meteo.com/v1/forecast`), sem chave | JSON (listas horárias paralelas) | Diária (dia anterior completo) | A cada hora | Contextualizar detecções: chuva → poças/piso liso, rajadas → galhos | pendente |
| Infraestrutura de calçada | OpenStreetMap via Overpass API, licença ODbL | JSON | Diária | Contínua (colaborativa) | Postes, árvores, lixeiras, rebaixamentos, piso tátil, faixas e pontos de ônibus no recorte | pendente |
| Zeladoria SP156 | Prefeitura de SP — camadas SP156 georreferenciadas do GeoSampa (WFS) | GeoJSON | Diária | Base agregada por semestre e atualizada trimestralmente (SMIT); GeoSampa mostra os últimos 6 meses | Barreiras reais (buraco, calçada, poda, sinalização) com status oficial e data de conclusão → "resolvidas" | pendente (camadas descobertas pelo validador) |
| Paradas de ônibus (opcional) | SPTrans Olho Vivo API v2.1, token gratuito | JSON | Diária | Cadastro estável | Destinos de rota e pontos de calçada congestionada | depende de token |
| Eventos dos óculos | Gerador `sintetico_v1` (este repositório) | JSON | Diária | — | Simular as detecções que Applied Computer Vision produzirá | testado (sintético) |

## Como os eventos sintéticos são gerados (`dags/guiding_hand_lib/sintetico.py`)

1. **Mundo fixo** de 45 obstáculos no recorte, criado com semente mestra `20261119` — o mesmo em todos os dias, para que o mesmo obstáculo seja redetectado em coletas diferentes.
2. Pesos por tipo inspirados nas dificuldades citadas na visão geral do projeto (placas, galhos, carros na calçada, buracos...).
3. Obstáculos temporários (carro, caçamba, entulho, galho) tendem a desaparecer em 2–7 dias, simulando remoção/resolução.
4. Por dia, cada obstáculo ativo é detectado por 0 a 4 dispositivos (8 óculos simulados), com ruído de GPS gaussiano (~6 m) e confiança ~N(0,80; 0,11).
5. **Ruído intencional** para exercitar a Prata: 6–12 detecções de pessoas (falso alarme), ~3% de reenvios com o mesmo `event_id`, e 5 registros inválidos marcados com `_injetado_para_teste`.
6. Semente do dia = semente mestra + data: **reexecutar a mesma data gera exatamente o mesmo arquivo**.

Todo registro tem `origem = "sintetico_v1"`; o arquivo traz `aviso: DADOS SINTETICOS`; na Ouro a origem aparece como `oculos_sintetico`.
