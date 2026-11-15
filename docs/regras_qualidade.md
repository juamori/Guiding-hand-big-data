# Regras de qualidade da Prata (Requisito 06)

Cada registro é avaliado na ordem abaixo; o primeiro problema encontrado define o código gravado em `prata_quarentena.regra`. A coluna "Barrados" deve ser preenchida com o resultado da célula de evidência do notebook `02_prata` **no Databricks**. Os números da execução local de teste aparecem só como referência e não são evidência.

| Código | Fonte | Descrição | Critério de validação | Barrados (Databricks) | Teste local | Justificativa |
|---|---|---|---|---|---|---|
| RQ1_CAMPOS_OBRIGATORIOS | todas | Campos essenciais presentes | chave, data/hora e coordenadas não nulas | ___ | 0 | Sem chave não há MERGE; sem posição não há mapa |
| RQ2_FORA_DO_RECORTE | eventos, OSM, SP156, Olho Vivo | Dentro do bairro piloto | lat entre -23,575 e -23,560; lon entre -46,710 e -46,694 | ___ | 5 | O MVP é do piloto de Pinheiros; ponto fora é erro de GPS ou de fonte |
| RQ3_TIPO_FORA_DO_DOMINIO | eventos | Tipo conhecido pelo produto | tipo ∈ domínio do contrato com Visão | ___ | 4 | Tipo desconhecido não gera alerta nem entra no mapa |
| RQ4_CONFIANCA_INVALIDA | eventos | Confiança é probabilidade | 0 ≤ confiança ≤ 1 | ___ | 2 | Valor fora do intervalo indica defeito do modelo/dispositivo |
| RQ5_CONFIANCA_ABAIXO_DO_LIMIAR | eventos | Detecção confiável | confiança ≥ 0,5 | ___ | 2 | Reduz falso alarme — problema relatado por usuários (visão geral) |
| RQ6_MEDIDA_FISICA_IMPOSSIVEL | clima | Medida plausível | precipitação ≥ 0, visibilidade ≥ 0, temperatura entre -10 e 50 °C | ___ | 1 | Valor impossível distorceria a análise de chuva |
| RQ7_DATAS_INCOERENTES | SP156 | Linha do tempo coerente | data de conclusão ≥ data de abertura | ___ | 1 | Sem isso o "tempo até resolver" (impacto) fica negativo |

Além das regras: **nulos** (precipitação nula → 0; origem nula → `desconhecida`), **normalização** (tipos em minúsculas e sem espaços; nomes de parada em capitalização padrão; status SP156 → aberta/resolvida/cancelada) e **deduplicação** (última versão por chave antes do MERGE; reenvios de `event_id` removidos).
