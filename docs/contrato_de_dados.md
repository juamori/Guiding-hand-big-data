# Contrato de dados entre Big Data e as outras disciplinas

## 1. Entrada: detecções dos óculos (Applied Computer Vision + Maker Lab → Big Data)

Arquivo diário `eventos_oculos_AAAA-MM-DD.json` com a chave `eventos` (lista). Enquanto o protótipo não envia dados de campo, o gerador sintético produz exatamente este formato.

| Campo | Tipo | Obrigatório | Observação |
|---|---|---|---|
| event_id | string (UUID) | sim | Único por detecção; reenvios repetem o mesmo id |
| device_id | string | sim | Pseudonimizado na Prata (SHA-256 + salt); nunca chega à Ouro em claro |
| ts_deteccao | string ISO 8601 com fuso | sim | ex.: `2026-11-12T08:15:00-03:00` |
| tipo_obstaculo | string | sim | Domínio: placa, galho_arvore, orelhao, poste, cacamba, carro_calcada, lixeira, entulho, buraco, calcada_irregular, pessoa |
| confianca | número 0–1 | sim | Detecções abaixo de 0,5 vão para a quarentena |
| lat, lon | número (WGS84) | sim | Precisão do GPS; generalizados na Prata (4 casas ≈ 11 m) |
| distancia_m | número | não | Distância estimada pelo sensor |
| altura | string | não | acima_cintura, abaixo_cintura, chao, movel |
| modelo_versao | string | não | Versão do modelo de visão |
| origem | string | sim | `sintetico_v1` ou identificador do dispositivo real |

**Imagens não fazem parte do contrato.** A câmera analisa e descarta; o pipeline nunca recebe nem armazena imagem.

## 2. Saída para o MVP (Big Data → aplicativo / Secure DevOps)

- Tabela: `workspace.guiding_hand.tb_mvp_mapa_barreiras`
- Entrega: `barreiras_mvp.json` exportado pelo notebook `databricks/mvp/05_exportar_mvp.py` (contrato `guiding-hand/tb_mvp_mapa_barreiras/v1`)

| Campo | Significado |
|---|---|
| barreira_id | Identificador estável da barreira (12 caracteres) |
| origem | `oculos_sintetico` ou `sp156` |
| tipo_obstaculo, descricao, altura | Tipo e descrição legível |
| bengala_costuma_detectar | `false` = obstáculo na altura da cabeça, que a bengala não alcança |
| lat, lon | Centro do agrupamento (nunca a posição de uma pessoa) |
| geohash7 | Quadra aproximada (~150 m) |
| n_ocorrencias, n_dispositivos, dias_com_deteccao | Força da evidência |
| primeira_vez, ultima_vez | Período observado |
| status_barreira | ativa, nao_redetectada (óculos) ou status oficial SP156 (aberta, resolvida, cancelada) |
| elegivel_confirmacao | Óculos: ≥ 2 dispositivos e ≥ 3 detecções |
| status_validacao | `pendente` até o RPA confirmar; `registro_oficial` para SP156 |
| nivel_alerta | `alto` quando a bengala não detecta |

## 3. Robotic Process Automation

O RPA **lê** as barreiras com `elegivel_confirmacao = true`, decide a confirmação e gera os relatórios para a zeladoria. A regra de confirmação é dele; o Big Data só fornece a evidência agregada. Para devolver o resultado, o RPA grava `confirmacoes_rpa_AAAA-MM-DD.json` (`barreira_id`, `status_validacao`, `confirmado_em`), que pode entrar como nova fonte da Bronze (extensão prevista, não implementada nesta entrega).

## 4. Business Intelligence & Generative AI

- BI: usa as consultas `databricks/sql/consultas_mvp.sql`, em especial a C5 (impacto) e a C6 (evolução).
- GenAI: não consome dados pessoais; pode usar `dim_tipo_obstaculo` e a contagem por tipo como contexto.
