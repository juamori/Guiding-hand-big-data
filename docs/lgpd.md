# Privacidade e LGPD

O uso do Guiding Hand revela uma deficiência; por isso todo dado ligado a um usuário é tratado como **dado pessoal sensível** (LGPD, art. 5º, II).

| Medida | Onde | Como |
|---|---|---|
| Sem imagens | Contrato com Visão | A câmera analisa e descarta; o pipeline só recebe tipo, posição e confiança |
| Pseudonimização do dispositivo | Prata e Ouro | `device_hash = SHA-256(device_id + salt)`; o `device_id` é descartado na Prata |
| Salt fora do código | Databricks | Secret scope `guiding_hand/salt` ou arquivo no Volume `controle` (nunca no Git) |
| Localização generalizada | Prata | Coordenadas arredondadas para 4 casas (~11 m); a coordenada exata existe só no bruto da Bronze |
| Localização da Ouro | Ouro | Fato usa a quadra (geohash 7, ~150 m); o mapa usa o **centro do agrupamento de detecções**, nunca a posição de uma pessoa |
| Tabela do MVP sem dado pessoal | Ouro | `tb_mvp_mapa_barreiras` não tem nenhuma coluna de dispositivo (teste automatizado garante) |
| Minimização na fonte pública | SP156 | A Prefeitura já remove dados pessoais; o pipeline não importa texto livre |
| Credenciais | Airflow | Token e webhook só em Variables; `.env` e `variables.json` fora do Git; `scripts/verificar_segredos.py` antes de publicar |
| Acesso ao bruto | Bronze | Em produção, Bronze restrita à engenharia; Ouro liberada ao produto |

Limitação assumida no protótipo: no Databricks Free Edition o salt pode ficar num Volume privado do workspace; em produção ele iria para um cofre de segredos.
