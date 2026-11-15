-- Evidências do Requisito 05 (seção 7 do relatório): contagens da Bronze por execução.
SELECT rotulo, linhas_antes, linhas_depois, arquivos_novos, executado_em
FROM workspace.guiding_hand.controle_execucoes_bronze
ORDER BY executado_em;

-- Arquivos por fonte e por dia de coleta (cada arquivo aparece uma única vez)
SELECT fonte, data_coleta, count(*) AS arquivos, count(DISTINCT hash_conteudo) AS conteudos_distintos
FROM workspace.guiding_hand.bronze_arquivos
GROUP BY fonte, data_coleta ORDER BY data_coleta, fonte;

-- Prova de não duplicação: nenhum arquivo carregado duas vezes
SELECT arquivo_origem, count(*) AS vezes
FROM workspace.guiding_hand.bronze_arquivos
GROUP BY arquivo_origem HAVING count(*) > 1;
