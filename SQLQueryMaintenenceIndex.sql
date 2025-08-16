-- =============================================
-- MANUTENÇÃO DE ÍNDICES - ANÁLISE DE FRAGMENTAÇÃO
-- =============================================

/*
Objetivo: Este conjunto de consultas ajuda a identificar e tratar a fragmentação de índices
no banco de dados atual, gerando recomendações e comandos de manutenção automatizados.
*/

-- ==================================================================
-- CONSULTA PRINCIPAL: ÍNDICES FRAGMENTADOS COM FILTROS (MAIS COMUM)
-- ==================================================================
-- Identifica índices com fragmentação significativa (>10%) e tamanho relevante (>100 páginas)
SELECT 
    OBJECT_NAME(ips.object_id) AS TableName,          -- Nome da tabela
    i.name AS IndexName,                              -- Nome do índice
    ips.index_type_desc AS IndexType,                 -- Tipo (CLUSTERED/NONCLUSTERED)
    ips.avg_fragmentation_in_percent AS Fragmentation, -- Percentual de fragmentação
    ips.page_count AS PageCount                       -- Quantidade de páginas no índice
FROM 
    sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips  -- DMF que retorna estatísticas físicas
INNER JOIN 
    sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id  -- Junta com metadados dos índices
WHERE 
    ips.avg_fragmentation_in_percent > 10  -- Filtra apenas índices com mais de 10% de fragmentação
    AND ips.page_count > 100               -- Filtra índices com mais de 100 páginas (ignora pequenos)
ORDER BY 
    ips.avg_fragmentation_in_percent DESC;  -- Ordena do mais fragmentado para o menos

-- ==================================================================
-- CONSULTA ALTERNATIVA: TODOS OS ÍNDICES (SEM FILTROS)
-- ==================================================================
-- Versão sem filtros para análise completa do banco
SELECT 
    OBJECT_NAME(ips.object_id) AS TableName,
    i.name AS IndexName,
    ips.index_type_desc AS IndexType,
    ips.avg_fragmentation_in_percent AS Fragmentation,
    ips.page_count AS PageCount
FROM 
    sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
INNER JOIN 
    sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
ORDER BY 
    ips.avg_fragmentation_in_percent DESC;  -- Mantém ordenação por fragmentação

-- ==================================================================
-- 1. IDENTIFICAÇÃO DE ÍNDICES CRITICAMENTE FRAGMENTADOS (>30%)
-- ==================================================================
-- Foco nos índices com alta fragmentação que impactam mais o desempenho
SELECT * FROM (
    SELECT 
        OBJECT_NAME(object_id) AS TableName,          -- Nome da tabela
        index_id,                                    -- ID do índice
        avg_fragmentation_in_percent                 -- Percentual de fragmentação
    FROM 
        sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED')
) AS FragStats
WHERE avg_fragmentation_in_percent > 30  -- Limiar para considerar crítica
ORDER BY avg_fragmentation_in_percent DESC;  -- Prioriza os piores casos

-- ==================================================================
-- 2. GERADOR DE RECOMENDAÇÕES (REBUILD/REORGANIZE)
-- ==================================================================
-- Classifica índices e sugere ação apropriada baseada no nível de fragmentação
SELECT 
    OBJECT_NAME(ips.object_id) AS TableName,
    i.name AS IndexName,
    ips.avg_fragmentation_in_percent,
    CASE
        WHEN ips.avg_fragmentation_in_percent > 30 THEN 'REBUILD'       -- Acima de 30%: Rebuild completo
        WHEN ips.avg_fragmentation_in_percent BETWEEN 10 AND 30 THEN 'REORGANIZE' -- 10-30%: Reorganize
        ELSE 'OK'                                                      -- Abaixo de 10%: Nada necessário
    END AS Recommendation
FROM 
    sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
JOIN 
    sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
WHERE 
    ips.avg_fragmentation_in_percent > 10  -- Filtra apenas os que precisam de ação
ORDER BY 
    ips.avg_fragmentation_in_percent DESC;  -- Ordena por gravidade

-- ==================================================================
-- 3. GERADOR DE SCRIPTS DE MANUTENÇÃO AUTOMÁTICA
-- ==================================================================
-- Cria comandos SQL prontos para execução baseados nas recomendações
SELECT 
    OBJECT_NAME(ips.object_id) AS TableName,
    i.name AS IndexName,
    ips.avg_fragmentation_in_percent,
    CASE
        WHEN ips.avg_fragmentation_in_percent > 30 THEN
            'ALTER INDEX [' + i.name + '] ON [' + OBJECT_NAME(ips.object_id) + '] REBUILD WITH (ONLINE = OFF)'  -- Rebuild para fragmentação alta
        WHEN ips.avg_fragmentation_in_percent BETWEEN 10 AND 30 THEN
            'ALTER INDEX [' + i.name + '] ON [' + OBJECT_NAME(ips.object_id) + '] REORGANIZE'  -- Reorganize para fragmentação moderada
    END AS MaintenanceCommand
FROM 
    sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
JOIN 
    sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
WHERE 
    ips.avg_fragmentation_in_percent > 10  -- Apenas índices que precisam de manutenção
    AND i.name IS NOT NULL                 -- Ignora índices sem nome (HEAP)
ORDER BY 
    ips.avg_fragmentation_in_percent DESC;  -- Ordena do mais fragmentado