-- =============================================
-- MANUTEN��O DE �NDICES - AN�LISE DE FRAGMENTA��O
-- =============================================

/*
Objetivo: Este conjunto de consultas ajuda a identificar e tratar a fragmenta��o de �ndices
no banco de dados atual, gerando recomenda��es e comandos de manuten��o automatizados.
*/

-- ==================================================================
-- CONSULTA PRINCIPAL: �NDICES FRAGMENTADOS COM FILTROS (MAIS COMUM)
-- ==================================================================
-- Identifica �ndices com fragmenta��o significativa (>10%) e tamanho relevante (>100 p�ginas)
SELECT 
    OBJECT_NAME(ips.object_id) AS TableName,          -- Nome da tabela
    i.name AS IndexName,                              -- Nome do �ndice
    ips.index_type_desc AS IndexType,                 -- Tipo (CLUSTERED/NONCLUSTERED)
    ips.avg_fragmentation_in_percent AS Fragmentation, -- Percentual de fragmenta��o
    ips.page_count AS PageCount                       -- Quantidade de p�ginas no �ndice
FROM 
    sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips  -- DMF que retorna estat�sticas f�sicas
INNER JOIN 
    sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id  -- Junta com metadados dos �ndices
WHERE 
    ips.avg_fragmentation_in_percent > 10  -- Filtra apenas �ndices com mais de 10% de fragmenta��o
    AND ips.page_count > 100               -- Filtra �ndices com mais de 100 p�ginas (ignora pequenos)
ORDER BY 
    ips.avg_fragmentation_in_percent DESC;  -- Ordena do mais fragmentado para o menos

-- ==================================================================
-- CONSULTA ALTERNATIVA: TODOS OS �NDICES (SEM FILTROS)
-- ==================================================================
-- Vers�o sem filtros para an�lise completa do banco
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
    ips.avg_fragmentation_in_percent DESC;  -- Mant�m ordena��o por fragmenta��o

-- ==================================================================
-- 1. IDENTIFICA��O DE �NDICES CRITICAMENTE FRAGMENTADOS (>30%)
-- ==================================================================
-- Foco nos �ndices com alta fragmenta��o que impactam mais o desempenho
SELECT * FROM (
    SELECT 
        OBJECT_NAME(object_id) AS TableName,          -- Nome da tabela
        index_id,                                    -- ID do �ndice
        avg_fragmentation_in_percent                 -- Percentual de fragmenta��o
    FROM 
        sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED')
) AS FragStats
WHERE avg_fragmentation_in_percent > 30  -- Limiar para considerar cr�tica
ORDER BY avg_fragmentation_in_percent DESC;  -- Prioriza os piores casos

-- ==================================================================
-- 2. GERADOR DE RECOMENDA��ES (REBUILD/REORGANIZE)
-- ==================================================================
-- Classifica �ndices e sugere a��o apropriada baseada no n�vel de fragmenta��o
SELECT 
    OBJECT_NAME(ips.object_id) AS TableName,
    i.name AS IndexName,
    ips.avg_fragmentation_in_percent,
    CASE
        WHEN ips.avg_fragmentation_in_percent > 30 THEN 'REBUILD'       -- Acima de 30%: Rebuild completo
        WHEN ips.avg_fragmentation_in_percent BETWEEN 10 AND 30 THEN 'REORGANIZE' -- 10-30%: Reorganize
        ELSE 'OK'                                                      -- Abaixo de 10%: Nada necess�rio
    END AS Recommendation
FROM 
    sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
JOIN 
    sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
WHERE 
    ips.avg_fragmentation_in_percent > 10  -- Filtra apenas os que precisam de a��o
ORDER BY 
    ips.avg_fragmentation_in_percent DESC;  -- Ordena por gravidade

-- ==================================================================
-- 3. GERADOR DE SCRIPTS DE MANUTEN��O AUTOM�TICA
-- ==================================================================
-- Cria comandos SQL prontos para execu��o baseados nas recomenda��es
SELECT 
    OBJECT_NAME(ips.object_id) AS TableName,
    i.name AS IndexName,
    ips.avg_fragmentation_in_percent,
    CASE
        WHEN ips.avg_fragmentation_in_percent > 30 THEN
            'ALTER INDEX [' + i.name + '] ON [' + OBJECT_NAME(ips.object_id) + '] REBUILD WITH (ONLINE = OFF)'  -- Rebuild para fragmenta��o alta
        WHEN ips.avg_fragmentation_in_percent BETWEEN 10 AND 30 THEN
            'ALTER INDEX [' + i.name + '] ON [' + OBJECT_NAME(ips.object_id) + '] REORGANIZE'  -- Reorganize para fragmenta��o moderada
    END AS MaintenanceCommand
FROM 
    sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
JOIN 
    sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
WHERE 
    ips.avg_fragmentation_in_percent > 10  -- Apenas �ndices que precisam de manuten��o
    AND i.name IS NOT NULL                 -- Ignora �ndices sem nome (HEAP)
ORDER BY 
    ips.avg_fragmentation_in_percent DESC;  -- Ordena do mais fragmentado