-- Run some complex query
SELECT
    M.MinifigType,
    SUM(NumberOfParts) AS TotalPartsPerMinifigType
FROM    
    Rebrickable.Minifigs AS M
GROUP BY
    M.MinifigType
ORDER BY
    TotalPartsPerMinifigType DESC
OPTION (LABEL = 'Result set caching test')

-- Enable Result set caching at DB level
-- Run on master DB!
ALTER DATABASE DWHTest SET RESULT_SET_CACHING ON;

-- Check requests
SELECT 
    PDW.request_id,
    PDW.command, 
    PDW.status, 
    PDW.result_cache_hit
FROM 
    sys.dm_pdw_exec_requests AS PDW 
WHERE 
    PDW.[label] ='Result set caching test'
ORDER BY
    PDW.submit_time DESC


-- Disable it (on master DB)
ALTER DATABASE DWHTest SET RESULT_SET_CACHING OFF;