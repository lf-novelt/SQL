;WITH CTE_TableStats AS    
(
    SELECT
        object_id,
        object_schema_name(object_id) + '.' + object_name(object_id) AS TableName,
        SUM (reserved_page_count) AS ReservedPages,
        SUM (used_page_count) AS UsedPages,
        SUM (
            CASE
                WHEN (index_id < 2) THEN (in_row_data_page_count + lob_used_page_count + row_overflow_used_page_count)
                ELSE 0
            END
        ) AS Pages,
        SUM (
            CASE
                WHEN (index_id < 2) THEN row_count
                ELSE 0
            END
            ) AS TableRowCount
    FROM sys.dm_db_partition_stats
    GROUP BY
        object_id
)
SELECT
    ts.TableName,
    ts.TableRowCount,
    CONVERT (decimal(15,3), (ts.Pages * 8.0) / 1024.0) AS 'TableSize (MB)'
FROM
    CTE_TableStats ts
INNER JOIN
    sys.objects o
ON
    o.object_id = ts.object_id
ORDER BY
    3 DESC