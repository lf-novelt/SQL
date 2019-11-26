--CREATE DATABASE CJFTest
--GO
USE CJFTEST
GO

IF object_id('dbo.PartitionedTable') IS NOT NULL
	DROP TABLE dbo.PartitionedTable

DECLARE @SchemeSQL varchar(255) = NULL

IF object_id('tempdb..#SchemeList') IS NOT NULL
	DROP TABLE #SchemeList


	SELECT
		'DROP PARTITION SCHEME ' + s.name AS SQLCommand
	INTO
		#SchemeList
	FROM
		sys.partition_schemes s
	INNER JOIN
		sys.partition_functions f
	ON
		s.function_id = f.function_id
	AND
		f.name = 'myRangePF1'

WHILE (SELECT COUNT(*) FROM #SchemeList) <> 0
BEGIN
	SELECT TOP 1 @SchemeSQL = SQLCommand
	FROM #SchemeList

	EXEC(@SchemeSQL)

	DELETE FROM #SchemeList
	WHERE SQLCommand = @SchemeSQL
END

IF object_id('tempdb..#SchemeList') IS NOT NULL
	DROP TABLE #SchemeList

GO
IF EXISTS (select * from sys.partition_functions where name like 'myRangePF1')
	DROP PARTITION FUNCTION myRangePF1
GO


--select * from sys.objects WHERE name = 'myRangePS1'

CREATE PARTITION FUNCTION myRangePF1 (INT)
AS RANGE LEFT FOR VALUES (3,6);

GO
CREATE PARTITION SCHEME myRangePS1
  AS PARTITION myRangePF1
  ALL TO ([PRIMARY]);

GO
CREATE TABLE dbo.PartitionedTable (col1 INT, col2 VARCHAR(20))
 ON myRangePS1 (col1);

GO
INSERT INTO dbo.PartitionedTable (col1, col2) VALUES (1, 'Test Value 1');

INSERT INTO dbo.PartitionedTable (col1, col2) VALUES (2, 'Test Value 2');

INSERT INTO dbo.PartitionedTable (col1, col2) VALUES (3, 'Test Value 3');

INSERT INTO dbo.PartitionedTable (col1, col2) VALUES (4, 'Test Value 4');

INSERT INTO dbo.PartitionedTable (col1, col2) VALUES (5, 'Test Value 5');

INSERT INTO dbo.PartitionedTable (col1, col2) VALUES (6, 'Test Value 6');

INSERT INTO dbo.PartitionedTable (col1, col2) VALUES (7, 'Test Value 7');

INSERT INTO dbo.PartitionedTable (col1, col2) VALUES (8, 'Test Value 8');

INSERT INTO dbo.PartitionedTable (col1, col2) VALUES (9, 'Test Value 9');

GO
CREATE NONCLUSTERED COLUMNSTORE INDEX nci_PartitionedTable

  ON dbo.PartitionedTable(col1,col2);

GO

SELECT 'PartitionedTable'
SELECT * FROM dbo.PartitionedTable;

GO
SELECT
     SCHEMA_NAME(t.schema_id) AS SchemaName
    ,OBJECT_NAME(i.object_id) AS ObjectName
    ,p.partition_number AS PartitionNumber
    ,fg.name AS FilegroupName
    ,rows AS 'Rows'
    ,au.total_pages AS 'TotalDataPages'
    ,CASE boundary_value_on_right
        WHEN 1 THEN 'less than'
        ELSE 'less than or equal to'
     END AS 'Comparison'
    ,value AS 'ComparisonValue'
    ,p.data_compression_desc AS 'DataCompression'
    ,p.partition_id
FROM sys.partitions p
    JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
    JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
    JOIN sys.partition_functions f ON f.function_id = ps.function_id
    LEFT JOIN sys.partition_range_values rv ON f.function_id = rv.function_id AND p.partition_number = rv.boundary_id
    JOIN sys.destination_data_spaces dds ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id = p.partition_number
    JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
    JOIN (SELECT container_id, sum(total_pages) as total_pages
            FROM sys.allocation_units
            GROUP BY container_id) AS au ON au.container_id = p.partition_id 
    JOIN sys.tables t ON p.object_id = t.object_id
WHERE i.index_id < 2
ORDER BY ObjectName,p.partition_number;
GO

IF object_id('dbo.NonPartitionedTable') IS NOT NULL
	DROP TABLE dbo.NonPartitionedTable

CREATE TABLE dbo.NonPartitionedTable
(
   col1 INT CHECK (col1 <= 3)
  ,col2 VARCHAR(20)
);
GO

INSERT INTO dbo.NonPartitionedTable (col1, col2)
SELECT col1, col2
FROM
	PartitionedTable
WHERE
	col1 BETWEEN 1 AND 3

SELECT 'NonPartitionedTable'
SELECT * FROM dbo.NonPartitionedTable;

CREATE NONCLUSTERED COLUMNSTORE INDEX nci_NonPartitionedTable
  ON dbo.NonPartitionedTable(col1,col2);
GO

SET XACT_ABORT ON;
GO

BEGIN TRANSACTION;
GO
SELECT 'PartitionedTable'
SELECT * FROM dbo.PartitionedTable;
/*
col1	col2
1		Test Value 1
2		changed a value
3		Test Value 3
4		Test Value 4
5		Test Value 5
6		Test Value 6
7		Test Value 7
8		Test Value 8
9		Test Value 9
*/
SELECT 'NonPartitionedTable'
SELECT * FROM dbo.NonPartitionedTable;
/*
col1	col2
*/

SELECT 'ALTER TABLE dbo.PartitionedTable SWITCH PARTITION 1 TO dbo.NonPartitionedTable;'
ALTER TABLE dbo.PartitionedTable SWITCH PARTITION $partition.myRangePF1(1) TO dbo.NonPartitionedTable; 
GO

SELECT 'PartitionedTable'
SELECT * FROM dbo.PartitionedTable;
/*
col1	col2
4		Test Value 4
5		Test Value 5
6		Test Value 6
7		Test Value 7
8		Test Value 8
9		Test Value 9
*/

SELECT 'NonPartitionedTable;'
SELECT * FROM dbo.NonPartitionedTable;
/*
col1	col2
1		Test Value 1
2		changed a value
3		Test Value 3
*/

RAISERROR('ALTER INDEX nci_NonPartitionedTable ON dbo.NonPartitionedTable DISABLE;', 0, 0) WITH NOWAIT
SELECT 'ALTER INDEX nci_NonPartitionedTable ON dbo.NonPartitionedTable DISABLE;'
ALTER INDEX nci_NonPartitionedTable ON dbo.NonPartitionedTable DISABLE;
GO

SELECT 'UPDATE NonPartitionedTable SET col2 = ''changed another'' WHERE col1 = 1;'
UPDATE NonPartitionedTable SET col2 = 'changed another' WHERE col1 = 1;
GO

RAISERROR('ALTER INDEX nci_NonPartitionedTable ON dbo.NonPartitionedTable REBUILD;', 0, 0) WITH NOWAIT
SELECT ('ALTER INDEX nci_NonPartitionedTable ON dbo.NonPartitionedTable REBUILD;')
ALTER INDEX nci_NonPartitionedTable ON dbo.NonPartitionedTable REBUILD;
GO
SELECT 'PartitionedTable'
SELECT * FROM dbo.PartitionedTable;
/*
col1	col2
4		Test Value 4
5		Test Value 5
6		Test Value 6
7		Test Value 7
8		Test Value 8
9		Test Value 9
*/

SELECT 'NonPartitionedTable'
SELECT * FROM dbo.NonPartitionedTable;
/*
col1	col2
1		changed another
2		changed a value
3		Test Value 3
*/

RAISERROR('ALTER TABLE dbo.NonPartitionedTable SWITCH TO dbo.PartitionedTable PARTITION 1;', 0, 0) WITH NOWAIT
SELECT 'ALTER TABLE dbo.NonPartitionedTable SWITCH TO dbo.PartitionedTable PARTITION ' + CONVERT(VARCHAR(10), $partition.myRangePF1(1)) + ';'
ALTER TABLE dbo.NonPartitionedTable SWITCH TO dbo.PartitionedTable PARTITION $partition.myRangePF1(1); 
GO
SELECT $partition.myRangePF1(1) AS 'PartitionNo'

SELECT 'PartitionedTable'
SELECT * FROM dbo.PartitionedTable;
/*
col1	col2
1		changed another
2		changed a value
3		Test Value 3
4		Test Value 4
5		Test Value 5
6		Test Value 6
7		Test Value 7
8		Test Value 8
9		Test Value 9
*/
SELECT 'NonPartitionedTable'
SELECT * FROM dbo.NonPartitionedTable;
/*
col1	col2
*/
--COMMIT TRANSACTION;
GO

