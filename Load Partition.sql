-- Create a partitioned table
Use DemoDW;
ALTER DATABASE DemoDW ADD FILEGROUP FG0000
GO
ALTER DATABASE DemoDW ADD FILE (NAME = F0000, FILENAME = 'D:\Demofiles\Mod09\F0000.ndf', SIZE = 3MB, FILEGROWTH = 50%) TO FILEGROUP FG0000;
GO
ALTER DATABASE DemoDW ADD FILEGROUP FG2000
GO
ALTER DATABASE DemoDW ADD FILE (NAME = F2000, FILENAME = 'D:\Demofiles\Mod09\F2000.ndf', SIZE = 3MB, FILEGROWTH = 50%) TO FILEGROUP FG2000;
GO
ALTER DATABASE DemoDW ADD FILEGROUP FG2001
GO
ALTER DATABASE DemoDW ADD FILE (NAME = F2001, FILENAME = 'D:\Demofiles\Mod09\F2001.ndf', SIZE = 3MB, FILEGROWTH = 50%) TO FILEGROUP FG2001;
GO
ALTER DATABASE DemoDW ADD FILEGROUP FG2002
GO
ALTER DATABASE DemoDW ADD FILE (NAME = F2002, FILENAME = 'D:\Demofiles\Mod09\F2002.ndf', SIZE = 3MB, FILEGROWTH = 50%) TO FILEGROUP FG2002;
GO

CREATE PARTITION FUNCTION PF (int) AS RANGE RIGHT FOR VALUES (20000101, 20010101, 20020101);
CREATE PARTITION SCHEME PS AS PARTITION PF TO (FG0000, FG2000, FG2001, FG2002);

CREATE TABLE fact_table
 (datekey int, measure int)
ON PS(datekey);
GO

INSERT fact_table VALUES (20000101, 100);
INSERT fact_table VALUES (20001231, 100);
INSERT fact_table VALUES (20010101, 100);
INSERT fact_table VALUES (20010403, 100);
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX csidx_fact_table
ON fact_table(datekey, measure);
GO

-- View partition metadata
SELECT i.index_id, i.name AS IndexName, ps.name AS PartitionScheme, pf.name AS PartitionFunction, p.partition_number AS PartitionNumber, fg.name AS Filegroup, prv_left.value AS StartKey, prv_right.value AS EndKey, p.row_count Rows
FROM sys.dm_db_partition_stats p
INNER JOIN sys.indexes i
ON i.OBJECT_ID = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN sys.data_spaces ds
ON ds.data_space_id = i.data_space_id
LEFT OUTER JOIN sys.partition_schemes ps
ON ps.data_space_id = i.data_space_id
LEFT OUTER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
LEFT OUTER JOIN sys.destination_data_spaces dds
ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id = p.partition_number
LEFT OUTER JOIN sys.filegroups fg
ON fg.data_space_id = dds.data_space_id
LEFT OUTER JOIN sys.partition_range_values prv_right
ON prv_right.function_id = ps.function_id AND prv_right.boundary_id = p.partition_number
LEFT OUTER JOIN sys.partition_range_values prv_left
ON prv_left.function_id = ps.function_id AND prv_left.boundary_id = p.partition_number - 1
WHERE OBJECT_NAME(p.object_id) = 'fact_table'
GO

-- Add a new filegroup and make it the next used
ALTER DATABASE DemoDW ADD FILEGROUP FG2003
GO
ALTER DATABASE DemoDW ADD FILE (NAME = F2003, FILENAME = 'D:\Demofiles\Mod09\F2003.ndf', SIZE = 3MB, FILEGROWTH = 50%) TO FILEGROUP FG2003;
GO
ALTER PARTITION SCHEME PS
NEXT USED FG2003;
GO

-- Split the empty partition at the end
ALTER PARTITION FUNCTION PF() SPLIT RANGE(20030101);
GO

-- View partition metadata again
SELECT i.name AS IndexName, ps.name AS PartitionScheme, pf.name AS PartitionFunction, p.partition_number AS PartitionNumber, fg.name AS Filegroup, prv_left.value AS StartKey, prv_right.value AS EndKey, p.row_count Rows
FROM sys.dm_db_partition_stats p
INNER JOIN sys.indexes i
ON i.OBJECT_ID = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN sys.data_spaces ds
ON ds.data_space_id = i.data_space_id
LEFT OUTER JOIN sys.partition_schemes ps
ON ps.data_space_id = i.data_space_id
LEFT OUTER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
LEFT OUTER JOIN sys.destination_data_spaces dds
ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id = p.partition_number
LEFT OUTER JOIN sys.filegroups fg
ON fg.data_space_id = dds.data_space_id
LEFT OUTER JOIN sys.partition_range_values prv_right
ON prv_right.function_id = ps.function_id AND prv_right.boundary_id = p.partition_number
LEFT OUTER JOIN sys.partition_range_values prv_left
ON prv_left.function_id = ps.function_id AND prv_left.boundary_id = p.partition_number - 1
WHERE OBJECT_NAME(p.object_id) = 'fact_table'
AND i.index_id = 0
GO

-- Create a load table
CREATE TABLE load_table
 (datekey int, measure int)
ON FG2002;
GO

-- Bulk load new data
INSERT load_table VALUES (20020101, 100);
INSERT load_table VALUES (20021005, 100);
GO

-- Add constraints and indexes
ALTER TABLE load_table
WITH CHECK ADD CONSTRAINT BOUNDS
CHECK (datekey >= 20020101 and datekey < 20030101 and datekey IS NOT NULL);
GO
CREATE NONCLUSTERED COLUMNSTORE INDEX csidx_load_table
ON load_table(datekey, measure);
GO

-- Switch the partition
ALTER TABLE load_table
SWITCH TO fact_table PARTITION $PARTITION.PF(20020101)
GO

-- Clean up and view partition metadata
DROP TABLE load_table;
GO
SELECT i.name AS IndexName, ps.name AS PartitionScheme, pf.name AS PartitionFunction, p.partition_number AS PartitionNumber, fg.name AS Filegroup, prv_left.value AS StartKey, prv_right.value AS EndKey, p.row_count Rows
FROM sys.dm_db_partition_stats p
INNER JOIN sys.indexes i
ON i.OBJECT_ID = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN sys.data_spaces ds
ON ds.data_space_id = i.data_space_id
LEFT OUTER JOIN sys.partition_schemes ps
ON ps.data_space_id = i.data_space_id
LEFT OUTER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
LEFT OUTER JOIN sys.destination_data_spaces dds
ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id = p.partition_number
LEFT OUTER JOIN sys.filegroups fg
ON fg.data_space_id = dds.data_space_id
LEFT OUTER JOIN sys.partition_range_values prv_right
ON prv_right.function_id = ps.function_id AND prv_right.boundary_id = p.partition_number
LEFT OUTER JOIN sys.partition_range_values prv_left
ON prv_left.function_id = ps.function_id AND prv_left.boundary_id = p.partition_number - 1
WHERE OBJECT_NAME(p.object_id) = 'fact_table'
AND i.index_id = 0
GO
