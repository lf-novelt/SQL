/****************************************************************************************************************************************************
	Title : Unused indexes create drop statement

	Description:
		This finds the indexes which are unused in the current database and generates a DROP INDEX statement for them as well as other stats.

	Change History:
		Date		Author          Version	Description
		----------	--------------- -------	------------------------------------
		2011-??-??	Chris Faulkner	1.00	Created

****************************************************************************************************************************************************/
DECLARE @schemaname varchar(10) = NULL,
	@TableName varchar(255) = NULL

SELECT
	schema_name(o.schema_id) + '.' + o.name AS TableName,
	i.name AS IndexName,
	i.index_id AS IndexID,
	s.user_seeks + s.user_scans + s.user_lookups AS Reads,
	s.user_seeks,
	s.user_scans,
	s.user_lookups,
	s.user_updates AS Writes,
	p.Rows,
	s.last_user_seek,
	s.last_user_scan,
	CASE
		WHEN s.user_updates < 1 THEN 100
		ELSE 1.00 * (s.user_seeks + s.user_scans + s.user_lookups) / s.user_updates
	END AS Reads_per_Write
FROM
	sys.dm_db_index_usage_stats s
INNER JOIN
	sys.indexes i
ON
	i.index_id = s.index_id
AND
	s.object_id = i.object_id 
INNER JOIN
	sys.objects o
ON
	s.object_id = o.object_id
AND
	schema_name(o.schema_id) = ISNULL(@SchemaName, schema_name(o.schema_id))
AND
	o.name = ISNULL(@TableName, o.name)
INNER JOIN
	sys.schemas c
ON
	o.schema_id = c.schema_id 
INNER JOIN
(	SELECT
		p.index_id,
		p.object_id,
		SUM(p.rows) as Rows
	FROM
		sys.partitions p
	GROUP BY
		p.index_id,
		p.object_id
) p
ON
	p.index_id = s.index_id
AND
	s.object_id = p.object_id
WHERE
	OBJECTPROPERTY(s.object_id,'IsUserTable') = 1
AND
	s.database_id = DB_ID() 
AND
	i.type_desc = 'nonclustered'
AND
	i.is_primary_key = 0
AND
	i.is_unique_constraint = 0
--AND
--	p.Rows > 10000
--AND
--(	
--	(s.user_seeks + s.user_scans + s.user_lookups = 0)
--	OR
--	(
--		((s.last_user_seek IS NOT NULL) AND (datediff(day, s.last_user_seek, getdate()) > 30))
--		AND
--		((s.last_user_scan IS NOT NULL) AND (datediff(day, s.last_user_scan, getdate()) > 30))
--	)
--)
ORDER BY
	--TableName,
	s.user_seeks + s.user_scans + s.user_lookups DESC,
	s.last_user_seek DESC,
	s.last_user_scan DESC,
	Reads_per_Write,
	IndexName

