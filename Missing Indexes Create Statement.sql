/****************************************************************************************************************************************************
	Title : Missing Indexes Create Statement

	Description:
		This finds the indexes which are missing in the current database and generates a CREATE INDEX statement for them as well as other stats.

	Change History:
		Date		Author          Version	Description
		----------	--------------- -------	------------------------------------
		2011-??-??	Chris Faulkner	1.00	Created

****************************************************************************************************************************************************/

;WITH CTE_Top500 AS
(
	SELECT TOP (500)
		group_handle
	FROM
		sys.dm_db_missing_index_group_stats WITH (nolock)
	ORDER BY
		(avg_total_user_cost * avg_user_impact) * (user_seeks + user_scans) DESC
)
SELECT
	so.SchemaName + '.' + so.ObjectName AS TableName,
	(migs.avg_total_user_cost * migs.avg_user_impact) * (migs.user_seeks + migs.user_scans) AS Impact,
	(migs.avg_user_impact) * (migs.user_seeks + migs.user_scans) AS AvgImpact,
--	migs.avg_total_user_cost,
	migs.avg_user_impact AS AvgPercentBenefit,
	migs.user_seeks,
	migs.user_scans,
	migs.last_user_seek,
	migs.last_user_scan,
	mid.equality_columns,
	mid.inequality_columns,
	mid.included_columns,
	'CREATE NONCLUSTERED INDEX ix_' + ISNULL(NULLIF(so.SchemaName, 'dbo') + '_', '') + so.ObjectName
		+ ISNULL('_' + REPLACE(REPLACE(REPLACE(mid.equality_columns, '[', ''), ']', ''), ', ', '_'), '')
		+ ISNULL('_' + REPLACE(REPLACE(REPLACE(mid.inequality_columns, '[', ''), ']', ''), ',', '_'), '')
		+ ' ON ' + so.SchemaName + '.' + so.ObjectName COLLATE DATABASE_DEFAULT + ' ( ' +
			IsNull(mid.equality_columns, '') +
			CASE
				WHEN mid.inequality_columns IS NULL THEN ''
				ELSE CASE WHEN mid.equality_columns IS NULL THEN '' ELSE ',' END + mid.inequality_columns
			END + ' ) ' +
			CASE
				WHEN mid.included_columns IS NULL THEN ''
				ELSE 'INCLUDE (' + mid.included_columns + ')'
			END + ';' AS CreateIndexStatement
 	FROM
		sys.dm_db_missing_index_group_stats AS migs
	INNER JOIN
		sys.dm_db_missing_index_groups AS mig
	ON
		migs.group_handle = mig.index_group_handle 
	INNER JOIN
		sys.dm_db_missing_index_details AS mid
	ON
		mig.index_handle = mid.index_handle AND mid.database_id = DB_ID() 
	INNER JOIN
	(	SELECT
			object_id,
			Name AS ObjectName,
			schema_name(schema_id) AS SchemaName
		FROM
			sys.objects  AS so WITH (nolock)
		WHERE
			OBJECTPROPERTY(OBJECT_ID, 'isusertable') = 1
	) so
	ON
		mid.OBJECT_ID = so.OBJECT_ID
	INNER JOIN
		CTE_Top500 ct500
	ON
		ct500.group_handle = migs.group_handle
ORDER BY 1, 2 DESC, 3 DESC, 6, 7, 8
