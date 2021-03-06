/************************************************************************************************************************************

	xxxxx SCHEMABINDING removal script

	If the deployment goes past xxxxx then the schemabinding has been added to all of the functions
	This will cause the next run of the deployment to fail so this script removes it all so that the
	deployment can be re-run

************************************************************************************************************************************/

	SELECT
		OBJECT_SCHEMA_NAME(id) + '.' + OBJECT_NAME(id)
	FROM
		sysobjects
	WHERE
		OBJECT_DEFINITION(id) LIKE '%WITH SCHEMABINDING%';

	IF object_id('tempdb..#idList') IS NOT NULL
		DROP TABLE #idList;

    DECLARE @PositionShemaBinding INT;
    DECLARE @Command NVARCHAR(MAX);
    DECLARE @ObjectName VARCHAR(MAX);
	DECLARE @ObjectType VARCHAR(MAX);
	DECLARE @UserMessage VARCHAR(MAX);

	WITH CTE_deplist AS
	(
		SELECT
			1 AS DepLevel,
			o.id AS ObjectID,
			CASE o.xtype
				WHEN 'V' THEN 'VIEW'
				WHEN 'P' THEN 'PROCEDURE'
				WHEN 'FN' THEN 'FUNCTION'
				WHEN 'IF' THEN 'FUNCTION'
				WHEN 'TF' THEN 'FUNCTION'
				ELSE NULL
			END AS ObjectType
		FROM
			sysobjects o
		LEFT JOIN
			sysdepends d
		ON
			d.depid = o.id
		WHERE
			d.depid IS NULL
		AND
			OBJECT_DEFINITION(o.id) LIKE '%WITH SCHEMABINDING%'
		UNION ALL
		SELECT
			DepLevel + 1 AS DepLevel,
			o.id AS ObjectID,
			CASE o.xtype
				WHEN 'V' THEN 'VIEW'
				WHEN 'P' THEN 'PROCEDURE'
				WHEN 'FN' THEN 'FUNCTION'
				WHEN 'IF' THEN 'FUNCTION'
				WHEN 'TF' THEN 'FUNCTION'
				ELSE NULL
			END AS ObjectType
		FROM
			CTE_deplist dl
		INNER JOIN
			sysdepends d
		ON
			d.id = dl.ObjectID
		INNER JOIN
			sysobjects o
		ON
			d.depid = o.id
		WHERE
			OBJECT_DEFINITION(o.id) LIKE '%WITH SCHEMABINDING%'
		AND
			d.depnumber = 0
	), CTE_ListFinal AS
	(	SELECT
			MAX(DepLevel) AS DepLevel,
			ObjectID,
			ObjectType
		FROM
			CTE_deplist
		WHERE
			ObjectType IS NOT NULL
		GROUP BY
			ObjectID,
			ObjectType
	)
	SELECT
		ROW_NUMBER() OVER (ORDER BY DepLevel, ObjectID, ObjectType) AS RowNumber,
		DepLevel,
		ObjectID,
		ObjectType
	INTO
		#idList
	FROM
		CTE_ListFinal
	ORDER BY
		DepLevel,
		ObjectID;

	DECLARE csrIDList cursor LOCAL FORWARD_ONLY READ_ONLY for
	SELECT
		OBJECT_DEFINITION(ObjectID),
		ObjectType,
		'[' + object_schema_name(ObjectID) + '].[' + object_name(ObjectID) + ']' AS ObjectName
	FROM
		#idList
	ORDER BY
		RowNumber;

	OPEN csrIDList;

	FETCH NEXT FROM csrIDList
	INTO @Command, @ObjectType, @ObjectName;

	
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @UserMessage = format(getdate(), 'yyyy-MM-dd HH:mm:ss.fff : ') + 'Changing ' + @ObjectName
		RAISERROR(@UserMessage, 0, 0) WITH NOWAIT

        -- WITH SCHEMA BINDING IS NOT PRESENT... Let's add it !
		SET @Command = REPLACE(@Command, 'CREATE ' + @ObjectType, 'ALTER ' + @ObjectType);

		SET @Command = REPLACE(@Command, 'WITH SCHEMABINDING', '');

		--RAISERROR(@Command, 0, 0) WITH NOWAIT;
		EXECUTE sp_executesql @Command;

		FETCH NEXT FROM csrIDList
		INTO @Command, @ObjectType, @ObjectName;
	END

	CLOSE csrIDList;
	DEALLOCATE csrIDList;

	SELECT
		OBJECT_SCHEMA_NAME(id) + '.' + OBJECT_NAME(id)
	FROM
		sysobjects
	WHERE
		OBJECT_DEFINITION(id) LIKE '%WITH SCHEMABINDING%';
