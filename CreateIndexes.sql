DECLARE
	@TableName varchar(255) = 'dbo.HotelFact',
	@CreatePK bit = 1,
	@CreateColumnStore bit = 1,
	@CreateOther bit = 1

SET NOCOUNT ON

DECLARE @ObjectID int = object_id(@TableName)
DECLARE @StagingObjectID int = object_id(@TableName + 'Staging')
DECLARE @IndexID int, @IndexName sysname
DECLARE @Indexes TABLE (indexid int, indexname varchar(255))
DECLARE @ColumnNames varchar(MAX), @IncludedColumnNames varchar(MAX)
DECLARE @IsClustered bit = 0, @IsColumnStore bit = 0, @IsPrimaryKey bit = 0, @IsUnique bit = 0
DECLARE @SQL varchar(MAX)

DECLARE @SQLCommands TABLE (ID int NOT NULL IDENTITY(1,1), SQLCmd varchar(MAX)) 

IF @StagingObjectID IS NOT NULL
BEGIN

	DECLARE csrIndexes CURSOR FORWARD_ONLY READ_ONLY
	FOR
		SELECT DISTINCT
			index_id
		FROM
			sys.indexes
		WHERE
			object_id = @ObjectID
		AND
		(	(@CreatePK = 1 AND is_primary_key = 1)
			OR
			(@CreateColumnStore = 1 AND type_desc LIKE '% COLUMNSTORE')
			OR
			(@CreateOther = 1 AND is_primary_key = 0 AND type_desc NOT LIKE '% COLUMNSTORE')
		)
		ORDER BY
			index_id

	OPEN csrIndexes

	FETCH NEXT FROM csrIndexes INTO @IndexID

	WHILE @@FETCH_STATUS = 0
	BEGIN

		SET @ColumnNames = NULL
		SET @IncludedColumnNames = NULL

		SELECT
			@ColumnNames = CASE WHEN is_included_column = 0 THEN ISNULL(@ColumnNames + ',', '') + c.name ELSE @ColumnNames END,
			@IncludedColumnNames = CASE WHEN is_included_column = 1 THEN ISNULL(@IncludedColumnNames + ',', '') + c.name ELSE @IncludedColumnNames END
		from
			sys.index_columns ic
		inner join
			sys.columns c
		on
			c.object_id = ic.object_id
		AND
			c.column_id = ic.column_id
		WHERE
			ic.object_id = @ObjectID
		AND
			ic.index_id = @IndexID

		SELECT
			@IndexName = LTRIM(RTRIM(name)),
			@IsClustered = CASE WHEN type_desc LIKE 'CLUSTERED%' THEN 1 ELSE 0 END,
			@IsColumnStore = CASE WHEN type_desc LIKE '% COLUMNSTORE' THEN 1 ELSE 0 END,
			@IsPrimaryKey = is_primary_key,
			@IsUnique = is_unique
		FROM
			sys.indexes
		WHERE
			object_id = @ObjectID
		AND
			index_id = @IndexID

		IF @IsPrimaryKey = 1
		BEGIN
			SET @SQL = 'IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = ''' + @IndexName + 'Staging'') ' +
			' ALTER TABLE ' + @TableName + 'Staging ' +
			' ADD CONSTRAINT ' + @IndexName + 'Staging PRIMARY KEY CLUSTERED (' + @ColumnNames + ')'

		END
		ELSE IF (@IsColumnStore = 1) AND (@CreateColumnStore = 1)
		BEGIN
			SET @SQL = 'IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = ''' + @IndexName + ''' ' +
			' AND object_id = ' + CONVERT(VARCHAR(10), ISNULL(@StagingObjectID, 0)) + ') ' +
			'CREATE ' +
			CASE @IsClustered   WHEN 1 THEN 'CLUSTERED '   ELSE 'NONCLUSTERED ' END +
			'COLUMNSTORE INDEX ' + @IndexName + ' ON ' + @TableName + 'Staging (' + @IncludedColumnNames + ')'
		END
		ELSE IF @CreateOther = 1
		BEGIN

			SET @SQL = 'IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = ''' + @IndexName + ''' ' +
			' AND object_id = ' + CONVERT(VARCHAR(10), @StagingObjectID) + ') ' +
			'CREATE ' +
			CASE @IsClustered   WHEN 1 THEN 'CLUSTERED '   ELSE 'NONCLUSTERED ' END +
			' INDEX ' + @IndexName + ' ON ' + @TableName + 'Staging (' + @ColumnNames + ')'  +
			ISNULL(' INCLUDE (' + @IncludedColumnNames + ')', '')
		
		END

		INSERT INTO @SQLCommands (SQLCmd) VALUES (@SQL)
		

		FETCH NEXT FROM csrIndexes INTO @IndexID
	END

	CLOSE csrIndexes
	DEALLOCATE csrIndexes

	IF (SELECT COUNT(*) FROM @SQLCommands) <> 0
	BEGIN
		DECLARE csrSQLCommands CURSOR FORWARD_ONLY READ_ONLY
		FOR
			SELECT
				SQLCmd
			FROM
				@SQLCommands
			WHERE
				SQLCmd IS NOT NULL
			ORDER BY
				ID

		OPEN csrSQLCommands

		FETCH NEXT FROM csrSQLCommands INTO @SQL

		WHILE @@FETCH_STATUS = 0
		BEGIN
			EXEC(@SQL)

			FETCH NEXT FROM csrSQLCommands INTO @SQL
		END

		CLOSE csrSQLCommands
		DEALLOCATE csrSQLCommands
	END

END