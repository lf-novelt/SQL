SET NOCOUNT ON

DECLARE @TableName varchar(100), @WhereClause varchar(8000), @IDColumn bit

SET @TableName = 'PortfolioDetails'
SET @WhereClause = 'PortfolioID = 10719321'

DECLARE @InsertString varchar(8000), @LastCol int


IF EXISTS(SELECT 1 FROM	syscolumns c INNER JOIN sysobjects o ON c.id = o.id WHERE o.name = @TableName AND c.ColStat = 1)
	SET @IDColumn = 1
ELSE
	SET @IDColumn = 0

IF @IDColumn = 1
	SELECT @InsertString = 'SELECT ''SET IDENTITY_INSERT ' + @TableName + ' ON ' + CHAR(13) + CHAR(10) + 'INSERT INTO ' + @TableName + ' ('
ELSE
	SELECT @InsertString = 'SELECT ''INSERT INTO ' + @TableName + ' (' 


SELECT @LastCol = Max(c.ColID)
FROM	syscolumns c
INNER JOIN sysobjects o
ON c.id = o.id
WHERE o.name = @TableName


SELECT @InsertString = @InsertString + c.name + Case c.ColId
		WHEN @LastCol THEN ')'
		ELSE ', '
	End
FROM	syscolumns c
INNER JOIN sysobjects o
ON c.id = o.id
WHERE o.name = @TableName
ORDER BY c.ColId


SELECT @InsertString = @InsertString + ' VALUES ('' + ' 

SELECT @InsertString = @InsertString +
	CASE 
		WHEN t.Name IN ('char', 'varchar', 'nchar', 'nvarchar') THEN 'IsNull(' +
			CASE
				WHEN c.Length < 2 THEN 'convert(varchar(4), '
				ELSE ''
			END +
			''''''''' + ' + c.name + ' + ''''''''' + 
			CASE

				WHEN c.Length < 2 THEN ')'
				ELSE ''
			END + ', ''NULL'')'
		WHEN t.name IN ('binary', 'varbinary') THEN
			'''convert(' + t.name + '(' + convert(varchar(5), c.Length) + '), '''''' + convert(varchar(255), ' + c.name + ') + '''''')'''
		WHEN t.name IN ('datetime', 'smalldatetime') THEN
			'IsNull('''''''' + convert(varchar(20), ' + c.name + ') + '''''''', ''NULL'')'
		ELSE 'IsNull(convert(varchar(50), '+ c.name + '), ''NULL'')'
	End
	+ Case c.ColId
		WHEN @LastCol THEN '+ '')'''
		ELSE '+ '', '' + '
	End
FROM	syscolumns c
INNER JOIN systypes t
ON	c.xtype = t.xtype
INNER JOIN sysobjects o
ON	c.id = o.id
WHERE o.name = @TableName
ORDER By ColID


IF @IDColumn = 1
	SELECT @InsertString = @InsertString + ' + CHAR(13) + CHAR(10) + ''SET IDENTITY_INSERT ' + @TableName + ' OFF '''

SET @InsertString = @InsertString + ' FROM ' + @TableName + IsNull(' WHERE ' + @WhereClause, '')

EXEC(@InsertString)

SET NOCOUNT OFF

