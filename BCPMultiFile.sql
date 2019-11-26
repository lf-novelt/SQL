/* BCPs an output to many files */

USE CJFTest

IF object_id('CJFTemp') IS NOT NULL
	DROP TABLE CJFTemp

;WITH CTE_Numbers AS
(	SELECT
		1 AS Number
	UNION ALL
	SELECT
		Number + 1 AS Number
	FROM
		CTE_Numbers
	WHERE
		Number < 100
)
SELECT Number, Number * Number AS NumberSquared
INTO CJFTemp
FROM CTE_Numbers

EXEC xp_cmdshell 'del d:\TempDB01\CJFFile*.*'

EXEC xp_cmdshell 'dir d:\TempDB01'

DECLARE
	@FirstRow int = 1,
	@BatchSize int = 10,
	@TableSize int,
	@strFirstRow varchar(10),
	@strLastRow varchar(10),
	@Command varchar(8000),
	@OutputFileName varchar(20)

SELECT	@TableSize = COUNT(*)
FROM	CJFTemp

--SET @Command = 'bcp CJFTemp out D:\TempDB01\CJFFile' + @strFirstRow + ' -F ' + @strFirstRow + ' -L ' + @strLastRow + ' -T'

WHILE @FirstRow <= @TableSize
BEGIN

	SET @strLastRow = CONVERT(VARCHAR(10), @FirstRow + @BatchSize - 1)
	SET @strFirstRow = CONVERT(VARCHAR(10), @FirstRow)
	SET @Command = 'bcp CJFTest.dbo.CJFTemp out D:\TempDB01\CJFFile' + @strFirstRow + '.CSV -c -t, -F ' + @strFirstRow + ' -L ' + @strLastRow + ' -T'

	SELECT @Command

	EXEC xp_cmdshell @Command

	SET @FirstRow = @FirstRow + @BatchSize

	SELECT @FirstRow, @TableSize
END

EXEC xp_cmdshell 'dir d:\TempDB01'

--EXEC xp_cmdshell 'bcp CJFTest.dbo.CJFTemp out D:\TempDB01\CJFFile1.CSV -c -F 1 -L 10 -T'