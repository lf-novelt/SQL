DECLARE @SCD TABLE
(	ID	INT	IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
	DimValue char(1),
	EffectiveDate	date
);

INSERT INTO @SCD (DimValue, EffectiveDate)
VALUES	('A', CONVERT(date, dateadd(day, -12, GETDATE()))),
		('B', CONVERT(date, dateadd(day, -9, GETDATE()))),
		('C', CONVERT(date, dateadd(day, -5, GETDATE()))),
		('D', CONVERT(date, dateadd(day, -2, GETDATE())));

SELECT * FROM @SCD;

WITH CTE_Dates AS
(
	SELECT CONVERT(date, dateadd(day, -10, GETDATE())) AS RecordDate
	UNION ALL
	SELECT CONVERT(date, dateadd(day, 1, RecordDate)) AS RecordDate
	FROM
		CTE_Dates
	WHERE
		RecordDate < GETDATE()
), CTE_SCD AS
(
	SELECT
		s.ID,
		s.DimValue,
		s.EffectiveDate AS EffectiveFrom,
		(SELECT MIN(EffectiveDate) FROM @SCD d WHERE d.EffectiveDate > s.EffectiveDate) AS EffectiveTo
		-- In this case I could have used the ID but it isn't always the case (as in CDC)
	FROM
		@SCD s
)
SELECT DISTINCT
	s.ID,
	s.DimValue,
	d.RecordDate
FROM
	CTE_SCD s
INNER JOIN
	CTE_Dates d
ON
	d.RecordDate >= s.EffectiveFrom
AND
	d.RecordDate < ISNULL(s.EffectiveTo, CONVERT(date, getdate()))
ORDER BY
	d.RecordDate;
