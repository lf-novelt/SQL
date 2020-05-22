USE AdventureWorks2012
GO
DECLARE @PageSize INT = 20
DECLARE @PageNumber INT = 0
DECLARE @RowCount INT = @PageSize
DECLARE @LastPage INT

SELECT COUNT(DISTINCT p.Name)
FROM
	Production.Product p
INNER JOIN [Production].[WorkOrder] w
	ON p.ProductID = w.ProductID
WHERE StartDate BETWEEN '2012-12-01' AND '2013-12-01'


--SELECT @LastPage = CEILING(CONVERT(float, (SELECT COUNT(DISTINCT Name) FROM Production.Product) / @PageSize))
	DROP TABLE IF EXISTS #ProductList

	CREATE TABLE #ProductList
	(	ProductName varchar(100) NOT NULL PRIMARY KEY CLUSTERED,
		ProductID INT NOT NULL
	)

	INSERT INTO #ProductList
	SELECT DISTINCT
	 p.Name, w.ProductID
	FROM
		Production.Product p
	INNER JOIN
		[Production].[WorkOrder] w
	ON p.ProductID = w.ProductID
	WHERE StartDate BETWEEN '2012-12-01' AND '2013-12-01'
	ORDER BY p.Name
	
	;WITH CTE_PageData AS
	(
		SELECT
			p.ProductID, 
			p.Name AS ProductName,
			MIN(w.StartDate) AS MinStartDate,
			MAX(w.StartDate) AS MaxStartDate,
			SUM(w.OrderQty) AS OrderQty,
			SUM(p.ListPrice * w.OrderQty) AS OrderValue
		FROM
			#ProductList pl
		INNER JOIN
			[Production].[WorkOrder] w
		ON pl.ProductID = w.ProductID
		INNER JOIN
			Production.Product p
		ON p.ProductID = w.ProductID
		WHERE StartDate BETWEEN '2012-12-01' AND '2013-12-01'
		GROUP BY p.ProductID, p.Name
		ORDER BY ProductName
		OFFSET (@PageSize * @PageNumber) rows fetch next @PageSize rows only
	)
	SELECT
		@PageNumber AS PageNumber,
		ProductID,
		ProductName,
		MinStartDate,
		MaxStartDate,
		OrderQty,
		OrderValue
	FROM
		CTE_PageData
	UNION
	SELECT
		@PageNumber AS PageNumber,
		NULL AS ProductID,
		'TOTAL' AS ProductName,
		MIN(MinStartDate) AS MinStartDate,
		MAX(MaxStartDate) AS MaxStartDate,
		SUM(OrderQty) AS OrderQty,
		SUM(OrderValue) AS OrderValue
	from CTE_PageData
