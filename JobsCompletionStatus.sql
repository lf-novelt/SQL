/****************************************************************************************************************************************************
	Title : Job COmpletion Status

	Description:
		Gives a list of runs for a job per day showing total runs, failures, successes, retries and cancellations.
		The @JobName doesn't need to be exact as it is joined with a LIKE statement
		Needs READ permission on msdb.dbo.sysjobs and msdb.dbo.sysjobhistory 
	
	Change History:
		Date		Author          Version	Description
		----------	--------------- -------	------------------------------------
		2013-??-??	Chris Faulkner	1.00	Created

****************************************************************************************************************************************************/
USE msdb
GO
DECLARE @JobName varchar(255) = NULL

SELECT DISTINCT
	name AS Job_Name,
	Run_Date,
	COUNT(*) AS TotalRuns,
	SUM(CASE WHEN run_status = 0 THEN 1 ELSE 0 END) AS Failed,
	SUM(CASE WHEN run_status = 1 THEN 1 ELSE 0 END) AS Succeeded,
	SUM(CASE WHEN run_status = 2 THEN 1 ELSE 0 END) AS Retries,
	SUM(CASE WHEN run_status = 3 THEN 1 ELSE 0 END) AS Cancelled
FROM
	dbo.sysjobs j
INNER JOIN
(	SELECT
		jh.job_id,
		CONVERT(date, CONVERT(VARCHAR(10), run_date), 112) AS Run_Date,
		CONVERT(datetime, LEFT(RIGHT('000000' + CONVERT(VARCHAR(10), run_time), 6), 2) + ':' + SUBSTRING(RIGHT('000000' + CONVERT(VARCHAR(10), run_time), 6), 3, 2) + ':' + RIGHT(RIGHT('000000' + CONVERT(VARCHAR(10), run_time), 6), 2), 108) AS start_time,
		CONVERT(datetime, LEFT(RIGHT('000000' + CONVERT(VARCHAR(10), run_duration), 6), 2) + ':' + SUBSTRING(RIGHT('000000' + CONVERT(VARCHAR(10), run_duration), 6), 3, 2) + ':' + RIGHT(RIGHT('000000' + CONVERT(VARCHAR(10), run_duration), 6), 2), 108) AS Duration,
		jh.run_status
	FROM
		dbo.sysjobhistory jh
	WHERE
		Step_ID = 0
) jh
ON
	jh.job_id = j.job_id
WHERE
	datediff(day, Run_Date, getdate()) <= 30
AND
	j.name LIKE '%' + ISNULL(@JobName, '') + '%'
GROUP BY
	j.name,
	Run_Date
ORDER BY
	j.Name,
	Run_Date
