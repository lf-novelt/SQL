/****************************************************************************************************************************************************
	Title : sp_who3

	Description:
		This gives a more detailled version of sp_who2 which also shows the code being run and can be used to identify blockages.
		If @SPID is set to a non NULL value then it will only look for that SPID or anything the SPID is blocking
		If @UserLogin is set to a non NULL value then it will only look for that login
		If @ShowBlocksOnly is set to a non NULL value then it will only show processes which are blocking or being blocked
		If @DBName is set to a non NULL value then it will only look for that database
		There is code to display the LockObject but it is commented out at the moment 

	Change History:
		Date		Author          Version	Description
		----------	--------------- -------	------------------------------------
		2011-??-??	Chris Faulkner	1.00	Created
sp_who2 115
backup log Expedient with truncate_only

KILL 347

238036118
****************************************************************************************************************************************************/
-- 100747359 kill 85
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

DECLARE @SPID INT = NULL,
	@ShowBlocksOnly bit = 0,
	@UserLogin varchar(255) = NULL,
	@DBName varchar(255) = NULL,
	@CommandType varchar(255) = NULL, --'RESTORE DATABASE', -- 'KILLED'
	@Status varchar(255) = NULL, --'runnable'
	@UserOnly bit = 0

--SET @UserLogin = '%lsallai%' --'SEA\cfaulkner'
--SET @UserLogin = '%cfaulkner'

/*
SELECT
	DB_Name(sp.database_id) AS DBName,
	sp.login_name AS Login,
	sp.status AS Status,
	sp.host_name AS Host,
	is_user_process,
	COUNT(DISTINCT sp.session_id) AS Connections
FROM
	sys.dm_exec_sessions sp WITH (NOLOCK)
WHERE
	sp.session_id <> @@SPID
GROUP BY
	DB_Name(sp.database_id),
	sp.login_name,
	sp.status,
	sp.host_name,
	is_user_process
ORDER BY
	DB_Name(sp.database_id),
	sp.login_name,
	sp.status,
	sp.host_name
*/


SELECT DISTINCT
	ses.session_id AS SPID,
	ses.login_time,
	DB_Name(ses.database_id) AS DBName,
	ses.status AS Status,
	ses.login_name AS Login,
	ses.host_name AS Host,
	wt.blocking_session_id AS BlockBy,
--	COALESCE(wt.blocking_Session_ID, td.Session_ID, td.request_session_id) AS WaitingFor,
--	wt.blocking_Session_ID AS WaitingFor, --, td.Session_ID, td.request_session_id) AS WaitingFor,
--	td.LockObjectType,
--	td.LockObject,
--	td.request_owner_type AS LockType,
--	td.BlockingSessionID,
	ses.open_transaction_count AS open_tran,
	der.command AS CommandType,
	--der.command,
	der.status,
	der.percent_complete,
	OBJECT_SCHEMA_NAME(qt.objectid, qt.dbid) + '.' + OBJECT_NAME(qt.objectid, qt.dbid) AS ObjectName,
	--CASE WHEN sp.status IN ('running', 'rollback', 'runnable') OR sp.CMD = 'KILLED/ROLLBACK ' THEN CONVERT(time(0), getdate() - Last_batch) ELSE NULL END AS ElapsedTime,
	der.logical_reads + der.reads  AS IOReads,
	der.writes  AS IOWrites,
	ses.row_count,
	ses.cpu_time AS CPUTime,
	wt.wait_type,
	wt.wait_duration_ms,
	wt.resource_description AS WaitResource,
	ses.lock_timeout,
	ses.deadlock_priority,
	der.start_time AS StartTime,
	--con.net_transport AS Protocol,
	CASE ses.transaction_isolation_level
		WHEN 0 THEN 'Unspecified' 
		WHEN 1 THEN 'Read Uncommitted' 
		WHEN 2 THEN 'Read Committed' 
		WHEN 3 THEN 'Repeatable' 
		WHEN 4 THEN 'Serializable' 
		WHEN 5 THEN 'Snapshot' 
	END AS transaction_isolation,
	--con.num_writes AS ConnectionWrites,
 --   con.num_reads AS ConnectionReads,
	--con.client_net_address AS ClientAddress,
 --   con.auth_scheme AS Authentication,
    CASE WHEN ses.program_name LIKE 'SQLAgent - TSQL JobStep (Job % : Step%' THEN
		(	SELECT
				'Job : ' + ISNULL(j.name, 'NULL') + ' (Step ' + CONVERT(VARCHAR(10), js.step_id) + ' - ' + ISNULL(js.step_name, 'NULL') + ')'
			FROM
				msdb.dbo.sysjobs j
			INNER JOIN
				msdb.dbo.sysjobsteps js
			ON
				j.job_id = js.job_id
			WHERE
				j.job_id = CONVERT(uniqueidentifier, CONVERT(varbinary(MAX), CONVERT(varchar(255), LEFT(REPLACE(ses.program_name, 'SQLAgent - TSQL JobStep (Job ', ''), CHARINDEX(':', REPLACE(ses.program_name, 'SQLAgent - TSQL JobStep (Job ', ''))-1)), 1))
			AND
				js.step_id = CONVERT(int, REPLACE(SUBSTRING(ses.program_name, CHARINDEX(': Step ', ses.program_name) + 7, 1000), ')', ''))
		)
	ELSE
		ses.program_name
	END AS ProgramName,
	CASE   
		WHEN der.[statement_start_offset] > 0 THEN  
			--The start of the active command is not at the beginning of the full command text 
			CASE der.[statement_end_offset]  
				WHEN -1 THEN  SUBSTRING(qt.TEXT, (der.[statement_start_offset]/2) + 1, 2147483647)
              --The end of the full command is also the end of the active statement 
					 
           ELSE   
              --The end of the active statement is not at the end of the full command 
              SUBSTRING(qt.TEXT, (der.[statement_start_offset]/2) + 1, (der.[statement_end_offset] - der.[statement_start_offset])/2)   
        END
     ELSE  
        --1st part of full command is running 
        CASE der.[statement_end_offset]  
           WHEN -1 THEN  
              --The end of the full command is also the end of the active statement 
              RTRIM(LTRIM(qt.[text]))  
           ELSE  
              --The end of the active statement is not at the end of the full command 
              LEFT(qt.TEXT, (der.[statement_end_offset]/2) +1)  
        END  
     END AS [executing statement],
	qt.Text AS [full statement],
	ses.session_id
	
	/*
	SUBSTRING (qt.text, sp.stmt_start/2, (	CASE
												WHEN sp.stmt_end = -1 THEN
													LEN(CONVERT(nvarchar(MAX), qt.text)) * 2  
												ELSE
													sp.stmt_end
											END - sp.stmt_start)/2) AS SQLStatement
	*/
FROM
	sys.dm_exec_sessions ses
LEFT JOIN
	sys.dm_exec_requests as der WITH (NOLOCK)
ON 
	ses.session_id = der.session_id
AND
	der.command LIKE ISNULL('%' + @CommandType + '%', der.command)
LEFT JOIN
	sys.dm_os_waiting_tasks as wt WITH (NOLOCK)
ON
	wt.Session_ID = ses.session_id
LEFT JOIN
	sys.dm_exec_requests as block_der WITH (NOLOCK)
ON
	wt.blocking_task_address = block_der.task_address
OUTER APPLY
	sys.dm_exec_sql_text(der.sql_handle) as qt
/*
LEFT JOIN
(	SELECT
		wt.session_id,
		tlo.request_session_id,
		resource_type + ISNULL('-' + NULLIF(resource_subtype, ''), '')  AS LockObjectType,
		CASE resource_type
			WHEN 'OBJECT' THEN
				DB_NAME(resource_database_id) + '..' + object_name(resource_associated_entity_id, resource_database_id)
			WHEN 'DATABASE' THEN
				DB_NAME(resource_database_id)
			ELSE
				NULL
		END AS LockObject,
--		NULL AS LockObject,
		tlo.request_owner_type,
		wt.blocking_session_id AS BlockingSessionID
	FROM
		sys.dm_tran_locks tlo WITH (NOLOCK)
	LEFT JOIN
		sys.dm_os_waiting_tasks as wt WITH (NOLOCK)
	ON
		tlo.lock_owner_address = wt.resource_address
	AND
		wt.Session_id = tlo.request_session_id
	WHERE
		tlo.resource_type IN ('OBJECT', 'DATABASE')
) td
ON
	td.request_session_id = sp.SPID
*/
WHERE
(	ses.session_id = ISNULL(@SPID, ses.session_id)
	OR
--	sp.blocked = ISNULL(@SPID, sp.blocked)
--	OR
	wt.blocking_Session_ID = ISNULL(@SPID, wt.blocking_Session_ID)
)
AND
	ses.login_name LIKE ISNULL('%' + @UserLogin + '%', ses.login_name)
AND
	ses.Status LIKE ISNULL('%' + @Status + '%', ses.Status)
--AND
--	LTRIM(RTRIM(ses.host_name)) <> '.'
AND
	ses.session_id <> @@SPID
AND
	DB_Name(ses.database_id) LIKE ISNULL(@DBName, DB_Name(ses.database_id))
AND
(	@UserOnly = 0
	OR
	ses.is_user_process = 1
)
ORDER BY 
--	sp.blocked DESC,
	wt.blocking_Session_ID DESC,
	--CASE WHEN COALESCE(wt.blocking_Session_ID, td.Session_ID, td.request_session_id) = sp.SPID THEN 
	--		COALESCE(wt.blocking_Session_ID, td.Session_ID, td.request_session_id) / 10.0
	--	ELSE COALESCE(wt.blocking_Session_ID, td.Session_ID, td.request_session_id)
	--END DESC,
--	sp.open_tran desc,
--	sp.loginame,
	ses.session_id

/*
dbcc traceon (3604)
go
dbcc page (18, 951674438, 19)
TAB: 18:951674438:19


TAB: 12:1313295263:7
SELECT * FROM sys.objects where object_id = 1313295263

dbcc page (12, 90886024, 17)
TAB: 12:90886024:19

12:3:5379440
dbcc page (12,3,5379440)

dbcc page (2,3,258816)

SELECT * FROM sys.databases where db_id = 2

select * from sysdatabases

select * from sys.objects where object_id = 1257770467

select * from sysfiles
select * from sysobjects

kill 59

TAB: 10:869693921:0 [COMPILE]
dbcc table (10,0,869693921)
select * from sysobjects where id = 869693921

select * from sysdatabases

select * from sysdatabases
where dbid = 15

select * from ExpedientStaging.sys.objects (NOLOCK)
where id = 951674438

KEY: 27:281474978938880 (324ccfdd4801)
KEY: 12:281474978938880 (948ab4f2aba3)

SELECT o.name, i.name 
FROM sys.partitions p 
JOIN sys.objects o ON p.object_id = o.object_id 
JOIN sys.indexes i ON p.object_id = i.object_id 
AND p.index_id = i.index_id 
WHERE p.hobt_id = 281474978938880


select * from information_schema.tables

select * from sys.sysobjects

select * from syscolumns
where name like '%object%'

select object_name(99)

*/	




/*
DECLARE @SPID INT = NULL

SELECT 
	ses.session_id AS SPID,
	ses.status AS Status,
	ses.login_name AS Login,
	ses.host_name AS Host,
	sp.blocked AS BlkBy,
	DB_Name(er.database_id) AS DBName,
	er.command AS CommandType,
	OBJECT_SCHEMA_NAME(qt.objectid, qt.dbid) + '.' + OBJECT_NAME(qt.objectid, qt.dbid) AS ObjectName,
	CONVERT(time(0), getdate() - Last_request_start_time) AS ElapsedTime,
	er.logical_reads + er.reads  AS IOReads,
	er.writes  AS IOWrites,
	er.cpu_time AS CPUTime,
	er.last_wait_type AS LastWaitType,
	er.start_time AS StartTime,
	con.net_transport AS Protocol,
	CASE ses.transaction_isolation_level
		WHEN 0 THEN 'Unspecified' 
		WHEN 1 THEN 'Read Uncommitted' 
		WHEN 2 THEN 'Read Committed' 
		WHEN 3 THEN 'Repeatable' 
		WHEN 4 THEN 'Serializable' 
		WHEN 5 THEN 'Snapshot' 
	END AS transaction_isolation,
	con.num_writes AS ConnectionWrites,
    con.num_reads AS ConnectionReads,
	con.client_net_address AS ClientAddress,
    con.auth_scheme AS Authentication,
    ses.program_name,
	SUBSTRING (qt.text, er.statement_start_offset/2, (CASE
														WHEN er.statement_end_offset = -1 THEN
															LEN(CONVERT(nvarchar(MAX), qt.text)) * 2  
														ELSE
															er.statement_end_offset
													  END - er.statement_start_offset)/2) AS SQLStatement
FROM
	sys.sysprocesses sp
INNER JOIN
	sys.dm_exec_sessions ses
ON
	sp.spid = ses.session_id
LEFT JOIN
	sys.dm_exec_requests er  
ON
	ses.session_id = er.session_id  
LEFT JOIN
	sys.dm_exec_connections con  
ON
	con.session_id = ses.session_id
OUTER APPLY
	sys.dm_exec_sql_text(sp.sql_handle) as qt  
WHERE
	sp.spid = ISNULL(@SPID, sp.spid)
AND
	sp.spid > 50  
AND
	sp.spid <> @@SPID
ORDER BY 
	sp.Blocked DESC,
	sp.spid
*/
/*
IOReads	IOWrites	CPUTime
1026310819	1907	14019796

KILL 70
*/
/*
SELECT hostname, COUNT(DISTINCT spid)
FROM sys.sysprocesses
GROUP BY hostname
ORDER BY hostname
*/
/*
sp_configure 'clr enabled', 1
 go
 RECONFIGURE
 go
 sp_configure 'clr enabled'
 go
*/
--exec master..csp_KillDBUsers 'GPCMaster'
/*
SELECT total_logical_reads, total_logical_writes,
total_physical_reads, total_worker_time, 
total_elapsed_time, sys.dm_exec_sql_text.TEXT
FROM sys.dm_exec_query_stats 
CROSS APPLY sys.dm_exec_sql_text(plan_handle) 
WHERE total_logical_reads <> 0
AND total_logical_writes <> 0
AND sys.dm_exec_sql_text.TEXT LIKE '%aacostarodriguez%'
ORDER BY (total_logical_reads + total_logical_writes) DESC
GO
*/
/*
objectlock lockPartition=22 objid=1534016596 subresource=FULL dbid=18 id=lock54c5e151a00 mode=X associatedObjectId=1534016596

select object_name(1534016596, 18)

CASE WHEN LEFT(WaitReso
*/
