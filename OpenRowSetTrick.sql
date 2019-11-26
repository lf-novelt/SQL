SELECT  who.loginame AS LoginName,
        who.HostName, 
        DB_NAME(locks.dbid) AS DatabaseName, 
		who.SPID,
        locks.Type
FROM    OPENROWSET ('SQLOLEDB','Server=(local);TRUSTED_CONNECTION=YES;','set fmtonly off exec master.dbo.sp_who')
AS  who
JOIN    OPENROWSET ('SQLOLEDB','Server=(local);TRUSTED_CONNECTION=YES;','set fmtonly off exec master.dbo.sp_lock')
AS  locks
ON  who.spid = locks.spid
--EXEC(@SQL) AT .......

SELECT  who.loginame AS LoginName,
        who.HostName, 
        DB_NAME(locks.dbid) AS DatabaseName, 
		who.SPID,
        locks.Type
FROM    OPENROWSET ('SQLCLI','Server=(local);TRUSTED_CONNECTION=YES;','set fmtonly off exec master.dbo.sp_who')
AS  who
JOIN    OPENROWSET ('SQLCLI','Server=(local);TRUSTED_CONNECTION=YES;','set fmtonly off exec master.dbo.sp_lock')
AS  locks
ON  who.spid = locks.spid
--EXEC(@SQL) AT .......