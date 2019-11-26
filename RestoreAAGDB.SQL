:setvar IsSqlCmdEnabled "True"
:setvar PrimaryServer "xxxxx"
:setvar SecondaryServer "xxxxx"
:setvar BackupServer "xxxxx"

:setvar DatabaseName "xxxxx"
:setvar FromBackupList "FROM DISK = ''\\$(BackupServer)\D$\Backup01\Bak\$(DatabaseName)_bak_a.bak'',DISK = ''\\$(BackupServer)\D$\Backup02\Bak\$(DatabaseName)_bak_b.bak'',DISK = ''\\$(BackupServer)\D$\Backup03\Bak\$(DatabaseName)_bak_c.bak''"

:setvar RestorePrimary 1
:setvar RestoreSecondary 0

GO

:Connect $(PrimaryServer)
EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Begin';

if N'$(IsSqlCmdEnabled)' not like N'True'
    begin
        EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Error', N'SQLCMD mode must be enabled to successfully execute this script. - Disabling script execution.';
        set noexec on;
    end
go

EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Info', 'Removing Secondary database from AAG'
go

:Connect $(SecondaryServer)
use master
GO
BEGIN TRY
	if exists(select *
	from sys.dm_hadr_database_replica_states
	where db_name(database_id)='$(DatabaseName)' and is_local=1 and is_primary_replica = CASE WHEN @@SERVERNAME = '$(PrimaryServer)' THEN 1 ELSE 0 END)
		ALTER DATABASE [$(DatabaseName)] SET HADR OFF;
END TRY
BEGIN CATCH
	DECLARE @ErrorMessage VARCHAR(MAX) = error_message()
	EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Error', @ErrorMessage
	RAISERROR(@ErrorMessage, 16, 1)
	:on error exit
END CATCH
GO



:Connect $(PrimaryServer)
USE [master]
GO
EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Info', 'Removing Primary database from AAG group'
go
if exists(select *
from sys.dm_hadr_database_replica_states
where db_name(database_id)='$(DatabaseName)' and is_local=1 AND is_primary_replica = CASE WHEN @@SERVERNAME = '$(PrimaryServer)' THEN 1 ELSE 0 END)
BEGIN
	DECLARE @SQL varchar(MAX)
	BEGIN TRY
		SELECT
			@SQL = 'ALTER AVAILABILITY GROUP [' + ag_name + '] REMOVE DATABASE [$(DatabaseName)];'
		FROM
			sys.databases d
		INNER JOIN
			sys.dm_hadr_database_replica_states rs
		ON
			d.database_id = rs.database_id
		INNER JOIN
			sys.dm_hadr_name_id_map m
		ON
			m.ag_id = rs.group_id
		WHERE
			d.name = '$(DatabaseName)'
		AND
			rs.is_local = 1
		AND
			rs.is_primary_replica = CASE WHEN @@SERVERNAME = '$(PrimaryServer)' THEN 1 ELSE 0 END

		EXEC(@SQL)
	END TRY
	BEGIN CATCH
		DECLARE @ErrorMessage VARCHAR(MAX) = error_message()
		EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Remove Availability $(DatabaseName)', 'Error', @ErrorMessage
		RAISERROR(@ErrorMessage, 16, 1)
		:on error exit
	END CATCH
END
GO

:Connect $(SecondaryServer)
use master
GO
DECLARE @SQL varchar(MAX)
IF $(RestoreSecondary) = 1
BEGIN

--	BEGIN TRY
		SELECT @SQL = ISNULL(@SQL, '') + 'MOVE ''' + name + ''' TO ''' + physical_name + ''',
		'
		FROM sys.master_files
		WHERE database_id = db_id('$(DatabaseName)')
		ORDER BY File_id
/*
		SET @SQL = '
		BEGIN TRY
			if exists(select 1 from sys.databases where name=''$(DatabaseName)'' and state_desc=''online'')
				ALTER DATABASE $(DatabaseName) SET OFFLINE with rollback immediate

			RESTORE DATABASE [$(DatabaseName)] 
			$(FromBackupList)
			WITH  FILE = 1, 
			' + @SQL
			+	' replace,stats=5,norecovery
		END TRY
		BEGIN CATCH
			DECLARE @ErrorMessage VARCHAR(MAX) = error_message()
			EXEC Common.dbo.MessageLog ''$(DatabaseName)'', ''Restore $(DatabaseName)'', ''Error'', @ErrorMessage
			RAISERROR(@ErrorMessage, 16, 1)
		END CATCH'
*/
		SET @SQL = '
			if exists(select 1 from sys.databases where name=''$(DatabaseName)'' and state_desc=''online'')
				ALTER DATABASE $(DatabaseName) SET OFFLINE with rollback immediate

			RESTORE DATABASE [$(DatabaseName)] 
			$(FromBackupList)
			WITH  FILE = 1, 
			' + @SQL
			+	' replace,stats=5,norecovery'

		EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Info', 'Restoring  database on $(SecondaryServer)'
		SELECT @SQL AS $(SecondaryServer)
		EXEC (@SQL)

--	END TRY
--	BEGIN CATCH
--		DECLARE @ErrorMessage VARCHAR(MAX) = error_message()
--		EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Error', @ErrorMessage
--		RAISERROR(@ErrorMessage, 16, 1)
--		:on error exit
--	END CATCH
END
GO

:Connect $(PrimaryServer)
USE [master]
GO
EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Info', 'Disable job ''Backup Log $(DatabaseName)'''
update msdb..sysjobs set enabled=0 where name='Backup Log $(DatabaseName)'
GO

IF $(RestorePrimary) = 1
BEGIN

	DECLARE @SQL varchar(MAX)
--	BEGIN TRY
		SELECT @SQL = ISNULL(@SQL, '') + 'MOVE ''' + name + ''' TO ''' + physical_name + ''',
		'
		FROM sys.master_files
		WHERE database_id = db_id('$(DatabaseName)')
		ORDER BY File_id
/*
		SET @SQL = '
		BEGIN TRY
			if exists(select 1 from sys.databases where name=''$(DatabaseName)'' and state_desc=''online'')
				ALTER DATABASE $(DatabaseName) SET OFFLINE with rollback immediate

			RESTORE DATABASE [$(DatabaseName)] 
			$(FromBackupList)
			WITH  FILE = 1, 
			' + @SQL
			+	' replace,stats=5
		END TRY
		BEGIN CATCH
			DECLARE @ErrorMessage VARCHAR(MAX) = error_message()
			EXEC Common.dbo.MessageLog ''$(DatabaseName)'', ''Restore $(DatabaseName)'', ''Error'', @ErrorMessage
			RAISERROR(@ErrorMessage, 16, 1)
		END CATCH'
*/
		SET @SQL = '
			if exists(select 1 from sys.databases where name=''$(DatabaseName)'' and state_desc=''online'')
				ALTER DATABASE $(DatabaseName) SET OFFLINE with rollback immediate

			RESTORE DATABASE [$(DatabaseName)] 
			$(FromBackupList)
			WITH  FILE = 1, 
			' + @SQL
			+	' replace,stats=5'

		EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Info', 'Restoring database on $(PrimaryServer)'
		SELECT @SQL AS $(PrimaryServer)
		EXEC(@SQL)
	--END TRY
	--BEGIN CATCH
	--	DECLARE @ErrorMessage VARCHAR(MAX) = error_message()
	--	EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Error', @ErrorMessage
	--	RAISERROR(@ErrorMessage, 16, 1)
	--	:on error exit
	--END CATCH
END
GO
--- YOU MUST EXECUTE THE FOLLOWING SCRIPT IN SQLCMD MODE.
:Connect $(PrimaryServer)

USE [master]

GO
EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Info', 'Adding primary database into AAG group'
go

--BEGIN TRY
	DECLARE @SQL varchar(MAX)

	SELECT
		@SQL = 'ALTER AVAILABILITY GROUP [' + ag.name + '] ADD DATABASE [$(DatabaseName)];'
	FROM
		master.sys.availability_groups as ag
	INNER JOIN
		[master].[sys].[dm_hadr_availability_group_states] ags
	ON
		ag.group_id = ags.group_id
	WHERE
		ags.primary_replica = '$(PrimaryServer)'

	EXEC(@SQL)
--END TRY
--BEGIN CATCH
--	DECLARE @ErrorMessage VARCHAR(MAX) = error_message()
--	EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Error', @ErrorMessage
--	RAISERROR(@ErrorMessage, 16, 1)
--	:on error exit
--END CATCH
GO

EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Info', 'Adding secondary database into AAG group'
GO
:Connect $(SecondaryServer)

-- Wait for the replica to start communicating
begin try
declare @conn bit
declare @count int
declare @replica_id uniqueidentifier 
declare @group_id uniqueidentifier
set @conn = 0
set @count = 30 -- wait for 5 minutes 

if (serverproperty('IsHadrEnabled') = 1)
	and (isnull((select member_state from master.sys.dm_hadr_cluster_members where upper(member_name COLLATE Latin1_General_CI_AS) = upper(cast(serverproperty('ComputerNamePhysicalNetBIOS') as nvarchar(256)) COLLATE Latin1_General_CI_AS)), 0) <> 0)
	and (isnull((select state from master.sys.database_mirroring_endpoints), 1) = 0)
begin
--    select @group_id = ags.group_id
--	from
--		master.sys.availability_groups as ags
--		where name = N'$(AvailabilityGroup)'

	SELECT
		@group_id = ags.group_id
	FROM
		[master].[sys].[dm_hadr_availability_group_states] ags
	WHERE
		ags.primary_replica = '$(PrimaryServer)'

	select @replica_id = replicas.replica_id from master.sys.availability_replicas as replicas where upper(replicas.replica_server_name COLLATE Latin1_General_CI_AS) = upper(@@SERVERNAME COLLATE Latin1_General_CI_AS) and group_id = @group_id
	while @conn <> 1 and @count > 0
	begin
		set @conn = isnull((select connected_state from master.sys.dm_hadr_availability_replica_states as states where states.replica_id = @replica_id), 1)
		if @conn = 1
		begin
			-- exit loop when the replica is connected, or if the query cannot find the replica status
			break
		end
		waitfor delay '00:00:10'
		set @count = @count - 1
	end
end
end try
begin catch
	-- If the wait loop fails, do not stop execution of the alter database statement
end catch

DECLARE @SQL varchar(MAX)
--BEGIN TRY
	SELECT @SQL = 'ALTER DATABASE [$(DatabaseName)] SET HADR AVAILABILITY GROUP = [' + ag.name + '];'
	FROM
		master.sys.availability_groups as ag
	INNER JOIN
		[master].[sys].[dm_hadr_availability_group_states] ags
	ON
		ag.group_id = ags.group_id
	WHERE
		ags.primary_replica = '$(PrimaryServer)'

	SELECT @SQL
	EXEC(@SQL)
--END TRY
--BEGIN CATCH
--	DECLARE @ErrorMessage VARCHAR(MAX) = error_message()
--	EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Error', @ErrorMessage
--	RAISERROR(@ErrorMessage, 16, 1)
--	:on error exit
--END CATCH
GO

:Connect $(PrimaryServer)

USE [master]

GO
EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Info', N'Enable job ''Backup Log $(DatabaseName)''';

update msdb..sysjobs set enabled=1 where name='Backup Log $(DatabaseName)'
go

EXEC Common.dbo.MessageLog '$(DatabaseName)', 'Restore $(DatabaseName)', 'Complete';


set noexec off;

-- There is insufficient free space on disk volume 'D:\Data03\' to create the database. The database requires 62277025792 additional free bytes, while only 30656299008 bytes are available.
/*
exec xp_cmdshell 'dir d:\Log03\ /os'

exec xp_cmdshell 'del d:\Data03\AtlasMaster_AtlasMaster_Data2.mdf'

SELECT * FROM sys.sysfiles
*/
/*
		if exists(select 1 from sys.databases where name='LPSMetrics' and state_desc='online')
			ALTER DATABASE LPSMetrics SET OFFLINE with rollback immediate

		RESTORE DATABASE [LPSMetrics] 
		FROM DISK = '\\CHCXSQLAAGMA001\D$\Backup01\Bak\LPSMetrics_bak_a.bak',DISK = '\\CHCXSQLAAGMA001\D$\Backup02\Bak\LPSMetrics_bak_b.bak',DISK = '\\CHCXSQLAAGMA001\D$\Backup03\Bak\LPSMetrics_bak_c.bak'
		WITH  FILE = 1, 
		MOVE 'LPSMetrics' TO 'D:\Data01\LPSMetrics_data1.ndf',
		MOVE 'LPSMetrics_log' TO 'D:\LOG01\LPSMetrics_log.ldf',
		MOVE 'LPSMetrics_2' TO 'D:\Data02\LPSMetrics_data2.ndf',
		MOVE 'LPSMetrics_3' TO 'D:\Data03\LPSMetrics_data3.ndf',
		MOVE 'LPSMetrics_4' TO 'D:\Data04\LPSMetrics_data4.ndf',
		MOVE 'LPSMetrics_5' TO 'D:\Data05\LPSMetrics_data5.ndf',
		MOVE 'LPSMetrics_6' TO 'D:\Data06\LPSMetrics_data6.ndf',
		MOVE 'LPSMetrics_log2' TO 'D:\LOG02\LPSMetrics_log2.ldf',
		MOVE 'LPSMetrics_log3' TO 'D:\LOG02\LPSMetrics_log3.ldf',
		 replace,stats=5,norecovery
*/

/*
			if exists(select 1 from sys.databases where name='GPCMaster' and state_desc='online')
				ALTER DATABASE GPCMaster SET OFFLINE with rollback immediate

			RESTORE DATABASE [GPCMaster] 
			FROM DISK = '\\chcxsqlaagma001\d$\Backup01\Bak\GPCMaster_bak_a.bak', DISK = '\\chcxsqlaagma001\d$\Backup02\Bak\GPCMaster_bak_b.bak', DISK = '\\chcxsqlaagma001\d$\Backup03\Bak\GPCMaster_bak_c.bak'
			WITH  FILE = 1, 
			MOVE 'GPCMaster_data1' TO 'D:\Data06\GPCMaster_data1.mdf',
		MOVE 'GPCMaster_log' TO 'D:\Log01\GPCMaster_log.ldf',
		MOVE 'GPCMaster_data2' TO 'D:\Data04\GPCMaster_data2.ndf',
		MOVE 'GPCMaster_log2' TO 'D:\Log02\GPCMaster_log2.ldf',
		MOVE 'GPCMaster_Data06' TO 'D:\Data06\GPCMaster_Data06.ndf',
		MOVE 'GPCMaster_Data05' TO 'D:\Data05\GPCMaster_Data05.ndf',
		MOVE 'GPCMaster_Data04' TO 'D:\Data04\GPCMaster_Data04.ndf',
		MOVE 'GPCMaster_Data03' TO 'D:\Data03\GPCMaster_Data03.ndf',
		 replace,stats=5
*/

/*

			if exists(select 1 from sys.databases where name='ExpedientApp' and state_desc='online')
				ALTER DATABASE ExpedientApp SET OFFLINE with rollback immediate

			RESTORE DATABASE [ExpedientApp] 
			FROM DISK = '\\chcxsqlaagma002\d$\Backup01\Bak\ExpedientApp.bak'
			WITH  FILE = 1, 
			MOVE 'ExpedientApp' TO 'D:\DATA02\ExpedientApp.mdf',
		MOVE 'ExpedientApp _log' TO 'D:\Log01\ExpedientApp_1.ldf',
		MOVE 'ExpedientApp_Data02' TO 'D:\Data02\ExpedientApp_Data02.ndf',
		MOVE 'ExpedientApp_Data03' TO 'D:\Data02\ExpedientApp_Data03.ndf',
		MOVE 'ExpedientApp_Data04' TO 'D:\Data02\ExpedientApp_Data04.ndf',
		MOVE 'ExpedientApp_Data05' TO 'D:\Data02\ExpedientApp_Data05.ndf',
		MOVE 'ExpedientApp_Data06' TO 'D:\Data02\ExpedientApp_Data06.ndf',
		 replace,stats=5
*/
