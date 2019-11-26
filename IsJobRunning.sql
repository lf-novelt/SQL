/*	This is just a SQL snippet to determine if a job is running
	It is an example of how OpenRowSet can be used to turn the results of a stored proc into a table
	Replace the xxxxxxx with a job name
*/

--IF NOT EXISTS
(select * 
from openrowset('SQLOLEDB',
                'Trusted_Connection=yes; Initial Catalog=Local',
                'SET FMTONLY OFF; exec msdb.dbo.sp_help_job ')
WHERE current_execution_status = 1
--AND name = 'xxxxxxx'
)
