/*
SELECT * FROM OPENQUERY (LDAP,
--'home-server;steve\MyUserID;MyPassword',
'SELECT    CN
FROM         ''LDAP://DC=SEA, DC=CORP, DC=EXPECN, DC=com''
WHERE     objectClass = ''USER''')
*/
SELECT * FROM OPENROWSET ('ADsDSOObject',
'bel-shost-01;;TRUSTED_CONNECTION=YES',
'SELECT displayName, distinguishedName, company, manager,objectSID, extensionAttribute4
FROM         ''LDAP://DC=SEA, DC=CORP, DC=EXPECN, DC=com''
WHERE     objectClass = ''USER'' AND objectCategory=''Person'''
)



/*
EXEC sp_configure 'show advanced options', 1
RECONFIGURE

EXEC sp_configure 'Ad Hoc Distributed Queries', 1
RECONFIGURE WITH OVERRIDE
*/