CREATE USER [tybuladf] FROM EXTERNAL PROVIDER;
GO

EXEC sp_addrolemember db_owner, [tybuladf];
GRANT ADMINISTER DATABASE BULK OPERATIONS to [tybuladf]

-- For Databricks
CREATE USER [DatabricksServicePrincipal] FROM EXTERNAL PROVIDER;
GO
EXEC sp_addrolemember db_owner, [DatabricksServicePrincipal];
GRANT ADMINISTER DATABASE BULK OPERATIONS to [DatabricksServicePrincipal]
