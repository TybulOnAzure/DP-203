SELECT * FROM Rebrickable.Minifigs_real
GO


TRUNCATE TABLE Rebrickable.Minifigs_real
GO

COPY INTO [Rebrickable].[Minifigs_real]
FROM 'https://tybuldatalakedemo.dfs.core.windows.net/curated/Rebrickable/Minifigs_Parquet/myData.snappy.parquet'
WITH
(
    CREDENTIAL = (IDENTITY = 'Managed Identity'),
    FILE_TYPE = 'PARQUET'
)