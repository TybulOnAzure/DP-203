CREATE DATABASE SCOPED CREDENTIAL ADLSg2Credential
WITH IDENTITY = 'MANAGED IDENTITY'
GO


CREATE EXTERNAL DATA SOURCE curatedLayer
WITH
  ( LOCATION = 'abfss://curated@tybuldatalakedemo.dfs.core.windows.net' ,
    CREDENTIAL = ADLSg2Credential ,
    TYPE = HADOOP -- required for CSV
  ) ;
GO

CREATE EXTERNAL FILE FORMAT MinifigsCSV
WITH (  
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"'
    )
);

CREATE EXTERNAL TABLE Rebrickable.Minifigs_external
(
    LastModifiedDatetimeOriginal    VARCHAR(255),
    LastModifiedDate                VARCHAR(255),
    LastModifiedDatetime            VARCHAR(255),
    Name                            VARCHAR(255),
    MinifigType                     VARCHAR(255),
    NumberOfParts                   VARCHAR(255),
    ImageURL                        VARCHAR(255),
    SetNumber                       VARCHAR(255),
    SetURL                          VARCHAR(255) 
)  
WITH (
    LOCATION = '/Rebrickable/Minifigs_CSV/myData.csv',
    DATA_SOURCE = curatedLayer,  
    FILE_FORMAT = MinifigsCSV
)
GO


SELECT * FROM Rebrickable.Minifigs_external

CREATE EXTERNAL FILE FORMAT MinifigsParquet
WITH (  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

DROP EXTERNAL TABLE Rebrickable.Minifigs_external

CREATE EXTERNAL TABLE Rebrickable.Minifigs_external
(
    LastModifiedDatetimeOriginal    VARCHAR(255),
    LastModifiedDate                DATE,
    LastModifiedDatetime            DATETIME2(3),
    Name                            VARCHAR(255),
    MinifigType                     VARCHAR(255),
    NumberOfParts                   SMALLINT,
    ImageURL                        VARCHAR(255),
    SetNumber                       VARCHAR(255),
    SetURL                          VARCHAR(255) 
)  
WITH (
    LOCATION = '/Rebrickable/Minifigs_Parquet/myData.snappy.parquet',
    DATA_SOURCE = curatedLayer,  
    FILE_FORMAT = MinifigsParquet
)
GO

SELECT * FROM Rebrickable.Minifigs_external

UPDATE
  Rebrickable.Minifigs_external
SET 
  Name = 'TEST'
WHERE
  Name = 'Martian, Arcturus'



CREATE TABLE Rebrickable.Minifigs_real
WITH
(
  DISTRIBUTION = ROUND_ROBIN
)
AS
SELECT
  *
FROM
  Rebrickable.Minifigs_external


SELECT * FROM Rebrickable.Minifigs_real

UPDATE
  Rebrickable.Minifigs_real
SET 
  Name = 'TEST'
WHERE
  SetNumber = 'fig-008420'






