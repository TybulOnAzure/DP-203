CREATE SCHEMA Rebrickable;

GO
CREATE TABLE Rebrickable.Minifigs
(
    SetNumber               VARCHAR(50)     NOT NULL UNIQUE NOT ENFORCED,
    LastModifiedDatetime    DATETIME2(3)    NOT NULL,
    Name                    VARCHAR(255)    NOT NULL,
    NumberOfParts           SMALLINT        NOT NULL,
    MinifigType             VARCHAR(50)     NULL
)
WITH
(
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = HASH(SetNumber)  
) 

GO

SELECT * FROM Rebrickable.Minifigs

GO

CREATE TABLE Rebrickable.MinifigsRoundRobin
WITH
(
    DISTRIBUTION = ROUND_ROBIN  
)
AS SELECT * FROM Rebrickable.Minifigs

