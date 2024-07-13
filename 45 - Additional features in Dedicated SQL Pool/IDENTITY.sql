DROP TABLE Rebrickable.MinifigsWithID

-- Create a dummy table
CREATE TABLE Rebrickable.MinifigsWithID
(    
    [ID]                        INT             IDENTITY(1,1) NOT NULL,
    [SetNumber]                 [varchar](50)   NOT NULL,
	[LastModifiedDatetime]      [datetime2](3)  NOT NULL,
	[Name]                      [varchar](255)  NOT NULL,
	[NumberOfParts]             [smallint]      NOT NULL,
	[MinifigType]               [varchar](50)   NULL    
)
WITH
(   
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)

-- Populate it with data
INSERT INTO Rebrickable.MinifigsWithID
(
    [SetNumber],
	[LastModifiedDatetime],
	[Name],
	[NumberOfParts],
	[MinifigType] 
)
SELECT
    [SetNumber],
	[LastModifiedDatetime],
	[Name],
	[NumberOfParts],
	[MinifigType]
FROM
    Rebrickable.Minifigs;


-- View the data
SELECT * FROM Rebrickable.MinifigsWithID ORDER BY ID ASC