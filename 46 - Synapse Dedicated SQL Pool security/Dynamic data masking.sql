-- Review our data
SELECT * FROM Rebrickable.Minifigs

-- Create default mask on LastModifiedDatetime
ALTER TABLE Rebrickable.Minifigs
ALTER COLUMN LastModifiedDatetime ADD MASKED WITH (FUNCTION = 'default()');

-- Create random mask on NumberOfParts
ALTER TABLE Rebrickable.Minifigs
ALTER COLUMN NumberOfParts ADD MASKED WITH (FUNCTION = 'random(1, 100)');

-- I can see original data
SELECT * FROM Rebrickable.Minifigs

-- But not Teacher
EXECUTE AS USER = 'Teacher'
SELECT * FROM Rebrickable.Minifigs

-- Note that NumberOfParts changes every time
EXECUTE AS USER = 'Teacher'
SELECT * FROM Rebrickable.Minifigs WHERE SetNumber = 'fig-010440' ORDER BY NumberOfParts DESC

-- Guessing the data is possible
EXECUTE AS USER = 'Teacher'
SELECT * FROM Rebrickable.Minifigs WHERE NumberOfParts = 18

-- Cleanup
ALTER TABLE Rebrickable.Minifigs ALTER COLUMN NumberOfParts DROP MASKED
ALTER TABLE Rebrickable.Minifigs ALTER COLUMN LastModifiedDatetime DROP MASKED

DROP SECURITY POLICY MinifigsFilter 
DROP FUNCTION Security.securityPredicate
DROP SCHEMA Security

DROP USER Student
DROP USER Teacher
-- master DB
DROP LOGIN Student
DROP LOGIN Teacher