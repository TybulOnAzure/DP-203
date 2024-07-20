CREATE SCHEMA Security

-- Create predicate
CREATE FUNCTION Security.securityPredicate(@MinifigType AS VARCHAR(255))  
    RETURNS TABLE  
WITH SCHEMABINDING  
AS  
    RETURN SELECT 1 AS securityPredicateResult
    WHERE 
        @MinifigType IN ('Droid', 'Toy') AND USER_NAME() = 'Teacher' 
        OR USER_NAME() = 'DBO'
GO

-- Create policy
CREATE SECURITY POLICY MinifigsFilter  
ADD FILTER PREDICATE Security.securityPredicate(MinifigType)
ON Rebrickable.Minifigs
WITH (STATE = ON);

-- Execute as me
SELECT MinifigType, COUNT(*) FROM Rebrickable.Minifigs GROUP BY MinifigType

-- Execute as Teacher
EXECUTE AS USER = 'Teacher'
SELECT MinifigType, COUNT(*) FROM Rebrickable.Minifigs GROUP BY MinifigType