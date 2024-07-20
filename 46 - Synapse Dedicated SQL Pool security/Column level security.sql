-- On master DB
CREATE LOGIN Student WITH PASSWORD = 'MySecretPa$$1'
CREATE LOGIN Teacher WITH PASSWORD = 'MySecretPa$$1'

-- On DWHTest DB
CREATE USER Student FROM LOGIN Student
CREATE USER Teacher FROM LOGIN Teacher

SELECT * FROM Rebrickable.Minifigs

-- Grant permissions
GRANT SELECT ON Rebrickable.Minifigs(SetNumber, Name) TO Student
GRANT SELECT ON Rebrickable.Minifigs TO Teacher

EXECUTE AS USER = 'Student'
SELECT * FROM Rebrickable.Minifigs

EXECUTE AS USER = 'Student'
SELECT USER_NAME() AS 'User', SetNumber, Name FROM Rebrickable.Minifigs

EXECUTE AS USER = 'Teacher'
SELECT USER_NAME() AS 'User',* FROM Rebrickable.Minifigs

