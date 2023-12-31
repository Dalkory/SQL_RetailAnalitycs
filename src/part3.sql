CREATE ROLE Administrator WITH LOGIN PASSWORD '142581';
CREATE ROLE Visitor;

-- edit it manually:
GRANT ALL PRIVILEGES ON DATABASE new_database TO Administrator;

-- IF YOU WANT TO ISSUE ROOT THEN ENTER THIS  :  ALTER ROLE Administrator WITH SUPERUSER;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO Administrator; 
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO Administrator; 
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO Administrator;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO Administrator; 

-- edit it manually:
GRANT CONNECT ON DATABASE new_database TO Visitor;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO Visitor;

-- DROP ROLE

REASSIGN OWNED BY Administrator TO postgres;
DROP OWNED BY Administrator;
DROP ROLE Administrator;

REASSIGN OWNED BY Visitor TO postgres;
DROP OWNED BY Visitor;
DROP ROLE Visitor;

-- CHECK ROLE;

SET ROLE Visitor;
SELECT CURRENT_USER;
