\connect warehouse

CREATE ROLE llm_readonly WITH LOGIN PASSWORD 'llm_readonly';
GRANT CONNECT ON DATABASE warehouse TO llm_readonly;

SET ROLE warehouse;
GRANT USAGE ON SCHEMA gold TO llm_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO llm_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE warehouse IN SCHEMA gold GRANT SELECT ON TABLES TO llm_readonly;
RESET ROLE;
