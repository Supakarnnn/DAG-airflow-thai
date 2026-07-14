\connect warehouse

\getenv llm_password LLM_DB_PASSWORD
\getenv app_password APP_DB_PASSWORD
CREATE ROLE llm_readonly WITH LOGIN PASSWORD :'llm_password';
CREATE ROLE app_readonly WITH LOGIN PASSWORD :'app_password';
GRANT CONNECT ON DATABASE warehouse TO llm_readonly;
GRANT CONNECT ON DATABASE warehouse TO app_readonly;

SET ROLE warehouse;
GRANT USAGE ON SCHEMA gold TO llm_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO llm_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE warehouse IN SCHEMA gold GRANT SELECT ON TABLES TO llm_readonly;
GRANT USAGE ON SCHEMA gold TO app_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO app_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE warehouse IN SCHEMA gold GRANT SELECT ON TABLES TO app_readonly;
RESET ROLE;

ALTER ROLE llm_readonly SET default_transaction_read_only = on;
ALTER ROLE llm_readonly SET statement_timeout = '10s';
ALTER ROLE app_readonly SET default_transaction_read_only = on;
ALTER ROLE app_readonly SET statement_timeout = '10s';
