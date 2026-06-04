-- sql/init.sql
-- This file runs automatically when the postgres-dw container first starts.
-- It creates the schemas so Spark JDBC writes don't fail on missing schema.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS marts;

-- Grant full access to the olist user on all schemas
GRANT ALL PRIVILEGES ON SCHEMA bronze TO olist;
GRANT ALL PRIVILEGES ON SCHEMA silver TO olist;
GRANT ALL PRIVILEGES ON SCHEMA gold   TO olist;
GRANT ALL PRIVILEGES ON SCHEMA marts  TO olist;
