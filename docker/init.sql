BEGIN;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'sweri') THEN
        CREATE DATABASE sweri;
    END IF;
END $$;
\c sweri
CREATE SCHEMA sweri;
CREATE EXTENSION postgis;
GRANT ALL PRIVILEGES ON DATABASE sweri TO sweri;
COMMIT;