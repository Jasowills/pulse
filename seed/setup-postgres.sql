-- Setup: PostgreSQL
-- The seed tool / test server create the `orders` table and Pulse creates its
-- `pulse._changes` log and triggers on the first subscribe. This script only needs
-- the database (and role) to exist.

-- Idempotently create the app database. Run as a superuser (default `postgres`).
SELECT 'CREATE DATABASE pulse'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'pulse')\gexec

-- If you are not the `postgres` superuser, grant what the app needs:
--   GRANT CREATE, USAGE ON SCHEMA public TO postgres;

-- Verification (from the seed tool this is automatic):
--   dotnet run --project seed/Pulse.TestApp.Seed -- verify-setup postgres