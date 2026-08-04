-- Setup: SQL Server
-- The `orders` table is created by the seed tool / test server. Pulse enables
-- database + table change tracking on the first subscribe (this requires
-- ALTER DATABASE / ALTER TABLE permission), so nothing here is strictly needed
-- beyond the database itself. This script is idempotent.

IF DB_ID(N'pulse') IS NULL
    CREATE DATABASE pulse;
GO

-- Optional: pre-enable change tracking so the first subscribe never has to.
--   USE pulse;
--   ALTER DATABASE pulse SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS);
--   ALTER TABLE dbo.orders ENABLE CHANGE_TRACKING;

-- Verification (from the seed tool this is automatic):
--   dotnet run --project seed/Pulse.TestApp.Seed -- verify-setup sqlserver