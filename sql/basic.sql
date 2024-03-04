--
-- basic sanity
--

CREATE EXTENSION percona_telemetry;

SELECT name FROM pg_settings WHERE name LIKE 'percona_telemetry.%';

SELECT percona_telemetry_version();
SELECT waiting_on_agent FROM percona_telemetry_status();

DROP EXTENSION percona_telemetry;