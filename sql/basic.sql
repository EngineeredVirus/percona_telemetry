--
-- basic sanity
--

CREATE EXTENSION percona_telemetry;

SELECT name FROM pg_settings WHERE name LIKE 'percona_telemetry.%';
SELECT percona_telemetry_version();
SELECT regexp_replace(output_file_name, '\d{6,}', '<INSTANCE ID>', 'g') AS output_file_name,
        (last_file_processed IS NOT NULL) AS last_file_processed,
        waiting_on_agent FROM percona_telemetry_status();

DROP EXTENSION percona_telemetry;
