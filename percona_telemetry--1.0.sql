-- percona_telemetry--1.0.sql

CREATE FUNCTION percona_telemetry_version()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION percona_telemetry_status(
    OUT last_file_processed     timestamptz,
    OUT waiting_on_agent        boolean)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;
