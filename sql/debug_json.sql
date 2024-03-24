--
-- json debugging
--

CREATE OR REPLACE FUNCTION read_json_file()
RETURNS text
AS $$
DECLARE
    file_path text;
    system_id text;
    pg_version text;
    file_content text;
BEGIN
    SELECT output_file_name INTO file_path FROM percona_telemetry_status();
    SELECT system_identifier::TEXT INTO system_id FROM pg_control_system();
    SELECT version() INTO pg_version;

    -- file_content := to_json('{' || pg_read_file(file_path) || '}');
    file_content := '{' || pg_read_file(file_path) || '}';

    RETURN file_content;
END;
$$ LANGUAGE plpgsql;

-- Let's sleep for a few seconds to ensure that leader has
-- generated the json file.
SELECT pg_sleep(3);

CREATE EXTENSION percona_telemetry;

SELECT 'db_instance_id' AS col FROM pg_control_system() WHERE '"' || system_identifier || '"' = (SELECT cast(read_json_file()::JSON->'db_instance_id' AS VARCHAR));
SELECT 'pillar_version' AS col WHERE '"' || current_setting('server_version') || '"' = (SELECT cast(read_json_file()::JSON->'pillar_version' AS VARCHAR));
SELECT 'databases_count' AS col FROM pg_database WHERE datallowconn = true HAVING '"' || count(*) || '"' = (SELECT cast(read_json_file()::JSON->'databases_count' AS VARCHAR));
SELECT 'settings' AS col FROM pg_settings WHERE name NOT LIKE 'plpgsql.%' HAVING count(*) = (SELECT json_array_length(read_json_file()::JSON->'settings'));

DROP EXTENSION percona_telemetry;
DROP FUNCTION read_json_file;