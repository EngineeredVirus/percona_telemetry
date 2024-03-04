# Percona Telemetry Extension for PostgreSQL

Welcome to `percona_telemetry` - extension for Percona telemetry data collection for PostgreSQL!

## Overview

The extenions it be provided with the Percona distribution. The extension, at no point in time, collects, maintains, or sends any user personal/identifiable/sensitive data.

### Configuration

You can find the configuration parameters of the `percona_telemetry` extension in the `pg_settings` view by filtering `name` column with `percona_telemetry` prefix. To change the default configuration, specify new values for the desired parameters using the GUC (Grand Unified Configuration) system. Alternatively, values may be changed by a superuser either at the server start time or via `ALTER SYSTEM` command followed `SELECT pg_reload_conf();` statement.

#### Parameters

1. `percona_telemetry.pg_telemetry_folder`

  * Default: /usr/local/percona/telemetry/pg

2. `percona_telemetry.scrape_interval`

  * Default: 86,400 (24 hours)
  * Unit: s
  * It is configured to wakeup the leader process every 24 hours by default. The leader
    may be woken up by a configuration reload interrupt or when the shutdown process is
    initiated.

3. `percona_telemetry.enabled`

  * Default: true
  * Unit: boolean
  * When set to `false`, the leader process terminates when the configuration is read.

#### Functions

1. `percona_telemetry_version()`

  * Outputs the version of the extension.

2. `percona_telemetry_status`

  * OUT: output_file_name       - TEXT
    - Path of the JSON output file.
  * OUT: last_file_processed    - Timestamp with Timezone
    - The timestamp with timezone when the last JSON file was written.
  * OUT: waiting_on_agent       - BOOLEAN
    - True if the last generated file was not removed by the Telemetery Agent.

### Setup

You can enable `percona_telemetry` when your `postgresql` instance is not running.

`percona_telemetry` needs to be loaded at the start time. The extension requires additional shared memory; therefore,  add the `percona_telemetry` value for the `shared_preload_libraries` parameter and restart the `postgresql` instance.

Use the [ALTER SYSTEM](https://www.postgresql.org/docs/current/sql-altersystem.html)command from `psql` terminal to modify the `shared_preload_libraries` parameter.

```sql
ALTER SYSTEM SET shared_preload_libraries = 'percona_telemetry';
```

> **NOTE**: If you’ve added other modules to the `shared_preload_libraries` parameter (for example, `pg_stat_monitor`), list all of them separated by commas for the `ALTER SYSTEM` command.

Start or restart the `postgresql` instance to apply the changes.

Create the extension using the [CREATE EXTENSION](https://www.postgresql.org/docs/current/sql-createextension.html) command. Using this command requires the privileges of a superuser or a database owner. Connect to `psql` as a superuser for a database and run the following command:


```sql
CREATE EXTENSION percona_telemetry;
```


This allows you to see the stats collected by `percona_telemetry`.

By default, `percona_telemetry` is created for the `postgres` database. To access its functions in other databases, you need to create the extension for every required database.

### Building from source

To build `percona_telemetry` from source code, you require the following:

* git
* make
* gcc
* postgresql-devel | postgresql-server-dev-all


You can download the source code of the latest release of `percona_telemetry` from [the releases page on GitHub](https://github.com/EngineeredVirus/percona_telemetry/releases) or using git:

```
git clone git://github.com/EngineeredVirus/percona_telemetry.git
```

Compile and install the extension

```
cd percona_telemetry
make
make install
```

To run the associated regression tests, a make target is available. ```make installcheck``` is deliberately disabled as the preload configuration may not be set for installed servers.

```
make check
```

### Uninstall `percona_telemetry`

To uninstall `percona_telemetry`, do the following:

1. Disable telemetry data collection. From the `psql` terminal or another similar client, run the following command:

    ```sql
    ALTER SYSTEM SET percona_telemetry.enabled = 0;
    SELECT pg_reload_conf();
    ```
**NOTE**: This will cause the leader process to exit. To restart the leader process, ensure that `percona_telemetry.enabled = 1`, and then restart the database server process.

2. Drop `percona_telemetry` extension:

    ```sql
    DROP EXTENSION percona_telemetry;
    ```

3. Remove `percona_telemetry` from the `shared_preload_libraries` configuration parameter:

    ```sql
    ALTER SYSTEM SET shared_preload_libraries = '';
    ```

    **Important**: If the `shared_preload_libraries` parameter includes other modules, specify them all for the `ALTER SYSTEM SET` command to keep using them.

4. Restart the `postgresql` instance to apply the changes.