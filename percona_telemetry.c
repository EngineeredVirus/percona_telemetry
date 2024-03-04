#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pg_config.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type_d.h"
#include "postmaster/bgworker.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"

#include <sys/stat.h>

PG_MODULE_MAGIC;

/* Struct to keep track of databases telemetry data */
typedef struct PTDatabaseInfo
{
    Oid  datid;
    char datname[NAMEDATALEN];
    int64 datsize;
} PTDatabaseInfo;

/* Struct to keep track of extensions of a database */
typedef struct PTExtensionInfo
{
    char     extname[NAMEDATALEN];
    PTDatabaseInfo *db_data;
} PTExtensionInfo;

/*
 * Shared state to telemetry. We don't need any locks as we run only one
 * background worker at a time which may update this in case of an error.
 */
typedef struct PTSharedState
{
    int error_code;
    bool write_in_progress;
    char dbtemp_filepath[MAXPGPATH];
    char dbinfo_filepath[MAXPGPATH];
    PTDatabaseInfo dbinfo;
    int json_file_indent;
    TimestampTz last_file_produced;
    bool waiting_for_agent;
    bool last_db_entry;
} PTSharedState;

/* General defines */
#define PT_BUILD_VERSION    "1.0"
#define PT_FILENAME_BASE    "percona_telemetry"

/* JSON formatting defines */
#define PT_INDENT_SIZE      2 + (5 * 2)
#define PT_FORMAT_JSON(dest, dest_size, str, indent_size)       \
            pg_snprintf(dest, dest_size, "%*s%s\n",             \
                        (indent_size) * PT_INDENT_SIZE, "", str)

#define PT_JSON_BLOCK_START             1
#define PT_JSON_BLOCK_END               1 << 1
#define PT_JSON_BLOCK_LAST              1 << 3
#define PT_JSON_BLOCK_KEY               1 << 4
#define PT_JSON_BLOCK_VALUE             1 << 5
#define PT_JSON_ARRAY_START             1 << 6
#define PT_JSON_ARRAY_END               1 << 7

#define PT_JSON_BLOCK_EMPTY             (PT_JSON_BLOCK_START | PT_JSON_BLOCK_END)
#define PT_JSON_BLOCK_SIMPLE            (PT_JSON_BLOCK_EMPTY | PT_JSON_BLOCK_KEY | PT_JSON_BLOCK_VALUE)
#define PT_JSON_BLOCK_ARRAY_VALUE       (PT_JSON_BLOCK_START | PT_JSON_BLOCK_KEY | PT_JSON_ARRAY_START)

static int json_blocks_started = 0;
static int json_arrays_started = 0;

/* Defining error codes */
#define PT_SUCCESS          0
#define PT_DB_ERROR         1
#define PT_FILE_ERROR       2
#define PT_JSON_ERROR       3

/* Must use to exit a background worker process. */
#define PT_WORKER_EXIT(e_code)                  \
{                                               \
    if (IsTransactionBlock())                   \
        CommitTransactionCommand();             \
    if (e_code != PT_SUCCESS && ptss)           \
        ptss->error_code = e_code;              \
    proc_exit(0);                               \
}

/* Init and exported functions */
void _PG_init(void);
PGDLLEXPORT void percona_telemetry_main(Datum);
PGDLLEXPORT void percona_telemetry_worker(Datum);

PG_FUNCTION_INFO_V1(percona_telemetry_status);
PG_FUNCTION_INFO_V1(percona_telemetry_version);


/* Internal init, shared memeory and signal functions */
static void pt_shmem_request(void);
static void pt_shmem_init(void);
static void init_guc(void);
static void pt_sigterm(SIGNAL_ARGS);

/* Helper functions */
static BgwHandleStatus setup_background_worker(const char *bgw_function_name, const char *bgw_name, const char *bgw_type, Oid datid, pid_t bgw_notify_pid);
static void start_leader(void);
static bool last_file_exists(void);

/* Database information collection and writing to file */
static List *get_database_list(void);
static List *get_extensions_list(PTDatabaseInfo *dbinfo, MemoryContext cxt);
static bool write_database_info(PTDatabaseInfo *dbinfo, List *extlist);

/* Shared state stuff */
static PTSharedState *ptss = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;

/* variable for signal handlers' variables */
static volatile sig_atomic_t sigterm_recvd = false;

/* GUC variables */
char *pg_telemetry_folder = NULL;
int scrape_interval = 86400;
bool telemetry_enabled = true;

/* General global variables */
static MemoryContext pt_cxt;

/*
 * Signal handler for SIGTERM. When received, sets the flag and the latch.
 */
static void
pt_sigterm(SIGNAL_ARGS)
{
    sigterm_recvd = true;

    /* Only if MyProc is set... */
    if (MyProc != NULL)
    {
        SetLatch(&MyProc->procLatch);
    }
}

/*
 * Initializing everything here
 */
void
_PG_init(void)
{
	init_guc();

    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = pt_shmem_request;

	start_leader();
}

/*
 * Start the leader process
 */
static void
start_leader(void)
{
    setup_background_worker("percona_telemetry_main", "percona_telemetry leader", "percona_telemetry leader", InvalidOid, 0);
}

/*
 * Select the status of percona_telemetry.
 */
Datum
percona_telemetry_status(PG_FUNCTION_ARGS)
{
#define PT_STATUS_COLUMN_COUNT  2

    TupleDesc tupdesc;
    Datum values[PT_STATUS_COLUMN_COUNT];
    bool nulls[PT_STATUS_COLUMN_COUNT] = {false};
    HeapTuple tup;
    Datum result;

    /* Initialize shmem */
    pt_shmem_init();

    tupdesc = CreateTemplateTupleDesc(PT_STATUS_COLUMN_COUNT);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "last_file_processed", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "waiting_on_agent", BOOLOID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    if (ptss->last_file_produced != 0)
        values[0] = TimestampTzGetDatum(ptss->last_file_produced);
    else
        nulls[0] = true;

    values[1] = BoolGetDatum(ptss->waiting_for_agent);

    tup = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tup);

	PG_RETURN_DATUM(result);
}

/*
 * Select the version of percona_telemetry.
 */
Datum
percona_telemetry_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(PT_BUILD_VERSION));
}

/*
 * Request additional shared memory required
 */
static void
pt_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(MAXALIGN(sizeof(PTSharedState)));
}

/*
 * Initialize the shared memory
 */
static void
pt_shmem_init(void)
{
    bool found;

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    ptss = (PTSharedState *) ShmemInitStruct("percona_telemetry shared state", sizeof(PTSharedState), &found);
    if (!found)
    {
        uint64 system_id = GetSystemIdentifier();

        /* Set paths */
        pg_snprintf(ptss->dbtemp_filepath, sizeof(ptss->dbtemp_filepath), "%s/%s-%lu.temp", pg_telemetry_folder, PT_FILENAME_BASE, system_id);
        pg_snprintf(ptss->dbinfo_filepath, sizeof(ptss->dbinfo_filepath), "%s/%s-%lu.json", pg_telemetry_folder, PT_FILENAME_BASE, system_id);

        /* Let's be optimistic here. No error code and no file currently being written. */
        ptss->error_code = PT_SUCCESS;
        ptss->write_in_progress = false;
        ptss->json_file_indent = 0;
        ptss->last_file_produced = 0;
        ptss->waiting_for_agent = false;
        ptss->last_db_entry = false;
    }

    LWLockRelease(AddinShmemInitLock);
}

/*
 * Intialize the GUCs
 */
static void
init_guc(void)
{
    /* file path */
    DefineCustomStringVariable("percona_telemetry.pg_telemetry_folder",
                               "Directory path for writing database info file(s)",
                               NULL,
                               &pg_telemetry_folder,
                               "/usr/local/percona/telemetry/pg",
                               PGC_SIGHUP,
                               0,
                               NULL,
                               NULL,
                               NULL);

    /* scan time interval for the main leader process */
    DefineCustomIntVariable("percona_telemetry.scrape_interval",
                            "Data scrape interval",
                            NULL,
                            &scrape_interval,
                            86400,
                            1,
                            INT_MAX,
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);

    /* is the extension enabled? */
    DefineCustomBoolVariable("percona_telemetry.enabled",
                             "Enable or disable the percona_telemetry extension",
                             NULL,
                             &telemetry_enabled,
                             true,
                             PGC_POSTMASTER,
                             0,
                             NULL,
                             NULL,
                             NULL);
}

/*
 * Sets up a background worker. If we have received a pid to notify, then
 * setup a dynamic worker and wait until it shuts down. This prevents ending
 * up with multiple background workers and prevents overloading system
 * resources. So, we process databases sequentially which is fine.
 *
 * datid is ignored for leader which is identified by a notify_pid = 0
 */
static BgwHandleStatus
setup_background_worker(const char *bgw_function_name, const char *bgw_name, const char *bgw_type, Oid datid, pid_t bgw_notify_pid)
{
    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;

    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
    strcpy(worker.bgw_library_name, "percona_telemetry");
    strcpy(worker.bgw_function_name, bgw_function_name);
    strcpy(worker.bgw_name, bgw_name);
    strcpy(worker.bgw_type, bgw_type);
    worker.bgw_main_arg = ObjectIdGetDatum(datid);
    worker.bgw_notify_pid = bgw_notify_pid;

    /* Case of a leader */
    if (bgw_notify_pid == 0)
    {
        /* Leader should never connect to a valid database. */
        worker.bgw_main_arg = ObjectIdGetDatum(InvalidOid);

        RegisterBackgroundWorker(&worker);

        /* Let's be optimistic about it's start. */
        return BGWH_STARTED;
    }

    /* Validate that it's a valid database Oid */
    Assert(datid != InvalidOid);
    worker.bgw_main_arg = ObjectIdGetDatum(datid);

    /*
     * Register the work and wait until it shuts down. This enforces creation
     * only one background worker process. So, don't have to implement any
     * locking for error handling or file writing.
     */
    RegisterDynamicBackgroundWorker(&worker, &handle);
    return WaitForBackgroundWorkerShutdown(handle);
}

/*
 * This function is critical to ensuring that we don't end up dumping files
 * when the agent is not picking up any. Therefore, we keep track of the
 * previously generated file. If it still exists, we don't generate any more.
 *
 * Returns true if the previous file was found otherwise false. In case
 * the file path is a directory, throw an error.
 */
static bool
last_file_exists(void)
{
    struct stat st;

    /* Assume we are not waiting for the agent. */
    ptss->waiting_for_agent = false;

    /* We didn't create a file yet. */
    if (ptss->dbinfo_filepath[0] == '\0')
        return ptss->waiting_for_agent;

    /* Previous file was not picked up by the agent. */
    if (stat(ptss->dbinfo_filepath, &st) == 0)
    {
        bool is_dir = S_ISDIR(st.st_mode);

        /* Possible case of path corruption as the file path is now a directory */
        if (is_dir)
        {
            ereport(LOG,
                    (errcode_for_file_access(),
                     errmsg("file path \"%s\" is not a directory: %m", ptss->dbinfo_filepath)));

            ptss->error_code = PT_FILE_ERROR;
        }

        ptss->waiting_for_agent = true;
    }

    /* Previous file was not found. So we're all good. */
    return ptss->waiting_for_agent;
}

/*
 * Return a list of databases from the pg_database catalog
 */
static List *
get_database_list(void)
{
    List          *dblist = NIL;
    Relation      rel;
    TableScanDesc scan;
    HeapTuple     tup;
    MemoryContext oldcxt;

    /* Start a transaction to access pg_database */
    StartTransactionCommand();

    rel = relation_open(DatabaseRelationId, AccessShareLock);
    scan = table_beginscan_catalog(rel, 0, NULL);

    while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
    {
        PTDatabaseInfo  *dbinfo;
        int64 datsize;
        Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);

        /* Ignore the template database as we can't connect to it */
        if (pgdatabase->oid < PostgresDbOid)
            continue;

        datsize = DatumGetInt64(DirectFunctionCall1(pg_database_size_oid, ObjectIdGetDatum(pgdatabase->oid)));

        /* Switch to our memory context instead of the transaction one */
        oldcxt = MemoryContextSwitchTo(pt_cxt);
        dbinfo = (PTDatabaseInfo *) palloc(sizeof(PTDatabaseInfo));

        /* Fill in the structure */
        dbinfo->datid = pgdatabase->oid;
        strncpy(dbinfo->datname, NameStr(pgdatabase->datname), sizeof(dbinfo->datname));
        dbinfo->datsize = datsize;

        /* Add to the list */
        dblist = lappend(dblist, dbinfo);

        /* Switch back the memory context */
        pt_cxt = MemoryContextSwitchTo(oldcxt);
    }

    /* Clean up */
    table_endscan(scan);
    relation_close(rel, AccessShareLock);

    CommitTransactionCommand();

    /* Return the list */
    return dblist;
}

/*
 * Get a list of installed extensions for a database
 */
static List *
get_extensions_list(PTDatabaseInfo *dbinfo, MemoryContext cxt)
{
    List *extlist = NIL;
    Relation rel;
    TableScanDesc scan;
    HeapTuple tup;
    MemoryContext oldcxt;

    Assert(dbinfo);

    /* Start a transaction to access pg_extensions */
    StartTransactionCommand();

    /* Open the extension catalog... */
    rel = table_open(ExtensionRelationId, AccessShareLock);
    scan = table_beginscan_catalog(rel, 0, NULL);

    while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
    {
        PTExtensionInfo *extinfo;
        Form_pg_extension extform = (Form_pg_extension) GETSTRUCT(tup);

        /* Switch to the given memory context */
        oldcxt = MemoryContextSwitchTo(cxt);
        extinfo = (PTExtensionInfo *) palloc(sizeof(PTExtensionInfo));

        /* Fill in the structure */
        extinfo->db_data = dbinfo;
        strncpy(extinfo->extname, NameStr(extform->extname), sizeof(extinfo->extname));

        /* Add to the list */
        extlist = lappend(extlist, extinfo);

        /* Switch back the memory context */
        cxt = MemoryContextSwitchTo(oldcxt);
    }

    /* Clean up */
    table_endscan(scan);
    table_close(rel, AccessShareLock);

    CommitTransactionCommand();

    /* Return the list */
    return extlist;
}

/*
 * Init json state.
 */
static bool
json_state_init()
{
    json_blocks_started = 0;
    json_arrays_started = 0;

    return true;
}

/*
 * Do we have unclosed square or curley brackets? If so, then
 * the JSON in our case isn't really valid.
 */
static bool
json_state_validate()
{
    return (json_blocks_started == 0 && json_arrays_started == 0);
}

/*
 * Fixes a JSON string to avoid malformation of a json value.
 */
static char *
json_fix_value(char *str, int sz)
{
    int i;
    char *s_copy = pstrdup(str);
    char *s = s_copy;

    for(i = 0; i < (sz - 1) && *s; i++)
    {
        if (*s == '"')
        {
            str[i++] = '\\';
            str[i] = '"';

            continue;
        }

        str[i] = *s;
        s++;
    }

    /* Ensure that we always end up with a string value. */
    str[i] = '\0';

    pfree(s_copy);

    return str;
}

/*
 * Construct a JSON block for writing.
 *
 * NOTE: We are not escape any quote characters in key or value strings, as
 * we don't expect to encounter that in extension names.
 */
static char *
construct_json_block(char *msg_block, size_t msg_block_sz, char *key, char *value, int flags)
{
    char msg[2048] = {0};
    char msg_json[2048] = {0};

    /* Make the string empty so that we can always concat. */
    msg_block[0] = '\0';

    if (flags & PT_JSON_BLOCK_START)
    {
        PT_FORMAT_JSON(msg_json, sizeof(msg_json), "{", ptss->json_file_indent);
        strlcpy(msg_block, msg_json, msg_block_sz);

        json_blocks_started++;
        ptss->json_file_indent++;
    }

    if (flags & PT_JSON_BLOCK_KEY)
    {
        pg_snprintf(msg, sizeof(msg), "\"key\": \"%s\",", key);
        PT_FORMAT_JSON(msg_json, sizeof(msg_json), msg, ptss->json_file_indent);
        strlcat(msg_block, msg_json, msg_block_sz);
    }

    if (flags & PT_JSON_BLOCK_VALUE)
    {
        pg_snprintf(msg, sizeof(msg), "\"value\": \"%s\"", value);
        PT_FORMAT_JSON(msg_json, sizeof(msg_json), msg, ptss->json_file_indent);
        strlcat(msg_block, msg_json, msg_block_sz);
    }

    if (flags & PT_JSON_ARRAY_START)
    {
        pg_snprintf(msg, sizeof(msg), "\"value\": [");
        PT_FORMAT_JSON(msg_json, sizeof(msg_json), msg, ptss->json_file_indent);
        strlcat(msg_block, msg_json, msg_block_sz);

        json_arrays_started++;
        ptss->json_file_indent++;
    }

    /* Value is not an array so we can close the block. */
    if (flags & PT_JSON_ARRAY_END)
    {
        ptss->json_file_indent--;

        PT_FORMAT_JSON(msg_json, sizeof(msg_json), "]", ptss->json_file_indent);
        strlcat(msg_block, msg_json, msg_block_sz);

        json_arrays_started--;
    }

    /* Value is not an array so we can close the block. */
    if (flags & PT_JSON_BLOCK_END)
    {
        char closing[3] = {'}', ',', '\0'};

        if (flags & PT_JSON_BLOCK_LAST)
        {
            /* Let's remove the comma in case this is the last block. */
            closing[1] = '\0';
        }

        ptss->json_file_indent--;

        PT_FORMAT_JSON(msg_json, sizeof(msg_json), closing, ptss->json_file_indent);
        strlcat(msg_block, msg_json, msg_block_sz);

        json_blocks_started--;
    }

    return msg_block;
}

/*
 * Open a file in the given mode.
 */
static FILE *
json_file_open(char *pathname, char *mode)
{
    FILE *fp;
    struct stat st;
    bool is_dir = false;

    /* Let's validate the path. */
    if (stat(pg_telemetry_folder, &st) == 0)
    {
        is_dir = S_ISDIR(st.st_mode);
    }

    if (is_dir == false)
    {
        ereport(LOG,
                (errcode_for_file_access(),
                 errmsg("percont_telemetry.pg_telemetry_folder \"%s\" is not set to a writeable folder or the folder does not exist.", pathname)));
        PT_WORKER_EXIT(PT_FILE_ERROR);
    }

    fp = fopen(pathname, mode);
    if (fp == NULL)
	{
        ereport(LOG,
                (errcode_for_file_access(),
                 errmsg("Could not open file %s for writing.", pathname)));
        PT_WORKER_EXIT(PT_FILE_ERROR);
    }

    return fp;
}

/*
 * Write JSON to file.
 */
static void
write_json_to_file(FILE *fp, char *json_str)
{
    int len;
    int bytes_written;

    len = strlen(json_str);
    bytes_written = fwrite(json_str, 1, len, fp);

    if (len != bytes_written)
    {
        ereport(LOG,
                (errcode_for_file_access(),
                 errmsg("Could not write to json file.")));

        fclose(fp);
        PT_WORKER_EXIT(PT_FILE_ERROR);
    }
}

/*
 * Writes database information along with names of the active extensions to
 * the file.
 */
static bool
write_database_info(PTDatabaseInfo *dbinfo, List *extlist)
{
    char msg[2048] = {0};
    char msg_json[4096] = {0};
    size_t sz_json;
    FILE *fp;
    ListCell *lc;
    int flags;

    sz_json = sizeof(msg_json);

    /* Open file in append mode. */
    fp = json_file_open(ptss->dbtemp_filepath, "a+");

    /* Initialize JSON state. */
    json_state_init();

    /* Construct and write the database OID block. */
    pg_snprintf(msg, sizeof(msg), "%u", dbinfo->datid);
    construct_json_block(msg_json, sz_json, "database_oid", msg, PT_JSON_BLOCK_SIMPLE);
    write_json_to_file(fp, msg_json);

    /* Construct and write the database size block. */
    pg_snprintf(msg, sizeof(msg), "%lu", dbinfo->datsize);
    construct_json_block(msg_json, sz_json, "database_size", msg, PT_JSON_BLOCK_SIMPLE);
    write_json_to_file(fp, msg_json);

    /* Construct and initiate the active extensions array block. */
    construct_json_block(msg_json, sz_json, "active_extensions", "", PT_JSON_BLOCK_ARRAY_VALUE);
    write_json_to_file(fp, msg_json);

    /* Iterate through all extensions and those to the array. */
    foreach(lc, extlist)
	{
        PTExtensionInfo *extinfo = lfirst(lc);

        flags = (list_tail(extlist) == lc) ? (PT_JSON_BLOCK_SIMPLE | PT_JSON_BLOCK_LAST) : PT_JSON_BLOCK_SIMPLE;

        construct_json_block(msg_json, sz_json, "extension_name", extinfo->extname, flags);
        write_json_to_file(fp, msg_json);
    }

    /* Close the array */
    construct_json_block(msg, sizeof(msg), "active_extensions", "", PT_JSON_ARRAY_END);
    strcpy(msg_json, msg);

    /* Close the extension block */
    flags = (ptss->last_db_entry) ? (PT_JSON_BLOCK_END | PT_JSON_BLOCK_LAST) : PT_JSON_BLOCK_END;
    construct_json_block(msg, sizeof(msg), "active_extensions", "", flags);
    strlcat(msg_json, msg, sz_json);

    /* Write both to file. */
    write_json_to_file(fp, msg_json);

    /* Clean up */
    fclose(fp);

    /* Validate JSON state. */
    if (json_state_validate() == false)
    {
        ereport(LOG,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("percona_telemetry: malformed json created.")));
        PT_WORKER_EXIT(PT_JSON_ERROR);
    }

    return true;
}

/*
 * Main function for the background leader process
 */
void
percona_telemetry_main(Datum main_arg)
{
	int rc = 0;
    List *dblist = NIL;
    ListCell *lc = NULL;
    char json_pg_version[1024];
    FILE *fp;
    char msg[2048] = {0};
    char msg_json[4096] = {0};
    size_t sz_json = sizeof(msg_json);
    bool first_time = true;

    /* Save the version in a JSON escaped stirng just to be safe. */
    strcpy(json_pg_version, PG_VERSION_STR);
    json_fix_value(json_pg_version, sizeof(json_pg_version));

    /* Setup signal callbacks */
    pqsignal(SIGTERM, pt_sigterm);

    /* We can now receive signals */
    BackgroundWorkerUnblockSignals();

    /* Initialize shmem */
    pt_shmem_init();

    /* Set up connection */
    BackgroundWorkerInitializeConnectionByOid(InvalidOid, InvalidOid, 0);

    /* Set name to make percona_telemetry visible in pg_stat_activity */
    pgstat_report_appname("percona_telemetry");

    /* This is the context that we will allocate our data in */
    pt_cxt = AllocSetContextCreate(TopMemoryContext, "Percona Telemetry Context", ALLOCSET_DEFAULT_SIZES);

    /* Should never really terminate unless... */
    while (telemetry_enabled && !sigterm_recvd && ptss->error_code == PT_SUCCESS)
	{
        /* Don't sleep the first time */
        if (first_time == false)
        {
            rc = WaitLatch(MyLatch,
                        WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                        scrape_interval * 1000L,
                        PG_WAIT_EXTENSION);

            ResetLatch(MyLatch);
        }

        CHECK_FOR_INTERRUPTS();

        /* Time to end the loop as the server is shutting down */
		if ((rc & WL_POSTMASTER_DEATH) || ptss->error_code != PT_SUCCESS)
			break;

        /* Let's continue to sleep until our last file is picked up by the agent. */
        if (last_file_exists())
            continue;

        /* We are not processing a cell at the moment. So, let's get the updated database list. */
        if (dblist == NIL && (rc & WL_TIMEOUT || first_time))
        {
            /* Data collection will start now */
            first_time = false;

            dblist = get_database_list();

            /* Set writing state to true */
            Assert(ptss->write_in_progress == false);
            ptss->write_in_progress = true;

            /* Initialise the json state here. We'll validate it when we complete the writing process. */
            json_state_init();

            /* Open file for writing. */
            fp = json_file_open(ptss->dbtemp_filepath, "w");

            /* Construct and write the database size block. */
            pg_snprintf(msg, sizeof(msg), "%lu", GetSystemIdentifier());
            construct_json_block(msg_json, sz_json, "instanceId", msg, PT_JSON_BLOCK_SIMPLE);
            write_json_to_file(fp, msg_json);

            /* Construct and initiate the active extensions array block. */
            construct_json_block(msg_json, sz_json, "pillar_version", json_pg_version, PT_JSON_BLOCK_SIMPLE);
            write_json_to_file(fp, msg_json);

            /* Construct and initiate the active extensions array block. */
            construct_json_block(msg_json, sz_json, "databases", "", PT_JSON_BLOCK_ARRAY_VALUE);
            write_json_to_file(fp, msg_json);

            /* Let's close the file now so that processes may add their stuff. */
            fclose(fp);

            ptss->last_file_produced = GetCurrentTimestamp();
        }

        /* Must be a valid list */
        if (dblist != NIL)
        {
            PTDatabaseInfo *dbinfo;
            BgwHandleStatus status;

            /* First or the next cell */
            lc = (lc) ? lnext(dblist, lc) : list_head(dblist);

            /*
             * We've reached end of the list. So, let's cleanup and go to
             * sleep until the timer runs out. Also, we need to move the
             * file to mark the process complete.
             */
            if (lc == NULL)
            {
                list_free_deep(dblist);
                dblist = NIL;

                /* We should always have write_in_progress true here. */
                Assert(ptss->write_in_progress == true);

                /* Close the open the file, close tags and close the file. */
                fp = json_file_open(ptss->dbtemp_filepath, "a+");

                /* Close the array */
                construct_json_block(msg, sizeof(msg), "databases", "", PT_JSON_ARRAY_END);
                strcpy(msg_json, msg);

                /* Close the extension block */
                construct_json_block(msg, sizeof(msg), "databases", "", PT_JSON_BLOCK_END | PT_JSON_BLOCK_LAST);
                strlcat(msg_json, msg, sz_json);

                /* Write both to file. */
                write_json_to_file(fp, msg_json);

                /* Clean up */
                fclose(fp);

                /* Validate JSON state. */
                if (json_state_validate() == false)
                {
                    ereport(LOG,
                            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                             errmsg("percona_telemetry: malformed json created.")));

                    ptss->error_code = PT_FILE_ERROR;
                    break;
                }

	            /* Let's rename the temp file so that agent can pick it up. */
	            if (rename(ptss->dbtemp_filepath, ptss->dbinfo_filepath) < 0)
	            {
                    ereport(LOG,
                            (errcode_for_file_access(),
                             errmsg("could not rename file \"%s\" to \"%s\": %m",
                                    ptss->dbtemp_filepath,
                                    ptss->dbinfo_filepath)));

                    ptss->error_code = PT_FILE_ERROR;
                    break;
                }

                ptss->write_in_progress = false;
                continue;
            }

            ptss->last_db_entry = (list_tail(dblist) == lc);
            dbinfo = lfirst(lc);
            memcpy(&ptss->dbinfo, dbinfo, sizeof(PTDatabaseInfo));

            /*
             * Run the dynamic background worker and wait for it's completion
             * so that we can wake up the leader process.
             */
        	status = setup_background_worker("percona_telemetry_worker",
                                                "percona_telemetry worker",
                                                "percona_telemetry worker",
                                                ptss->dbinfo.datid, MyProcPid);

            /* Wakeup the main process since the worker has stopped. */
            if (status == BGWH_STOPPED)
                SetLatch(&MyProc->procLatch);
        }
    }

    /* Shouldn't really ever be here unless an error was encountered. So exit with the error code */
    ereport(LOG,
            (errmsg("Percona Telemetry main (PID %d) exited due to errono %d", MyProcPid, ptss->error_code)));
	PT_WORKER_EXIT(PT_SUCCESS);
}

/*
 * Worker process main function
 */
void
percona_telemetry_worker(Datum main_arg)
{
    Oid datid;
    MemoryContext tmpcxt;
    List *extlist = NIL;

    /* Get the argument. Ensure that it's a valid oid in case of a worker */
    datid = DatumGetObjectId(main_arg);

    /* Initialize shmem */
    pt_shmem_init();
    Assert(datid != InvalidOid && ptss->dbinfo.datid == datid);

    /* Set up connection */
    BackgroundWorkerInitializeConnectionByOid(datid, InvalidOid, 0);

    /* This is the context that we will allocate our data in */
    tmpcxt = AllocSetContextCreate(TopMemoryContext, "Percona Telemetry Context (tmp)", ALLOCSET_DEFAULT_SIZES);

    /* Set name to make percona_telemetry visible in pg_stat_activity */
    pgstat_report_appname("percona_telemetry_worker");

    extlist = get_extensions_list(&ptss->dbinfo, tmpcxt);

    if (write_database_info(&ptss->dbinfo, extlist) == false)
        PT_WORKER_EXIT(PT_FILE_ERROR);

    /* Ending the worker... */
    PT_WORKER_EXIT(PT_SUCCESS);
}