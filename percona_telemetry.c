#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_database.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/backend_status.h"
#include "utils/guc.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"


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
    int worker_error_code;
    PTDatabaseInfo dbinfo;
} PTSharedState;

/* Defining codes for background worker exit */
#define PT_WORKER_SUCCESS       0
#define PT_WORKER_DB_ERROR      1
#define PT_WORKER_FILE_ERROR    2

/* Must use to exit a background worker process. */
#define PT_WORKER_EXIT(error_code)              \
{                                               \
    if (IsTransactionBlock())                   \
        CommitTransactionCommand();             \
    if (error_code != PT_WORKER_SUCCESS)        \
        ptss->worker_error_code = error_code;   \
    proc_exit(0);                               \
}

/* Init and exported functions */
void _PG_init(void);
PGDLLEXPORT void percona_telemetry_main(Datum);
PGDLLEXPORT void percona_telemetry_worker(Datum);

/* Internal init, shared memeory and signal functions */
static void pt_shmem_request(void);
static void pt_shmem_init(void);
static void init_guc(void);
static void pt_sigterm(SIGNAL_ARGS);

/* Helper functions */
static BgwHandleStatus setup_background_worker(const char *bgw_function_name, const char *bgw_name, const char *bgw_type, Oid datid, pid_t bgw_notify_pid);
static List *get_database_list(void);
static List *get_extensions_list(PTDatabaseInfo *dbinfo, MemoryContext cxt);
static bool write_database_info(PTDatabaseInfo *dbinfo, List *extlist);

/* Shared state stuff */
static PTSharedState *ptss = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;

/* variable for signal handlers' variables */
static volatile sig_atomic_t sigterm_recvd = false;

/* GUC variables */
char *file_path = NULL;
int scan_time = 5;
bool extension_enabled = true;

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

	setup_background_worker("percona_telemetry_main", "percona_telemetry leader", "percona_telemetry leader", InvalidOid, 0);
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
        ptss->worker_error_code = PT_WORKER_SUCCESS;
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
    DefineCustomStringVariable("percona_telemetry.file_path",
                               "File path for writing database info",
                               NULL,
                               &file_path,
                               "/tmp/percona.telemetry",
                               PGC_SIGHUP,
                               0,
                               NULL,
                               NULL,
                               NULL);

    /* scan time interval for the main leader process */
    DefineCustomIntVariable("percona_telemetry.scan_time",
                            "Time between scans in seconds",
                            NULL,
                            &scan_time,
                            5,
                            1,
                            INT_MAX,
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);

    /* is the extension enabled? */
    DefineCustomBoolVariable("percona_telemetry.enable",
                             "Enable or disable the percona_telemetry extension",
                             NULL,
                             &extension_enabled,
                             true,
                             PGC_POSTMASTER,
                             0,
                             NULL,
                             NULL,
                             NULL);
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
 * TODO: FIX THIS FUNCTION
 * - Write the information in the required file.
 */
static bool
write_database_info(PTDatabaseInfo *dbinfo, List *extlist)
{
    FILE *fp;
    ListCell *lc;
    char msg[1024] = {0};
    int len;
    int bytes_written;

    ereport(LOG, (errmsg("About to write to log file %s", file_path)));

    fp = fopen(file_path, "a+");
    if (fp == NULL)
	{
        ereport(WARNING, (errmsg("Could not open file %s for writing.", file_path)));
        return false;
    }

    pg_snprintf(msg, sizeof(msg), "{\n  database: %s,\n  size: %ld,\n  active_extenstions:\n  {\n", dbinfo->datname, dbinfo->datsize);

    len = strlen(msg);
    bytes_written = fwrite(msg, 1, len, fp);
    if (bytes_written != len)
    {
        ereport(WARNING, (errmsg("Unable to write database information to file. %d => %d", len, bytes_written)));
        return false;
    }

    /* Iterate through all extensions */
    foreach(lc, extlist)
	{
        PTExtensionInfo *extinfo = lfirst(lc);

        pg_snprintf(msg, sizeof(msg), "    extension_name: %s,\n", extinfo->extname);
        len = strlen(msg);
        bytes_written = fwrite(msg, 1, len, fp);
    }

    pg_snprintf(msg, sizeof(msg), "  }\n}\n");
    len = strlen(msg);
    bytes_written = fwrite(msg, 1, len, fp);

    /* Clean up */
    fclose(fp);

    return true;
}

/*
 * Main function for the background leader process
 */
void
percona_telemetry_main(Datum main_arg)
{
	int rc;
    List *dblist = NIL;
    ListCell *lc = NULL;

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
    while (extension_enabled && !sigterm_recvd)
	{
        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       scan_time * 1000L,
                       PG_WAIT_EXTENSION);

        ResetLatch(MyLatch);

        CHECK_FOR_INTERRUPTS();

        /* Time to end the loop as the server is shutting down */
		if ((rc & WL_POSTMASTER_DEATH) || ptss->worker_error_code != PT_WORKER_SUCCESS)
			break;

        /* We are not processing a cell at the moment. So, let's get the updated database list. */
        if (dblist == NIL && (rc & WL_TIMEOUT))
            dblist = get_database_list();

        /* Must be a valid list */
        if (dblist != NIL)
        {
            PTDatabaseInfo *dbinfo;
            BgwHandleStatus status;

            /* First or the next cell */
            lc = (lc) ? lnext(dblist, lc) : list_head(dblist);

            /* We've reached end of the list. So, let's cleanup and go to sleep until the timer runs out. */
            if (lc == NULL)
            {
                list_free_deep(dblist);
                dblist = NIL;
                continue;
            }

            dbinfo = lfirst(lc);
            memcpy(&ptss->dbinfo, dbinfo, sizeof(PTDatabaseInfo));
            ereport(LOG, (errmsg("About to run a worker %d", ptss->dbinfo.datid)));

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
    ereport(LOG, (errmsg("Percona Telemetry main (PID %d) exited due to errono %d", MyProcPid, ptss->worker_error_code)));
	proc_exit(0);
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

    ereport(LOG, (errmsg("Worker Running for database %d", datid)));

    /* Set up connection */
    BackgroundWorkerInitializeConnectionByOid(datid, InvalidOid, 0);

    /* This is the context that we will allocate our data in */
    tmpcxt = AllocSetContextCreate(TopMemoryContext, "Percona Telemetry Context (tmp)", ALLOCSET_DEFAULT_SIZES);

    /* Set name to make percona_telemetry visible in pg_stat_activity */
    pgstat_report_appname("percona_telemetry_worker");

    extlist = get_extensions_list(&ptss->dbinfo, tmpcxt);

    if (write_database_info(&ptss->dbinfo, extlist) == false)
        PT_WORKER_EXIT(PT_WORKER_FILE_ERROR);

    /* Ending the worker... */
    PT_WORKER_EXIT(PT_WORKER_SUCCESS);
}