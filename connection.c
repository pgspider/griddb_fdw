/*
 * GridDB Foreign Data Wrapper
 *
 * Portions Copyright (c) 2018, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  connection.c
 *
 */
#include "postgres.h"

#include "griddb_fdw.h"

#include "access/xact.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

/*
 * Connection cache hash table entry
 *
 * The lookup key in this hash table is the user mapping OID. We use just one
 * connection per user mapping ID, which ensures that all the scans use the
 * same snapshot during a query.  Using the user mapping OID rather than
 * the foreign server OID + user OID avoids creating multiple connections when
 * the public user mapping applies to all user OIDs.
 *
 * The "conn" pointer can be NULL if we don't currently have a live connection.
 * When we do have a connection.
 */
typedef Oid ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;			/* hash key (must be first) */
	GSGridStore *store;			/* connection to foreign server, or NULL */
	HTAB	   *cont_hash;		/* used container list */
	int			xact_depth;		/* 0 = no xact open, 1 = main xact open */
	bool		changing_xact_state;	/* xact state change in process */
	bool		invalidated;	/* true if reconnect is pending */
	Oid			serverid;		/* foreign server OID used to get server name */
	uint32		server_hashvalue;	/* hash value of foreign server OID */
	uint32		mapping_hashvalue;	/* hash value of user mapping OID */
	bool		keep_connections;	/* setting value of keep_connections
									 * server option */
} ConnCacheEntry;

typedef Oid ContCacheKey;

/*
 * Container cache hash table entry
 *
 * We memorize contaners which are used in a transaction.
 */
typedef struct ContCacheEntry
{
	ContCacheKey key;			/* hash key (must be first) */
	GSContainer *cont;
}			ContCacheEntry;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(griddb_get_connections);
PG_FUNCTION_INFO_V1(griddb_disconnect);
PG_FUNCTION_INFO_V1(griddb_disconnect_all);

/*
 * Connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;

/* tracks whether any work is needed in callback functions */
static volatile bool xact_got_connection = false;

/* prototypes of private functions */
static GSGridStore * griddb_connect_server(char *address, char *port,
										   char *member, char *database,
										   char *cluster, char *user,
										   char *passwd);
static void make_new_connection(ConnCacheEntry *entry,
								UserMapping *user,
								Oid foreigntableid);
static void griddb_begin_xact(ConnCacheEntry *entry);
static void griddb_end_xact(ConnCacheEntry *entry, bool isCommit,
							GSGridStore * store);
static void griddb_xact_callback(XactEvent event, void *arg);
static void griddb_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
									SubTransactionId parentSubid, void *arg);
static void griddb_abort_cleanup(ConnCacheEntry *entry, bool toplevel);
static void griddb_reset_xact_state(ConnCacheEntry *entry, bool toplevel);
#if PG_VERSION_NUM >= 140000
static void griddb_inval_callback(Datum arg, int cacheid, uint32 hashvalue);
static bool disconnect_cached_connections(Oid serverid);
#endif

/*
 * griddb_get_connection:
 *			Get a connection which can be used to execute queries on
 * the remote GridDB server with the user's authorization. A new connection
 * is established if we don't already have a suitable one.
 */
GSGridStore *
griddb_get_connection(UserMapping *user, bool will_prep_stmt, Oid foreigntableid)
{
	bool		found;
	ConnCacheEntry *entry;
	ConnCacheKey key;

	/* First time through, initialize connection cache hashtable */
	if (ConnectionHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);
		/* allocate ConnectionHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
		ConnectionHash = hash_create("griddb_fdw connections", 8,
									 &ctl,
#if PG_VERSION_NUM >= 140000
									 HASH_ELEM | HASH_BLOBS);
#else
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
#endif

		/*
		 * Register some callback functions that manage connection cleanup.
		 * This should be done just once in each backend.
		 */
		RegisterXactCallback(griddb_xact_callback, NULL);
		RegisterSubXactCallback(griddb_subxact_callback, NULL);
#if PG_VERSION_NUM >= 140000
		CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
									  griddb_inval_callback, (Datum) 0);
		CacheRegisterSyscacheCallback(USERMAPPINGOID,
									  griddb_inval_callback, (Datum) 0);
#endif
	}

	/* Set flag that we did GetConnection during the current transaction */
	xact_got_connection = true;

	/* Create hash key for the entry.  Assume no pad bytes in key struct */
	key = user->umid;

	/*
	 * Find or create cached entry for requested connection.
	 */
	entry = (ConnCacheEntry *) hash_search(ConnectionHash, &key, HASH_ENTER,
										   &found);
	if (!found)
	{
		/* initialize new hashtable entry (key is already filled in) */
		entry->store = NULL;
		entry->cont_hash = NULL;
	}

	/*
	 * If the connection needs to be remade due to invalidation, disconnect as
	 * soon as we're out of all transactions.
	 */
	if (entry->store != NULL && entry->invalidated && entry->xact_depth == 0)
	{
		elog(DEBUG3, "griddb_fdw: closing connection %p for option changes to take effect",
			 entry->store);
		gsCloseGridStore(&entry->store, GS_TRUE);
		entry->store = NULL;
	}

	/*
	 * If cache entry doesn't have a connection, we have to establish a new
	 * connection.
	 */
	if (entry->store == NULL)
		make_new_connection(entry, user, foreigntableid);

	/*
	 * Start a new transaction if needed.
	 */
	griddb_begin_xact(entry);

	return entry->store;
}

/*
 * Reset all transient state fields in the cached connection entry and
 * establish new connection to the remote server.
 */
static void
make_new_connection(ConnCacheEntry *entry, UserMapping *user, Oid foreigntableid)
{
	ForeignServer *server = GetForeignServer(user->serverid);

	/* Fetch the options */
	griddb_opt *opt = griddb_get_options(foreigntableid);

	/*
	 * Determine whether to keep the connection that we're about to make here
	 * open even after the transaction using it ends, so that the subsequent
	 * transactions can re-use it.
	 *
	 * It's enough to determine this only when making new connection because
	 * all the connections to the foreign server whose keep_connections option
	 * is changed will be closed and re-made later.
	 */
	entry->keep_connections = opt->keep_connections;
	entry->serverid = server->serverid;
	entry->xact_depth = 0;
	entry->changing_xact_state = false;
	entry->invalidated = false;
	entry->server_hashvalue =
		GetSysCacheHashValue1(FOREIGNSERVEROID,
							  ObjectIdGetDatum(server->serverid));
	entry->mapping_hashvalue =
		GetSysCacheHashValue1(USERMAPPINGOID,
							  ObjectIdGetDatum(user->umid));
	entry->store = griddb_connect_server(
										 opt->svr_address,
										 opt->svr_port,
										 opt->svr_notification_member,
										 opt->svr_database,
										 opt->svr_clustername,
										 opt->svr_username,
										 opt->svr_password);
	elog(DEBUG3, "griddb_fdw: new griddb_fdw connection %p for server \"%s\""
		 " (user mapping oid %u, userid %u)", entry->store,
		 server->servername, user->umid, user->userid);
}

/*
 * Connect to remote server using specified server and user mapping properties.
 */
static GSGridStore *
griddb_connect_server(char *address, char *port, char *member, char *database,
					  char *cluster, char *user, char *passwd)
{
	GSGridStore *store = NULL;
	size_t		propCount;
	GSResult	ret;

	if (database == NULL)
		database = "public";

	if (member == NULL)
	{
		/* multicast mode */
		const		GSPropertyEntry props[] = {
			{"database", database},
			{"clusterName", cluster},
			{"user", user},
			{"password", passwd},
			/* multicast address */
			{"notificationAddress", address},
			{"notificationPort", port},
		};

		propCount = sizeof(props) / sizeof(*props);
		ret = gsGetGridStore(gsGetDefaultFactory(), props, propCount, &store);
	}
	else
	{
		/* fixed list mode */
		const		GSPropertyEntry props[] = {
			{"clusterName", cluster},
			{"database", database},
			{"user", user},
			{"password", passwd},
			/* list of address and port */
			{"notificationMember", member},
		};

		propCount = sizeof(props) / sizeof(*props);
		ret = gsGetGridStore(gsGetDefaultFactory(), props, propCount, &store);
	}

	if (!GS_SUCCEEDED(ret))
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("griddb_fdw: could not connect to server \"%s\", port %s, cluster \"%s\"",
						address, port, cluster)));

	return store;
}

/*
 * Returns the name of a given relation in foreign server by confirming
 * a table name option.
 * Returns a palloc'd copy of the string, or NULL if no such relation.
 */
char *
griddb_get_rel_name(Oid relid)
{
	ForeignTable *table;
	ListCell   *lc;

	table = GetForeignTable(relid);

	/*
	 * Use value of FDW options if any, instead of the name of object itself.
	 */
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, OPTION_TABLE) == 0)
			return pstrdup(defGetString(def));
	}

	return get_rel_name(relid);
}

/*
 * Get a GSContainer which can be used to execute queries on the remote GridDB
 * It is memorized by associating with connection.
 * When transaction is ended, each container has to be commited or
 * aborted by griddb_end_transaction().
 */
GSContainer *
griddb_get_container(UserMapping *user, Oid relid, GSGridStore * store)
{
	bool		found;
	ConnCacheEntry *conn_entry;
	ConnCacheKey conn_key;
	ContCacheEntry *cont_entry;
	ContCacheKey cont_key;

	conn_key = user->umid;

	/* Search cached entry for connection. */
	conn_entry = (ConnCacheEntry *) hash_search(ConnectionHash, &conn_key, HASH_ENTER, &found);
	Assert(found);

	/* Create hash table for container. */
	if (conn_entry->cont_hash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ContCacheKey);
		ctl.entrysize = sizeof(ContCacheEntry);
		conn_entry->cont_hash = hash_create("griddb_fdw containers", 8,
											&ctl,
#if PG_VERSION_NUM >= 140000
											HASH_ELEM | HASH_BLOBS);
#else
											HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
#endif
	}

	cont_key = relid;

	/* Search container from hash and create new entry if not exists. */
	cont_entry = (ContCacheEntry *) hash_search(conn_entry->cont_hash, &cont_key, HASH_ENTER, &found);
	if (!found)
	{
		GSResult	ret;
		char	   *tablename = griddb_get_rel_name(relid);

		ret = gsGetContainerGeneral(store, tablename, &cont_entry->cont);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, store);

		if (cont_entry->cont == NULL)
			elog(ERROR, "griddb_fdw: No such container: %s", tablename);

		ret = gsSetAutoCommit(cont_entry->cont, GS_FALSE);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, cont_entry->cont);

		/*
		 * Transaction will be started automatically when container is
		 * accessed.
		 */
	}
	return cont_entry->cont;
}

/*
 * Release connection reference count created by calling GetConnection.
 */
void
griddb_release_connection(GSGridStore * store)
{
	/*
	 * Currently, we don't actually track connection references because all
	 * cleanup is managed on a transaction or subtransaction basis instead. So
	 * there's nothing to do here.
	 */
}

/*
 * cleanup_connection:
 * Delete all the cache entries on backend exists.
 */
void
griddb_cleanup_connection(void)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	if (ConnectionHash == NULL)
		return;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		if (entry->store == NULL)
			continue;

		elog(DEBUG3, "griddb_fdw: disconnecting griddb_fdw connection %p", entry->store);
		griddb_end_xact(entry, GS_FALSE, entry->store);
		gsCloseGridStore(&entry->store, GS_TRUE);
		entry->store = NULL;
	}
}

/*
 * Get an error from the remote server and append it to StringInfoData.
 *
 * gsResource: resource which error occurrs.
 */
void
griddb_error_message(void *gsResource, StringInfoData *str)
{
	size_t		stack_size = gsGetErrorStackSize(gsResource);
	size_t		i;
	size_t		buf_size = 0;
	GSChar	   *buf = NULL;

	for (i = 0; i < stack_size; i++)
	{
		size_t		msg_len;

		/* Get message length */
		msg_len = gsFormatErrorMessage(gsResource, i, NULL, 0);
		/* Allocate buffer and store error message */
		if (buf_size == 0)
		{
			buf = (GSChar *) palloc0(msg_len * sizeof(GSChar));
			buf_size = msg_len;
		}
		else if (buf_size < msg_len)
		{
			buf = (GSChar *) repalloc(buf, msg_len * sizeof(GSChar));
			buf_size = msg_len;
		}
		/* Store an error message */
		gsFormatErrorMessage(gsResource, i, buf, msg_len);
		appendStringInfo(str, "  %s\n", buf);
	}
	if (buf)
		pfree(buf);
}

/*
 * Report an error we got from the remote server.
 *
 * elevel: error level to use (typically ERROR, but might be less)
 * res: GSResult indicating the error number
 * gsResource: resource which error occurrs
 */
void
griddb_report(int elevel, GSResult res, void *gsResource, const char *fname, unsigned int line)
{
	StringInfoData buf;

	initStringInfo(&buf);
	if (log_min_messages <= DEBUG1) /* print line of code */
		appendStringInfo(&buf, "GridDB-API is failed by %d at %s: %d\n", res, fname, line);
	else
		appendStringInfo(&buf, "GridDB-API is failed by %d\n", res);

	griddb_error_message(gsResource, &buf);
	elog(elevel, "%s", buf.data);
}

/*
 * Start remote transaction. Transaction is started automatically.
 * So flag is set to true only.
 */
static void
griddb_begin_xact(ConnCacheEntry *entry)
{
	entry->xact_depth = 1;
	xact_got_connection = true;
}

/*
 * Commit all containers.
 * ConnCacheEntry has container list.
 * When accessing container, the list is updated by griddb_add_cont_list().
 */
static void
griddb_end_xact(ConnCacheEntry *entry, bool isCommit, GSGridStore * store)
{
	HASH_SEQ_STATUS scan;
	ContCacheEntry *cont_entry;

	if (entry->cont_hash == NULL)
		return;

	/*
	 * Scan all container cache entries to close remote transactions.
	 */
	hash_seq_init(&scan, entry->cont_hash);

	while ((cont_entry = (ContCacheEntry *) hash_seq_search(&scan)))
	{
		GSResult	ret;
		GSContainer *cont = cont_entry->cont;

		if (cont_entry->cont != NULL)
		{

			if (isCommit)
				ret = gsCommit(cont);
			else
				ret = gsAbort(cont);
			if (!GS_SUCCEEDED(ret))
				griddb_REPORT_ERROR(ERROR, ret, cont);

			gsCloseContainer(&cont, true);
		}

		/* Remove container from the hash list even if it is NULL or not */
		hash_search(entry->cont_hash, &cont_entry->key, HASH_REMOVE, NULL);
	}

	/* Destroy container hash list after use to avoid memory leak. */
	hash_destroy(entry->cont_hash);
	/* close store */
	entry->cont_hash = NULL;
	entry->xact_depth = 0;
}

/*
 * griddb_xact_callback --- cleanup at main-transaction end.
 */
static void
griddb_xact_callback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	/* Quick exit if no connections were touched in this transaction. */
	if (!xact_got_connection)
		return;

	/*
	 * Scan all connection cache entries to find open remote transactions, and
	 * close them.
	 */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore cache entry if no open connection right now */
		if (entry->store == NULL)
			continue;

		/* If it has an open remote transaction, try to close it */
		if (entry->xact_depth > 0)
		{
			/* If it has an open remote transaction, try to close it */
			elog(DEBUG3, "griddb_fdw: closing remote transaction on connection %p",
				 entry->store);

			switch (event)
			{
				case XACT_EVENT_PARALLEL_PRE_COMMIT:
				case XACT_EVENT_PRE_COMMIT:
					/* Commit all remote transactions during pre-commit */
					entry->changing_xact_state = true;
					griddb_end_xact(entry, true, entry->store);
					entry->changing_xact_state = false;
					break;
				case XACT_EVENT_PRE_PREPARE:

					/*
					 * We disallow remote transactions that modified anything,
					 * since it's not very reasonable to hold them open until
					 * the prepared transaction is committed.  For the moment,
					 * throw error unconditionally; later we might allow
					 * read-only cases.  Note that the error will cause us to
					 * come right back here with event == XACT_EVENT_ABORT, so
					 * we'll clean up the connection state at that point.
					 */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("griddb_fdw: cannot prepare a transaction that modified remote tables")));
					break;
				case XACT_EVENT_PARALLEL_COMMIT:
				case XACT_EVENT_COMMIT:
				case XACT_EVENT_PREPARE:
					/* Pre-commit should have closed the open transaction */
					elog(ERROR, "griddb_fdw: missed cleaning up connection during pre-commit");
					break;
				case XACT_EVENT_PARALLEL_ABORT:
				case XACT_EVENT_ABORT:
					griddb_abort_cleanup(entry, true);
					break;
			}
		}
		griddb_reset_xact_state(entry, true);
	}

	/*
	 * Regardless of the event type, we can now mark ourselves as out of the
	 * transaction.  (Note: if we are here during PRE_COMMIT or PRE_PREPARE,
	 * this saves a useless scan of the hashtable during COMMIT or PREPARE.)
	 */
	xact_got_connection = false;
}

/*
 * griddb_transaction_callback --- cleanup at main-transaction end.
 */
static void
griddb_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						SubTransactionId parentSubid, void *arg)
{
	elog(WARNING, "griddb_fdw: Subtransaction is not supported. So griddb_fdw do nothing.");
}

/*
 * Abort remote transaction or subtransaction.
 *
 * "toplevel" should be set to true if toplevel (main) transaction is
 * rollbacked, false otherwise.
 *
 * Set entry->changing_xact_state to false on success, true on failure.
 */
static void
griddb_abort_cleanup(ConnCacheEntry *entry, bool toplevel)
{
	if (toplevel)
	{
		/* If we're aborting, abort all remote transactions too */
		entry->changing_xact_state = true;
		griddb_end_xact(entry, false, entry->store);
		entry->changing_xact_state = false;
	}
}

/*
 * Reset state to show we're out of a (sub)transaction.
 */
static void
griddb_reset_xact_state(ConnCacheEntry *entry, bool toplevel)
{
	if (toplevel)
	{
		/* Reset state to show we're out of a transaction */
		entry->xact_depth = 0;

		/*
		 * If the connection isn't in a good idle state, it is marked as
		 * invalid or keep_connections option of its server is disabled, then
		 * discard it to recover. Next GetConnection will open a new
		 * connection.
		 */
		if (entry->changing_xact_state ||
			entry->invalidated ||
			!entry->keep_connections)
		{
			if (entry->store != NULL)
			{
				elog(DEBUG3, "griddb_fdw: discarding connection %p", entry->store);
				gsCloseGridStore(&entry->store, GS_TRUE);
				entry->store = NULL;
			}
		}
	}
}

/*
 * List active foreign server connections.
 *
 * This function takes no input parameter and returns setof record made of
 * following values:
 * - server_name - server name of active connection. In case the foreign server
 *   is dropped but still the connection is active, then the server name will
 *   be NULL in output.
 * - valid - true/false representing whether the connection is valid or not.
 * 	 Note that the connections can get invalidated in griddb_inval_callback.
 *
 * No records are returned when there are no cached connections at all.
 */
Datum
griddb_get_connections(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM < 140000
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Function: %s not support for Postgres %s", __func__, PG_VERSION)));
#else
#define GRIDDB_GET_CONNECTIONS_COLS	2
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
#if PG_VERSION_NUM < 150000
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
#endif
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

#if PG_VERSION_NUM > 150000
	InitMaterializedSRF(fcinfo, 0);
#elif PG_VERSION_NUM == 150000
	SetSingleFuncCall(fcinfo, 0);
#else
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("griddb_fdw: set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("griddb_fdw: materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "griddb_fdw: return type must be a row type");

	/* Build tuplestore to hold the result rows */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);
#endif

	/* If cache doesn't exist, we return no records */
	if (!ConnectionHash)
	{
#if PG_VERSION_NUM < 150000
		/* clean up and return the tuplestore */
		tuplestore_donestoring(tupstore);
#endif

		PG_RETURN_VOID();
	}

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		ForeignServer *server;
		Datum		values[GRIDDB_GET_CONNECTIONS_COLS];
		bool		nulls[GRIDDB_GET_CONNECTIONS_COLS];

		/* We only look for open remote connections */
		if (!entry->store)
			continue;

		server = GetForeignServerExtended(entry->serverid, FSV_MISSING_OK);

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		/*
		 * The foreign server may have been dropped in current explicit
		 * transaction. It is not possible to drop the server from another
		 * session when the connection associated with it is in use in the
		 * current transaction, if tried so, the drop query in another session
		 * blocks until the current transaction finishes.
		 *
		 * Even though the server is dropped in the current transaction, the
		 * cache can still have associated active connection entry, say we
		 * call such connections dangling. Since we can not fetch the server
		 * name from system catalogs for dangling connections, instead we show
		 * NULL value for server name in output.
		 *
		 * We could have done better by storing the server name in the cache
		 * entry instead of server oid so that it could be used in the output.
		 * But the server name in each cache entry requires 64 bytes of
		 * memory, which is huge, when there are many cached connections and
		 * the use case i.e. dropping the foreign server within the explicit
		 * current transaction seems rare. So, we chose to show NULL value for
		 * server name in output.
		 *
		 * Such dangling connections get closed either in next use or at the
		 * end of current explicit transaction in griddb_xact_callback.
		 */
		if (!server)
		{
			/*
			 * If the server has been dropped in the current explicit
			 * transaction, then this entry would have been invalidated in
			 * griddb_inval_callback at the end of drop server command. Note
			 * that this connection would not have been closed in
			 * griddb_inval_callback because it is still being used in the
			 * current explicit transaction. So, assert that here.
			 */
			Assert(entry->store && entry->xact_depth > 0 && entry->invalidated);

			/* Show null, if no server name was found */
			nulls[0] = true;
		}
		else
			values[0] = CStringGetTextDatum(server->servername);

		values[1] = BoolGetDatum(!entry->invalidated);

#if PG_VERSION_NUM >= 150000
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
#else
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
#endif
	}

#if PG_VERSION_NUM < 150000
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
#endif

	PG_RETURN_VOID();
#endif
}

#if PG_VERSION_NUM >= 140000
/*
 * Connection invalidation callback function
 *
 * After a change to a pg_foreign_server or pg_user_mapping catalog entry,
 * close connections depending on that entry immediately if current transaction
 * has not used those connections yet. Otherwise, mark those connections as
 * invalid and then make griddb_xact_callback() close them at the end of current
 * transaction, since they cannot be closed in the midst of the transaction
 * using them. Closed connections will be remade at the next opportunity if
 * necessary.
 *
 * Although most cache invalidation callbacks blow away all the related stuff
 * regardless of the given hashvalue, connections are expensive enough that
 * it's worth trying to avoid that.
 *
 * NB: We could avoid unnecessary disconnection more strictly by examining
 * individual option values, but it seems too much effort for the gain.
 */
static void
griddb_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	Assert(cacheid == FOREIGNSERVEROID || cacheid == USERMAPPINGOID);

	/* ConnectionHash must exist already, if we're registered */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore invalid entries */
		if (entry->store == NULL)
			continue;

		/* hashvalue == 0 means a cache reset, must clear all state */
		if (hashvalue == 0 ||
			(cacheid == FOREIGNSERVEROID &&
			 entry->server_hashvalue == hashvalue) ||
			(cacheid == USERMAPPINGOID &&
			 entry->mapping_hashvalue == hashvalue))
		{
			/*
			 * Close the connection immediately if it's not used yet in this
			 * transaction. Otherwise mark it as invalid so that
			 * griddb_xact_callback() can close it at the end of this
			 * transaction.
			 */
			if (entry->xact_depth == 0)
			{
				elog(DEBUG3, "griddb_fdw: discarding connection %p", entry->store);
				gsCloseGridStore(&entry->store, GS_TRUE);
				entry->store = NULL;
			}
			else
				entry->invalidated = true;
		}
	}
}
#endif

/*
 * Disconnect the specified cached connections.
 *
 * This function discards the open connections that are established by
 * griddb_fdw from the local session to the foreign server with
 * the given name. Note that there can be multiple connections to
 * the given server using different user mappings. If the connections
 * are used in the current local transaction, they are not disconnected
 * and warning messages are reported. This function returns true
 * if it disconnects at least one connection, otherwise false. If no
 * foreign server with the given name is found, an error is reported.
 */
Datum
griddb_disconnect(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM < 140000
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Function: %s not support for Postgres %s", __func__, PG_VERSION)));
#else
	ForeignServer *server;
	char	   *servername;

	servername = text_to_cstring(PG_GETARG_TEXT_PP(0));
	server = GetForeignServerByName(servername, false);

	PG_RETURN_BOOL(disconnect_cached_connections(server->serverid));
#endif
}

/*
 * Disconnect all the cached connections.
 *
 * This function discards all the open connections that are established by
 * griddb_fdw from the local session to the foreign servers.
 * If the connections are used in the current local transaction, they are
 * not disconnected and warning messages are reported. This function
 * returns true if it disconnects at least one connection, otherwise false.
 */
Datum
griddb_disconnect_all(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM < 140000
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Function: %s not support for Postgres %s", __func__, PG_VERSION)));
#else
	PG_RETURN_BOOL(disconnect_cached_connections(InvalidOid));
#endif
}

/*
 * Workhorse to disconnect cached connections.
 *
 * This function scans all the connection cache entries and disconnects
 * the open connections whose foreign server OID matches with
 * the specified one. If InvalidOid is specified, it disconnects all
 * the cached connections.
 *
 * This function emits a warning for each connection that's used in
 * the current transaction and doesn't close it. It returns true if
 * it disconnects at least one connection, otherwise false.
 *
 * Note that this function disconnects even the connections that are
 * established by other users in the same local session using different
 * user mappings. This leads even non-superuser to be able to close
 * the connections established by superusers in the same local session.
 *
 * XXX As of now we don't see any security risk doing this. But we should
 * set some restrictions on that, for example, prevent non-superuser
 * from closing the connections established by superusers even
 * in the same session?
 */
#if PG_VERSION_NUM >= 140000
static bool
disconnect_cached_connections(Oid serverid)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
	bool		all = !OidIsValid(serverid);
	bool		result = false;

	/*
	 * Connection cache hashtable has not been initialized yet in this
	 * session, so return false.
	 */
	if (!ConnectionHash)
		return false;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore cache entry if no open connection right now. */
		if (!entry->store)
			continue;

		if (all || entry->serverid == serverid)
		{
			/*
			 * Emit a warning because the connection to close is used in the
			 * current transaction and cannot be disconnected right now.
			 */
			if (entry->xact_depth > 0)
			{
				ForeignServer *server;

				server = GetForeignServerExtended(entry->serverid,
												  FSV_MISSING_OK);

				if (!server)
				{
					/*
					 * If the foreign server was dropped while its connection
					 * was used in the current transaction, the connection
					 * must have been marked as invalid by
					 * griddb_inval_callback at the end of DROP SERVER
					 * command.
					 */
					Assert(entry->invalidated);

					ereport(WARNING,
							(errmsg("griddb_fdw: cannot close dropped server connection because it is still in use")));
				}
				else
					ereport(WARNING,
							(errmsg("griddb_fdw: cannot close connection for server \"%s\" because it is still in use",
									server->servername)));
			}
			else
			{
				elog(DEBUG3, "griddb_fdw: discarding connection %p", entry->store);
				gsCloseGridStore(&entry->store, GS_TRUE);
				entry->store = NULL;
				result = true;
			}
		}
	}

	return result;
}
#endif
