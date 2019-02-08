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
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

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
	bool		have_error;		/* have any subxacts aborted in this xact? */
	HTAB	   *cont_hash;		/* used container list */
} ConnCacheEntry;

typedef Oid ContCacheKey;

/*
 * Container cache hash table entry
 *
 * We memorize contaners which are used in a transaction.
 */
typedef struct ContCacheEntry
{
	ConnCacheKey key;			/* hash key (must be first) */
	GSContainer *cont;
}			ContCacheEntry;

/*
 * Connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;

/* tracks whether any work is needed in callback functions */
static bool xact_got_connection = false;

/* prototypes of private functions */
static GSGridStore * griddb_connect_server(char *address, char *port,
										   char *cluster, char *user,
										   char *passwd);
static void griddb_begin_xact(ConnCacheEntry *entry);
static void griddb_end_xact(ConnCacheEntry *entry, bool isCommit,
				GSGridStore * store);
static void griddb_xact_callback(XactEvent event, void *arg);
static void griddb_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						SubTransactionId parentSubid, void *arg);

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
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		/*
		 * Register some callback functions that manage connection cleanup.
		 * This should be done just once in each backend.
		 */
		RegisterXactCallback(griddb_xact_callback, NULL);
		RegisterSubXactCallback(griddb_subxact_callback, NULL);
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
		entry->have_error = false;
		entry->cont_hash = NULL;
	}

	/*
	 * If cache entry doesn't have a connection, we have to establish a new
	 * connection.
	 */
	if (entry->store == NULL)
	{
		ForeignServer *server = GetForeignServer(user->serverid);

		/* Fetch the options */
		griddb_opt *opt = griddb_get_options(foreigntableid);

		entry->have_error = false;
		entry->store = griddb_connect_server(
											 opt->svr_address,
											 opt->svr_port,
											 opt->svr_clustername,
											 opt->svr_username,
											 opt->svr_password);
		elog(DEBUG3, "new griddb_fdw connection %p for server \"%s\""
			 " (user mapping oid %u, userid %u)", entry->store,
			 server->servername, user->umid, user->userid);
	}

	/*
	 * Start a new transaction if needed.
	 */
	griddb_begin_xact(entry);

	return entry->store;
}

/*
 * Connect to remote server using specified server and user mapping properties.
 */
static GSGridStore *
griddb_connect_server(char *address, char *port, char *cluster, char *user,
					  char *passwd)
{
	GSGridStore *store = NULL;
	const		GSPropertyEntry props[] = {
		{"notificationAddress", address},
		{"notificationPort", port},
		{"clusterName", cluster},
		{"user", user},
		{"password", passwd},
	};
	const size_t propCount = sizeof(props) / sizeof(*props);
	GSResult	ret;

	ret = gsGetGridStore(gsGetDefaultFactory(), props, propCount, &store);
	if (!GS_SUCCEEDED(ret))
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not connect to server \"%s\", port %s, cluster \"%s\"",
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
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ContCacheEntry);
		conn_entry->cont_hash = hash_create("griddb_fdw containers", 8,
											&ctl,
											HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	cont_key = relid;

	/* Search container from hash and create new entry if not exists. */
	cont_entry = (ContCacheEntry *) hash_search(conn_entry->cont_hash, &cont_key, HASH_ENTER, &found);
	if (!found)
	{
		GSResult	ret;
		GSContainer *cont;
		char	   *tablename = griddb_get_rel_name(relid);

		ret = gsGetContainerGeneral(store, tablename, &cont);
		pfree(tablename);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, store);

		if (cont == NULL)
			elog(ERROR, "No such container: %s", tablename);

		cont_entry->cont = cont;

		ret = gsSetAutoCommit(cont, GS_FALSE);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, cont);

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

		elog(DEBUG3, "disconnecting griddb_fdw connection %p", entry->store);
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
	appendStringInfo(&buf, "GridDB-API is failed by %d at %s: %d\n", res, fname, line);
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

		if (isCommit)
			ret = gsCommit(cont);
		else
			ret = gsAbort(cont);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, cont);

		gsCloseContainer(&cont, true);
		if (hash_search(entry->cont_hash, &cont_entry->key, HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "hash table corrupted");
	}
}

/*
 * griddb_transaction_callback --- cleanup at main-transaction end.
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
		elog(DEBUG3, "closing remote transaction on connection %p",
			 entry->store);

		switch (event)
		{
			case XACT_EVENT_PARALLEL_PRE_COMMIT:
			case XACT_EVENT_PRE_COMMIT:
				/* Commit all remote transactions during pre-commit */
				griddb_end_xact(entry, true, entry->store);
				break;
			case XACT_EVENT_PRE_PREPARE:

				/*
				 * We disallow remote transactions that modified anything,
				 * since it's not very reasonable to hold them open until the
				 * prepared transaction is committed.  For the moment, throw
				 * error unconditionally; later we might allow read-only
				 * cases.  Note that the error will cause us to come right
				 * back here with event == XACT_EVENT_ABORT, so we'll clean up
				 * the connection state at that point.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot prepare a transaction that modified remote tables")));
				break;
			case XACT_EVENT_PARALLEL_COMMIT:
			case XACT_EVENT_COMMIT:
			case XACT_EVENT_PREPARE:
				/* Pre-commit should have closed the open transaction */
				elog(ERROR, "missed cleaning up connection during pre-commit");
				break;
			case XACT_EVENT_PARALLEL_ABORT:
			case XACT_EVENT_ABORT:
				/* If we're aborting, abort all remote transactions too */
				griddb_end_xact(entry, false, entry->store);
				break;
		}
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
	elog(WARNING, "Subtransaction is not supported. So griddb_fdw do nothing.");
}
