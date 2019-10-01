/*
 * GridDB Foreign Data Wrapper
 *
 * Portions Copyright (c) 2018, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  griddb_fdw.c
 *
 */

#include "postgres.h"

#include "griddb_fdw.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "nodes/pg_list.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/elog.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 */
enum FdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwScanPrivateSelectSql,
	/* List of restriction clauses that can be executed remotely */
	FdwScanPrivateRemoteConds,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs,
	/* Integer representing UPDATE/DELETE target */
	FdwScanPrivateForUpdate,
};

/*
 * Similarly, this enum describes what's kept in the fdw_private list for
 * a ModifyTable node referencing a postgres_fdw foreign table.  We store:
 *
 * 1) INSERT/UPDATE/DELETE statement text to be sent to the remote server
 * 2) Integer list of target attribute numbers for INSERT/UPDATE
 *	  (NIL for a DELETE)
 */
enum FdwModifyPrivateIndex
{
	/* Integer list of target attribute numbers for INSERT/UPDATE */
	FdwModifyPrivateTargetAttnums,
};

/*
 * Similarly, this enum describes what's kept in the fdw_private list for
 * a ForeignScan node that modifies a foreign table directly.  We store:
 *
 * 1) UPDATE/DELETE statement text to be sent to the remote server
 * 2) Boolean flag showing if the remote query has a RETURNING clause
 * 3) Integer list of attribute numbers retrieved by RETURNING, if any
 * 4) Boolean flag showing if we set the command es_processed
 */
enum FdwDirectModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwDirectModifyPrivateUpdateSql,
	/* has-returning flag (as an integer Value node) */
	FdwDirectModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwDirectModifyPrivateRetrievedAttrs,
	/* set-processed flag (as an integer Value node) */
	FdwDirectModifyPrivateSetProcessed
};

/*
 * The following structures are used for sharing data between scaning
 * functions and modification functions.
 * In griddb_fdw, the data modification (UPDATE/DELETE) is done via rowset
 * which is created by ForeignScan. So rowset must be passed from ForeignScan
 * to ForeignModify.
 */
typedef Oid GridDBFdwSMRelayKey;	/* foreigntableid */
typedef struct GridDBFdwSMRelay
{
	GridDBFdwSMRelayKey key;	/* hash key (must be first) */
	GSRowSet   *row_set;		/* result set */
	GSRow	   *row;			/* row for the update */
	GridDBFdwFieldInfo field_info;	/* column information */
	Datum		rowkey_val;		/* rowkey the cursor is pointing */
}			GridDBFdwSMRelay;

static HTAB *griddb_sm_share = NULL;
static bool griddb_enable_partial_execution = false;

/*
 * Execution state of a foreign scan using griddb_fdw.
 */
typedef struct GridDBFdwScanState
{
	Relation	rel;			/* relcache entry for the foreign table. NULL
								 * for a foreign join scan. */
	TupleDesc	tupdesc;		/* tuple descriptor of scan */

	/* extracted fdw_private data */
	char	   *query;			/* text of SELECT command */
	List	   *retrieved_attrs;	/* list of retrieved attribute numbers */

	/* for remote query execution */
	GSGridStore *store;			/* connection for the scan */
	GSChar	   *cont_name;		/* container name */
	GSContainer *cont;			/* container to be selected */
	GSBool		for_update;		/* GS_TRUE if UPDATE/DELETE target */
	GridDBFdwFieldInfo field_info;	/* field information */
	GSRowSet   *row_set;		/* result set */
	GSRow	   *row;			/* row for the update */

	/* for storing result tuples */
	unsigned int cursor;		/* result set cursor pointing current index */

	/* for sharing data with ForeignModify */
	GridDBFdwSMRelay *smrelay;	/* cache of the relay */
}			GridDBFdwScanState;

/*
 * Execution state of a foreign insert/update/delete operation.
 */
typedef struct GridDBFdwModifyState
{
	Relation	rel;			/* relcache entry for the foreign table */

	/* for remote query execution */
	GSGridStore *store;			/* connection for the scan */
	GSChar	   *cont_name;		/* container name */
	GSContainer *cont;			/* container to be modified */

	/* extracted fdw_private data */
	List	   *target_attrs;	/* list of target attribute numbers */
	bool		bulk_mode;		/* true if UPDATE/DELETE targets are pointing
								 * different rows from result set cursor */
	AttrNumber	junk_att_no;	/* rowkey attribute number */
	HTAB	   *modified_rowkeys;	/* rowkey hash */
	GridDBFdwModifiedRows modified_rows;
	CmdType		operation;		/* INSERT, UPDATE, or DELETE */

	/* for sharing data with ForeignScan */
	GridDBFdwSMRelay *smrelay;	/* cache of the relay */
}			GridDBFdwModifyState;

/*
 * SQL functions
 */
extern Datum griddb_fdw_handler(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(griddb_fdw_handler);

void		_PG_init(void);
void		_PG_fini(void);

/*
 * FDW callback routines
 */

static void griddbGetForeignRelSize(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid);
static void griddbGetForeignPaths(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid);
static ForeignScan *griddbGetForeignPlan(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid,
					 ForeignPath *best_path,
					 List *tlist,
					 List *scan_clauses,
					 Plan *outer_plan);
static void griddbBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *griddbIterateForeignScan(ForeignScanState *node);
static void griddbReScanForeignScan(ForeignScanState *node);
static void griddbEndForeignScan(ForeignScanState *node);
static void griddbAddForeignUpdateTargets(Query *parsetree,
							  RangeTblEntry *target_rte,
							  Relation target_relation);
static List *griddbPlanForeignModify(PlannerInfo *root,
						ModifyTable *plan,
						Index resultRelation,
						int subplan_index);
static void griddbBeginForeignModify(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo,
						 List *fdw_private,
						 int subplan_index,
						 int eflags);
static TupleTableSlot *griddbExecForeignInsert(EState *estate,
						ResultRelInfo *resultRelInfo,
						TupleTableSlot *slot,
						TupleTableSlot *planSlot);
static TupleTableSlot *griddbExecForeignUpdate(EState *estate,
						ResultRelInfo *resultRelInfo,
						TupleTableSlot *slot,
						TupleTableSlot *planSlot);
static TupleTableSlot *griddbExecForeignDelete(EState *estate,
						ResultRelInfo *resultRelInfo,
						TupleTableSlot *slot,
						TupleTableSlot *planSlot);
static void griddbEndForeignModify(EState *estate,
					   ResultRelInfo *resultRelInfo);
#if (PG_VERSION_NUM >= 110000)
static void griddbEndForeignInsert(EState *estate,
					   ResultRelInfo *resultRelInfo);
static void griddbBeginForeignInsert(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo);
#endif
static int	griddbIsForeignRelUpdatable(Relation rel);
static bool griddbPlanDirectModify(PlannerInfo *root,
					   ModifyTable *plan,
					   Index resultRelation,
					   int subplan_index);
static void griddbExplainForeignScan(ForeignScanState *node,
						 ExplainState *es);
static void griddbExplainForeignModify(ModifyTableState *mtstate,
						   ResultRelInfo *rinfo,
						   List *fdw_private,
						   int subplan_index,
						   ExplainState *es);
static bool griddbAnalyzeForeignTable(Relation relation,
						  AcquireSampleRowsFunc *func,
						  BlockNumber *totalpages);
static List *griddbImportForeignSchema(ImportForeignSchemaStmt *stmt,
						  Oid serverOid);

/*
 * Helper functions
 */
static void griddb_fdw_exit(int code, Datum arg);
static void estimate_path_cost_size(PlannerInfo *root,
						RelOptInfo *baserel,
						List *join_conds,
						List *pathkeys,
						double *p_rows, int *p_width,
						Cost *p_startup_cost, Cost *p_total_cost);

static void griddb_make_column_info(GSContainerInfo * cont_info,
						GridDBFdwFieldInfo * field_info);
static void griddb_free_column_info(GridDBFdwFieldInfo * field_info);
static Oid	griddb_pgtyp_from_gstyp(GSType gs_type, const char **name);
static Timestamp griddb_convert_gs2pg_timestamp(GSTimestamp ts);
static GSTimestamp griddb_convert_pg2gs_timestamp(Timestamp dt);
static void griddb_execute_and_fetch(ForeignScanState *node);
static void griddb_find_junk_attno(GridDBFdwModifyState * fmstate, List *targetlist);
static void griddb_judge_bulk_mode(GridDBFdwModifyState * fmstate, TupleTableSlot *planSlot);
static void griddb_bind_for_putrow(GridDBFdwModifyState * fmstate,
					   TupleTableSlot *slot,
					   GSRow * row, Relation rel,
					   GridDBFdwFieldInfo * field_info);
static void griddb_add_column_name_and_type(StringInfoData *buf,
								GSContainerInfo * info);
static GSChar * *grifddb_name_list_dup(const GSChar * const *src,
									   size_t cont_size);
static void grifddb_name_list_free(GSChar * *p, size_t cont_size);
static void griddb_execute_commands(List *cmd_list);

static int set_transmission_modes();
static void reset_transmission_modes(int nestlevel);

void
_PG_init()
{
	on_proc_exit(&griddb_fdw_exit, PointerGetDatum(NULL));

	DefineCustomBoolVariable("griddbfdw.enable_partial_execution",
							 "enable partial execution",
							 NULL,
							 &griddb_enable_partial_execution,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}

/*
 * griddb_fdw_exit: Exit callback function.
 */
static void
griddb_fdw_exit(int code, Datum arg)
{
	griddb_cleanup_connection();
}

void
_PG_fini()
{
}

Datum
griddb_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = griddbGetForeignRelSize;
	routine->GetForeignPaths = griddbGetForeignPaths;
	routine->GetForeignPlan = griddbGetForeignPlan;
	routine->BeginForeignScan = griddbBeginForeignScan;
	routine->IterateForeignScan = griddbIterateForeignScan;
	routine->ReScanForeignScan = griddbReScanForeignScan;
	routine->EndForeignScan = griddbEndForeignScan;

	/* Functions for updating foreign tables */
	routine->AddForeignUpdateTargets = griddbAddForeignUpdateTargets;
	routine->PlanForeignModify = griddbPlanForeignModify;
	routine->BeginForeignModify = griddbBeginForeignModify;
	routine->ExecForeignInsert = griddbExecForeignInsert;
	routine->ExecForeignUpdate = griddbExecForeignUpdate;
	routine->ExecForeignDelete = griddbExecForeignDelete;
	routine->EndForeignModify = griddbEndForeignModify;
	routine->IsForeignRelUpdatable = griddbIsForeignRelUpdatable;
#if (PG_VERSION_NUM >= 110000)
	routine->BeginForeignInsert = griddbBeginForeignInsert;
	routine->EndForeignInsert = griddbEndForeignInsert;
#endif
	routine->PlanDirectModify = griddbPlanDirectModify;
	routine->BeginDirectModify = NULL;
	routine->IterateDirectModify = NULL;
	routine->EndDirectModify = NULL;

	/* Function for EvalPlanQual rechecks */
	routine->RecheckForeignScan = NULL;

	/* Support functions for EXPLAIN */
	routine->ExplainForeignScan = griddbExplainForeignScan;
	routine->ExplainForeignModify = griddbExplainForeignModify;
	routine->ExplainDirectModify = NULL;

	/* Support functions for ANALYZE */
	routine->AnalyzeForeignTable = griddbAnalyzeForeignTable;

	/* Support functions for IMPORT FOREIGN SCHEMA */
	routine->ImportForeignSchema = griddbImportForeignSchema;

	/* Not support functions for join push-down */
	routine->GetForeignJoinPaths = NULL;

	PG_RETURN_POINTER(routine);
}

/*
 * Get a hash entry of GridDBFdwScanModifyRelay corresponding to
 * the foreign table oid from a global hash variable.
 */
static GridDBFdwSMRelay *
griddb_get_smrelay(Oid foreigntableid)
{
	bool		found;
	GridDBFdwSMRelay *entry;
	GridDBFdwSMRelayKey key;

	/* First time through, initialize connection cache hashtable */
	if (griddb_sm_share == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(GridDBFdwSMRelayKey);
		ctl.entrysize = sizeof(GridDBFdwSMRelay);
		/* allocate ConnectionHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
		griddb_sm_share = hash_create("griddb_fdw scan modify relay", 8,
									  &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	/* Create hash key for the entry.  Assume no pad bytes in key struct */
	key = foreigntableid;

	/*
	 * Find or create cached entry for requested connection.
	 */
	entry = (GridDBFdwSMRelay *) hash_search(griddb_sm_share, &key, HASH_ENTER,
											 &found);
	if (!found)
	{
		/* initialize new hashtable entry (key is already filled in) */
		entry->row_set = NULL;
		entry->row = NULL;
		memset(&entry->field_info, 0, sizeof(GridDBFdwFieldInfo));
		entry->rowkey_val = 0;
	}

	return entry;
}

static void
griddb_close_smrelay(Oid foreigntableid)
{
	GridDBFdwSMRelayKey key = foreigntableid;

	Assert(griddb_sm_share);
	hash_search(griddb_sm_share, &key, HASH_REMOVE, NULL);
}

/*
 * griddbGetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 *
 * We should consider the effect of all baserestrictinfo clauses here, but
 * not any join clauses.
 */
static void
griddbGetForeignRelSize(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid)
{
	GriddbFdwRelationInfo *fpinfo;
	ListCell   *lc;
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	griddb_opt *options = NULL;

	/*
	 * We use PgFdwRelationInfo to pass various information to subsequent
	 * functions.
	 */
	fpinfo =
		(GriddbFdwRelationInfo *) palloc0(sizeof(GriddbFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(foreigntableid);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	/* Fetch options */
	options = griddb_get_options(foreigntableid);

	/*
	 * Extract user-settable option values.  Note that per-table setting of
	 * use_remote_estimate overrides per-server setting.
	 */
	fpinfo->use_remote_estimate = options->use_remote_estimate;
	fpinfo->fdw_startup_cost = options->fdw_startup_cost;
	fpinfo->fdw_tuple_cost = options->fdw_tuple_cost;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * identify which user to do remote access as during planning.  This
	 * should match what ExecCheckRTEPerms() does.  If we fail due to lack of
	 * permissions, the query would have failed at runtime anyway.
	 */
	if (fpinfo->use_remote_estimate)
	{
		Oid			userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

		fpinfo->user = GetUserMapping(userid, fpinfo->server->serverid);
	}
	else
		fpinfo->user = NULL;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	griddb_classify_conditions(root, baserel, baserel->baserestrictinfo,
							   &fpinfo->remote_conds, &fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */
	fpinfo->attrs_used = NULL;
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fpinfo->attrs_used);
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 baserel->relid,
													 JOIN_INNER,
													 NULL);

	/*
	 * Set cached relation costs to some negative value, so that we can detect
	 * when they are set to some sensible costs during one (usually the first)
	 * of the calls to estimate_path_cost_size().
	 */
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction clauses, as well as the
	 * average row width.  Otherwise, estimate using whatever statistics we
	 * have locally, in a way similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate)
	{
		ereport(ERROR, (errmsg("Remote estimation is unsupported")));
	}
	else
	{
		/*
		 * If the foreign table has never been ANALYZEd, it will have relpages
		 * and reltuples equal to zero, which most likely has nothing to do
		 * with reality.  We can't do a whole lot about that if we're not
		 * allowed to consult the remote server, but we can use a hack similar
		 * to plancat.c's treatment of empty relations: use a minimum size
		 * estimate of 10 pages, and divide by the column-datatype-based width
		 * estimate to get the corresponding number of tuples.
		 */
		if (baserel->pages == 0 && baserel->tuples == 0)
		{
			baserel->pages = 10;
			baserel->tuples =
				(10 * BLCKSZ) / (baserel->reltarget->width +
								 MAXALIGN(SizeofHeapTupleHeader));
		}

		/* Estimate baserel size as best we can with local statistics. */
		set_baserel_size_estimates(root, baserel);

		/* Fill in basically-bogus cost estimates for use later. */
		estimate_path_cost_size(root, baserel, NIL, NIL,
								&fpinfo->rows, &fpinfo->width,
								&fpinfo->startup_cost, &fpinfo->total_cost);
	}
}

/*
 * GetForeignPaths
 *		create access path for a scan on the foreign table
 */
static void
griddbGetForeignPaths(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid)
{
	GriddbFdwRelationInfo *fpinfo =
	(GriddbFdwRelationInfo *) baserel->fdw_private;
	ForeignPath *path;

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 */
	path = create_foreignscan_path(root, baserel,
								   NULL,	/* default pathtarget */
								   fpinfo->rows,
								   fpinfo->startup_cost,
								   fpinfo->total_cost,
								   NIL, /* no pathkeys */
								   NULL,	/* no outer rel either */
								   NULL,	/* no extra plan */
								   NIL);	/* no fdw_private list */
	add_path(baserel, (Path *) path);

	/*
	 * If we're not using remote estimates, stop here.  We have no way to
	 * estimate whether any join clauses would be worth sending across, so
	 * don't bother building parameterized paths.
	 */
	if (!fpinfo->use_remote_estimate)
		return;

	ereport(ERROR, (errmsg("Remote estimation is unsupported")));
}

/*
 * Force assorted GUC parameters to settings that ensure that we'll output
 * data values in a form that is unambiguous to the remote server.
 *
 * This is rather expensive and annoying to do once per row, but there's
 * little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting
 * user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * reset_transmission_modes() to undo things.
 */
static int
set_transmission_modes(void)
{
	int			nestlevel = NewGUCNestLevel();

	/*
	 * The values set here should match what pg_dump does.  See also
	 * configure_remote_session in connection.c.
	 */
	if (DateStyle != USE_ISO_DATES)
		(void) set_config_option("datestyle", "ISO",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (IntervalStyle != INTSTYLE_POSTGRES)
		(void) set_config_option("intervalstyle", "postgres",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (extra_float_digits < 3)
		(void) set_config_option("extra_float_digits", "3",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);

	return nestlevel;
}

/*
 * Undo the effects of set_transmission_modes().
 */
static void
reset_transmission_modes(int nestlevel)
{
	AtEOXact_GUC(true, nestlevel);
}

/*
 * GetForeignPlan
 *		Create ForeignScan plan node which implements selected best path
 */
static ForeignScan *
griddbGetForeignPlan(PlannerInfo *root,
					 RelOptInfo *foreignrel,
					 Oid foreigntableid,
					 ForeignPath *best_path,
					 List *tlist,
					 List *scan_clauses,
					 Plan *outer_plan)
{
	GriddbFdwRelationInfo *fpinfo =
	(GriddbFdwRelationInfo *) foreignrel->fdw_private;
	Index		scan_relid = foreignrel->relid;
	List	   *fdw_private;
	List	   *remote_conds = NIL;
	List	   *remote_exprs = NIL;
	List	   *local_exprs = NIL;
	List	   *params_list = NIL;
	List	   *retrieved_attrs;
	StringInfoData sql;
	ListCell   *lc;
	int			for_update = 0;
	int guc_level = 0;
	/*
	 * Separate the scan_clauses into those that can be executed remotely and
	 * those that can't.  baserestrictinfo clauses that were previously
	 * determined to be safe or unsafe by classifyConditions are shown in
	 * fpinfo->remote_conds and fpinfo->local_conds.  Anything else in the
	 * scan_clauses list will be a join clause, which we have to check for
	 * remote-safety.
	 *
	 * Note: the join clauses we see here should be the exact same ones
	 * previously examined by postgresGetForeignPaths.  Possibly it'd be worth
	 * passing forward the classification work done then, rather than
	 * repeating it here.
	 *
	 * This code must match "extract_actual_clauses(scan_clauses, false)"
	 * except for the additional decision about remote versus local execution.
	 * Note however that we don't strip the RestrictInfo nodes from the
	 * remote_conds list, since appendWhereClause expects a list of
	 * RestrictInfos.
	 */
	foreach(lc, scan_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		Assert(IsA(rinfo, RestrictInfo));

		/* Ignore any pseudoconstants, they're dealt with elsewhere */
		if (rinfo->pseudoconstant)
			continue;

		if (list_member_ptr(fpinfo->remote_conds, rinfo))
		{
			remote_conds = lappend(remote_conds, rinfo);
			remote_exprs = lappend(remote_exprs, rinfo->clause);
		}
		else if (list_member_ptr(fpinfo->local_conds, rinfo))
			local_exprs = lappend(local_exprs, rinfo->clause);
		else if (is_foreign_expr(root, foreignrel, rinfo->clause))
		{
			remote_conds = lappend(remote_conds, rinfo);
			remote_exprs = lappend(remote_exprs, rinfo->clause);
		}
		else
			local_exprs = lappend(local_exprs, rinfo->clause);
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	/* Deparse timestamp as ISO style */
	guc_level = set_transmission_modes();
	griddb_deparse_select(&sql, root, foreignrel, remote_conds,
						  best_path->path.pathkeys,
						  &retrieved_attrs, &params_list);
	reset_transmission_modes(guc_level);
	griddb_deparse_locking_clause(root, foreignrel, &for_update);

	if (foreignrel->relid == root->parse->resultRelation &&
		(root->parse->commandType == CMD_UPDATE ||
		 root->parse->commandType == CMD_DELETE))
	{
		/* Relation is UPDATE/DELETE target, so use FOR UPDATE */
		for_update = 1;
	}

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match order in enum FdwScanPrivateIndex.
	 */
	fdw_private = list_make4(makeString(sql.data),
							 remote_conds,
							 retrieved_attrs,
							 makeInteger(for_update));

	/*
	 * Create the ForeignScan node for the given relation.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							params_list,
							fdw_private,
							NULL,
							remote_exprs,
							outer_plan);
}

/*
 * BeginForeignScan
 *	 called during executor startup. perform any initialization
 *	 needed, but not start the actual scan.
 */

static void
griddbBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	GridDBFdwScanState *fsstate = NULL;
	RangeTblEntry *rte;
	Oid			userid;
	ForeignTable *table;
	UserMapping *user;
	int			rtindex;
	int			for_update;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	fsstate = (GridDBFdwScanState *) palloc0(sizeof(GridDBFdwScanState));
	node->fdw_state = (void *) fsstate;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.  In case of a join, use the lowest-numbered
	 * member RTE as a representative; we would get the same result from any.
	 */
	if (fsplan->scan.scanrelid > 0)
		rtindex = fsplan->scan.scanrelid;
	else
		rtindex = bms_next_member(fsplan->fs_relids, -1);
	rte = rt_fetch(rtindex, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	table = GetForeignTable(rte->relid);
	user = GetUserMapping(userid, table->serverid);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	fsstate->store = griddb_get_connection(user, false,
										   RelationGetRelid(node->ss.ss_currentRelation));

	fsstate->cont_name = griddb_get_rel_name(rte->relid);
	fsstate->cont = griddb_get_container(user, rte->relid, fsstate->store);

	fsstate->query = strVal(list_nth(fsplan->fdw_private,
									 FdwScanPrivateSelectSql));
	fsstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private,
												 FdwScanPrivateRetrievedAttrs);
	for_update = intVal(list_nth(fsplan->fdw_private,
								 FdwScanPrivateForUpdate));
	fsstate->for_update = for_update ? GS_TRUE : GS_FALSE;
	if (for_update)
		fsstate->smrelay = griddb_get_smrelay(rte->relid);
	fsstate->row_set = NULL;
	fsstate->row = NULL;
	fsstate->cursor = 0;

	/*
	 * Get info we'll need for converting data fetched from the foreign server
	 * into local representation and error reporting during that process.
	 */
	if (fsplan->scan.scanrelid > 0)
	{
		fsstate->rel = node->ss.ss_currentRelation;
		fsstate->tupdesc = RelationGetDescr(fsstate->rel);
	}
	else
	{
		fsstate->rel = NULL;
		fsstate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	}

	griddb_execute_and_fetch(node);
}

/*
 * IterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 *	 Fetch one row from the foreign source, returning it in a tuple table slot
 *	  (the node's ScanTupleSlot should be used for this purpose).
 *	Return NULL if no more rows are available.
 */
static TupleTableSlot *
griddbIterateForeignScan(ForeignScanState *node)
{
	GridDBFdwScanState *fsstate = (GridDBFdwScanState *) node->fdw_state;
	TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
	TupleDesc	tupleDescriptor = tupleSlot->tts_tupleDescriptor;

	memset(tupleSlot->tts_values, 0, sizeof(Datum) * tupleDescriptor->natts);
	memset(tupleSlot->tts_isnull, true, sizeof(bool) * tupleDescriptor->natts);
	ExecClearTuple(tupleSlot);

	/*
	 * Return the next tuple.
	 */
	if (gsHasNextRow(fsstate->row_set))
	{
		GSResult	ret = GS_RESULT_OK;
		ListCell   *lc = NULL;
		GSType	   *column_types = fsstate->field_info.column_types;

		Assert(gsHasNextRow(fsstate->row_set) == GS_TRUE);
		ret = gsGetNextRow(fsstate->row_set, fsstate->row);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, fsstate->row_set);

		/* Construct tuple slot */
		foreach(lc, fsstate->retrieved_attrs)
		{
			int			attnum = lfirst_int(lc);

			if (attnum > 0)
			{
				GSBool		isnull;

				ret = gsGetRowFieldNull(fsstate->row, attnum - 1, &isnull);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, fsstate->row);

				if (isnull == GS_TRUE)
				{
					tupleSlot->tts_isnull[attnum - 1] = true;
				}
				else
				{
					Oid			pgtype;
					Relation	rel = fsstate->rel;
					TupleDesc	tupdesc;

					if (rel)
						tupdesc = RelationGetDescr(rel);
					else
						tupdesc = fsstate->tupdesc;
					pgtype = TupleDescAttr(tupdesc, attnum - 1)->atttypid;
					tupleSlot->tts_values[attnum - 1] =
						griddb_make_datum_from_row(fsstate->row, attnum - 1,
												   column_types[attnum - 1],
												   pgtype);
					tupleSlot->tts_isnull[attnum - 1] = false;
				}
			}
		}
		ExecStoreVirtualTuple(tupleSlot);
		fsstate->cursor++;
	}

	/* Memorize rowkey value which the cursor is pointing. */
	if (fsstate->for_update)
		fsstate->smrelay->rowkey_val = tupleSlot->tts_values[ROWKEY_ATTNO - 1];

	return tupleSlot;
}

/*
 * ReScanForeignScan
 *		Restart the scan from the beginning
 */
static void
griddbReScanForeignScan(ForeignScanState *node)
{
	GridDBFdwScanState *fsstate = (GridDBFdwScanState *) node->fdw_state;

	/* If we haven't fetched the result set yet, nothing to do. */
	if (fsstate->row_set == NULL)
		return;

	fsstate->cursor = 0;
	griddb_free_column_info(&fsstate->field_info);
	gsCloseRow(&fsstate->row);
	gsCloseRowSet(&fsstate->row_set);

	/* Execute TQL again */
	griddb_execute_and_fetch(node);
}

/*
 * EndForeignScan
 *	End the scan and release resources.
 */
static void
griddbEndForeignScan(ForeignScanState *node)
{
	GridDBFdwScanState *fsstate = (GridDBFdwScanState *) node->fdw_state;

	/* if fsstate is NULL, we are in EXPLAIN; nothing to do */
	if (fsstate == NULL)
		return;

	gsCloseRow(&fsstate->row);
	gsCloseRowSet(&fsstate->row_set);

	griddb_free_column_info(&fsstate->field_info);
	if (fsstate->for_update)
	{
		griddb_free_column_info(&fsstate->smrelay->field_info);
		fsstate->smrelay = NULL;
	}

	/* Release remote connection */
	griddb_release_connection(fsstate->store);
	fsstate->store = NULL;

	/* MemoryContexts will be deleted automatically. */
}

/*
 * AddForeignUpdateTargets
 *	  Add resjunk column(s) needed for update/delete on a foreign table
 *	  We add first column forcibly. So we are adding that into target list.
 */
static void
griddbAddForeignUpdateTargets(Query *parsetree,
							  RangeTblEntry *target_rte,
							  Relation target_relation)
{
	Var		   *var = NULL;
	const char *attrname = NULL;
	TargetEntry *tle = NULL;

	/*
	 * What we need is the rowkey which is the first column
	 */
	Form_pg_attribute attr =
	TupleDescAttr(RelationGetDescr(target_relation), ROWKEY_ATTNO - 1);

	/* Make a Var representing the desired value */
	var = makeVar(parsetree->resultRelation,
				  ROWKEY_ATTNO,
				  attr->atttypid,
				  attr->atttypmod,
				  attr->attcollation,
				  0);

	/* Wrap it in a TLE with the right name ... */
	attrname = NameStr(attr->attname);

	tle = makeTargetEntry((Expr *) var,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname),
						  true);

	/* ... and add it to the query's targetlist */
	parsetree->targetList = lappend(parsetree->targetList, tle);
}

/*
 * postgresPlanForeignModify
 *		Plan an insert/update/delete operation on a foreign table
 */
static List *
griddbPlanForeignModify(PlannerInfo *root,
						ModifyTable *plan,
						Index resultRelation,
						int subplan_index)
{
	CmdType		operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation	rel;
	List	   *targetAttrs = NIL;

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

	/*
	 * In an INSERT, we transmit all columns that are defined in the foreign
	 * table.
	 */
	if (operation == CMD_INSERT ||
		(operation == CMD_UPDATE &&
		 rel->trigdesc &&
		 rel->trigdesc->trig_update_before_row))
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}

	/*
	 * In an UPDATE, we transmit only columns that were explicitly targets of
	 * the UPDATE, so as to avoid unnecessary data transmission. (We can't do
	 * that for INSERT since we would miss sending default values for columns
	 * not listed in the source statement.)
	 */
	else if (operation == CMD_UPDATE)
	{
		int			col;

		col = -1;
		while ((col = bms_next_member(rte->updatedCols, col)) >= 0)
		{
			/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber	attno = col + FirstLowInvalidHeapAttributeNumber;

			if (attno <= InvalidAttrNumber) /* shouldn't happen */
				elog(ERROR, "system-column update is not supported");

			if (attno == ROWKEY_ATTNO)
				elog(ERROR, "rowkey-column update is not supported");
			targetAttrs = lappend_int(targetAttrs, attno);
		}
	}

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		List	   *options;
		ListCell   *lc;
		bool		hasRowKey = false;

		/*
		 * Check rowkey option is set. This option is required to modify
		 * table. 1st column is rowkey column because of GridDB's
		 * specification.
		 */
		options = GetForeignColumnOptions(rte->relid, ROWKEY_ATTNO);
		foreach(lc, options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if ((strcmp(def->defname, OPTION_ROWKEY) == 0) && (defGetBoolean(def) == true))
				hasRowKey = true;
		}
		if (!hasRowKey)
			elog(ERROR, "Cannot modify a table if rowkey is not assigned to the 1st column.");
	}

	/*
	 * Cannot use RETURNING.
	 */
	if (plan->returningLists)
		elog(ERROR, "RETURNING is not supported by griddb_fdw");

	/*
	 * ON CONFLICT is not supported.
	 */
	if (plan->onConflictAction != ONCONFLICT_NONE)
		elog(ERROR, "ON CONFLICT is not supported");

	heap_close(rel, NoLock);

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwModifyPrivateIndex, above.
	 */
	return list_make1(targetAttrs);
}

static GridDBFdwModifyState *
create_foreign_modify(EState *estate,
					  RangeTblEntry *rte,
					  ResultRelInfo *resultRelInfo,
					  CmdType operation,
					  Plan *subplan,
					  List *target_attrs)
{
	GridDBFdwModifyState *fmstate;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	ForeignTable *table;
	UserMapping *user;
	Oid			serverOid;

	/* Begin constructing PgFdwModifyState. */
	fmstate = (GridDBFdwModifyState *) palloc0(sizeof(GridDBFdwModifyState));
	fmstate->rel = rel;

	/* Get info about foreign table. */
	table = GetForeignTable(RelationGetRelid(rel));
	serverOid = table->serverid;
	user = GetUserMapping(GetUserId(), serverOid);

	/* Open connection; report that we'll create a prepared statement. */
	fmstate->store = griddb_get_connection(user, false, serverOid);

	/* Set up remote query information. */
	fmstate->target_attrs = target_attrs;

	/* Get the row structure for gsPutRow */
	fmstate->cont_name = griddb_get_rel_name(rte->relid);
	fmstate->cont = griddb_get_container(user, rte->relid, fmstate->store);

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		griddb_find_junk_attno(fmstate, subplan->targetlist);
		griddb_modify_target_init(&fmstate->modified_rows, list_length(fmstate->target_attrs));
		fmstate->smrelay = griddb_get_smrelay(rte->relid);
		fmstate->modified_rowkeys = griddb_rowkey_hash_create(&fmstate->smrelay->field_info);
	}
	fmstate->operation = operation;

	return fmstate;
}

/*
 * griddbBeginForeignModify
 *		Begin an insert/update/delete operation on a foreign table
 */
static void
griddbBeginForeignModify(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo,
						 List *fdw_private,
						 int subplan_index,
						 int eflags)
{
	GridDBFdwModifyState *fmstate;
	EState	   *estate = mtstate->ps.state;
	List	   *target_attrs;
	RangeTblEntry *rte;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  resultRelInfo->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Deconstruct fdw_private data. */
	target_attrs = (List *) list_nth(fdw_private,
									 FdwModifyPrivateTargetAttnums);

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */
	rte = rt_fetch(resultRelInfo->ri_RangeTableIndex, estate->es_range_table);

	fmstate = create_foreign_modify(mtstate->ps.state,
									rte,
									resultRelInfo,
									mtstate->operation,
									mtstate->mt_plans[subplan_index]->plan,
									target_attrs);

	resultRelInfo->ri_FdwState = fmstate;
}

/*
 * griddbExecForeignInsert
 *		Insert one row into a foreign table
 */
static TupleTableSlot *
griddbExecForeignInsert(EState *estate,
						ResultRelInfo *resultRelInfo,
						TupleTableSlot *slot,
						TupleTableSlot *planSlot)
{
	GridDBFdwModifyState *fmstate =
	(GridDBFdwModifyState *) resultRelInfo->ri_FdwState;
	GSResult	ret;
	GSBool		exists;
	GSContainerInfo cont_info = GS_CONTAINER_INFO_INITIALIZER;
	GSRow	   *row;
	GridDBFdwFieldInfo field_info = {0};

	ret = gsCreateRowByContainer(fmstate->cont, &row);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, fmstate->cont);

	/* Get schema information for binding */
	ret = gsGetContainerInfo(fmstate->store, fmstate->cont_name, &cont_info,
							 &exists);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, fmstate->cont);
	Assert(exists == GS_TRUE);
	griddb_make_column_info(&cont_info, &field_info);

	/* Create row structure for gsPutRow */
	griddb_bind_for_putrow(fmstate, slot, row, resultRelInfo->ri_RelationDesc,
						   &field_info);
	griddb_free_column_info(&field_info);

	/* Insert row */
	ret = gsPutRow(fmstate->cont, NULL, row, &exists);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, fmstate->cont);

	if (exists)
		elog(WARNING, "row was updated instead of insert "
			 "because same row key has already existed.");

	gsCloseRow(&row);

	return slot;
}

/*
 * griddbExecForeignUpdate
 *		Update one row in a foreign table
 */
static TupleTableSlot *
griddbExecForeignUpdate(EState *estate,
						ResultRelInfo *resultRelInfo,
						TupleTableSlot *slot,
						TupleTableSlot *planSlot)
{
	GridDBFdwModifyState *fmstate =
	(GridDBFdwModifyState *) resultRelInfo->ri_FdwState;
	GSResult	ret;
	GridDBFdwSMRelay *smrelay = fmstate->smrelay;
	Datum		rowkey;
	bool		isnull;
	bool		found;
	GridDBFdwRowKeyHashEntry *rowket_hash_entry;

	/* Check if it is already modified or not. */
	rowkey = ExecGetJunkAttribute(planSlot, fmstate->junk_att_no, &isnull);
	Assert(isnull == false);
	rowket_hash_entry = griddb_rowkey_hash_search(fmstate->modified_rowkeys, rowkey, &found);
	if (found)
		return NULL;

	if (!fmstate->bulk_mode)
		griddb_judge_bulk_mode(fmstate, planSlot);

	if (fmstate->bulk_mode)
	{
		/*
		 * Memorize modified row information. They will be updated in
		 * griddbEndForeignModify.
		 */
		Datum		rowkey_datum = griddb_modify_target_insert(&fmstate->modified_rows, slot, planSlot, fmstate->junk_att_no, fmstate->target_attrs, &smrelay->field_info);

		griddb_rowkey_hash_set(rowket_hash_entry, rowkey_datum, NULL);
	}
	else
	{
		Form_pg_attribute attr;

		/* Create row structure for gsPutRow */
		griddb_bind_for_putrow(fmstate, slot, smrelay->row,
							   resultRelInfo->ri_RelationDesc,
							   &smrelay->field_info);

		/* Update row */
		ret = gsUpdateCurrentRow(smrelay->row_set, smrelay->row);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, fmstate->cont);

		attr = TupleDescAttr(planSlot->tts_tupleDescriptor, fmstate->junk_att_no - 1);
		griddb_rowkey_hash_set(rowket_hash_entry, rowkey, attr);
	}

	/* Return NULL if nothing was updated on the remote end */
	return slot;
}

/*
 * griddbExecForeignDelete
 *		Delete one row from a foreign table
 */
static TupleTableSlot *
griddbExecForeignDelete(EState *estate,
						ResultRelInfo *resultRelInfo,
						TupleTableSlot *slot,
						TupleTableSlot *planSlot)
{
	GridDBFdwModifyState *fmstate =
	(GridDBFdwModifyState *) resultRelInfo->ri_FdwState;
	GSResult	ret;
	GridDBFdwSMRelay *smrelay = fmstate->smrelay;
	Datum		rowkey;
	bool		isnull;
	bool		found;
	GridDBFdwRowKeyHashEntry *rowket_hash_entry;

	/* Check if it is already modified or not. */
	rowkey = ExecGetJunkAttribute(planSlot, fmstate->junk_att_no, &isnull);
	Assert(isnull == false);
	rowket_hash_entry = griddb_rowkey_hash_search(fmstate->modified_rowkeys, rowkey, &found);
	if (found)
		return NULL;

	if (!fmstate->bulk_mode)
		griddb_judge_bulk_mode(fmstate, planSlot);

	if (fmstate->bulk_mode)
	{
		/*
		 * Memorize modified row information. They will be deleted in
		 * griddbEndForeignModify.
		 */
		Datum		rowkey_datum = griddb_modify_target_insert(&fmstate->modified_rows, slot, planSlot, fmstate->junk_att_no, fmstate->target_attrs, &smrelay->field_info);

		griddb_rowkey_hash_set(rowket_hash_entry, rowkey_datum, NULL);
	}
	else
	{
		Form_pg_attribute attr;

		/* Delete row */
		ret = gsDeleteCurrentRow(smrelay->row_set);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, fmstate->cont);

		attr = TupleDescAttr(planSlot->tts_tupleDescriptor, fmstate->junk_att_no - 1);
		griddb_rowkey_hash_set(rowket_hash_entry, rowkey, attr);
	}

	/* Return NULL if nothing was deleted on the remote end */
	return slot;
}

/*
 * griddbEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
static void
griddbEndForeignModify(EState *estate,
					   ResultRelInfo *resultRelInfo)
{
	GridDBFdwModifyState *fmstate =
	(GridDBFdwModifyState *) resultRelInfo->ri_FdwState;

	/* If fmstate is NULL, we are in EXPLAIN; nothing to do */
	if (fmstate == NULL)
		return;

	if (fmstate->operation == CMD_UPDATE || fmstate->operation == CMD_DELETE)
	{
		GridDBFdwSMRelay *smrelay = fmstate->smrelay;
		TupleDesc	tupdesc = RelationGetDescr(fmstate->rel);
		Oid			pgkeytype = TupleDescAttr(tupdesc, ROWKEY_ATTNO - 1)->atttypid;

		griddb_modify_target_sort(&fmstate->modified_rows, &smrelay->field_info);
		griddb_modify_targets_apply(&fmstate->modified_rows, fmstate->cont_name, fmstate->cont, fmstate->target_attrs, &smrelay->field_info, pgkeytype, fmstate->operation);
		griddb_modify_target_fini(&fmstate->modified_rows);
		griddb_rowkey_hash_free(fmstate->modified_rowkeys);
		griddb_close_smrelay(fmstate->rel->rd_id);
		fmstate->smrelay = NULL;
	}

	/* Container will be closed by griddb_xact_callback */

	/* Release remote connection */
	griddb_release_connection(fmstate->store);
	fmstate->store = NULL;
}

#if (PG_VERSION_NUM >= 110000)
/*
 * griddBeginForeignInsert
 *		Begin an insert operation on a foreign table
 */
static void
griddbBeginForeignInsert(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo)
{
	GridDBFdwModifyState *fmstate;
	ModifyTable *plan = castNode(ModifyTable, mtstate->ps.plan);
	EState	   *estate = mtstate->ps.state;
	Index		resultRelation = resultRelInfo->ri_RangeTableIndex;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	RangeTblEntry *rte;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			attnum;
	StringInfoData sql;
	List	   *targetAttrs = NIL;

	initStringInfo(&sql);

	/* We transmit all columns that are defined in the foreign table. */
	for (attnum = 1; attnum <= tupdesc->natts; attnum++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

		if (!attr->attisdropped)
			targetAttrs = lappend_int(targetAttrs, attnum);
	}


	/* Check if we add the ON CONFLICT clause to the remote query. */
	if (plan)
	{
		OnConflictAction onConflictAction = plan->onConflictAction;

		/*
		 * Only DO UPDATE is supported for ON CONFLICT.
		 */
		if (onConflictAction == ONCONFLICT_NOTHING)
			elog(ERROR, "unsupported ON CONFLICT specification: %d",
				 (int) onConflictAction);
		else if (onConflictAction != ONCONFLICT_NONE)
			elog(ERROR, "unexpected ON CONFLICT specification: %d",
				 (int) onConflictAction);
	}

	/*
	 * If the foreign table is a partition, we need to create a new RTE
	 * describing the foreign table for use by deparseInsertSql and
	 * create_foreign_modify() below, after first copying the parent's RTE and
	 * modifying some fields to describe the foreign partition to work on.
	 * However, if this is invoked by UPDATE, the existing RTE may already
	 * correspond to this partition if it is one of the UPDATE subplan target
	 * rels; in that case, we can just use the existing RTE as-is.
	 */
	rte = (RangeTblEntry *) list_nth(estate->es_range_table, resultRelation - 1);
	if (rte->relid != RelationGetRelid(rel))
	{
		rte = (RangeTblEntry *) copyObject(rte);
		rte->relid = RelationGetRelid(rel);
		rte->relkind = RELKIND_FOREIGN_TABLE;

		/*
		 * For UPDATE, we must use the RT index of the first subplan target
		 * rel's RTE, because the core code would have built expressions for
		 * the partition, such as RETURNING, using that RT index as varno of
		 * Vars contained in those expressions.
		 */
		if (plan && plan->operation == CMD_UPDATE &&
			resultRelation == plan->nominalRelation)
			resultRelation = mtstate->resultRelInfo[0].ri_RangeTableIndex;
	}

	/* Construct an execution state. */
	fmstate = create_foreign_modify(mtstate->ps.state,
									rte,
									resultRelInfo,
									CMD_INSERT,
									NULL,
									targetAttrs);

	resultRelInfo->ri_FdwState = fmstate;
}

/*
 * griddbEndForeignInsert
 *		Finish an insert operation on a foreign table
 */
static void
griddbEndForeignInsert(EState *estate,
					   ResultRelInfo *resultRelInfo)
{
	GridDBFdwModifyState *fmstate = (GridDBFdwModifyState *) resultRelInfo->ri_FdwState;

	elog(DEBUG1, "griddb_fdw: %s for %s", __FUNCTION__, fmstate->cont_name);

	Assert(fmstate != NULL);

	/* Release remote connection */
	griddb_release_connection(fmstate->store);
	fmstate->store = NULL;
}
#endif

/*
 * griddbIsForeignRelUpdatable
 *		Determine whether a foreign table supports INSERT, UPDATE and/or
 *		DELETE.
 */
static int
griddbIsForeignRelUpdatable(Relation rel)
{
	bool		updatable;
	ForeignTable *table;
	ForeignServer *server;
	ListCell   *lc;

	/*
	 * By default, all griddb_fdw foreign tables are assumed updatable. This
	 * can be overridden by a per-server setting, which in turn can be
	 * overridden by a per-table setting.
	 */
	updatable = true;

	table = GetForeignTable(RelationGetRelid(rel));
	server = GetForeignServer(table->serverid);

	foreach(lc, server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, OPTION_UPDATABLE) == 0)
			updatable = defGetBoolean(def);
	}
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, OPTION_UPDATABLE) == 0)
			updatable = defGetBoolean(def);
	}

	/*
	 * Currently "updatable" means support for INSERT, UPDATE and DELETE.
	 */
	return updatable ?
		(1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE) : 0;
}

/*
 * griddbPlanDirectModify
 *		Consider a direct foreign table modification
 *
 * Decide whether it is safe to modify a foreign table directly, and if so,
 * rewrite subplan accordingly.
 */
static bool
griddbPlanDirectModify(PlannerInfo *root,
					   ModifyTable *plan,
					   Index resultRelation,
					   int subplan_index)
{
	/* Always return false because GridDB is not suport UPDATE/DELETE command */
	return false;
}

/*
 * griddbExplainForeignScan
 *		Produce extra output for EXPLAIN of a ForeignScan on a foreign table
 */
static void
griddbExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	List	   *fdw_private;
	char	   *sql;

	fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;

	/*
	 * Add remote query, when VERBOSE option is specified.
	 */
	if (es->verbose)
	{
		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
		ExplainPropertyText("Remote SQL", sql, es);
	}
}

/*
 * griddbExplainForeignModify
 *		Produce extra output for EXPLAIN of a ModifyTable on a foreign table
 */
static void
griddbExplainForeignModify(ModifyTableState *mtstate,
						   ResultRelInfo *rinfo,
						   List *fdw_private,
						   int subplan_index,
						   ExplainState *es)
{
	/* Not support now. */
}

/*
 * griddbAnalyzeForeignTable
 *		Test whether analyzing this foreign table is supported
 */
static bool
griddbAnalyzeForeignTable(Relation relation,
						  AcquireSampleRowsFunc *func,
						  BlockNumber *totalpages)
{
	/* Not support now. */
	return false;
}


/*
 * Import a foreign schema
 * "recreate" option can be used.
 * IMPORT FOREIGN SCHEMA ... OPTIONS (recreate 'true')
 * If foreign table already exists, it is droppped and recreated.
 * The default is 'false'.
 *
 * DROP commands are executed by SPI.
 * SIP_connect() creates new memory context and switch to it.
 * And SPI_finish() destorys it. We have to take care of memory
 * allocation. Firstly, we store DROP commands into LIST. After
 * LIST is completed, we execute commands by SPI.
 */
static List *
griddbImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
	List	   *commands = NIL;
	List	   *commands_drop = NIL;
	bool		recreate = false;
	ForeignServer *server;
	UserMapping *mapping;
	GSGridStore *store;
	StringInfoData buf;
	GSResult	ret;
	GSPartitionController *controller;
	int32_t		partition_count;
	int32_t		part_idx;
	ListCell   *lc;

	/* Parse statement options */
	foreach(lc, stmt->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, OPTION_RECREATE) == 0)
			recreate = defGetBoolean(def);
		else
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname)));
	}

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	server = GetForeignServer(serverOid);
	mapping = GetUserMapping(GetUserId(), server->serverid);
	store = griddb_get_connection(mapping, false, serverOid);

	/* Create workspace for strings */
	initStringInfo(&buf);

	ret = gsGetPartitionController(store, &controller);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, store);

	ret = gsGetPartitionCount(controller, &partition_count);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, controller);

	/* We can get container information for each partion */
	for (part_idx = 0; part_idx < partition_count; part_idx++)
	{
		int			i;
		const		GSChar *const *name_list = NULL;
		GSChar	  **cont_name_list;
		size_t		cont_size;

		/* Get container information */
		ret = gsGetPartitionContainerNames(controller, part_idx, 0, NULL,
										   &name_list, &cont_size);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, controller);

		/*
		 * gsGetPartitionContainerNames uses a temporary memory area in
		 * GridDB. When gsGetContainerInfo is called, it might be freed. So we
		 * need to copy name_list.
		 */
		cont_name_list = grifddb_name_list_dup(name_list, cont_size);

		/* Construct "CREATE FOREIGN table" statement for each container */
		for (i = 0; i < cont_size; i++)
		{
			GSContainerInfo info = GS_CONTAINER_INFO_INITIALIZER;
			GSBool		exists;
			const		GSChar *cont_name = cont_name_list[i];

			/* Get schema of container */
			ret = gsGetContainerInfo(store, cont_name, &info, &exists);
			if (!GS_SUCCEEDED(ret))
				griddb_REPORT_ERROR(ERROR, ret, store);
			Assert(exists == GS_TRUE);

			resetStringInfo(&buf);
			if (recreate)
			{
				appendStringInfo(&buf, "DROP FOREIGN TABLE IF EXISTS %s",
								 quote_identifier(cont_name));
				commands_drop = lappend(commands_drop, pstrdup(buf.data));
				resetStringInfo(&buf);
				appendStringInfo(&buf, "CREATE FOREIGN TABLE %s(",
								 quote_identifier(cont_name));
			}
			else
			{
				appendStringInfo(&buf, "CREATE FOREIGN TABLE IF NOT EXISTS %s(",
								 quote_identifier(cont_name));
			}
			/* Add column name and type */
			griddb_add_column_name_and_type(&buf, &info);

			/*
			 * Add server name and table-level options.  We specify remote
			 * schema and table name as options (the latter to ensure that
			 * renaming the foreign table doesn't break the association).
			 */
			appendStringInfo(&buf, ") SERVER %s;",
							 quote_identifier(server->servername));

			commands = lappend(commands, pstrdup(buf.data));
		}
		grifddb_name_list_free(cont_name_list, cont_size);
	}

	griddb_release_connection(store);
	if (recreate)
	{
		griddb_execute_commands(commands_drop);
		list_free_deep(commands_drop);
	}

	return commands;
}

/*
 * estimate_path_cost_size
 *		Get cost and size estimates for a foreign scan on given foreign relation
 *		either a base relation or a join between foreign relations.
 *
 * param_join_conds are the parameterization clauses with outer relations.
 * pathkeys specify the expected sort order if any for given path being costed.
 *
 * The function returns the cost and size estimates in p_row, p_width,
 * p_startup_cost and p_total_cost variables.
 */
static void
estimate_path_cost_size(PlannerInfo *root,
						RelOptInfo *foreignrel,
						List *param_join_conds,
						List *pathkeys,
						double *p_rows, int *p_width,
						Cost *p_startup_cost, Cost *p_total_cost)
{
	GriddbFdwRelationInfo *fpinfo =
	(GriddbFdwRelationInfo *) foreignrel->fdw_private;
	double		rows;
	double		retrieved_rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	Cost		cpu_per_tuple;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction+join clauses.  Otherwise,
	 * estimate rows using whatever statistics we have locally, in a way
	 * similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate)
	{
		ereport(ERROR, (errmsg("Remote estimation is unsupported")));
	}
	else
	{
		Cost		run_cost = 0;

		/*
		 * We don't support join conditions in this mode (hence, no
		 * parameterized paths can be made).
		 */
		Assert(param_join_conds == NIL);

		/*
		 * Use rows/width estimates made by set_baserel_size_estimates() for
		 * base foreign relations and set_joinrel_size_estimates() for join
		 * between foreign relations.
		 */
		rows = foreignrel->rows;
		width = foreignrel->reltarget->width;

		/* Back into an estimate of the number of retrieved rows. */
		retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);

		/*
		 * We will come here again and again with different set of pathkeys
		 * that caller wants to cost. We don't need to calculate the cost of
		 * bare scan each time. Instead, use the costs if we have cached them
		 * already.
		 */
		if (fpinfo->rel_startup_cost > 0 && fpinfo->rel_total_cost > 0)
		{
			startup_cost = fpinfo->rel_startup_cost;
			run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;
		}
		else
		{
			Assert(foreignrel->reloptkind != RELOPT_JOINREL);
			/* Clamp retrieved rows estimates to at most foreignrel->tuples. */
			retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

			/*
			 * Cost as though this were a seqscan, which is pessimistic.  We
			 * effectively imagine the local_conds are being evaluated
			 * remotely, too.
			 */
			startup_cost = 0;
			run_cost = 0;
			run_cost += seq_page_cost * foreignrel->pages;

			startup_cost += foreignrel->baserestrictcost.startup;
			cpu_per_tuple =
				cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
			run_cost += cpu_per_tuple * foreignrel->tuples;
		}

		/*
		 * Without remote estimates, we have no real way to estimate the cost
		 * of generating sorted output.  It could be free if the query plan
		 * the remote side would have chosen generates properly-sorted output
		 * anyway, but in most cases it will cost something.  Estimate a value
		 * high enough that we won't pick the sorted path when the ordering
		 * isn't locally useful, but low enough that we'll err on the side of
		 * pushing down the ORDER BY clause when it's useful to do so.
		 */
		if (pathkeys != NIL)
		{
			startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
			run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
		}

		total_cost = startup_cost + run_cost;
	}

	/*
	 * Cache the costs for scans without any pathkeys or parameterization
	 * before adding the costs for transferring data from the foreign server.
	 * These costs are useful for costing the join between this relation and
	 * another foreign relation or to calculate the costs of paths with
	 * pathkeys for this relation, when the costs can not be obtained from the
	 * foreign server. This function will be called at least once for every
	 * foreign relation without pathkeys and parameterization.
	 */
	if (pathkeys == NIL && param_join_conds == NIL)
	{
		fpinfo->rel_startup_cost = startup_cost;
		fpinfo->rel_total_cost = total_cost;
	}

	/*
	 * Add some additional cost factors to account for connection overhead
	 * (fdw_startup_cost), transferring data across the network
	 * (fdw_tuple_cost per retrieved row), and local manipulation of the data
	 * (cpu_tuple_cost per retrieved row).
	 */
	startup_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
	total_cost += cpu_tuple_cost * retrieved_rows;

	/* Return results. */
	*p_rows = rows;
	*p_width = width;
	*p_startup_cost = startup_cost;
	*p_total_cost = total_cost;
}

/*
 * Find an equivalence class member expression, all of whose Vars, come from
 * the indicated relation.
 */
extern Expr *
find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell   *lc_em;

	foreach(lc_em, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc_em);

		if (bms_is_subset(em->em_relids, rel->relids))
		{
			/*
			 * If there is more than one equivalence member whose Vars are
			 * taken entirely from this relation, we'll be content to choose
			 * any one of those.
			 */
			return em->em_expr;
		}
	}

	/* We didn't find any suitable equivalence class expression */
	return NULL;
}

/*
 * Store column count and column names and column types into
 * output variable. This is used because gsGetContainerInfo
 * uses a temporary memory area in GridDB.
 * When the other GridDB API is called, it might be freed.
 * So we need to copy container information to fdw local memory.
 */
static void
griddb_make_column_info(GSContainerInfo * cont_info,
						GridDBFdwFieldInfo * field_info)
{
	size_t		i;

	field_info->column_count = cont_info->columnCount;

	field_info->column_names =
		(GSChar * *) palloc0(sizeof(GSChar *) * cont_info->columnCount);
	for (i = 0; i < cont_info->columnCount; i++)
		field_info->column_names[i] =
			pstrdup(cont_info->columnInfoList[i].name);

	field_info->column_types =
		(GSType *) palloc0(sizeof(GSType) * cont_info->columnCount);
	for (i = 0; i < cont_info->columnCount; i++)
		field_info->column_types[i] = cont_info->columnInfoList[i].type;
}

static void
griddb_free_column_info(GridDBFdwFieldInfo * field_info)
{
	size_t		i;

	for (i = 0; i < field_info->column_count; i++)
		pfree(field_info->column_names[i]);
	if (field_info->column_names != NULL)
		pfree(field_info->column_names);
	if (field_info->column_types)
		pfree(field_info->column_types);
	memset(field_info, 0, sizeof(GridDBFdwFieldInfo));
}

/*
 * Return PG type for GridDB data type
 */
static Oid
griddb_pgtyp_from_gstyp(GSType gs_type, const char **pg_name)
{
	Oid			pg_type;
	const char *name;

	switch (gs_type)
	{
		case GS_TYPE_STRING:
			pg_type = TEXTOID;
			name = "text";
			break;

		case GS_TYPE_BOOL:
			pg_type = BOOLOID;
			name = "boolean";
			break;

		case GS_TYPE_BYTE:
		case GS_TYPE_SHORT:
			pg_type = INT2OID;
			name = "smallint";
			break;

		case GS_TYPE_INTEGER:
			pg_type = INT4OID;
			name = "integer";
			break;

		case GS_TYPE_LONG:
			pg_type = INT8OID;
			name = "bigint";
			break;

		case GS_TYPE_FLOAT:
			pg_type = FLOAT4OID;
			name = "real";
			break;

		case GS_TYPE_DOUBLE:
			pg_type = FLOAT8OID;
			name = "double precision";
			break;

		case GS_TYPE_TIMESTAMP:
			pg_type = TIMESTAMPOID;
			name = "timestamp";
			break;

		case GS_TYPE_GEOMETRY:
			pg_type = TEXTOID;
			name = "text";
			break;

		case GS_TYPE_BLOB:
			pg_type = BYTEAOID;
			name = "bytea";
			break;

		case GS_TYPE_STRING_ARRAY:
			pg_type = TEXTARRAYOID;
			name = "text[]";
			break;

		case GS_TYPE_BOOL_ARRAY:
			pg_type = GSFDW_BOOLARRAYOID;
			name = "boolean[]";
			break;

		case GS_TYPE_BYTE_ARRAY:
		case GS_TYPE_SHORT_ARRAY:
			pg_type = INT2ARRAYOID;
			name = "smallint[]";
			break;

		case GS_TYPE_INTEGER_ARRAY:
			pg_type = INT4ARRAYOID;
			name = "integer[]";
			break;

		case GS_TYPE_LONG_ARRAY:
			pg_type = GSFDW_INT8ARRAYOID;
			name = "bigint[]";
			break;

		case GS_TYPE_FLOAT_ARRAY:
			pg_type = FLOAT4ARRAYOID;
			name = "real[]";
			break;

		case GS_TYPE_DOUBLE_ARRAY:
			pg_type = GSFDW_FLOAT8ARRAYOID;
			name = "double precision[]";
			break;

		case GS_TYPE_TIMESTAMP_ARRAY:
			pg_type = GSFDW_TIMESTAMPARRAYOID;
			name = "timestamp[]";
			break;

		default:
			elog(ERROR, "cannot convert from GSType %d", gs_type);
	}

	if (pg_name)
		*pg_name = name;

	return pg_type;
}

static Timestamp
griddb_convert_gs2pg_timestamp(GSTimestamp ts)
{
	char		buf[MAXDATELEN + 1];
	struct pg_tm tm;
	fsec_t		fsec;			/* Micro seconds */
	Timestamp	timestamp;

	gsFormatTime(ts, buf, sizeof(buf));
	sscanf(buf, "%4d-%2d-%2dT%2d:%2d:%2d.%3dZ", &tm.tm_year, &tm.tm_mon,
		   &tm.tm_mday, &tm.tm_hour, &tm.tm_min, &tm.tm_sec, &fsec);
	fsec *= 1000;				/* Because of micro second */

	if (tm2timestamp(&tm, fsec, NULL, &timestamp) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	return timestamp;
}


/* Convert to GridDB timestamp format as a string.
 * USE_XSD_DATES format is YYYY-MM-DDThh:mm:ss.SSSSSS.
 *	 After the decimal point is optional.
 * GridDB format is YYYY-MM-DDThh:mm:ss.SSSZ.
 *	 After the decimal point is optional but always 3 digits.
 *
 * The 2nd argument is the output. The buffer must be allocated
 * outside of this function. It requires MAXDATELEN+1.
 */
void
griddb_convert_pg2gs_timestamp_string(Timestamp dt, char *buf)
{
	struct pg_tm tm;
	fsec_t		fsec;			/* Micro seconds */

	if (timestamp2tm(dt, NULL, &tm, &fsec, NULL, NULL) == 0)
		EncodeDateTime(&tm, fsec, false, 0, NULL, USE_XSD_DATES, buf);
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	/* If there is the decimal point, converts to 3 disits */
	if (buf[19] == '.')
	{
		/* YYYY-MM-DDThh:mm:ss.S -> YYYY-MM-DDThh:mm:ss.S0 */
		if (buf[21] == '\0')
			buf[21] = '0';

		/* YYYY-MM-DDThh:mm:ss.SS -> YYYY-MM-DDThh:mm:ss.SS0 */
		if (buf[22] == '\0')
			buf[22] = '0';
	}
	strcat(buf, "Z");
}

/* Convert from Timestamp to GSTimestamp.
 */
static GSTimestamp
griddb_convert_pg2gs_timestamp(Timestamp dt)
{
	char		buf[MAXDATELEN + 1] = {0};
	GSResult	ret;
	GSTimestamp timestampVal;

	griddb_convert_pg2gs_timestamp_string(dt, buf);

	ret = gsParseTime(buf, &timestampVal);
	if (ret != GS_TRUE)
		elog(ERROR, "Failed to convert Timestamp to GSTimestamp: %s", buf);

	return timestampVal;
}

/*
 * Search system cache and get types for output function.
 * They are used for data type conversion.
 */
static void
griddb_get_datatype_for_convertion(Oid pg_type, regproc *typeinput,
								   int *typemod)
{
	HeapTuple	hptuple;

	hptuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pg_type));
	if (!HeapTupleIsValid(hptuple))
		elog(ERROR, "cache lookup failed for type%u", pg_type);

	*typeinput = ((Form_pg_type) GETSTRUCT(hptuple))->typinput;
	*typemod = ((Form_pg_type) GETSTRUCT(hptuple))->typtypmod;
	ReleaseSysCache(hptuple);
}

Datum
griddb_make_datum_from_row(GSRow * row, int32_t attid, GSType gs_type,
						   Oid pg_type)
{
	GSResult	ret;
	regproc		typeinput;
	int			typemod;
	Datum		value_datum;	/* Temporary datum */
	Datum	   *value_datums;	/* Temporary datum for array */
	size_t		size;
	size_t		i;
	ArrayType  *arry;
	Datum		valueDatum = 0;

	if (pg_type != griddb_pgtyp_from_gstyp(gs_type, NULL))
		elog(ERROR, "Type conversion mismatch");

	switch (gs_type)
	{
		case GS_TYPE_STRING:
			{
				const		GSChar *strVal;

				griddb_get_datatype_for_convertion(pg_type, &typeinput, &typemod);
				ret = gsGetRowFieldAsString(row, attid, &strVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				value_datum = CStringGetDatum((char *) strVal);
				valueDatum = OidFunctionCall3(typeinput, value_datum,
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(typemod));
				break;
			}

		case GS_TYPE_GEOMETRY:
			{
				const		GSChar *strVal;

				griddb_get_datatype_for_convertion(pg_type, &typeinput, &typemod);
				ret = gsGetRowFieldAsGeometry(row, attid, &strVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				value_datum = CStringGetDatum((char *) strVal);
				valueDatum = OidFunctionCall3(typeinput, value_datum,
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(typemod));
				break;
			}

		case GS_TYPE_BOOL:
			{
				GSBool		boolVal;

				ret = gsGetRowFieldAsBool(row, attid, &boolVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				valueDatum = BoolGetDatum(boolVal);
				break;
			}

		case GS_TYPE_BYTE:
			{
				int8_t		byteVal;

				ret = gsGetRowFieldAsByte(row, attid, &byteVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				valueDatum = Int16GetDatum((int16_t)byteVal);
				break;
			}

		case GS_TYPE_SHORT:
			{
				int16_t		shortVal;

				ret = gsGetRowFieldAsShort(row, attid, &shortVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				valueDatum = Int16GetDatum(shortVal);
				break;
			}

		case GS_TYPE_INTEGER:
			{
				int32_t		intVal;

				ret = gsGetRowFieldAsInteger(row, attid, &intVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				valueDatum = Int32GetDatum(intVal);
				break;
			}

		case GS_TYPE_LONG:
			{
				int64_t		longVal;

				ret = gsGetRowFieldAsLong(row, attid, &longVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				valueDatum = Int64GetDatum(longVal);
				break;
			}

		case GS_TYPE_FLOAT:
			{
				float		floatVal;

				ret = gsGetRowFieldAsFloat(row, attid, &floatVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				valueDatum = Float4GetDatum(floatVal);
				break;
			}

		case GS_TYPE_DOUBLE:
			{
				double		doubleVal;

				ret = gsGetRowFieldAsDouble(row, attid, &doubleVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				valueDatum = Float8GetDatum(doubleVal);
				break;
			}

		case GS_TYPE_TIMESTAMP:
			{
				GSTimestamp tsVal;
				Timestamp	timestamp;

				ret = gsGetRowFieldAsTimestamp(row, attid, &tsVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				timestamp = griddb_convert_gs2pg_timestamp(tsVal);
				valueDatum = TimestampGetDatum(timestamp);
				break;
			}

		case GS_TYPE_BLOB:
			{
				GSBlob		blobVal;

				ret = gsGetRowFieldAsBlob(row, attid, &blobVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				valueDatum = (Datum) palloc0(blobVal.size + VARHDRSZ);
				memcpy(VARDATA(valueDatum), blobVal.data, blobVal.size);
				SET_VARSIZE(valueDatum, blobVal.size + VARHDRSZ);
				break;
			}

		case GS_TYPE_STRING_ARRAY:
			{
				const		GSChar *const *strVal;

				griddb_get_datatype_for_convertion(TEXTOID, &typeinput, &typemod);
				ret = gsGetRowFieldAsStringArray(row, attid, &strVal, &size);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				value_datums = (Datum *) palloc0(sizeof(Datum) * size);
				for (i = 0; i < size; i++)
				{
					value_datum = CStringGetDatum(strVal[i]);
					value_datums[i] = OidFunctionCall3(typeinput, value_datum,
													   ObjectIdGetDatum(InvalidOid),
													   Int32GetDatum(typemod));
				}
				arry = construct_array(value_datums, size, TEXTOID,
									   -1, false, 'i');
				valueDatum = PointerGetDatum(arry);
				break;
			}

		case GS_TYPE_BOOL_ARRAY:
			{
				const		GSBool *boolVal;

				ret = gsGetRowFieldAsBoolArray(row, attid, &boolVal, &size);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				value_datums = (Datum *) palloc0(sizeof(Datum) * size);
				for (i = 0; i < size; i++)
					value_datums[i] = BoolGetDatum(boolVal[i]);
				arry = construct_array(value_datums, size, BOOLOID,
									   sizeof(GSBool), true, 'c');
				valueDatum = PointerGetDatum(arry);
				break;
			}

		case GS_TYPE_BYTE_ARRAY:
			{
				const		int8_t *byteVal;

				ret = gsGetRowFieldAsByteArray(row, attid, &byteVal, &size);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				value_datums = (Datum *) palloc0(sizeof(Datum) * size);
				for (i = 0; i < size; i++)
					value_datums[i] = Int16GetDatum((int16_t)byteVal[i]);
				arry = construct_array(value_datums, size, INT2OID,
									   sizeof(int16_t), true, 's');
				valueDatum = PointerGetDatum(arry);
				break;
			}

		case GS_TYPE_SHORT_ARRAY:
			{
				const		int16_t *shortVal;

				ret = gsGetRowFieldAsShortArray(row, attid, &shortVal, &size);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				value_datums = (Datum *) palloc0(sizeof(Datum) * size);
				for (i = 0; i < size; i++)
					value_datums[i] = Int16GetDatum(shortVal[i]);
				arry = construct_array(value_datums, size, INT2OID,
									   sizeof(int16_t), true, 's');
				valueDatum = PointerGetDatum(arry);
				break;
			}

		case GS_TYPE_INTEGER_ARRAY:
			{
				const int32_t *intVal;

				ret = gsGetRowFieldAsIntegerArray(row, attid, &intVal, &size);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				value_datums = (Datum *) palloc0(sizeof(Datum) * size);
				for (i = 0; i < size; i++)
					value_datums[i] = Int32GetDatum(intVal[i]);
				arry = construct_array(value_datums, size, INT4OID,
									   sizeof(int32_t), true, 'i');
				valueDatum = PointerGetDatum(arry);
				break;
			}

		case GS_TYPE_LONG_ARRAY:
			{
				const		int64_t *longVal;

				ret = gsGetRowFieldAsLongArray(row, attid, &longVal, &size);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				value_datums = (Datum *) palloc0(sizeof(Datum) * size);
				for (i = 0; i < size; i++)
					value_datums[i] = Int64GetDatum(longVal[i]);
				arry = construct_array(value_datums, size, INT8OID,
									   sizeof(int64_t), true, 'd');
				valueDatum = PointerGetDatum(arry);
				break;
			}

		case GS_TYPE_FLOAT_ARRAY:
			{
				const float *floatVal;

				ret = gsGetRowFieldAsFloatArray(row, attid, &floatVal, &size);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				value_datums = (Datum *) palloc0(sizeof(Datum) * size);
				for (i = 0; i < size; i++)
					value_datums[i] = Float4GetDatum(floatVal[i]);
				arry = construct_array(value_datums, size, FLOAT4OID,
									   sizeof(float4), FLOAT4PASSBYVAL, 'i');
				valueDatum = PointerGetDatum(arry);
				break;
			}

		case GS_TYPE_DOUBLE_ARRAY:
			{
				const double *doubleVal;
				size_t		size;
				size_t		i;
				Datum	   *value_datum;
				ArrayType  *arry;

				ret = gsGetRowFieldAsDoubleArray(row, attid, &doubleVal, &size);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				value_datum = (Datum *) palloc0(sizeof(Datum) * size);
				for (i = 0; i < size; i++)
					value_datum[i] = Float8GetDatum(doubleVal[i]);
				arry = construct_array(value_datum, size, FLOAT8OID,
									   sizeof(float8), FLOAT8PASSBYVAL, 'd');
				valueDatum = PointerGetDatum(arry);
				break;
			}

		case GS_TYPE_TIMESTAMP_ARRAY:
			{
				const		GSTimestamp *tsVal;

				ret = gsGetRowFieldAsTimestampArray(row, attid, &tsVal, &size);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				value_datums = (Datum *) palloc0(sizeof(Datum) * size);
				for (i = 0; i < size; i++)
				{
					Timestamp	timestamp = griddb_convert_gs2pg_timestamp(tsVal[i]);

					value_datums[i] = TimestampGetDatum(timestamp);
				}
				arry = construct_array(value_datums, size, TIMESTAMPOID,
									   sizeof(Timestamp), true, 'd');
				valueDatum = PointerGetDatum(arry);
				break;
			}

		default:
			/* Should not happen, we have just check this above */
			elog(ERROR, "unsupported field type %d", gs_type);
	}

	return valueDatum;
}

/*
 * Execute TQL on foreign server and fetch row set.
 */
static void
griddb_execute_and_fetch(ForeignScanState *node)
{
	GridDBFdwScanState *fsstate = (GridDBFdwScanState *) node->fdw_state;
	GSResult	ret;
	GSQuery    *query;
	GSBool		exists;
	GSContainerInfo cont_info = GS_CONTAINER_INFO_INITIALIZER;
	GSBool		option = GS_TRUE;

	/* Execute TQL */
	ret = gsQuery(fsstate->cont, fsstate->query, &query);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, fsstate->cont);

	if (griddb_enable_partial_execution) {
		ret = gsSetFetchOption(query, GS_FETCH_PARTIAL_EXECUTION, &option, GS_TYPE_BOOL);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, fsstate->cont);
	}

	/* Fetch result set and schema information */
	ret = gsFetch(query, fsstate->for_update, &fsstate->row_set);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, query);

	ret = gsGetContainerInfo(fsstate->store, fsstate->cont_name, &cont_info, &exists);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, fsstate->store);
	Assert(exists == GS_TRUE);
	griddb_make_column_info(&cont_info, &fsstate->field_info);

	ret = gsCreateRowByContainer(fsstate->cont, &fsstate->row);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, fsstate->cont);

	/* smrelay is shared with ForeignModify */
	if (fsstate->for_update)
	{
		GridDBFdwSMRelay *smrelay = fsstate->smrelay;

		smrelay->row_set = fsstate->row_set;
		smrelay->row = fsstate->row;
		griddb_make_column_info(&cont_info, &smrelay->field_info);
	}
}

/*
 * griddb_update_rows_init
 *		Check whether data type of slot is as expected.
 */
void
griddb_check_slot_type(TupleTableSlot *slot, int attnum, GridDBFdwFieldInfo * field_info)
{
	GSType		gs_type;
	Oid			gs_type_oid;
	Oid			pg_type;

	gs_type = field_info->column_types[attnum - 1];
	gs_type_oid = griddb_pgtyp_from_gstyp(gs_type, NULL);
	pg_type = TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1)->atttypid;
	if (pg_type != gs_type_oid)
		elog(ERROR, "Unexpected data type. pgtype is %d, but GridDB expects %d.",
			 pg_type, gs_type_oid);
}

static void
griddb_find_junk_attno(GridDBFdwModifyState * fmstate, List *targetlist)
{
	Oid			relId = RelationGetRelid(fmstate->rel);
	char	   *attName = get_attname(relId, ROWKEY_ATTNO
#if (PG_VERSION_NUM >= 110000)
									  ,false
#endif
	);

	fmstate->junk_att_no = ExecFindJunkAttributeInTlist(targetlist, attName);
}

/*
 * Check if the cursor pointing GridDB result set is same as slot value.
 * If the cursor is pointing the different row, we change to the bulk
 * mode.
 */
static void
griddb_judge_bulk_mode(GridDBFdwModifyState * fmstate, TupleTableSlot *planSlot)
{
	Datum	   *value1[1];
	Datum	   *value2[1];
	Datum		val1;
	int			(*comparator) (const void *, const void *);
	bool		isnull;
	GridDBFdwSMRelay *smrelay = fmstate->smrelay;
	GSType		gs_type = smrelay->field_info.column_types[ROWKEY_ATTNO - 1];

	/* No need to judge if it is already bulk mode. */
	if (fmstate->bulk_mode == true)
		return;

	/* Chenge to bulk mode if the cursor is poinitng NULL. */
	if (!smrelay->rowkey_val)
	{
		fmstate->bulk_mode = true;
		return;
	}

	val1 = ExecGetJunkAttribute(planSlot, fmstate->junk_att_no, &isnull);
	Assert(isnull == false);

	value1[0] = &val1;
	value2[0] = &smrelay->rowkey_val;
	comparator = griddb_get_comparator_tuplekey(gs_type);
	if (comparator((const void *) &value1, (const void *) &value2) != 0)
		fmstate->bulk_mode = true;
}

/*
 * Call gsSetRowFieldByXXX(). Data is given by the argument as a Datum type.
 */
void
griddb_set_row_field(GSRow * row, Datum value, GSType gs_type, int pindex)
{
	GSResult	ret;
	ArrayType  *array;
	Oid			elmtype;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	int			num_elems;
	Datum	   *elem_values;
	bool	   *elem_nulls;
	Oid			outputFunctionId;
	bool		typeVarLength;
	int			i;

	switch (gs_type)
	{
		case GS_TYPE_STRING:
			{
				char	   *textVal;
				Oid			outputFunctionId;
				bool		typeVarLength;

				getTypeOutputInfo(TEXTOID, &outputFunctionId, &typeVarLength);
				textVal = OidOutputFunctionCall(outputFunctionId, value);

				ret = gsSetRowFieldByString(row, pindex, textVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				break;
			}

		case GS_TYPE_GEOMETRY:
			{
				char	   *geomVal;
				Oid			outputFunctionId;
				bool		typeVarLength;

				getTypeOutputInfo(TEXTOID, &outputFunctionId, &typeVarLength);
				geomVal = OidOutputFunctionCall(outputFunctionId, value);

				ret = gsSetRowFieldByGeometry(row, pindex, geomVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				break;
			}

		case GS_TYPE_BOOL:
			{
				bool		boolVal = DatumGetBool(value);

				ret = gsSetRowFieldByBool(row, pindex, boolVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				break;
			}

		case GS_TYPE_BYTE:
			{
				int16		byteVal = DatumGetInt16(value);
				if (byteVal < INT8_MIN || byteVal > INT8_MAX)
					elog(ERROR, "Integer %d is out of range of BYTE", byteVal);

				ret = gsSetRowFieldByByte(row, pindex, (int8_t)byteVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				break;
			}

		case GS_TYPE_SHORT:
			{
				int16		shortVal = DatumGetInt16(value);

				ret = gsSetRowFieldByShort(row, pindex, shortVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				break;
			}

		case GS_TYPE_INTEGER:
			{
				int32		intVal = DatumGetInt32(value);

				ret = gsSetRowFieldByInteger(row, pindex, intVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				break;
			}

		case GS_TYPE_LONG:
			{
				int64		longVal = DatumGetInt64(value);

				ret = gsSetRowFieldByLong(row, pindex, longVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				break;
			}


		case GS_TYPE_FLOAT:
			{
				float		floatVal = DatumGetFloat4(value);

				ret = gsSetRowFieldByFloat(row, pindex, floatVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				break;
			}

		case GS_TYPE_DOUBLE:
			{
				double		doubleVal = DatumGetFloat8(value);

				ret = gsSetRowFieldByDouble(row, pindex, doubleVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				break;
			}

		case GS_TYPE_TIMESTAMP:
			{
				Timestamp	timestamp = DatumGetTimestamp(value);
				GSTimestamp timestampVal = griddb_convert_pg2gs_timestamp(timestamp);

				ret = gsSetRowFieldByTimestamp(row, pindex, timestampVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				break;
			}

		case GS_TYPE_BLOB:
			{
				GSBlob		blobVal;
				char	   *result = DatumGetPointer(value);

				if (VARATT_IS_1B(result))
				{
					blobVal.size = VARSIZE_1B(result) - VARHDRSZ_SHORT;
					blobVal.data = (const void *) VARDATA_1B(result);
				}
				else
				{
					blobVal.size = VARSIZE_4B(result) - VARHDRSZ;
					blobVal.data = (const void *) VARDATA_4B(result);
				}

				ret = gsSetRowFieldByBlob(row, pindex, &blobVal);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);
				break;
			}

		case GS_TYPE_STRING_ARRAY:
			{
				const		GSChar **stringaData;

				array = DatumGetArrayTypeP(value);
				elmtype = ARR_ELEMTYPE(array);

				get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
				deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);
				getTypeOutputInfo(TEXTOID, &outputFunctionId, &typeVarLength);

				stringaData = (const GSChar * *) palloc0(sizeof(GSChar *) * num_elems);
				for (i = 0; i < num_elems; i++)
				{
					Assert(!elem_nulls[i]);
					stringaData[i] = OidOutputFunctionCall(outputFunctionId, elem_values[i]);
				}

				ret = gsSetRowFieldByStringArray(row, pindex, stringaData, num_elems);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				pfree(stringaData);
				pfree(elem_values);
				pfree(elem_nulls);
				break;
			}

		case GS_TYPE_BOOL_ARRAY:
			{
				GSBool	   *boolaData;

				array = DatumGetArrayTypeP(value);
				elmtype = ARR_ELEMTYPE(array);

				get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
				deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);

				boolaData = (GSBool *) palloc0(sizeof(GSBool) * num_elems);
				for (i = 0; i < num_elems; i++)
					boolaData[i] = DatumGetBool(elem_values[i]);

				ret = gsSetRowFieldByBoolArray(row, pindex, boolaData, num_elems);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				pfree(boolaData);
				pfree(elem_values);
				pfree(elem_nulls);
				break;
			}

		case GS_TYPE_BYTE_ARRAY:
			{
				int8_t	   *byteaData;

				array = DatumGetArrayTypeP(value);
				elmtype = ARR_ELEMTYPE(array);

				get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
				deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);
				getTypeOutputInfo(BPCHAROID, &outputFunctionId, &typeVarLength);

				byteaData = (int8_t *) palloc0(sizeof(int8_t) * num_elems);
				for (i = 0; i < num_elems; i++)
				{
					int16		byteVal = DatumGetInt16(elem_values[i]);

					if (byteVal < INT8_MIN || byteVal > INT8_MAX)
						elog(ERROR, "Integer %d is out of range of BYTE", byteVal);

					byteaData[i] = (int8_t)byteVal;
				}

				ret = gsSetRowFieldByByteArray(row, pindex, byteaData, num_elems);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				pfree(byteaData);
				pfree(elem_values);
				pfree(elem_nulls);
				break;
			}

		case GS_TYPE_SHORT_ARRAY:
			{
				int16_t    *shortaData;

				array = DatumGetArrayTypeP(value);
				elmtype = ARR_ELEMTYPE(array);

				get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
				deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);

				shortaData = (int16_t *) palloc0(sizeof(int16_t) * num_elems);
				for (i = 0; i < num_elems; i++)
					shortaData[i] = DatumGetInt16(elem_values[i]);

				ret = gsSetRowFieldByShortArray(row, pindex, shortaData, num_elems);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				pfree(shortaData);
				pfree(elem_values);
				pfree(elem_nulls);
				break;
			}

		case GS_TYPE_INTEGER_ARRAY:
			{
				int32_t    *intaData;

				array = DatumGetArrayTypeP(value);
				elmtype = ARR_ELEMTYPE(array);

				get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
				deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);

				intaData = (int32_t *) palloc0(sizeof(int32_t) * num_elems);
				for (i = 0; i < num_elems; i++)
					intaData[i] = DatumGetInt32(elem_values[i]);

				ret = gsSetRowFieldByIntegerArray(row, pindex, intaData, num_elems);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				pfree(intaData);
				pfree(elem_values);
				pfree(elem_nulls);
				break;
			}

		case GS_TYPE_LONG_ARRAY:
			{
				int64_t    *longaData;

				array = DatumGetArrayTypeP(value);
				elmtype = ARR_ELEMTYPE(array);

				get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
				deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);

				longaData = (int64_t *) palloc0(sizeof(int64_t) * num_elems);
				for (i = 0; i < num_elems; i++)
					longaData[i] = DatumGetInt64(elem_values[i]);

				ret = gsSetRowFieldByLongArray(row, pindex, longaData, num_elems);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				pfree(longaData);
				pfree(elem_values);
				pfree(elem_nulls);
				break;
			}

		case GS_TYPE_FLOAT_ARRAY:
			{
				float	   *floataData;

				array = DatumGetArrayTypeP(value);
				elmtype = ARR_ELEMTYPE(array);

				get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
				deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);

				floataData = (float *) palloc0(sizeof(float) * num_elems);
				for (i = 0; i < num_elems; i++)
					floataData[i] = DatumGetFloat4(elem_values[i]);

				ret = gsSetRowFieldByFloatArray(row, pindex, floataData, num_elems);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				pfree(floataData);
				pfree(elem_values);
				pfree(elem_nulls);
				break;
			}

		case GS_TYPE_DOUBLE_ARRAY:
			{
				double	   *doubleaData;

				array = DatumGetArrayTypeP(value);
				elmtype = ARR_ELEMTYPE(array);

				get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
				deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);

				doubleaData = (double *) palloc0(sizeof(double) * num_elems);
				for (i = 0; i < num_elems; i++)
					doubleaData[i] = DatumGetFloat8(elem_values[i]);

				ret = gsSetRowFieldByDoubleArray(row, pindex, doubleaData, num_elems);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				pfree(doubleaData);
				pfree(elem_values);
				pfree(elem_nulls);
				break;
			}

		case GS_TYPE_TIMESTAMP_ARRAY:
			{
				GSTimestamp *tsaData;

				array = DatumGetArrayTypeP(value);
				elmtype = ARR_ELEMTYPE(array);

				get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
				deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);

				tsaData = (GSTimestamp *) palloc0(sizeof(GSTimestamp) * num_elems);
				for (i = 0; i < num_elems; i++)
				{
					Timestamp	timestamp = DatumGetTimestamp(elem_values[i]);

					tsaData[i] = griddb_convert_pg2gs_timestamp(timestamp);
				}

				ret = gsSetRowFieldByTimestampArray(row, pindex, tsaData, num_elems);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row);

				pfree(tsaData);
				pfree(elem_values);
				pfree(elem_nulls);
				break;
			}

		case GS_TYPE_NULL:
			ret = gsSetRowFieldNull(row, pindex);
			if (!GS_SUCCEEDED(ret))
				griddb_REPORT_ERROR(ERROR, ret, row);
			break;

		default:
			/* Should not happen, we have just check this above */
			elog(ERROR, "unsupported field type(GS) %d", gs_type);
	}
}

/*
 * griddb_bind_for_putrow
 *		Generate new GSRow value which will be updated by gsPutRow.
 */
static void
griddb_bind_for_putrow(GridDBFdwModifyState * fmstate,
					   TupleTableSlot *slot, GSRow * row,
					   Relation rel, GridDBFdwFieldInfo * field_info)
{
	ListCell   *lc;

	foreach(lc, fmstate->target_attrs)
	{
		int			attnum = lfirst_int(lc);
		bool		isnull;
		Datum		value;
		GSType		type;

		if (attnum < 0)
			continue;

		griddb_check_slot_type(slot, attnum, field_info);

		value = slot_getattr(slot, attnum, &isnull);

		if (isnull)
			type = GS_TYPE_NULL;
		else
			type = field_info->column_types[attnum - 1];

		griddb_set_row_field(row, value, type, attnum - 1);
	}
}

/*
 * Append column name and type into buffer of SQL.
 * Column information is stored in GSContainerInfo.
 * For each column, a type conversion from GSType to pgtype is done.
 * Then a column type name is decided.
 */
static void
griddb_add_column_name_and_type(StringInfoData *buf, GSContainerInfo * info)
{
	size_t		iCol;

	/* Add column information */
	for (iCol = 0; iCol < info->columnCount; iCol++)
	{
		const char *attname = info->columnInfoList[iCol].name;
		const char *type_name;

		griddb_pgtyp_from_gstyp(info->columnInfoList[iCol].type, &type_name);

		if (iCol != 0)
			appendStringInfoString(buf, ",");

		/* Print column name and type */
		appendStringInfo(buf, " %s %s",
						 quote_identifier(attname),
						 type_name);

		/* Add option if the column is rowkey. */
		if (iCol == 0 && info->rowKeyAssigned)
			appendStringInfo(buf, " OPTIONS (%s 'true')", OPTION_ROWKEY);
	}
}

static GSChar * *grifddb_name_list_dup(const GSChar * const *src, size_t cont_size)
{
	size_t		i;
	GSChar	  **dst = (GSChar * *) palloc0(sizeof(GSChar *) * cont_size);

	for (i = 0; i < cont_size; i++)
		dst[i] = pstrdup(src[i]);

	return dst;
}

static void
grifddb_name_list_free(GSChar * *p, size_t cont_size)
{
	size_t		i;

	for (i = 0; i < cont_size; i++)
		pfree(p[i]);
	pfree(p);
}

/*
 * Executes commands given by an argument.
 */
static void
griddb_execute_commands(List *cmd_list)
{
	ListCell   *lc;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(WARNING, "SPI_connect failed");

	foreach(lc, cmd_list)
	{
		char	   *cmd = (char *) lfirst(lc);

		if (SPI_exec(cmd, 0) != SPI_OK_UTILITY)
			elog(WARNING, "SPI_exec failed: %s", cmd);
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(WARNING, "SPI_finish failed");
}
