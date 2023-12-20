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
#include "catalog/pg_proc.h"
#include "catalog/pg_opfamily.h"
#include "commands/explain.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#if (PG_VERSION_NUM >= 140000)
#include "optimizer/appendinfo.h"
#endif
#include "optimizer/cost.h"
#include "optimizer/inherit.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
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
#if (PG_VERSION_NUM < 100000)
#include "utils/bytea.h"
#endif
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
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs,
	/* Integer representing UPDATE/DELETE target */
	FdwScanPrivateForUpdate,
	/* Scan tlist */
	FdwScanTlist,
	/* RTE */
	FDWScanRTE,
	/* Integer representing aggregate function name */
	FdwScanPrivateAggRefName,
	/* Integer representing aggregate function  */
	FdwScanPrivateAggRefColumn,
};

/* Callback argument for ec_member_matches_foreign */
typedef struct ec_member_foreign_arg
{
	Expr	   *current;		/* current expr, or NULL if not yet found */
	List	   *already_used;	/* expressions already dealt with */
} ec_member_foreign_arg;

/*
 * Similarly, this enum describes what's kept in the fdw_private list for
 * a ModifyTable node referencing a griddb_fdw foreign table.  We store:
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
 * This enum describes what's kept in the fdw_private list for a ForeignPath.
 * We store:
 *
 * 1) Boolean flag showing if the remote query has the final sort
 * 2) Boolean flag showing if the remote query has the LIMIT clause
 */
enum FdwPathPrivateIndex
{
	/* has-final-sort flag (as an integer Value node) */
	FdwPathPrivateHasFinalSort,
	/* has-limit flag (as an integer Value node) */
	FdwPathPrivateHasLimit
};

/*
 * DDL commands
 */
enum GridDBFdwDDLCommand
{
	DDL_COMMAND_CREATE,		/* Create data source */
	DDL_COMMAND_DROP		/* Drop data source */
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
	List	   *fdw_scan_tlist; /* optional tlist describing scan tuple */

	/* for remote query execution */
	GSGridStore *store;			/* connection for the scan */
	GSChar	   *cont_name;		/* container name */
	GSContainer *cont;			/* container to be selected */
	GSBool		for_update;		/* GS_TRUE if UPDATE/DELETE target */
	GridDBFdwFieldInfo field_info;	/* field information */
	GSRowSet   *row_set;		/* result set */
	GSRow	   *row;			/* row for the update */
	GridDBAggref *aggref;		/* aggregate function information */

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
	int			batch_size;		/* value of FDW option "batch_size" */

	MemoryContext temp_cxt;		/* context for temporary data */

	struct GridDBFdwModifyState *aux_fmstate;	/* foreign-insert state, if
												 * created */
}			GridDBFdwModifyState;

/*
 * SQL functions
 */
extern Datum griddb_fdw_handler(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(griddb_fdw_handler);
PG_FUNCTION_INFO_V1(griddb_fdw_version);

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

#if PG_VERSION_NUM < 140000
static void griddbAddForeignUpdateTargets(Query *parsetree,
										  RangeTblEntry *target_rte,
										  Relation target_relation);
#else
static void griddbAddForeignUpdateTargets(PlannerInfo *root,
										  Index rtindex,
										  RangeTblEntry *target_rte,
										  Relation target_relation);
#endif
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
#if PG_VERSION_NUM >= 140000
static TupleTableSlot **griddbExecForeignBatchInsert(EState *estate,
													 ResultRelInfo *resultRelInfo,
													 TupleTableSlot **slots,
													 TupleTableSlot **planSlots,
													 int *numSlots);
static int	griddbGetForeignModifyBatchSize(ResultRelInfo *resultRelInfo);
#endif
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
static void griddbGetForeignUpperPaths(PlannerInfo *root,
									   UpperRelationKind stage,
									   RelOptInfo *input_rel,
									   RelOptInfo *output_rel
#if (PG_VERSION_NUM >= 110000)
									   ,void *extra
#endif
);
static void griddb_get_datatype_for_conversion(Oid pg_type, regproc *typeinput,
											   int *typemod);
static bool griddb_foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel);
static void griddb_add_foreign_grouping_paths(PlannerInfo *root,
											  RelOptInfo *input_rel,
											  RelOptInfo *grouped_rel
#if (PG_VERSION_NUM >= 110000)
											  ,GroupPathExtraData *extra
#endif
);

/*
 * Helper functions
 */
static void griddb_fdw_exit(int code, Datum arg);
static void estimate_path_cost_size(PlannerInfo *root,
									RelOptInfo *baserel,
									List *join_conds,
									List *pathkeys,
									GriddbFdwPathExtraData * fpextra,
									double *p_rows, int *p_width,
									Cost *p_startup_cost, Cost *p_total_cost);

static void griddb_make_column_info(GSContainerInfo * cont_info,
									GridDBFdwFieldInfo * field_info);
static void griddb_free_column_info(GridDBFdwFieldInfo * field_info);
static Oid	griddb_pgtyp_from_gstyp(GSType gs_type, const char **name);
static Timestamp griddb_convert_gs2pg_timestamp(GSTimestamp ts);
static char *griddb_convert_gs2pg_timestamp_to_string(GSTimestamp ts);
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

int			griddb_set_transmission_modes();
void		griddb_reset_transmission_modes(int nestlevel);

static void griddb_check_rowkey_update(GridDBFdwModifyState * fmstate, TupleTableSlot *new_slot);
static Oid	griddb_get_agg_type(GridDBFdwFieldInfo field_info, GridDBAggref * aggref);

static Datum griddb_make_datum_record(StringInfoData *values, TupleDesc tupdesc, GSType * column_types,
									  GSRow * row, regproc typeinput, int typemod);

static List *griddb_get_useful_pathkeys_for_relation(PlannerInfo *root,
													 RelOptInfo *rel);
static List *griddb_get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel);
static void griddb_add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
												   Path *epq_path);
static void griddb_add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel,
										   RelOptInfo *final_rel
#if (PG_VERSION_NUM >= 120000)
										   ,FinalPathExtraData *extra
#endif
);

#if (PG_VERSION_NUM >= 140000)
static int	get_batch_size_option(Relation rel);
#endif

static bool check_existed_container(Oid serverOid, char* tbl_name);

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
griddb_fdw_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(CODE_VERSION);
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
#if (PG_VERSION_NUM >= 140000)
	/* Support function for Batch Insert */
	routine->ExecForeignBatchInsert = griddbExecForeignBatchInsert;
	routine->GetForeignModifyBatchSize = griddbGetForeignModifyBatchSize;

	/* Curently gridDB does not support asynchronous execution */
	routine->IsForeignPathAsyncCapable = NULL;
	routine->ForeignAsyncRequest = NULL;
	routine->ForeignAsyncConfigureWait = NULL;
	routine->ForeignAsyncNotify = NULL;
#endif

	/* Support functions for IMPORT FOREIGN SCHEMA */
	routine->ImportForeignSchema = griddbImportForeignSchema;

	/* Not support functions for join push-down */
	routine->GetForeignJoinPaths = NULL;

	/* Support functions for upper relation push-down */
	routine->GetForeignUpperPaths = griddbGetForeignUpperPaths;

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
#if PG_VERSION_NUM >= 140000
									  HASH_ELEM | HASH_BLOBS);
#else
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
#endif
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
	griddb_opt *options = NULL;

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	/*
	 * We use GriddbFdwRelationInfo to pass various information to subsequent
	 * functions.
	 */
	fpinfo =
		(GriddbFdwRelationInfo *) palloc0(sizeof(GriddbFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Base foreign tables need to be pushed down always. */
	fpinfo->pushdown_safe = true;

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
	fpinfo->shippable_extensions = NIL;
	fpinfo->fetch_size = 100;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * identify which user to do remote access as during planning.  This
	 * should match what ExecCheckPermissions() does.  If we fail due to lack
	 * of permissions, the query would have failed at runtime anyway.
	 */
	if (fpinfo->use_remote_estimate)
	{
#if (PG_VERSION_NUM >= 160000)
		Oid			userid;
		userid = OidIsValid(baserel->userid) ? baserel->userid : GetUserId();
#else
		RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
		Oid			userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
#endif
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
#if PG_VERSION_NUM >= 90600
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);
#else
	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid, &fpinfo->attrs_used);
#endif
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
#if PG_VERSION_NUM < 140000
		if (baserel->pages == 0 && baserel->tuples == 0)
#else
		if (baserel->tuples < 0)
#endif
		{
			baserel->pages = 10;
			baserel->tuples =
				(10 * BLCKSZ) / (baserel->reltarget->width +
								 MAXALIGN(SizeofHeapTupleHeader));
		}

		/* Estimate baserel size as best we can with local statistics. */
		set_baserel_size_estimates(root, baserel);

		/* Fill in basically-bogus cost estimates for use later. */
		estimate_path_cost_size(root, baserel, NIL, NIL, NULL,
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

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 */
	path = create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
								   NULL,	/* default pathtarget */
#endif
								   fpinfo->rows,
								   fpinfo->startup_cost,
								   fpinfo->total_cost,
								   NIL, /* no pathkeys */
#if (PG_VERSION_NUM >= 120000)
								   baserel->lateral_relids,
#else
								   NULL,	/* no outer rel either */
#endif
								   NULL,	/* no extra plan */
								   NIL);	/* no fdw_private list */
	add_path(baserel, (Path *) path);

	/* Add paths with pathkeys */
	griddb_add_paths_with_pathkeys_for_rel(root, baserel, NULL);

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
 * persist only until the caller calls griddb_reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * griddb_reset_transmission_modes() to undo things.
 */
int
griddb_set_transmission_modes(void)
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

	/*
	 * In addition force restrictive search_path, in case there are any
	 * regproc or similar constants to be printed.
	 */
	(void) set_config_option("search_path", "pg_catalog",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);

	return nestlevel;
}

/*
 * Undo the effects of griddb_set_transmission_modes().
 */
void
griddb_reset_transmission_modes(int nestlevel)
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
	GriddbFdwRelationInfo *fpinfo = (GriddbFdwRelationInfo *) foreignrel->fdw_private;
	Index		scan_relid = foreignrel->relid;
	List	   *fdw_private;
	List	   *remote_conds = NIL;
	List	   *remote_exprs = NIL;
	List	   *local_exprs = NIL;
	List	   *params_list = NIL;
	List	   *fdw_scan_tlist = NIL;
	List	   *retrieved_attrs;
	StringInfoData sql;
	ListCell   *lc;
	int			for_update = 0;
	int			guc_level = 0;
	bool		has_limit = false;
	RangeTblEntry *rte;

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	/* Decide to execute function pushdown support in the target list. */
	fpinfo->is_tlist_func_pushdown = griddb_is_foreign_function_tlist(root, foreignrel, tlist);

	/*
	 * Get FDW private data created by griddbGetForeignUpperPaths(), if any.
	 */
	if (best_path->fdw_private)
	{
		has_limit = intVal(list_nth(best_path->fdw_private, FdwPathPrivateHasLimit));
	}

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
	if ((foreignrel->reloptkind == RELOPT_BASEREL ||
		 foreignrel->reloptkind == RELOPT_OTHER_MEMBER_REL) &&
		fpinfo->is_tlist_func_pushdown == false)
	{
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
			else if (griddb_is_foreign_expr(root, foreignrel, rinfo->clause, false))
			{
				remote_conds = lappend(remote_conds, rinfo);
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			}
			else
				local_exprs = lappend(local_exprs, rinfo->clause);

		}
	}
	else
	{
		/*
		 * For a join rel, baserestrictinfo is NIL and we are not considering
		 * parameterization right now, so there should be no scan_clauses for
		 * a joinrel or an upper rel either.
		 */
		if (fpinfo->is_tlist_func_pushdown == false)
		{
			scan_relid = 0;
			Assert(!scan_clauses);
		}

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 */
		remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

		if (fpinfo->is_tlist_func_pushdown == true)
		{
			foreach(lc, tlist)
			{
				TargetEntry *tle = lfirst_node(TargetEntry, lc);

				/*
				 * Pull out function from FieldSelect clause and add to
				 * fdw_scan_tlist to push down function portion only
				 */
				if (fpinfo->is_tlist_func_pushdown == true && IsA((Node *) tle->expr, FieldSelect))
				{
					fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist,
													   griddb_pull_func_clause((Node *) tle->expr));
				}
				else
				{
					fdw_scan_tlist = lappend(fdw_scan_tlist, tle);
				}
			}

			foreach(lc, fpinfo->local_conds)
			{
				RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

				fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist,
												   pull_var_clause((Node *) rinfo->clause,
																   PVC_RECURSE_PLACEHOLDERS));
			}
		}
		else
		{
			fdw_scan_tlist = griddb_build_tlist_to_deparse(foreignrel);
		}

	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	/* Deparse timestamp as ISO style */
	guc_level = griddb_set_transmission_modes();
	griddb_deparse_select(&sql, root, foreignrel, remote_conds,
						  best_path->path.pathkeys,
						  &retrieved_attrs, &params_list, fdw_scan_tlist, has_limit);
	griddb_reset_transmission_modes(guc_level);
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
	fdw_private = list_make3(makeString(sql.data),
							 retrieved_attrs,
							 makeInteger(for_update));
	fdw_private = lappend(fdw_private, fdw_scan_tlist);

	if IS_UPPER_REL
		(foreignrel)
	{
		rte = planner_rt_fetch(((GriddbFdwRelationInfo *) ((RelOptInfo *) foreignrel)->fdw_private)->outerrel->relid, root);
	}
	else
	{
		rte = planner_rt_fetch(foreignrel->relid, root);
	}
	fdw_private = lappend(fdw_private, rte);

	if (((GriddbFdwRelationInfo *) foreignrel->fdw_private)->aggref)
	{
		fdw_private = lappend(fdw_private, makeString(((GriddbFdwRelationInfo *) foreignrel->fdw_private)->aggref->aggname->data));
		fdw_private = lappend(fdw_private, makeString(((GriddbFdwRelationInfo *) foreignrel->fdw_private)->aggref->columnname->data));
	}

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
							fdw_scan_tlist,
							remote_exprs,
							outer_plan);
}

#if PG_VERSION_NUM >= 140000
/*
 * Construct a tuple descriptor for the scan tuples handled by a foreign join.
 */
static TupleDesc
griddb_get_tupdesc_for_join_scan_tuples(ForeignScanState *node)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	TupleDesc	tupdesc;
	int			i;

	/*
	 * The core code has already set up a scan tuple slot based on
	 * fsplan->fdw_scan_tlist, and this slot's tupdesc is mostly good enough,
	 * but there's one case where it isn't.  If we have any whole-row row
	 * identifier Vars, they may have vartype RECORD, and we need to replace
	 * that with the associated table's actual composite type.  This ensures
	 * that when we read those ROW() expression values from the remote server,
	 * we can convert them to a composite type the local server knows.
	 */
	tupdesc = CreateTupleDescCopy(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		Var		   *var;
		RangeTblEntry *rte;
		Oid			reltype;

		/* Nothing to do if it's not a generic RECORD attribute */
		if (att->atttypid != RECORDOID || att->atttypmod >= 0)
			continue;

		/*
		 * If we can't identify the referenced table, do nothing.  This'll
		 * likely lead to failure later, but perhaps we can muddle through.
		 */
		var = (Var *) list_nth_node(TargetEntry, fsplan->fdw_scan_tlist, i)->expr;
		if (!IsA(var, Var) || var->varattno != 0)
			continue;
		rte = list_nth(estate->es_range_table, var->varno - 1);
		if (rte->rtekind != RTE_RELATION)
			continue;
		reltype = get_rel_type_id(rte->relid);
		if (!OidIsValid(reltype))
			continue;
		att->atttypid = reltype;
		/* shouldn't need to change anything else */
	}
	return tupdesc;
}
#endif

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

	elog(DEBUG1, "griddb_fdw: %s", __func__);

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
	 * ExecCheckPermissions() does.
	 */
	if (fsplan->scan.scanrelid > 0)
	{
		rtindex = fsplan->scan.scanrelid;
		rte = rt_fetch(rtindex, estate->es_range_table);
	}
	else
	{
		rte = list_nth(fsplan->fdw_private, FDWScanRTE);
	}

#if (PG_VERSION_NUM >= 160000)
	userid = OidIsValid(fsplan->checkAsUser) ? fsplan->checkAsUser : GetUserId();
#else
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
#endif

	/* Get info about foreign table. */
	table = GetForeignTable(rte->relid);
	user = GetUserMapping(userid, table->serverid);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	fsstate->store = griddb_get_connection(user, false,
										   rte->relid);

	fsstate->cont_name = griddb_get_rel_name(rte->relid);
	fsstate->cont = griddb_get_container(user, rte->relid, fsstate->store);

	fsstate->query = strVal(list_nth(fsplan->fdw_private,
									 FdwScanPrivateSelectSql));
	fsstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private,
												 FdwScanPrivateRetrievedAttrs);
	for_update = intVal(list_nth(fsplan->fdw_private,
								 FdwScanPrivateForUpdate));
	fsstate->fdw_scan_tlist = (List *) list_nth(fsplan->fdw_private,
												FdwScanTlist);
	fsstate->for_update = for_update ? GS_TRUE : GS_FALSE;
	if (list_length(fsplan->fdw_private) - 1 == FdwScanPrivateAggRefColumn)
	{
		fsstate->aggref = palloc0(sizeof(GridDBAggref));
		fsstate->aggref->aggname = makeStringInfo();
		fsstate->aggref->aggname->data = strVal(list_nth(fsplan->fdw_private,
														 FdwScanPrivateAggRefName));
		fsstate->aggref->columnname = makeStringInfo();
		fsstate->aggref->columnname->data = strVal(list_nth(fsplan->fdw_private,
															FdwScanPrivateAggRefColumn));
	}
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
#if (PG_VERSION_NUM >= 140000)
		fsstate->tupdesc = griddb_get_tupdesc_for_join_scan_tuples(node);
#else
		fsstate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
#endif
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

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	memset(tupleSlot->tts_values, 0, sizeof(Datum) * tupleDescriptor->natts);
	memset(tupleSlot->tts_isnull, true, sizeof(bool) * tupleDescriptor->natts);
	ExecClearTuple(tupleSlot);

	/*
	 * Return the next tuple.
	 */
	if (gsHasNextRow(fsstate->row_set))
	{
		GSResult	ret = GS_RESULT_OK;
		ListCell   *lc = NULL,
				   *scanlc = NULL;
		GSType	   *column_types = fsstate->field_info.column_types;
		List	   *fdw_scan_tlist = fsstate->fdw_scan_tlist;
		GSAggregationResult *agg_res;	/* aggregation result */
		GSRowSetType row_type;	/* Row type of result */

		/*
		 * In case target is aggregation only
		 */
		row_type = gsGetRowSetType(fsstate->row_set);
		if (row_type == GS_ROW_SET_AGGREGATION_RESULT)
		{
			if (gsHasNextRow(fsstate->row_set))
			{
				ret = gsGetNextAggregation(fsstate->row_set, &agg_res);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, fsstate->row_set);
			}
		}
		else
		{
			Assert(gsHasNextRow(fsstate->row_set) == GS_TRUE);
			ret = gsGetNextRow(fsstate->row_set, fsstate->row);
			if (!GS_SUCCEEDED(ret))
				griddb_REPORT_ERROR(ERROR, ret, fsstate->row_set);
		}

		/* Construct tuple slot */
		foreach(lc, fsstate->retrieved_attrs)
		{
			int			attnum = lfirst_int(lc);

			scanlc = list_head(fdw_scan_tlist);

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
					bool		is_record = false;

					if (rel)
						tupdesc = RelationGetDescr(rel);
					else
						tupdesc = fsstate->tupdesc;

					if (scanlc != NULL)
					{
						TargetEntry *tle = lfirst(scanlc);
						Node	   *node = (Node *) tle->expr;

						if (IsA(node, FuncExpr))
						{
							const char *proname;
							HeapTuple	proctup;
							FuncExpr   *fe = (FuncExpr *) node;

							proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(fe->funcid));
							if (!HeapTupleIsValid(proctup))
							{
								elog(ERROR, "cache lookup failed for function %u", fe->funcid);
							}
							proname = pstrdup(((Form_pg_proc) GETSTRUCT(proctup))->proname.data);
							ReleaseSysCache(proctup);

							/* The following functions return a record */
							if (strcmp(proname, "time_next") == 0 ||
								strcmp(proname, "time_next_only") == 0 ||
								strcmp(proname, "time_prev") == 0 ||
								strcmp(proname, "time_prev_only") == 0 ||
								strcmp(proname, "time_interpolated") == 0 ||
								strcmp(proname, "max_rows") == 0 ||
								strcmp(proname, "min_rows") == 0 ||
								strcmp(proname, "time_sampling") == 0)
								is_record = true;
						}
					}

					/*
					 * When is_record is true, build a record as text and
					 * return it. Example: (value1, value2, value3)
					 */
					if (is_record)
					{
						StringInfoData values;
						regproc		typeinput;
						int			typemod;

						initStringInfo(&values);
						griddb_get_datatype_for_conversion(TEXTOID, &typeinput, &typemod);

						tupleSlot->tts_values[attnum - 1] =
							griddb_make_datum_record(&values, tupdesc, column_types,
													 fsstate->row, typeinput, typemod);
						tupleSlot->tts_isnull[attnum - 1] = false;
					}
					else if (row_type == GS_ROW_SET_AGGREGATION_RESULT)
					{
						GSType		type = griddb_get_agg_type(fsstate->field_info, fsstate->aggref);

						pgtype = TupleDescAttr(tupdesc, attnum - 1)->atttypid;

						tupleSlot->tts_values[attnum - 1] =
							griddb_make_datum_from_row(fsstate->row, attnum - 1,
													   type,
													   pgtype, row_type, agg_res, fsstate->aggref);
						tupleSlot->tts_isnull[attnum - 1] = false;
					}
					else
					{
						pgtype = TupleDescAttr(tupdesc, attnum - 1)->atttypid;
						tupleSlot->tts_values[attnum - 1] =
							griddb_make_datum_from_row(fsstate->row, attnum - 1,
													   column_types[attnum - 1],
													   pgtype, row_type, agg_res, false);
						tupleSlot->tts_isnull[attnum - 1] = false;
					}
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

	elog(DEBUG1, "griddb_fdw: %s", __func__);

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

	elog(DEBUG1, "griddb_fdw: %s", __func__);

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
#if PG_VERSION_NUM < 140000
griddbAddForeignUpdateTargets(Query *parsetree,
							  RangeTblEntry *target_rte,
							  Relation target_relation)
#else
griddbAddForeignUpdateTargets(PlannerInfo *root,
							  Index rtindex,
							  RangeTblEntry *target_rte,
							  Relation target_relation)
#endif
{
	Var		   *var = NULL;
	const char *attrname = NULL;
#if PG_VERSION_NUM < 140000
	TargetEntry *tle = NULL;
#endif
	Form_pg_attribute attr = NULL;

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	/*
	 * What we need is the rowkey which is the first column
	 */
	attr = TupleDescAttr(RelationGetDescr(target_relation), ROWKEY_ATTNO - 1);

	/* Make a Var representing the desired value */
	var = makeVar(
#if PG_VERSION_NUM < 140000
				  parsetree->resultRelation,
#else
				  rtindex,
#endif
				  ROWKEY_ATTNO,
				  attr->atttypid,
				  attr->atttypmod,
				  attr->attcollation,
				  0);

	/* Wrap it in a TLE with the right name ... */
	attrname = NameStr(attr->attname);
#if PG_VERSION_NUM < 140000
	tle = makeTargetEntry((Expr *) var,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname),
						  true);

	/* ... and add it to the query's targetlist */
	parsetree->targetList = lappend(parsetree->targetList, tle);
#else
	add_row_identity_var(root, var, rtindex, attrname);
#endif
}

/*
 * griddbPlanForeignModify
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

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = table_open(rte->relid, NoLock);

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
		Bitmapset  *allUpdatedCols;

#if ((PG_VERSION_NUM >= 130010 && PG_VERSION_NUM < 140000) || \
	 (PG_VERSION_NUM >= 140007 && PG_VERSION_NUM < 150000) || \
	  PG_VERSION_NUM >= 150002)
		/* get_rel_all_updated_cols is supported from pg 13.10, 14.7, 15.2 and 16 */
		RelOptInfo *rel = find_base_rel(root, resultRelation);
		allUpdatedCols = get_rel_all_updated_cols(root, rel);
#elif (PG_VERSION_NUM >= 120000)
		allUpdatedCols = bms_union(rte->updatedCols, rte->extraUpdatedCols);
#else
		allUpdatedCols = rte->updatedCols;
#endif

		col = -1;
		while ((col = bms_next_member(allUpdatedCols, col)) >= 0)
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

	table_close(rel, NoLock);

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
	Oid			userid;

	/* Begin constructing PgFdwModifyState. */
	fmstate = (GridDBFdwModifyState *) palloc0(sizeof(GridDBFdwModifyState));
	fmstate->rel = rel;

	/* Identify which user to do the remote access as. */
#if (PG_VERSION_NUM >= 160000)
	userid = ExecGetResultRelCheckAsUser(resultRelInfo, estate);
#else
	userid = GetUserId();
#endif

	/* Get info about foreign table. */
	table = GetForeignTable(RelationGetRelid(rel));
	serverOid = table->serverid;
	user = GetUserMapping(userid, serverOid);

	/* Open connection; report that we'll create a prepared statement. */
	fmstate->store = griddb_get_connection(user, false, serverOid);

	/* Set up remote query information. */
	fmstate->target_attrs = target_attrs;

	/* Create context for per-tuple temp workspace */
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "griddb_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

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

	/* Initialize auxiliary state */
	fmstate->aux_fmstate = NULL;
#if PG_VERSION_NUM >= 140000
	/* Set batch_size from foreign server/table options. */
	if (operation == CMD_INSERT)
		fmstate->batch_size = get_batch_size_option(rel);
#endif
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

	elog(DEBUG1, "griddb_fdw: %s", __func__);

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
#if PG_VERSION_NUM < 140000
									mtstate->mt_plans[subplan_index]->plan,
#else
									outerPlanState(mtstate)->plan,
#endif
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
	GridDBFdwModifyState *fmstate = (GridDBFdwModifyState *) resultRelInfo->ri_FdwState;
	GSResult	ret;
	GSBool		exists;
	GSContainerInfo cont_info = GS_CONTAINER_INFO_INITIALIZER;
	GSRow	   *row;
	GridDBFdwFieldInfo field_info = {0};
	MemoryContext oldcontext;

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	/*
	 * If the fmstate has aux_fmstate set, use the aux_fmstate (see
	 * griddbBeginForeignInsert())
	 */
	if (fmstate->aux_fmstate)
	{
		fmstate = fmstate->aux_fmstate;
	}

	ret = gsCreateRowByContainer(fmstate->cont, &row);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, fmstate->cont);

	/* Get schema information for binding */
	ret = gsGetContainerInfo(fmstate->store, fmstate->cont_name, &cont_info,
							 &exists);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, fmstate->cont);
	Assert(exists == GS_TRUE);

	/* Switch to temp context, the allocated memory will be freed at the end of this routine */
	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	griddb_make_column_info(&cont_info, &field_info);

	/* Create row structure for gsPutRow */
	griddb_bind_for_putrow(fmstate, slot, row, resultRelInfo->ri_RelationDesc,
						   &field_info);
	griddb_free_column_info(&field_info);

	/* Switch to old context */
	MemoryContextSwitchTo(oldcontext);

	/* Insert row */
	ret = gsPutRow(fmstate->cont, NULL, row, &exists);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, fmstate->cont);

	if (exists)
		elog(WARNING, "row was updated instead of insert "
			 "because same row key has already existed.");

	/* Clean up */
	gsCloseRow(&row);

	/* Free allocated memory in temp context */
	MemoryContextReset(fmstate->temp_cxt);

	return slot;
}

#if PG_VERSION_NUM >= 140000
/*
 * griddbExecForeignBatchInsert
 *		Insert multiple rows into a foreign table
 */
static TupleTableSlot **
griddbExecForeignBatchInsert(EState *estate,
							 ResultRelInfo *resultRelInfo,
							 TupleTableSlot **slots,
							 TupleTableSlot **planSlots,
							 int *numSlots)
{
	GridDBFdwModifyState *fmstate = (GridDBFdwModifyState *) resultRelInfo->ri_FdwState;
	GSRow	  **rowList;
	GSResult	ret;
	GSBool		exists;
	GSContainerInfo cont_info = GS_CONTAINER_INFO_INITIALIZER;
	GSRow	   *row;
	GridDBFdwFieldInfo field_info = {0};
	int			i;
	const void *const *rowObj;
	MemoryContext oldcontext;

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	/*
	 * If the fmstate has aux_fmstate set, use the aux_fmstate (see
	 * griddbBeginForeignInsert())
	 */
	if (fmstate->aux_fmstate)
		fmstate = fmstate->aux_fmstate;

	/* Get schema information for binding */
	ret = gsGetContainerInfo(fmstate->store, fmstate->cont_name, &cont_info,
							 &exists);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, fmstate->cont);
	Assert(exists == GS_TRUE);

	/* Switch to temp context, the allocated memory will be freed at the end of this routine */
	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	rowList = (GSRow * *) palloc0(sizeof(GSRow *) * (*numSlots));

	griddb_make_column_info(&cont_info, &field_info);

	for (i = 0; i < *numSlots; i++)
	{
		ret = gsCreateRowByContainer(fmstate->cont, &row);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, fmstate->cont);
		/* Create element for rowList for gsPutMultipleRows */
		griddb_bind_for_putrow(fmstate, slots[i], row, resultRelInfo->ri_RelationDesc,
							   &field_info);
		rowList[i] = row;
	}
	griddb_free_column_info(&field_info);

	/* Switch to old context */
	MemoryContextSwitchTo(oldcontext);

	rowObj = (void *) rowList;

	ret = gsPutMultipleRows(fmstate->cont, rowObj, *numSlots, &exists);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, fmstate->cont);

	/* exists is alway return false by gsPutMultipleRows */
	if (exists)
		elog(WARNING, "row was updated instead of insert "
			 "because same row key has already existed.");

	for (i = 0; i < *numSlots; i++)
	{
		gsCloseRow(&rowList[i]);
	}

	/* Free allocated memory in temp context */
	MemoryContextReset(fmstate->temp_cxt);

	return slots;
}

/*
 * griddbGetForeignModifyBatchSize
 *		Determine the maximum number of tuples that can be inserted in bulk
 *
 * Returns the batch size specified for server or table. When batching is not
 * allowed (e.g. for tables with AFTER ROW triggers or with RETURNING clause),
 * returns 1.
 */
static int
griddbGetForeignModifyBatchSize(ResultRelInfo *resultRelInfo)
{
	int			batch_size;
	GridDBFdwModifyState *fmstate = (GridDBFdwModifyState *) resultRelInfo->ri_FdwState;

	/* should be called only once */
	Assert(resultRelInfo->ri_BatchSize == 0);

	/*
	 * Should never get called when the insert is being performed on a table
	 * that is also among the target relations of an UPDATE operation, because
	 * postgresBeginForeignInsert() currently rejects such insert attempts.
	 */
	Assert(fmstate == NULL || fmstate->aux_fmstate == NULL);

	/*
	 * In EXPLAIN without ANALYZE, ri_fdwstate is NULL, so we have to lookup
	 * the option directly in server/table options. Otherwise just use the
	 * value we determined earlier.
	 */
	if (fmstate)
		batch_size = fmstate->batch_size;
	else
		batch_size = get_batch_size_option(resultRelInfo->ri_RelationDesc);

	/*
	 * Disable batching when we have to use RETURNING or there are any
	 * BEFORE/AFTER ROW INSERT triggers on the foreign table.
	 *
	 * When there are any BEFORE ROW INSERT triggers on the table, we can't
	 * support it, because such triggers might query the table we're inserting
	 * into and act differently if the tuples that have already been processed
	 * and prepared for insertion are not there.
	 */
	if (resultRelInfo->ri_projectReturning != NULL ||
		resultRelInfo->ri_WithCheckOptions != NIL ||
		(resultRelInfo->ri_TrigDesc &&
		 (resultRelInfo->ri_TrigDesc->trig_insert_before_row ||
		  resultRelInfo->ri_TrigDesc->trig_insert_after_row)))
		return 1;

	/*
	 * If the foreign table has no columns, disable batching as the INSERT
	 * syntax doesn't allow batching multiple empty rows into a zero-column
	 * table in a single statement.  This is needed for COPY FROM, in which
	 * case fmstate must be non-NULL.
	 */
	if (fmstate && list_length(fmstate->target_attrs) == 0)
		return 1;

	/*
	 * GridDB c-API has no limit with max number of row (rowCount) in
	 * gsPutMultipleRows(), but rowCount should be limited to 65535 (uint16)
	 * same as postgres_fdw for safe.
	 */
	if (fmstate)
		batch_size = Min(batch_size, DEFAULT_QUERY_PARAM_MAX_LIMIT);

	return batch_size;
}
#endif

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
	GridDBFdwRowKeyHashEntry *rowkey_hash_entry;
	MemoryContext oldcontext;

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	/* Check if it is already modified or not. */
	rowkey = ExecGetJunkAttribute(planSlot, fmstate->junk_att_no, &isnull);
	Assert(isnull == false);

	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);
	rowkey_hash_entry = griddb_rowkey_hash_search(fmstate->modified_rowkeys, rowkey, &found);
	if (found)
		return NULL;

	griddb_check_rowkey_update(fmstate, slot);

	if (!fmstate->bulk_mode)
		griddb_judge_bulk_mode(fmstate, planSlot);

	MemoryContextSwitchTo(oldcontext);

	if (fmstate->bulk_mode)
	{
		/*
		 * Memorize modified row information. They will be updated in
		 * griddbEndForeignModify.
		 */
		Datum		rowkey_datum = griddb_modify_target_insert(&fmstate->modified_rows, slot, planSlot, fmstate->junk_att_no, fmstate->target_attrs, &smrelay->field_info);

		griddb_rowkey_hash_set(rowkey_hash_entry, rowkey_datum, NULL);
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
		griddb_rowkey_hash_set(rowkey_hash_entry, rowkey, attr);
	}

	/* Free memory in temp context */
	MemoryContextReset(fmstate->temp_cxt);

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
	GridDBFdwRowKeyHashEntry *rowkey_hash_entry;
	MemoryContext	oldcontext;

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	/* Check if it is already modified or not. */
	rowkey = ExecGetJunkAttribute(planSlot, fmstate->junk_att_no, &isnull);
	Assert(isnull == false);

	/*
	* griddb_rowkey_hash_search will create new entry if not found, so griddb_rowkey_hash_create will be called,
	* and then griddb_get_comparator_datum is called. In griddb_get_comparator_datum, some variables will be allocated
	* for each data type, the lifecyle of these variables is only within griddbExecForeignDelete, so we need to manage
	* memory of these variables by using memory context.
	*/
	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	rowkey_hash_entry = griddb_rowkey_hash_search(fmstate->modified_rowkeys, rowkey, &found);
	if (found)
		return NULL;

	if (!fmstate->bulk_mode)
		griddb_judge_bulk_mode(fmstate, planSlot);

	MemoryContextSwitchTo(oldcontext);

	if (fmstate->bulk_mode)
	{
		/*
		 * Memorize modified row information. They will be deleted in
		 * griddbEndForeignModify.
		 */
		Datum		rowkey_datum = griddb_modify_target_insert(&fmstate->modified_rows, slot, planSlot, fmstate->junk_att_no, fmstate->target_attrs, &smrelay->field_info);

		griddb_rowkey_hash_set(rowkey_hash_entry, rowkey_datum, NULL);
	}
	else
	{
		Form_pg_attribute attr;

		/* Delete row */
		ret = gsDeleteCurrentRow(smrelay->row_set);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, fmstate->cont);

		attr = TupleDescAttr(planSlot->tts_tupleDescriptor, fmstate->junk_att_no - 1);
		griddb_rowkey_hash_set(rowkey_hash_entry, rowkey, attr);
	}

	/* Free memory which is allocated in the temp context */
	MemoryContextReset(fmstate->temp_cxt);

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

	elog(DEBUG1, "griddb_fdw: %s", __func__);

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
	Index		resultRelation;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	RangeTblEntry *rte;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			attnum;
	StringInfoData sql;
	List	   *targetAttrs = NIL;

	elog(DEBUG1, "griddb_fdw: %s", __func__);

#if (PG_VERSION_NUM >= 110007)

	/*
	 * If the foreign table we are about to insert routed rows into is also an
	 * UPDATE subplan result rel that will be updated later, proceeding with
	 * the INSERT will result in the later UPDATE incorrectly modifying those
	 * routed rows, so prevent the INSERT --- it would be nice if we could
	 * handle this case; but for now, throw an error for safety.
	 */
	if (plan && plan->operation == CMD_UPDATE &&
		(resultRelInfo->ri_usesFdwDirectModify ||
		 resultRelInfo->ri_FdwState)
#if PG_VERSION_NUM < 140000
		&& resultRelInfo > mtstate->resultRelInfo + mtstate->mt_whichplan
#endif
		)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot route tuples into foreign table to be updated \"%s\"",
						RelationGetRelationName(rel))));
	}
#endif

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
	if (resultRelInfo->ri_RangeTableIndex == 0)
	{
		ResultRelInfo *rootResultRelInfo = resultRelInfo->ri_RootResultRelInfo;
#if (PG_VERSION_NUM >= 120000)
		rte = exec_rt_fetch(rootResultRelInfo->ri_RangeTableIndex, estate);
#else
		rte = list_nth(estate->es_range_table, rootResultRelInfo->ri_RangeTableIndex - 1);
#endif
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
#if (PG_VERSION_NUM >= 120000)
			rootResultRelInfo->ri_RangeTableIndex == plan->rootRelation
#else
			rootResultRelInfo->ri_RangeTableIndex == plan->nominalRelation
#endif
			)
			resultRelation = mtstate->resultRelInfo[0].ri_RangeTableIndex;
		else
			resultRelation = rootResultRelInfo->ri_RangeTableIndex;

	}
	else
	{
		resultRelation = resultRelInfo->ri_RangeTableIndex;
#if (PG_VERSION_NUM >= 120000)
		rte = exec_rt_fetch(resultRelation, estate);
#else
		rte = list_nth(estate->es_range_table, resultRelation - 1);
#endif
	}
	/* Construct an execution state. */
	fmstate = create_foreign_modify(mtstate->ps.state,
									rte,
									resultRelInfo,
									CMD_INSERT,
									NULL,
									targetAttrs);

	if (resultRelInfo->ri_FdwState)
	{
		Assert(plan && plan->operation == CMD_UPDATE);
		Assert(resultRelInfo->ri_usesFdwDirectModify == false);
		((GridDBFdwModifyState *) resultRelInfo->ri_FdwState)->aux_fmstate = fmstate;
	}
	else
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

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	Assert(fmstate != NULL);

	/*
	 * If the fmstate has aux_fmstate set, get the aux_fmstate (see
	 * griddbBeginForeignInsert())
	 */
	if (fmstate->aux_fmstate)
		fmstate = fmstate->aux_fmstate;

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

	elog(DEBUG1, "griddb_fdw: %s", __func__);

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

	elog(DEBUG1, "griddb_fdw: %s", __func__);

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
	elog(DEBUG1, "griddb_fdw: %s", __func__);
#if (PG_VERSION_NUM >= 140000)
	if (es->verbose)
	{
		/*
		 * For batch insert we should have batch size > 1, we only show the
		 * property with BATCH INSERT feature
		 */
		if (rinfo->ri_BatchSize > 1)
		{
			ExplainPropertyInteger("Batch Size", NULL, rinfo->ri_BatchSize, es);
		}
	}
#endif
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

	elog(DEBUG1, "griddb_fdw: %s", __func__);

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
 * get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 */
static List *
griddb_get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_pathkeys_list = NIL;
	List	   *useful_eclass_list;
	GriddbFdwRelationInfo *fpinfo = (GriddbFdwRelationInfo *) rel->fdw_private;
	EquivalenceClass *query_ec = NULL;
	ListCell   *lc;

	/*
	 * Pushing the query_pathkeys to the remote server is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	fpinfo->qp_is_pushdown_safe = false;
	if (root->query_pathkeys)
	{
		bool		query_pathkeys_ok = true;

		foreach(lc, root->query_pathkeys)
		{
			PathKey    *pathkey = (PathKey *) lfirst(lc);
			EquivalenceMember *em = griddb_find_em_for_rel(root, pathkey->pk_eclass, rel);

			/*
			 * The planner and executor don't have any clever strategy for
			 * taking data sorted by a prefix of the query's pathkeys and
			 * getting it to be sorted by all of those pathkeys. We'll just
			 * end up resorting the entire data set.  So, unless we can push
			 * down all of the query pathkeys, forget it.
			 *
			 * For Griddb, NULL value is sorted as max value. We don't
			 * pushdown ORDER BY clause in the following cases: (1) DESC NULLS
			 * LAST (2) ASC NULLS FIRST
			 *
			 * Griddb does not support ORDER BY functions/formulas.
			 */
			if (!griddb_is_foreign_pathkey(root, rel, pathkey) ||
				((pathkey->pk_strategy == BTLessStrategyNumber) &&
				 pathkey->pk_nulls_first) ||	/* ASC NULLS FIRST */
				((pathkey->pk_strategy != BTLessStrategyNumber) &&
				 !pathkey->pk_nulls_first) ||	/* DESC NULLS LAST */
				nodeTag(em->em_expr) == T_OpExpr ||
				nodeTag(em->em_expr) == T_FuncExpr ||
				nodeTag(em->em_expr) == T_BoolExpr)
			{
				query_pathkeys_ok = false;
				break;
			}
		}

		if (query_pathkeys_ok)
		{
			useful_pathkeys_list = list_make1(list_copy(root->query_pathkeys));
			fpinfo->qp_is_pushdown_safe = true;
		}
	}

	/*
	 * Even if we're not using remote estimates, having the remote side do the
	 * sort generally won't be any worse than doing it locally, and it might
	 * be much better if the remote side can generate data in the right order
	 * without needing a sort at all.  However, what we're going to do next is
	 * try to generate pathkeys that seem promising for possible merge joins,
	 * and that's more speculative.  A wrong choice might hurt quite a bit, so
	 * bail out if we can't use remote estimates.
	 */
	if (!fpinfo->use_remote_estimate)
		return useful_pathkeys_list;

	/* Get the list of interesting EquivalenceClasses. */
	useful_eclass_list = griddb_get_useful_ecs_for_relation(root, rel);

	/* Extract unique EC for query, if any, so we don't consider it again. */
	if (list_length(root->query_pathkeys) == 1)
	{
		PathKey    *query_pathkey = linitial(root->query_pathkeys);

		query_ec = query_pathkey->pk_eclass;
	}

	/*
	 * As a heuristic, the only pathkeys we consider here are those of length
	 * one.  It's surely possible to consider more, but since each one we
	 * choose to consider will generate a round-trip to the remote side, we
	 * need to be a bit cautious here.  It would sure be nice to have a local
	 * cache of information about remote index definitions...
	 */
	foreach(lc, useful_eclass_list)
	{
		EquivalenceClass *cur_ec = lfirst(lc);
		PathKey    *pathkey;

		/* If redundant with what we did above, skip it. */
		if (cur_ec == query_ec)
			continue;

		/* Can't push down the sort if the EC's opfamily is not built-in. */
		if (!griddb_is_builtin(linitial_oid(cur_ec->ec_opfamilies)))
			continue;

		/* If no pushable expression for this rel, skip it. */
		if (griddb_find_em_for_rel(root, cur_ec, rel) == NULL)
			continue;

		/* Looks like we can generate a pathkey, so let's do it. */
		pathkey = make_canonical_pathkey(root, cur_ec,
										 linitial_oid(cur_ec->ec_opfamilies),
										 BTLessStrategyNumber,
										 false);
		useful_pathkeys_list = lappend(useful_pathkeys_list,
									   list_make1(pathkey));
	}

	return useful_pathkeys_list;
}

/*
 * get_useful_ecs_for_relation Determine which EquivalenceClasses might be
 * involved in useful orderings of this relation.
 *
 * This function is in some respects a mirror image of the core function
 * pathkeys_useful_for_merging: for a regular table, we know what indexes we
 * have and want to test whether any of them are useful.  For a foreign
 * table, we don't know what indexes are present on the remote side but want
 * to speculate about which ones we'd like to use if they existed.
 *
 * This function returns a list of potentially-useful equivalence classes,
 * but it does not guarantee that an EquivalenceMember exists which contains
 * Vars only from the given relation.  For example, given ft1 JOIN t1 ON
 * ft1.x + t1.x = 0, this function will say that the equivalence class
 * containing ft1.x + t1.x is potentially useful.  Supposing ft1 is remote
 * and t1 is local (or on a different server), it will turn out that no
 * useful ORDER BY clause can be generated.  It's not our job to figure that
 * out here; we're only interested in identifying relevant ECs.
 */
static List *
griddb_get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_eclass_list = NIL;
	ListCell   *lc;
	Relids		relids;

	/*
	 * First, consider whether any active EC is potentially useful for a merge
	 * join against this relation.
	 */
	if (rel->has_eclass_joins)
	{
		foreach(lc, root->eq_classes)
		{
			EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc);

			if (eclass_useful_for_merging(root, cur_ec, rel))
				useful_eclass_list = lappend(useful_eclass_list, cur_ec);
		}
	}

	/*
	 * Next, consider whether there are any non-EC derivable join clauses that
	 * are merge-joinable.  If the joininfo list is empty, we can exit
	 * quickly.
	 */
	if (rel->joininfo == NIL)
		return useful_eclass_list;

	/*
	 * If this is a child rel, we must use the topmost parent rel to search.
	 */
	if (IS_OTHER_REL(rel))
	{
#if (PG_VERSION_NUM >= 100000)
		Assert(!bms_is_empty(rel->top_parent_relids));
		relids = rel->top_parent_relids;
#else
		relids = find_childrel_top_parent(root, rel)->relids;
#endif
	}
	else
		relids = rel->relids;

	/* Check each join clause in turn. */
	foreach(lc, rel->joininfo)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);

		/* Consider only mergejoinable clauses */
		if (restrictinfo->mergeopfamilies == NIL)
			continue;

		/* Make sure we've got canonical ECs. */
		update_mergeclause_eclasses(root, restrictinfo);

		/*
		 * restrictinfo->mergeopfamilies != NIL is sufficient to guarantee
		 * that left_ec and right_ec will be initialized, per comments in
		 * distribute_qual_to_rels.
		 *
		 * We want to identify which side of this merge-joinable clause
		 * contains columns from the relation produced by this RelOptInfo. We
		 * test for overlap, not containment, because there could be extra
		 * relations on either side.  For example, suppose we've got something
		 * like ((A JOIN B ON A.x = B.x) JOIN C ON A.y = C.y) LEFT JOIN D ON
		 * A.y = D.y. The input rel might be the joinrel between A and B, and
		 * we'll consider the join clause A.y = D.y. relids contains a
		 * relation not involved in the join class (B) and the equivalence
		 * class for the left-hand side of the clause contains a relation not
		 * involved in the input rel (C). Despite the fact that we have only
		 * overlap and not containment in either direction, A.y is potentially
		 * useful as a sort column.
		 *
		 * Note that it's even possible that relids overlaps neither side of
		 * the join clause.  For example, consider A LEFT JOIN B ON A.x = B.x
		 * AND A.x = 1.  The clause A.x = 1 will appear in B's joininfo list,
		 * but overlaps neither side of B.  In that case, we just skip this
		 * join clause, since it doesn't suggest a useful sort order for this
		 * relation.
		 */
		if (bms_overlap(relids, restrictinfo->right_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->right_ec);
		else if (bms_overlap(relids, restrictinfo->left_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->left_ec);
	}

	return useful_eclass_list;
}

static void
griddb_add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
									   Path *epq_path)
{
	List	   *useful_pathkeys_list = NIL; /* List of all pathkeys */
	ListCell   *lc;
	double		rows;
	Cost		startup_cost;
	Cost		total_cost;

	/* Use small cost to push down ORDER BY always */
	rows = startup_cost = total_cost = 10;

	useful_pathkeys_list = griddb_get_useful_pathkeys_for_relation(root, rel);

	/*
	 * Before creating sorted paths, arrange for the passed-in EPQ path, if
	 * any, to return columns needed by the parent ForeignScan node so that
	 * they will propagate up through Sort nodes injected below, if necessary.
	 */
	if (epq_path != NULL && useful_pathkeys_list != NIL)
	{
		GriddbFdwRelationInfo *fpinfo = (GriddbFdwRelationInfo *) rel->fdw_private;
		PathTarget *target = copy_pathtarget(epq_path->pathtarget);

		/* Include columns required for evaluating PHVs in the tlist. */
		add_new_columns_to_pathtarget(target,
									  pull_var_clause((Node *) target->exprs,
													  PVC_RECURSE_PLACEHOLDERS));

		/* Include columns required for evaluating the local conditions. */
		foreach(lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			add_new_columns_to_pathtarget(target,
										  pull_var_clause((Node *) rinfo->clause,
														  PVC_RECURSE_PLACEHOLDERS));
		}

		/*
		 * If we have added any new columns, adjust the tlist of the EPQ path.
		 *
		 * Note: the plan created using this path will only be used to execute
		 * EPQ checks, where accuracy of the plan cost and width estimates
		 * would not be important, so we do not do set_pathtarget_cost_width()
		 * for the new pathtarget here.  See also postgresGetForeignPlan().
		 */
		if (list_length(target->exprs) > list_length(epq_path->pathtarget->exprs))
		{
			/* The EPQ path is a join path, so it is projection-capable. */
			Assert(is_projection_capable_path(epq_path));

			/*
			 * Use create_projection_path() here, so as to avoid modifying it
			 * in place.
			 */
			epq_path = (Path *) create_projection_path(root,
													   rel,
													   epq_path,
													   target);
		}
	}

	/* Create one path for each set of pathkeys we found above. */
	foreach(lc, useful_pathkeys_list)
	{
		List	   *useful_pathkeys = lfirst(lc);
		Path	   *sorted_epq_path;

		/*
		 * The EPQ path must be at least as well sorted as the path itself, in
		 * case it gets used as input to a mergejoin.
		 */
		sorted_epq_path = epq_path;
		if (sorted_epq_path != NULL &&
			!pathkeys_contained_in(useful_pathkeys,
								   sorted_epq_path->pathkeys))
			sorted_epq_path = (Path *)
				create_sort_path(root,
								 rel,
								 sorted_epq_path,
								 useful_pathkeys,
								 -1.0);

		if (IS_SIMPLE_REL(rel))
			add_path(rel, (Path *)
					 create_foreignscan_path(root, rel,
											 NULL,
											 rows,
											 startup_cost,
											 total_cost,
											 useful_pathkeys,
											 rel->lateral_relids,
											 sorted_epq_path,
											 NIL));
		else
			add_path(rel, (Path *)
#if PG_VERSION_NUM >= 120000
					 create_foreign_join_path
#else
					 create_foreignscan_path
#endif
					 (root, rel,
					  NULL,
					  rows,
					  startup_cost,
					  total_cost,
					  useful_pathkeys,
#if (PG_VERSION_NUM >= 120000)
					  rel->lateral_relids,
#else
					  NULL,		/* no outer rel either */
#endif
					  sorted_epq_path,
					  NIL));
	}
}

/*
 * Assess whether the aggregation, grouping and having operations can be pushed
 * down to the foreign server.  As a side effect, save information we obtain in
 * this function to GriddbFdwRelationInfo of the input relation.
 */
static bool
griddb_foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel)
{
	Query	   *query = root->parse;
	GriddbFdwRelationInfo *fpinfo = (GriddbFdwRelationInfo *) grouped_rel->fdw_private;
	PathTarget *grouping_target;
	GriddbFdwRelationInfo *ofpinfo;
	ListCell   *lc;
	int			i;
	List	   *tlist = NIL;

	/* We currently don't support pushing Grouping Sets. */
	if (query->groupingSets)
		return false;

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (GriddbFdwRelationInfo *) fpinfo->outerrel->fdw_private;

	/*
	 * If underlying scan relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	/*
	 * For version before 11, the targetlist expected from this node and the
	 * targetlist pushed down to the foreign server may be different. The latter
	 * requires sortgrouprefs to be set to push down GROUP BY clause, but should
	 * not have those arising from ORDER BY clause. These sortgrouprefs may be
	 * different from those in the plan's targetlist. Use a copy of path
	 * target to record the new sortgrouprefs.
	 * For later version, can use reltarget directly.
	 */
#if PG_VERSION_NUM < 110000
	grouping_target = copy_pathtarget(root->upper_targets[UPPERREL_GROUP_AGG]);
#else
	grouping_target = grouped_rel->reltarget;
#endif
	/*
	 * Examine grouping expressions, as well as other expressions we'd need to
	 * compute, and check whether they are safe to push down to the foreign
	 * server.  All GROUP BY expressions will be part of the grouping target
	 * and thus there is no need to search for them separately.  Add grouping
	 * expressions into target list which will be passed to foreign server.
	 *
	 * A tricky fine point is that we must not put any expression into the
	 * target list that is just a foreign param (that is, something that
	 * deparse.c would conclude has to be sent to the foreign server).  If we
	 * do, the expression will also appear in the fdw_exprs list of the plan
	 * node, and setrefs.c will get confused and decide that the fdw_exprs
	 * entry is actually a reference to the fdw_scan_tlist entry, resulting in
	 * a broken plan.  Somewhat oddly, it's OK if the expression contains such
	 * a node, as long as it's not at top level; then no match is possible.
	 */
	i = 0;

	foreach(lc, grouping_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		ListCell   *l;

		/*
		 * Non-grouping expression we need to compute.  Can we ship it as-is
		 * to the foreign server?
		 */
		if (griddb_is_foreign_expr(root, grouped_rel, expr, true) &&
			!griddb_is_foreign_param(root, grouped_rel, expr))
		{
			/* Yes, so add to tlist as-is; OK to suppress duplicates */
			tlist = add_to_flat_tlist(tlist, list_make1(expr));

			/*
			 * GridDB does not support selecting multiple target, so do not
			 * push down when there are multiple items in target list.
			 */
			if (list_length(tlist) > 1)
				return false;
		}
		else
		{
			/* Not pushable as a whole; extract its Vars and aggregates */
			List	   *aggvars;

			aggvars = pull_var_clause((Node *) expr,
									  PVC_INCLUDE_AGGREGATES);

			/*
			 * If any aggregate expression is not shippable, then we cannot
			 * push down aggregation to the foreign server.  (We don't have to
			 * check is_foreign_param, since that certainly won't return true
			 * for any such expression.)
			 */
			if (!griddb_is_foreign_expr(root, grouped_rel, (Expr *) aggvars, true))
				return false;

			/*
			 * Add aggregates, if any, into the targetlist.  Plain Vars
			 * outside an aggregate can be ignored, because they should be
			 * either same as some GROUP BY column or part of some GROUP BY
			 * expression.  In either case, they are already part of the
			 * targetlist and thus no need to add them again.  In fact
			 * including plain Vars in the tlist when they do not match a
			 * GROUP BY column would cause the foreign server to complain that
			 * the shipped query is invalid.
			 */
			foreach(l, aggvars)
			{
				Expr	   *aggref = (Expr *) lfirst(l);

				if (IsA(aggref, Aggref))
					tlist = add_to_flat_tlist(tlist, list_make1(aggref));
			}

			/*
			 * GridDB does not support selecting multiple target, so do not
			 * push down when there are multiple items in target list.
			 */
			if (list_length(tlist) > 1)
				return false;
		}

		i++;
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds)
	{
		List	   *aggvars = NIL;
		ListCell   *lc;

		foreach(lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_INCLUDE_AGGREGATES));
		}

		foreach(lc, aggvars)
		{
			Expr	   *expr = (Expr *) lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.  Again, we need not check
			 * is_foreign_param for a foreign aggregate.
			 */
			if (IsA(expr, Aggref))
			{
				if (!griddb_is_foreign_expr(root, grouped_rel, expr, true))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to estimate_path_cost_size.
	 */
	fpinfo->retrieved_rows = -1;
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.  Note that the decoration we add
	 * to the base relation name mustn't include any digits, or it'll confuse
	 * postgresExplainForeignScan.
	 */

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();

	return true;
}

/*
 * griddb_add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
griddb_add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
								  RelOptInfo *grouped_rel
#if (PG_VERSION_NUM >= 110000)
								  ,GroupPathExtraData *extra
#endif
)
{
	Query	   *parse = root->parse;
	GriddbFdwRelationInfo *ifpinfo = input_rel->fdw_private;
	GriddbFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
	ForeignPath *grouppath;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;

	/*
	 * Nothing to be done, if there is no aggregation required. Griddb does
	 * not support GROUP BY, GROUPING SET, HAVING, so also return when there
	 * are those clauses.
	 */
	if (parse->groupClause ||
		parse->groupingSets ||
		root->hasHavingQual ||
		!parse->hasAggs)
		return;

#if (PG_VERSION_NUM >= 110000)
	Assert(extra->patype == PARTITIONWISE_AGGREGATE_NONE ||
		   extra->patype == PARTITIONWISE_AGGREGATE_FULL);
#endif

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	fpinfo->user = ifpinfo->user;

	/*
	 * Assess if it is safe to push down aggregation and grouping.
	 *
	 * Use HAVING qual from extra. In case of child partition, it will have
	 * translated Vars.
	 */
	if (!griddb_foreign_grouping_ok(root, grouped_rel))
		return;

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  (Currently we create just a single
	 * path here, but in future it would be possible that we build more paths
	 * such as pre-sorted paths as in postgresGetForeignPaths and
	 * postgresGetForeignJoinPaths.)  The best we can do for these conditions
	 * is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 0,
													 JOIN_INNER,
													 NULL);

	/* Use small cost to push down aggregate always */
	rows = width = startup_cost = total_cost = 1;

	/* Now update this information in the fpinfo */
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/* Create and add foreign path to the grouping relation. */
#if (PG_VERSION_NUM >= 120000)
	grouppath = create_foreign_upper_path(root,
										  grouped_rel,
										  grouped_rel->reltarget,
										  rows,
										  startup_cost,
										  total_cost,
										  NIL,	/* no pathkeys */
										  NULL,
										  NIL); /* no fdw_private */
#else
	grouppath = create_foreignscan_path(root,
										grouped_rel,
										root->upper_targets[UPPERREL_GROUP_AGG],
										rows,
										startup_cost,
										total_cost,
										NIL,	/* no pathkeys */
										NULL,	/* no required_outer */
										NULL,
										NIL);	/* no fdw_private */
#endif

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *) grouppath);

}

/*
 * griddb_add_foreign_final_paths
 *		Add foreign paths for performing the final processing remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given final_rel.
 */
static void
griddb_add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel,
							   RelOptInfo *final_rel
#if (PG_VERSION_NUM >= 120000)
							   ,FinalPathExtraData *extra
#endif
)
{
	Query	   *parse = root->parse;
	GriddbFdwRelationInfo *ifpinfo = (GriddbFdwRelationInfo *) input_rel->fdw_private;
	GriddbFdwRelationInfo *fpinfo = (GriddbFdwRelationInfo *) final_rel->fdw_private;
	bool		has_final_sort = false;
	List	   *pathkeys = NIL;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *fdw_private;
	ForeignPath *final_path;

	/*
	 * Currently, we only support this for SELECT commands
	 */
	if (parse->commandType != CMD_SELECT)
		return;

	/*
	 * Currently, we do not support FOR UPDATE/SHARE
	 */
	if (parse->rowMarks)
		return;

	/*
	 * No work if there is no FOR UPDATE/SHARE clause and if there is no need
	 * to add a LIMIT node
	 */
	if (!parse->rowMarks
#if (PG_VERSION_NUM >= 120000)
		&& !extra->limit_needed
#endif
		)
		return;

#if (PG_VERSION_NUM >= 100000)
	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;
#endif
	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	fpinfo->user = ifpinfo->user;

#if (PG_VERSION_NUM >= 120000)

	/*
	 * If there is no need to add a LIMIT node, there might be a ForeignPath
	 * in the input_rel's pathlist that implements all behavior of the query.
	 * Note: we would already have accounted for the query's FOR UPDATE/SHARE
	 * (if any) before we get here.
	 */
	if (!extra->limit_needed)
	{
		ListCell   *lc;

		Assert(parse->rowMarks);

		/*
		 * Grouping and aggregation are not supported with FOR UPDATE/SHARE,
		 * so the input_rel should be a base, join, or ordered relation; and
		 * if it's an ordered relation, its input relation should be a base or
		 * join relation.
		 */
		Assert(input_rel->reloptkind == RELOPT_BASEREL ||
			   input_rel->reloptkind == RELOPT_JOINREL ||
			   (input_rel->reloptkind == RELOPT_UPPER_REL &&
				ifpinfo->stage == UPPERREL_ORDERED &&
				(ifpinfo->outerrel->reloptkind == RELOPT_BASEREL ||
				 ifpinfo->outerrel->reloptkind == RELOPT_JOINREL)));

		foreach(lc, input_rel->pathlist)
		{
			Path	   *path = (Path *) lfirst(lc);

			/*
			 * apply_scanjoin_target_to_paths() uses create_projection_path()
			 * to adjust each of its input paths if needed, whereas
			 * create_ordered_paths() uses apply_projection_to_path() to do
			 * that.  So the former might have put a ProjectionPath on top of
			 * the ForeignPath; look through ProjectionPath and see if the
			 * path underneath it is ForeignPath.
			 */
			if (IsA(path, ForeignPath) ||
				(IsA(path, ProjectionPath) &&
				 IsA(((ProjectionPath *) path)->subpath, ForeignPath)))
			{
				/*
				 * Create foreign final path; this gets rid of a
				 * no-longer-needed outer plan (if any), which makes the
				 * EXPLAIN output look cleaner
				 */
#if (PG_VERSION_NUM >= 120000)
				final_path = create_foreign_upper_path(root,
													   path->parent,
													   path->pathtarget,
													   path->rows,
													   path->startup_cost,
													   path->total_cost,
													   path->pathkeys,
													   NULL,	/* no extra plan */
													   NULL);	/* no fdw_private */
#else
				final_path = create_foreignscan_path(root,
													 input_rel,
													 root->upper_targets[UPPERREL_FINAL],
													 rows,
													 startup_cost,
													 total_cost,
													 pathkeys,
													 NULL,	/* no required_outer */
													 NULL,	/* no extra plan */
													 fdw_private);
#endif
				/* and add it to the final_rel */
				add_path(final_rel, (Path *) final_path);

				/* Safe to push down */
				fpinfo->pushdown_safe = true;

				return;
			}
		}

		/*
		 * If we get here it means no ForeignPaths; since we would already
		 * have considered pushing down all operations for the query to the
		 * remote server, give up on it.
		 */
		return;
	}

	Assert(extra->limit_needed);
#endif

	/*
	 * If the input_rel is an ordered relation, replace the input_rel with its
	 * input relation
	 */
	if (input_rel->reloptkind == RELOPT_UPPER_REL &&
		ifpinfo->stage == UPPERREL_ORDERED)
	{
		input_rel = ifpinfo->outerrel;
		ifpinfo = (GriddbFdwRelationInfo *) input_rel->fdw_private;
		has_final_sort = true;
		pathkeys = root->sort_pathkeys;
	}

	/* The input_rel should be a base, join, or grouping relation */
	Assert(input_rel->reloptkind == RELOPT_BASEREL ||
		   input_rel->reloptkind == RELOPT_JOINREL ||
		   (input_rel->reloptkind == RELOPT_UPPER_REL &&
			ifpinfo->stage == UPPERREL_GROUP_AGG));

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying base, join, or grouping relation to perform the final
	 * sort (if has_final_sort) and the LIMIT restriction remotely, which is
	 * stored into the fdw_private list of the resulting path.  (We
	 * re-estimate the costs of sorting the underlying relation, if
	 * has_final_sort.)
	 */

	/*
	 * Assess if it is safe to push down the LIMIT and OFFSET to the remote
	 * server
	 */

	/*
	 * If the underlying relation has any local conditions, the LIMIT/OFFSET
	 * cannot be pushed down.
	 */
	if (ifpinfo->local_conds)
		return;

	/*
	 * When query contains OFFSET but no LIMIT, do not push down because
	 * GridDB does not support.
	 */
	if (!parse->limitCount && parse->limitOffset)
		return;

	/*
	 * Also, the LIMIT/OFFSET cannot be pushed down, if their expressions are
	 * not safe to remote.
	 */
	if (!griddb_is_foreign_expr(root, input_rel, (Expr *) parse->limitOffset, false) ||
		!griddb_is_foreign_expr(root, input_rel, (Expr *) parse->limitCount, false))
		return;

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* Use small cost to push down limit always */
	rows = width = startup_cost = total_cost = 1;
	/* Now update this information in the fpinfo */
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/*
	 * Build the fdw_private list that will be used by postgresGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeInteger(has_final_sort)
#if (PG_VERSION_NUM >= 120000)
							 ,makeInteger(extra->limit_needed));
#else
							 ,makeInteger(false));
#endif

	/*
	 * Create foreign final path; this gets rid of a no-longer-needed outer
	 * plan (if any), which makes the EXPLAIN output look cleaner
	 */
#if (PG_VERSION_NUM >= 120000)
	final_path = create_foreign_upper_path(root,
										   input_rel,
										   root->upper_targets[UPPERREL_FINAL],
										   rows,
										   startup_cost,
										   total_cost,
										   pathkeys,
										   NULL,	/* no extra plan */
										   fdw_private);
#else
	final_path = create_foreignscan_path(root,
										 input_rel,
										 root->upper_targets[UPPERREL_FINAL],
										 rows,
										 startup_cost,
										 total_cost,
										 pathkeys,
										 NULL,	/* no required_outer */
										 NULL,	/* no extra plan */
										 fdw_private);
#endif

	/* and add it to the final_rel */
	add_path(final_rel, (Path *) final_path);
}

/*
 * griddbGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 *		Currently, we only support push down LIMIT...OFFSET
 */
static void
griddbGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
						   RelOptInfo *input_rel, RelOptInfo *output_rel
#if (PG_VERSION_NUM >= 110000)
						   ,
						   void *extra
#endif
)
{
	GriddbFdwRelationInfo *fpinfo;

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the foreign server.
	 */
	if (!input_rel->fdw_private ||
		!((GriddbFdwRelationInfo *) input_rel->fdw_private)->pushdown_safe)
		return;

	/*
	 * Ignore stages we don't support; and skip any duplicate calls. We only
	 * support LIMIT...OFFSET and aggregation push down
	 */
	if ((stage != UPPERREL_GROUP_AGG &&
		 stage != UPPERREL_FINAL) ||
		output_rel->fdw_private)
		return;

	fpinfo = (GriddbFdwRelationInfo *) palloc0(sizeof(GriddbFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	fpinfo->stage = stage;
	output_rel->fdw_private = fpinfo;

	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
			griddb_add_foreign_grouping_paths(root, input_rel, output_rel
#if (PG_VERSION_NUM >= 110000)
											  ,(GroupPathExtraData *) extra
#endif
				);
			break;
		case UPPERREL_FINAL:
			griddb_add_foreign_final_paths(root, input_rel, output_rel
#if (PG_VERSION_NUM >= 120000)
										   ,(FinalPathExtraData *) extra
#endif
				);
			break;
		default:
			elog(ERROR, "unexpected upper relation: %d", (int) stage);
			break;
	}
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
						GriddbFdwPathExtraData * fpextra,
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

			/*
			 * If we estimate the costs of a foreign scan or a foreign join
			 * with additional post-scan/join-processing steps, the scan or
			 * join costs obtained from the cache wouldn't yet contain the
			 * eval costs for the final scan/join target, which would've been
			 * updated by apply_scanjoin_target_to_paths(); add the eval costs
			 * now.
			 */
			if (fpextra && !IS_UPPER_REL(foreignrel))
			{
				/* Shouldn't get here unless we have LIMIT */
				Assert(fpextra->has_limit);
				Assert(foreignrel->reloptkind == RELOPT_BASEREL ||
					   foreignrel->reloptkind == RELOPT_JOINREL);
				startup_cost += foreignrel->reltarget->cost.startup;
				run_cost += foreignrel->reltarget->cost.per_tuple * rows;
			}
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
 * Given an EquivalenceClass and a foreign relation, find an EC member
 * that can be used to sort the relation remotely according to a pathkey
 * using this EC.
 *
 * If there is more than one suitable candidate, return an arbitrary
 * one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given
 * rel and is shippable.  Caller must separately verify that the pathkey's
 * ordering operator is shippable.
 */
EquivalenceMember *
griddb_find_em_for_rel(PlannerInfo *root, EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell   *lc;

	foreach(lc, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);

		/*
		 * Note we require !bms_is_empty, else we'd accept constant
		 * expressions which are not suitable for the purpose.
		 */
		if (bms_is_subset(em->em_relids, rel->relids) &&
			!bms_is_empty(em->em_relids) &&
			griddb_is_foreign_expr(root, rel, em->em_expr, false))
			return em;
	}

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
 * Return Griddb type for aggregate function of GridDB
 */
static Oid
griddb_get_agg_type(GridDBFdwFieldInfo field_info, GridDBAggref * aggref)
{
	char	   *aggname = aggref->aggname->data;
	size_t		i;
	GSType		type = -1;

	/* count returns LONG type */
	if (strcmp(aggname, "count") == 0)
	{
		type = GS_TYPE_LONG;
	}
	/* avg, variance, stddev returns DOUBLE */
	else if ((strcmp(aggname, "avg") == 0) ||
			 (strcmp(aggname, "variance") == 0) ||
			 (strcmp(aggname, "stddev") == 0))
	{
		type = GS_TYPE_DOUBLE;
	}
	/* max, min, sum depends on the type of the specified column */
	else if ((strcmp(aggname, "max") == 0) ||
			 (strcmp(aggname, "min") == 0) ||
			 (strcmp(aggname, "sum") == 0))
	{
		for (i = 0; i < field_info.column_count; i++)
		{
			if (strcmp(aggref->columnname->data, field_info.column_names[i]) == 0)
			{
				type = field_info.column_types[i];
				break;
			}
		}

		/*
		 * sum returns LONG if the column is integer type, returns DOUBLE if
		 * the column is floating-point type
		 */
		if (strcmp(aggname, "sum") == 0)
		{
			if (type == GS_TYPE_BYTE ||
				type == GS_TYPE_SHORT ||
				type == GS_TYPE_INTEGER ||
				type == GS_TYPE_LONG)
				return GS_TYPE_LONG;
			else if (type == GS_TYPE_FLOAT ||
					 type == GS_TYPE_DOUBLE)
				type = GS_TYPE_DOUBLE;
		}
	}
	else if (strcmp(aggname, "time_avg") == 0)
	{
		type = GS_TYPE_DOUBLE;
	}
	/* Unsupported aggregate function */
	else
	{
		elog(ERROR, "GridDB does not support aggregate function %s", aggname);
	}
	return type;
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
			pg_type = BOOLARRAYOID;
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
			pg_type = INT8ARRAYOID;
			name = "bigint[]";
			break;

		case GS_TYPE_FLOAT_ARRAY:
			pg_type = FLOAT4ARRAYOID;
			name = "real[]";
			break;

		case GS_TYPE_DOUBLE_ARRAY:
			pg_type = FLOAT8ARRAYOID;
			name = "double precision[]";
			break;

		case GS_TYPE_TIMESTAMP_ARRAY:
			pg_type = TIMESTAMPARRAYOID;
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

static char *
griddb_convert_gs2pg_timestamp_to_string(GSTimestamp ts)
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

	return strdup(buf);
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

/*
 * Convert from Timestamp to GSTimestamp.
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
griddb_get_datatype_for_conversion(Oid pg_type, regproc *typeinput,
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
						   Oid pg_type, GSRowSetType row_type, GSAggregationResult * agg_res, GridDBAggref * aggref)
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
	bool		skip_check_match_type = false;

	if (row_type == GS_ROW_SET_AGGREGATION_RESULT)
	{
		char	   *aggname = aggref->aggname->data;

		/*
		 * avg, stddev, variance of postgres expects NUMERIC type, while
		 * GridDB returns DOUBLE type. => skip check type.
		 */
		if (pg_type == NUMERICOID &&
			(strcmp(aggname, "avg") == 0 ||
			 strcmp(aggname, "stddev") == 0 ||
			 strcmp(aggname, "variance") == 0))
			skip_check_match_type = true;

		/*
		 * For sum, postgres expects: + NUMERIC type for BIGINT arguments.
		 * GridDB returns LONG type => skip check type. + REAL type for REAL
		 * arguments. GridDB returns DOUBLE type => skip check type. + Other
		 * cases are equal to GridDB type => no need to skip check type.
		 */
		if ((pg_type == FLOAT4OID || pg_type == NUMERICOID) && (strcmp(aggname, "sum") == 0))
			skip_check_match_type = true;
	}

	if (pg_type != griddb_pgtyp_from_gstyp(gs_type, NULL) && skip_check_match_type == false)
		elog(ERROR, "Type conversion mismatch");

	switch (gs_type)
	{
		case GS_TYPE_STRING:
			{
				const		GSChar *strVal;

				griddb_get_datatype_for_conversion(pg_type, &typeinput, &typemod);
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

				griddb_get_datatype_for_conversion(pg_type, &typeinput, &typemod);
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
				valueDatum = Int16GetDatum((int16_t) byteVal);
				break;
			}

		case GS_TYPE_SHORT:
			{
				if (row_type == GS_ROW_SET_AGGREGATION_RESULT && agg_res)
				{
					int64_t		longVal;

					ret = gsGetAggregationValueAsLong(agg_res, &longVal, NULL);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, agg_res);
					valueDatum = Int64GetDatum(longVal);
				}
				else
				{
					int16_t		shortVal;

					ret = gsGetRowFieldAsShort(row, attid, &shortVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
					valueDatum = Int16GetDatum(shortVal);
				}
				break;
			}

		case GS_TYPE_INTEGER:
			{
				if (row_type == GS_ROW_SET_AGGREGATION_RESULT && agg_res)
				{
					int64_t		longVal;

					ret = gsGetAggregationValueAsLong(agg_res, &longVal, NULL);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, agg_res);
					valueDatum = Int64GetDatum(longVal);
				}
				else
				{
					int32_t		intVal;

					ret = gsGetRowFieldAsInteger(row, attid, &intVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
					valueDatum = Int32GetDatum(intVal);
				}
				break;
			}

		case GS_TYPE_LONG:
			{
				int64_t		longVal;

				if (row_type == GS_ROW_SET_AGGREGATION_RESULT && agg_res)
				{
					ret = gsGetAggregationValueAsLong(agg_res, &longVal, NULL);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, agg_res);
				}
				else
				{
					ret = gsGetRowFieldAsLong(row, attid, &longVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
				}
				if (pg_type == NUMERICOID)
					valueDatum = DirectFunctionCall1(int8_numeric, Int64GetDatum((int64) longVal));
				else
					valueDatum = Int64GetDatum(longVal);

				break;
			}

		case GS_TYPE_FLOAT:
			{
				if (row_type == GS_ROW_SET_AGGREGATION_RESULT && agg_res)
				{
					double		doubleVal;

					ret = gsGetAggregationValueAsDouble(agg_res, &doubleVal, NULL);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, agg_res);
					valueDatum = DirectFunctionCall1(dtof, Float8GetDatum((float8) doubleVal));
				}
				else
				{
					float		floatVal;

					ret = gsGetRowFieldAsFloat(row, attid, &floatVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
					valueDatum = Float4GetDatum(floatVal);
				}
				break;
			}

		case GS_TYPE_DOUBLE:
			{
				double		doubleVal;

				if (row_type == GS_ROW_SET_AGGREGATION_RESULT && agg_res)
				{
					ret = gsGetAggregationValueAsDouble(agg_res, &doubleVal, NULL);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, agg_res);
				}
				else
				{
					ret = gsGetRowFieldAsDouble(row, attid, &doubleVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
				}
				if (pg_type == NUMERICOID)
					valueDatum = DirectFunctionCall1(float8_numeric, Float8GetDatum((float8) doubleVal));
				else if (pg_type == FLOAT4OID)
					valueDatum = DirectFunctionCall1(dtof, Float8GetDatum((float8) doubleVal));
				else
					valueDatum = Float8GetDatum(doubleVal);

				break;
			}

		case GS_TYPE_TIMESTAMP:
			{
				GSTimestamp tsVal;
				Timestamp	timestamp;

				if (row_type == GS_ROW_SET_AGGREGATION_RESULT && agg_res)
				{
					ret = gsGetAggregationValueAsTimestamp(agg_res, &tsVal, NULL);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, agg_res);
				}
				else
				{
					ret = gsGetRowFieldAsTimestamp(row, attid, &tsVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
				}

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

				griddb_get_datatype_for_conversion(TEXTOID, &typeinput, &typemod);
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
					value_datums[i] = Int16GetDatum((int16_t) byteVal[i]);
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

static Datum
griddb_make_datum_record(StringInfoData *values, TupleDesc tupdesc, GSType * column_types,
						 GSRow * row, regproc typeinput, int typemod)
{
	int			index;
	Datum		value_datum;	/* Temporary datum */
	Datum		valueDatum;
	size_t		size;
	size_t		i;
	regproc		tmp_typeinput;
	int			tmp_typemod;

	appendStringInfoChar(values, '(');
	for (index = 0; index < tupdesc->natts; index++)
	{
		GSResult	ret;
		Oid			pg_type = TupleDescAttr(tupdesc, index)->atttypid;

		if (index != 0)
			appendStringInfo(values, ",");

		switch (column_types[index])
		{
			case GS_TYPE_STRING:
				{
					const		GSChar *strVal;

					griddb_get_datatype_for_conversion(pg_type, &tmp_typeinput, &tmp_typemod);
					ret = gsGetRowFieldAsString(row, index, &strVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
					appendStringInfo(values, "%s", (char *) quote_identifier(strVal));
					break;
				}
			case GS_TYPE_GEOMETRY:
				{
					const		GSChar *strVal;

					griddb_get_datatype_for_conversion(pg_type, &tmp_typeinput, &tmp_typemod);
					ret = gsGetRowFieldAsGeometry(row, index, &strVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
					appendStringInfo(values, "%s", (char *) strVal);
					break;
				}
			case GS_TYPE_BOOL:
				{
					GSBool		boolVal;

					ret = gsGetRowFieldAsBool(row, index, &boolVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
					appendStringInfo(values, "%c", boolVal ? 't' : 'f');
					break;
				}
			case GS_TYPE_BYTE:
				{
					int8_t		byteVal;

					ret = gsGetRowFieldAsByte(row, index, &byteVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
					appendStringInfo(values, "%d", byteVal);
					break;
				}
			case GS_TYPE_SHORT:
				{
					int16_t		shortVal;

					ret = gsGetRowFieldAsShort(row, index, &shortVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
					appendStringInfo(values, "%d", shortVal);
					break;
				}
			case GS_TYPE_INTEGER:
				{
					int32_t		intVal;

					ret = gsGetRowFieldAsInteger(row, index, &intVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
					appendStringInfo(values, "%d", intVal);
					break;
				}
			case GS_TYPE_LONG:
				{
					int64_t		longVal;

					ret = gsGetRowFieldAsLong(row, index, &longVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
					appendStringInfo(values, "%ld", longVal);
					break;
				}
			case GS_TYPE_FLOAT:
				{
					float		floatVal;

					ret = gsGetRowFieldAsFloat(row, index, &floatVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
					appendStringInfo(values, "%g", floatVal);
					break;
				}
			case GS_TYPE_DOUBLE:
				{
					double		doubleVal;

					ret = gsGetRowFieldAsDouble(row, index, &doubleVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);
					appendStringInfo(values, "%g", doubleVal);
					break;
				}
			case GS_TYPE_TIMESTAMP:
				{
					GSTimestamp tsVal;
					char	   *buf;

					ret = gsGetRowFieldAsTimestamp(row, index, &tsVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);

					buf = griddb_convert_gs2pg_timestamp_to_string(tsVal);
					appendStringInfo(values, "%s", buf);
					break;
				}
			case GS_TYPE_BLOB:
				{
					GSBlob		blobVal;
					char	   *buf;
					Datum		tmpDatum;

					ret = gsGetRowFieldAsBlob(row, index, &blobVal);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);

					tmpDatum = (Datum) palloc0(blobVal.size + VARHDRSZ);
					memcpy(VARDATA(tmpDatum), blobVal.data, blobVal.size);
					SET_VARSIZE(tmpDatum, blobVal.size + VARHDRSZ);

					buf = DatumGetCString(DirectFunctionCall1(byteaout, tmpDatum));

					/*
					 * Append \\ to buf because record_in function will remove
					 * original \\ and then make wrong input for byteain
					 * function
					 */
					appendStringInfo(values, "\\%s", buf);
					break;
				}
			case GS_TYPE_STRING_ARRAY:
				{
					const		GSChar *const *strVal;

					griddb_get_datatype_for_conversion(pg_type, &tmp_typeinput, &tmp_typemod);
					ret = gsGetRowFieldAsStringArray(row, index, &strVal, &size);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);

					appendStringInfoChar(values, '{');
					for (i = 0; i < size; i++)
					{
						if (i > 0)
							appendStringInfo(values, "\\,");
						appendStringInfo(values, "'%s'", (char *) strVal[i]);
					}
					appendStringInfoChar(values, '}');
					break;
				}
			case GS_TYPE_BOOL_ARRAY:
				{
					const		GSBool *boolVal;

					ret = gsGetRowFieldAsBoolArray(row, index, &boolVal, &size);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);

					appendStringInfoChar(values, '{');
					for (i = 0; i < size; i++)
					{
						if (i > 0)
							appendStringInfo(values, "\\,");
						appendStringInfo(values, "%c", boolVal[i] ? 't' : 'f');
					}
					appendStringInfoChar(values, '}');
					break;
				}
			case GS_TYPE_BYTE_ARRAY:
				{
					const		int8_t *byteVal;

					ret = gsGetRowFieldAsByteArray(row, index, &byteVal, &size);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);

					appendStringInfoChar(values, '{');
					for (i = 0; i < size; i++)
					{
						if (i > 0)
							appendStringInfo(values, "\\,");
						appendStringInfo(values, "%d", (int16_t) byteVal[i]);
					}
					appendStringInfoChar(values, '}');
					break;
				}

			case GS_TYPE_SHORT_ARRAY:
				{
					const		int16_t *shortVal;

					ret = gsGetRowFieldAsShortArray(row, index, &shortVal, &size);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);

					appendStringInfoChar(values, '{');
					for (i = 0; i < size; i++)
					{
						if (i > 0)
							appendStringInfo(values, "\\,");
						appendStringInfo(values, "%d", (int16_t) shortVal[i]);
					}
					appendStringInfoChar(values, '}');
					break;
				}

			case GS_TYPE_INTEGER_ARRAY:
				{
					const int32_t *intVal;

					ret = gsGetRowFieldAsIntegerArray(row, index, &intVal, &size);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);

					appendStringInfoChar(values, '{');
					for (i = 0; i < size; i++)
					{
						if (i > 0)
							appendStringInfo(values, "\\,");
						appendStringInfo(values, "%d", (int32_t) intVal[i]);
					}
					appendStringInfoChar(values, '}');
					break;
				}

			case GS_TYPE_LONG_ARRAY:
				{
					const		int64_t *longVal;

					ret = gsGetRowFieldAsLongArray(row, index, &longVal, &size);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);

					appendStringInfoChar(values, '{');
					for (i = 0; i < size; i++)
					{
						if (i > 0)
							appendStringInfo(values, "\\,");
						appendStringInfo(values, "%ld", (int64_t) longVal[i]);
					}
					appendStringInfoChar(values, '}');
					break;
				}

			case GS_TYPE_FLOAT_ARRAY:
				{
					const float *floatVal;

					ret = gsGetRowFieldAsFloatArray(row, index, &floatVal, &size);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);

					appendStringInfoChar(values, '{');
					for (i = 0; i < size; i++)
					{
						if (i > 0)
							appendStringInfo(values, "\\,");
						appendStringInfo(values, "%f", (float4) floatVal[i]);
					}
					appendStringInfoChar(values, '}');
					break;
				}

			case GS_TYPE_DOUBLE_ARRAY:
				{
					const double *doubleVal;
					size_t		size;
					size_t		i;


					ret = gsGetRowFieldAsDoubleArray(row, index, &doubleVal, &size);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);

					appendStringInfoChar(values, '{');
					for (i = 0; i < size; i++)
					{
						if (i > 0)
							appendStringInfo(values, "\\,");
						appendStringInfo(values, "%f", (float8) doubleVal[i]);
					}
					appendStringInfoChar(values, '}');
					break;
				}

			case GS_TYPE_TIMESTAMP_ARRAY:
				{
					const		GSTimestamp *tsVal;
					char	   *buf;

					ret = gsGetRowFieldAsTimestampArray(row, index, &tsVal, &size);
					if (!GS_SUCCEEDED(ret))
						griddb_REPORT_ERROR(ERROR, ret, row);

					appendStringInfoChar(values, '{');
					for (i = 0; i < size; i++)
					{
						if (i > 0)
							appendStringInfo(values, "\\,");
						buf = griddb_convert_gs2pg_timestamp_to_string(tsVal[i]);
						appendStringInfo(values, "%s", buf);
					}
					appendStringInfoChar(values, '}');
					break;
				}
			default:
				/* Should not happen, we have just check this above */
				elog(ERROR, "unsupported field type %d", column_types[index]);
		}
	}
	appendStringInfoChar(values, ')');
	value_datum = CStringGetDatum((char *) values->data);
	valueDatum = OidFunctionCall3(typeinput, value_datum,
								  RECORDOID,
								  Int32GetDatum(typemod));
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

	if (griddb_enable_partial_execution)
	{
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
	GridDBFdwModifiedRowData value1[1];
	GridDBFdwModifiedRowData value2[1];
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

	value1[0].values = &val1;
	value1[0].isnulls = false;
	value2[0].values = &smrelay->rowkey_val;
	value2[0].isnulls = false;
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

				ret = gsSetRowFieldByByte(row, pindex, (int8_t) byteVal);
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

					byteaData[i] = (int8_t) byteVal;
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

/*
 * Check whether new row-key is updated or not.
 */
static void
griddb_check_rowkey_update(GridDBFdwModifyState * fmstate, TupleTableSlot *new_slot)
{
	GridDBFdwSMRelay *smrelay = fmstate->smrelay;
	int			(*comparator) (const void *, const void *);
	ListCell   *lc;

	foreach(lc, fmstate->target_attrs)
	{
		int			attnum = lfirst_int(lc);
		bool		isnull;
		Datum		value;
		GSType		type;
		GridDBFdwModifiedRowData value1[1];
		GridDBFdwModifiedRowData value2[1];

		if ((attnum < 0) || (attnum != ROWKEY_ATTNO))
			continue;

		griddb_check_slot_type(new_slot, attnum, &smrelay->field_info);
		value = slot_getattr(new_slot, attnum, &isnull);
		if (isnull)
			type = GS_TYPE_NULL;
		else
			type = smrelay->field_info.column_types[attnum - 1];

		value1[0].values = &value;
		value1[0].isnulls = false;
		value2[0].values = &smrelay->rowkey_val;
		value2[0].isnulls = false;
		comparator = griddb_get_comparator_tuplekey(type);
		if (comparator((const void *) &value1, (const void *) &value2) != 0)
			elog(ERROR, "new rowkey column update is not supported");

	}
}

#if (PG_VERSION_NUM >= 140000)
/*
 * Determine batch size for a given foreign table. The option specified for
 * a table has precedence.
 */
static int
get_batch_size_option(Relation rel)
{
	Oid			foreigntableid = RelationGetRelid(rel);
	ForeignTable *table;
	ForeignServer *server;
	List	   *options;
	ListCell   *lc;

	/* we use 1 by default, which means "no batching" */
	int			batch_size = 1;

	/*
	 * Load options for table and server. We append server options after table
	 * options, because table options take precedence.
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);

	options = NIL;
	options = list_concat(options, table->options);
	options = list_concat(options, server->options);

	/* See if either table or server specifies batch_size. */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, OPTION_BATCH_SIZE) == 0)
		{
			batch_size = strtol(defGetString(def), NULL, 10);
			break;
		}
	}

	return batch_size;
}
#endif

/*
 * ExecForeignDDL is a public function that is called by core code.
 * It executes DDL command on remote server.
 *
 * serverOid: remote server to get connected
 * rel: relation to be created
 * operation:
 * 		0: CREATE command
 * 		1: DROP command
 * exists_flag:
 *		in CREATE DDL: true if `IF NOT EXIST` is specified
 *		in DROP DDL: true if `IF EXIST` is specified
 */
int
ExecForeignDDL(Oid serverOid,
			   Relation rel,
			   int operation,
			   bool exists_flag)
{
	GSContainerInfo	containerinfo;
	UserMapping *user = NULL;
	GSGridStore *store = NULL;
	GSContainer *container;
	GSResult		ret = GS_RESULT_OK;
	char *tbl_name = NULL;
	ListCell   *lc = NULL;
	ForeignTable *table;
	bool RowKeyAssigned = false;
	List *column_options;
	bool isContainerExisted = false;

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	if (operation != DDL_COMMAND_CREATE && operation != DDL_COMMAND_DROP)
	{
		elog(ERROR, "Only support CREATE/DROP DATASOURCE");
	}

	/* obtain additional catalog information. */
	table = GetForeignTable(RelationGetRelid(rel));

	/*
	 * Use value of FDW options if any, instead of the name of object itself.
	 */
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, OPTION_TABLE) == 0)
		{
			tbl_name = defGetString(def);
			break;
		}
	}

	if (tbl_name == NULL)
		tbl_name = RelationGetRelationName(rel);

	column_options = GetForeignColumnOptions(RelationGetRelid(rel), ROWKEY_ATTNO);

	foreach(lc, column_options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, OPTION_ROWKEY) == 0)
		{
			RowKeyAssigned = true;
			break;
		}
	}

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	user = GetUserMapping(GetUserId(), serverOid);
	store = griddb_get_connection(user, false, RelationGetRelid(rel));

	isContainerExisted = check_existed_container(serverOid, tbl_name);

	/* create query */
	if (operation == DDL_COMMAND_CREATE)
	{
		containerinfo = griddb_set_container_info(containerinfo, rel);
		/* Create a Collection (Delete if schema setting is NULL) */
		containerinfo.type = GS_CONTAINER_COLLECTION;
		if (RowKeyAssigned)
			containerinfo.rowKeyAssigned = GS_FALSE;

		if (exists_flag)
		{
			/*
			 * If container is not existed, create container.
			 * If container is existed, do nothing.
			 */
			if (!isContainerExisted)
				ret = gsPutContainerGeneral(store, tbl_name, &containerinfo, GS_FALSE, &container);
		}
		else
		{
			/*
			 * If container is existed, show error message and do not create container.
			 * If container is not existed, create container.
			 */
			if (isContainerExisted)
			{
				elog(ERROR, "Container \"%s\" has already existed\n", tbl_name);
			}
			else
			{
				ret = gsPutContainerGeneral(store, tbl_name, &containerinfo, GS_TRUE, &container);
			}
		}

		if (!GS_SUCCEEDED(ret))
		{
			elog(ERROR, "Create container \"%s\" failed\n", tbl_name);
		}
	}
	else
	{
		/* operation == DDL_COMMAND_DROP */
		if (exists_flag)
		{
			if (isContainerExisted)
				gsDropContainer(store, tbl_name);
		}
		else
		{
			if (!isContainerExisted)
			{
				elog(ERROR, "Container \"%s\" is not existed\n", tbl_name);
			}
			else
			{
				ret = gsDropContainer(store, tbl_name);
			}
		}

		if (!GS_SUCCEEDED(ret))
		{
			elog(ERROR, "Can not drop container \"%s\"\n", tbl_name);
		}
	}

	/* Releasing resource */
	griddb_release_connection(store);

	return 0;
}

/* Check container is exised or not */
static bool
check_existed_container(Oid serverOid, char* tbl_name)
{
	ForeignServer *server;
	UserMapping *mapping;
	GSGridStore *store;
	GSResult	ret;
	GSContainerInfo info = GS_CONTAINER_INFO_INITIALIZER;
	GSBool		exists;

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	server = GetForeignServer(serverOid);
	mapping = GetUserMapping(GetUserId(), server->serverid);
	store = griddb_get_connection(mapping, false, serverOid);

	/* Get schema of container */
	ret = gsGetContainerInfo(store, (GSChar *)tbl_name, &info, &exists);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, store);

	/* Container has already existed */
	if (exists == GS_TRUE)
	{
		return true;
	}

	return false;
}
