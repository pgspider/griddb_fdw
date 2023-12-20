/*
 *
 * GridDB Foreign Data Wrapper
 *
 * Portions Copyright (c) 2018, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  griddb_fdw.h
 *
 */
#ifndef GRIDDB_FDW_H
#define GRIDDB_FDW_H

#include "postgres.h"

#include "gridstore.h"

#include "executor/tuptable.h"
#include "foreign/foreign.h"
#if (PG_VERSION_NUM >= 120000)
#include "nodes/pathnodes.h"
#include "nodes/execnodes.h"
#include "access/table.h"
#include "utils/float.h"
#include "optimizer/optimizer.h"
#else
#include "nodes/relation.h"
#include "optimizer/var.h"
#endif
#include "utils/hsearch.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"

#if (PG_VERSION_NUM < 100000)
/* Is the given relation an "other" relation? */
#define IS_OTHER_REL(rel) ((rel)->reloptkind == RELOPT_OTHER_MEMBER_REL)
#define IS_JOIN_REL(rel) ((rel)->reloptkind == RELOPT_JOINREL)
#define IS_UPPER_REL(rel) ((rel)->reloptkind == RELOPT_UPPER_REL)
#endif

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST	100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST		0.01

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/* An area size for storing updated/deleted rows information. */
#define INITIAL_TARGET_VALUE_ROWS 1000

/* Record count fetched by random update feature. */
#define BULK_ROWS_COUNT 100

#define DEFAULT_GRIDDB_IP_ADDRESS	"239.0.0.1"
#define DEFAULT_GRIDDB_PORT			"31999"

#ifndef BOOLARRAYOID
#define BOOLARRAYOID			1000	/* Oid for BOOLARRAY */
#endif

#ifndef BYTEARRAYOID
#define BYTEARRAYOID			1014	/* Oid for BYTEARRAY */
#endif

#ifndef INT8ARRAYOID
#define INT8ARRAYOID			1016	/* Oid for INT8ARRAY */
#endif

#ifndef FLOAT8ARRAYOID
#define FLOAT8ARRAYOID			1022	/* Oid for FLOAT8ARRAY */
#endif

#ifndef TIMESTAMPARRAYOID
#define TIMESTAMPARRAYOID 		1115	/* Oid for TIMESTAMPARRAYOID */
#endif

#ifndef TIMESTAMPTZARRAYOID
#define TIMESTAMPTZARRAYOID		1185	/* Oid for TIMESTAMPZARRAYOID */
#endif

/* Option name for IMPORT FOREIGN SCHEMA. */
#define OPTION_RECREATE "recreate"
/* Option name for CREATE FOREIGN SERVER. */
#define OPTION_HOST		"host"
#define OPTION_PORT		"port"
#define OPTION_CLUSTER	"clustername"
#define OPTION_DATABASE	"database"
#define OPTION_MEMBER	"notification_member"

/* Option name for CREATE USER MAPPING. */
#define OPTION_USER		"username"
#define OPTION_PWD		"password"

/* Option name for CREATE FOREIGN TABLE. */
#define OPTION_TABLE	"table_name"
#define OPTION_COLUMN	"column_name"
#define OPTION_ROWKEY	"rowkey"

#define OPTION_UPDATABLE	   "updatable"
#define OPTION_REMOTE_ESTIMATE "use_remote_estimate"
#define OPTION_STARTUP_COST    "fdw_startup_cost"
#define OPTION_TUPLE_COST	   "fdw_tuple_cost"
#define OPTION_BATCH_SIZE	   "batch_size"
#define OPTION_KEEP_CONN	   "keep_connections"
#define CODE_VERSION 20300
/* Attribute number of rowkey column. 1st column is assigned rowkey in GridDB. */
#define ROWKEY_ATTNO 1
/*
 * GridDB c-API has no limit with max number of row (rowCount) in gsPutMultipleRows(),
 * but rowCount should be limited to 65535 (uint16) same as postgres_fdw for safe.
 */
#define DEFAULT_QUERY_PARAM_MAX_LIMIT 65535

/* The index of rowkey for array storing rowkey and modified record values. It cannot be changed. */
#define ROWKEY_IDX 0

/* float4 values are passed by value if 'true', by reference if 'false' */
#ifndef FLOAT4PASSBYVAL
#define FLOAT4PASSBYVAL true
#endif

#if (PG_VERSION_NUM < 120000)
#define table_close(rel, lock)	heap_close(rel, lock)
#define table_open(rel, lock)	heap_open(rel, lock)
#endif

#if (PG_VERSION_NUM < 100000)
/*
 * Is the given relation a simple relation i.e a base or "other" member
 * relation?
 */
#define IS_SIMPLE_REL(rel) \
	((rel)->reloptkind == RELOPT_BASEREL || \
	 (rel)->reloptkind == RELOPT_OTHER_MEMBER_REL)

/* Is the given relation a join relation? */
#define IS_JOIN_REL(rel)	\
	((rel)->reloptkind == RELOPT_JOINREL)

/* Is the given relation an upper relation? */
#define IS_UPPER_REL(rel)	\
	((rel)->reloptkind == RELOPT_UPPER_REL)

/* Is the given relation an "other" relation? */
#define IS_OTHER_REL(rel) \
	((rel)->reloptkind == RELOPT_OTHER_MEMBER_REL)
#endif

/*
 * Options structure to store the MySQL
 * server information
 */
typedef struct griddb_opt
{
	char	   *svr_address;	/* GridDB server ip address */
	char	   *svr_port;		/* GridDB port number */
	char	   *svr_username;	/* GridDB user name */
	char	   *svr_password;	/* GridDB password */
	char	   *svr_clustername;	/* GridDB cluster name */
	char	   *svr_database;	/* GridDB database name */
	char	   *svr_notification_member;	/* GridDB notification member for
											 * fixed list */

	bool		use_remote_estimate;	/* use remote estimate for rows */
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;
	bool		keep_connections;	/* connections to foreign server is kept
									 * open or not */
}			griddb_opt;

/* Struct for extra information passed to estimate_path_cost_size() */
typedef struct
{
	PathTarget *target;
	bool		has_final_sort;
	bool		has_limit;
	double		limit_tuples;
	int64		count_est;
	int64		offset_est;
}			GriddbFdwPathExtraData;

typedef struct GridDBAggref
{
	StringInfo	aggname;
	StringInfo	columnname;
}			GridDBAggref;

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * foreign table.  This information is collected by griddbGetForeignRelSize.
 */
typedef struct GriddbFdwRelationInfo
{
	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool		pushdown_safe;

	/*
	 * Restriction clauses, divided into safe and unsafe to pushdown subsets.
	 *
	 * For a base foreign relation this is a list of clauses along-with
	 * RestrictInfo wrapper. Keeping RestrictInfo wrapper helps while dividing
	 * scan_clauses in postgresGetForeignPlan into safe and unsafe subsets.
	 * Also it helps in estimating costs since RestrictInfo caches the
	 * selectivity and qual cost for the clause in it.
	 *
	 * For a join relation, however, they are part of otherclause list
	 * obtained from extract_actual_join_clauses, which strips RestrictInfo
	 * construct. So, for a join relation they are list of bare clauses.
	 */
	List	   *remote_conds;
	List	   *local_conds;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;

	/* True means that the query_pathkeys is safe to push down */
	bool		qp_is_pushdown_safe;

	/* Cost and selectivity of local_conds. */
	Selectivity local_conds_sel;

	/* Estimated size and cost for a scan or join. */
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	/* Costs excluding costs for transferring data from the foreign server */
	Cost		rel_startup_cost;
	Cost		rel_total_cost;

	/* Options extracted from catalogs. */
	bool		use_remote_estimate;
	double		retrieved_rows;
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;
	List	   *shippable_extensions;	/* OIDs of whitelisted extensions */

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;			/* only set in use_remote_estimate mode */

	int			fetch_size;		/* fetch size for this remote table */

	/*
	 * Name of the relation, for use while EXPLAINing ForeignScan.  It is used
	 * for join and upper relations but is set for all relations.  For a base
	 * relation, this is really just the RT index as a string; we convert that
	 * while producing EXPLAIN output.  For join and upper relations, the name
	 * indicates which base foreign tables are included and the join type or
	 * aggregation type used.
	 */
	StringInfo	relation_name;

	RelOptInfo *outerrel;

	/* Upper relation information */
	UpperRelationKind stage;
	/* Grouping information */
	List	   *grouped_tlist;

	/* Function pushdown surppot in target list */
	bool		is_tlist_func_pushdown;

	/* Aggregate function information */
	GridDBAggref *aggref;
}			GriddbFdwRelationInfo;

/*
 * When a schema information is acqured from GridDB, it is stored a
 * temporary space in GridDB client library. So griddb_fdw copies it
 * to griddb_fdw library.
 */
typedef struct GridDBFdwFieldInfo
{
	size_t		column_count;	/* column count */
	GSChar	  **column_names;	/* column name */
	GSType	   *column_types;	/* column type */
}			GridDBFdwFieldInfo;

/*
 * Modified rowkey information.
 */
typedef Datum GridDBFdwRowKeyHashKey;
typedef struct GridDBFdwRowKeyHashEntry
{
	GridDBFdwRowKeyHashKey rowkey;
	bool		allocated;		/* True if rowkey should be freed */
}			GridDBFdwRowKeyHashEntry;

/*
 * Modified row data.
 */
typedef struct GridDBFdwModifiedRowData
{
	Datum	  *values;
	bool	  *isnulls;
} GridDBFdwModifiedRowData;

/*
 * Modified row information.
 */
typedef struct GridDBFdwModifiedRows
{
	GridDBFdwModifiedRowData *target_values; /* update or delete target row information */
	int			field_num;		/* # of field */
	uint64_t	num_target;		/* # of stored modified row */
	uint64_t	max_target;		/* # of storable modified row */
}			GridDBFdwModifiedRows;

/* in griddb_fdw.c */
extern void griddb_convert_pg2gs_timestamp_string(Timestamp dt, char *buf);
extern void griddb_check_slot_type(TupleTableSlot *slot, int attnum, GridDBFdwFieldInfo * field_info);
extern Datum griddb_make_datum_from_row(GSRow * row, int32_t attid, GSType gs_type, Oid pg_type,
										GSRowSetType row_type, GSAggregationResult * agg_res, GridDBAggref * aggref);
extern void griddb_set_row_field(GSRow * row, Datum value, GSType gs_type, int pindex);
extern int	griddb_set_transmission_modes(void);
extern void griddb_reset_transmission_modes(int nestlevel);
extern
#if PG_VERSION_NUM >= 160000
PGDLLEXPORT
#endif
int	ExecForeignDDL(Oid serverOid,
				   Relation rel,
				   int operation,
				   bool if_not_exists);

/* in option.c */
extern bool griddb_is_valid_option(const char *option, Oid context);
extern griddb_opt * griddb_get_options(Oid foreigntableid);

/* in connection.c */
extern GSGridStore * griddb_get_connection(UserMapping *user, bool will_prep_stmt,
										   Oid foreigntableid);
extern char *griddb_get_rel_name(Oid relid);
extern GSContainer * griddb_get_container(UserMapping *user, Oid relid, GSGridStore * store);
extern void griddb_release_connection(GSGridStore * store);
extern void griddb_report(int elevel, GSResult res, void *gsResource,
						  const char *fname, unsigned int line);
extern void griddb_cleanup_connection(void);
extern void griddb_error_message(void *gsResource, StringInfoData *str);
#define griddb_REPORT_ERROR(elevel, res, gsResource) \
	griddb_report(elevel, res, gsResource, __FILE__, __LINE__)

/* in deparse.c */
extern void griddb_classify_conditions(PlannerInfo *root,
									   RelOptInfo *baserel,
									   List *input_conds,
									   List **remote_conds,
									   List **local_conds);
extern bool griddb_is_foreign_expr(PlannerInfo *root,
								   RelOptInfo *baserel,
								   Expr *expr,
								   bool for_tlist);
extern bool griddb_is_foreign_param(PlannerInfo *root,
									RelOptInfo *baserel,
									Expr *expr);
extern bool griddb_is_foreign_pathkey(PlannerInfo *root,
									  RelOptInfo *baserel,
									  PathKey *pathkey);
extern EquivalenceMember *griddb_find_em_for_rel(PlannerInfo *root,
												 EquivalenceClass *ec,
												 RelOptInfo *rel);
extern void griddb_deparse_select(StringInfo buf, PlannerInfo *root,
								  RelOptInfo *foreignrel, List *remote_conds,
								  List *pathkeys, List **retrieved_attrs,
								  List **params_list, List *tlist, bool has_limit);
extern void griddb_deparse_locking_clause(PlannerInfo *root, RelOptInfo *rel,
										  int *for_update);
extern List *griddb_build_tlist_to_deparse(RelOptInfo *foreignrel);

extern bool griddb_is_foreign_function_tlist(PlannerInfo *root,
											 RelOptInfo *baserel,
											 List *tlist);
extern List *griddb_pull_func_clause(Node *node);
extern bool griddb_is_builtin(Oid oid);
extern GSContainerInfo griddb_set_container_info(GSContainerInfo containerinfo, Relation rel);

/* in store.c */
extern HTAB *griddb_rowkey_hash_create(GridDBFdwFieldInfo * field_info);
extern GridDBFdwRowKeyHashEntry * griddb_rowkey_hash_search(HTAB *modified_rowkeys, Datum rowkey, bool *found);
extern void griddb_rowkey_hash_set(GridDBFdwRowKeyHashEntry * entry, Datum rowkey, Form_pg_attribute attr);
extern void griddb_rowkey_hash_free(HTAB *modified_rowkeys);
extern void griddb_modify_target_init(GridDBFdwModifiedRows * modified_rows, int attnum);
extern void griddb_modify_target_expand(GridDBFdwModifiedRows * modified_rows);
extern void griddb_modify_target_fini(GridDBFdwModifiedRows * modified_rows);
extern Datum griddb_modify_target_insert(GridDBFdwModifiedRows * modified_rows,
										 TupleTableSlot *slot, TupleTableSlot *planSlot,
										 AttrNumber junk_att_no, List *target_attrs,
										 GridDBFdwFieldInfo * field_info);
extern void griddb_modify_target_sort(GridDBFdwModifiedRows * modified_rows,
									  GridDBFdwFieldInfo * field_info);
extern void griddb_modify_targets_apply(GridDBFdwModifiedRows * modified_rows,
										char *cont_name, GSContainer * cont, List *target_attrs,
										GridDBFdwFieldInfo * field_info, Oid pgkeytype, CmdType operation);

/* in compare.c */
extern int	(*griddb_get_comparator_tuplekey(GSType gs_type)) (const void *, const void *);
extern int	(*griddb_get_comparator_datum(GSType gs_type)) (const void *, const void *);

#endif							/* GRIDDB_FDW_H */
