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

#include "gridstore.h"

#include "foreign/foreign.h"
#include "nodes/relation.h"
#include "utils/relcache.h"

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

#define GSFDW_BOOLARRAYOID		1000	/* Oid for BOOLARRAY */
#define GSFDW_BYTEARRAYOID		1014	/* Oid for BYTEARRAY */
#define GSFDW_INT8ARRAYOID		1016	/* Oid for INT8ARRAY */
#define GSFDW_FLOAT8ARRAYOID	1022	/* Oid for FLOAT8ARRAY */
#define GSFDW_TIMESTAMPARRAYOID 1115	/* Oid for TIMESTAMPARRAYOID */

/* Option name for IMPORT FOREIGN SCHEMA. */
#define OPTION_RECREATE "recreate"
/* Option name for CREATE FOREIGN SERVER. */
#define OPTION_HOST		"host"
#define OPTION_PORT		"port"
#define OPTION_CLUSTER	"clustername"
/* Option name for CREATE USER MAPPING. */
#define OPTION_USER		"username"
#define OPTION_PWD		"password"

/* Option name for CREATE FOREIGN TABLE. */
#define OPTION_TABLE	"table_name"
#define OPTION_ROWKEY	"rowkey"

#define OPTION_UPDATABLE	   "updatable"
#define OPTION_REMOTE_ESTIMATE "use_remote_estimate"
#define OPTION_STARTUP_COST    "fdw_startup_cost"
#define OPTION_TUPLE_COST	   "fdw_tuple_cost"

/* Attribute number of rowkey column. 1st column is assigned rowkey in GridDB. */
#define ROWKEY_ATTNO 1

/* The index of rowkey for array storing rowkey and modified record values. It cannot be changed. */
#define ROWKEY_IDX 0

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
	char	   *svr_clustername;	/* GridDB database name */

	bool		use_remote_estimate;	/* use remote estimate for rows */
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;
}			griddb_opt;

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * foreign table.  This information is collected by griddbGetForeignRelSize.
 */
typedef struct GriddbFdwRelationInfo
{
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
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;			/* only set in use_remote_estimate mode */
}			GriddbFdwRelationInfo;

/* option.c headers */
extern bool griddb_is_valid_option(const char *option, Oid context);
extern griddb_opt * griddb_get_options(Oid foreigntableid);

/* in connection.c */
extern GSGridStore * griddb_get_connection(UserMapping *user, bool will_prep_stmt,
										   Oid foreigntableid);
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
extern bool is_foreign_expr(PlannerInfo *root,
				RelOptInfo *baserel,
				Expr *expr);
extern Expr *find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);
extern void griddb_deparse_select(StringInfo buf, PlannerInfo *root,
					  RelOptInfo *foreignrel, List *remote_conds,
					  List *pathkeys, List **retrieved_attrs,
					  List **params_list);
extern void griddb_deparse_locking_clause(PlannerInfo *root, RelOptInfo *rel,
							  int *for_update);

#endif							/* GRIDDB_FDW_H */
