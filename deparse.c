/*
 * GridDB Foreign Data Wrapper
 *
 * Portions Copyright (c) 2018, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  deparse.c
 *
 */

#include "postgres.h"

#include "griddb_fdw.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/clauses.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "time.h"

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	Relids		relids;			/* relids of base relations in the underlying
								 * scan */
	bool		for_tlist;		/* whether evaluation for the expression of
								 * tlist */
	bool		is_inner_func;	/* exist or not in inner exprs */
} foreign_glob_cxt;

/*
 * Local (per-tree-level) context for foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum
{
	FDW_COLLATE_NONE,			/* expression is of a noncollatable type */
	FDW_COLLATE_SAFE,			/* collation derives from a foreign Var */
	FDW_COLLATE_UNSAFE			/* collation derives from something else */
} FDWCollateState;

typedef struct foreign_loc_cxt
{
	Oid			collation;		/* OID of current collation, if any */
	FDWCollateState state;		/* state of current collation choice */
	bool		can_skip_cast;	/* outer function can skip
								 * int2/int4/int8/float4/float8 cast */
	bool		can_pushdown_stable;	/* true if query contains time series
										 * functions */
	bool		can_pushdown_volatile;	/* true if query contains
										 * griddb_now(), timestampdiff(),
										 * timestampadd() */
} foreign_loc_cxt;

/*
 * Context for griddb_deparse_expr
 */
typedef struct deparse_expr_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	RelOptInfo *scanrel;		/* the underlying scan relation. Same as
								 * foreignrel, when that represents a join or
								 * a base relation. */

	StringInfo	buf;			/* output buffer to append to */
	List	  **params_list;	/* exprs that will become remote Params */
	bool		can_skip_cast;	/* outer function can skip
								 * int2/int4/int8/float4/float8 cast */
	GridDBAggref *aggref;
} deparse_expr_cxt;

/*
 * Struct to pull out function
 */
typedef struct pull_func_clause_context
{
	List	   *funclist;
}			pull_func_clause_context;

/*
 * Functions to construct string representation of a node tree.
 */
static void griddb_deparse_expr(Expr *expr, deparse_expr_cxt *context);
static void griddb_deparse_var(Var *node, deparse_expr_cxt *context);
static void griddb_deparse_const(Const *node, deparse_expr_cxt *context);

static void griddb_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context);
static void griddb_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context);
static void griddb_deparse_operator_name(StringInfo buf, Form_pg_operator opform);
static void griddb_deparse_distinct_expr(DistinctExpr *node, deparse_expr_cxt *context);
static void griddb_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node,
												deparse_expr_cxt *context);
static void griddb_deparse_relabel_type(RelabelType *node, deparse_expr_cxt *context);
static void griddb_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context);
static void griddb_deparse_null_test(NullTest *node, deparse_expr_cxt *context);
static void griddb_append_limit_clause(deparse_expr_cxt *context);
static void griddb_deparse_row_expr(RowExpr *node, deparse_expr_cxt *context);
static void griddb_deparse_field_select_expr(FieldSelect *node, deparse_expr_cxt *context);

static void griddb_deparse_relation(StringInfo buf, Relation rel);
static void griddb_deparse_target_list(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel,
									   Bitmapset *attrs_used, List **retrieved_attrs);
static void griddb_deparse_column_ref(StringInfo buf, int varno, int varattno,
									  deparse_expr_cxt *context);
static void griddb_deparse_aggref(Aggref *node, deparse_expr_cxt *context);
static void griddb_append_order_by_clause(List *pathkeys, deparse_expr_cxt *context);
static void griddb_append_where_clause(List *exprs, deparse_expr_cxt *context);
static bool griddb_contain_functions_walker(Node *node, void *context);
static void griddb_append_agg_order_by(List *orderList, List *targetList,
									   deparse_expr_cxt *context);
static void griddb_append_order_by_suffix(Oid sortop, Oid sortcoltype, bool nulls_first,
										  deparse_expr_cxt *context);
static Node *griddb_deparse_sort_group_clause(Index ref, List *tlist, bool force_colno,
											  deparse_expr_cxt *context);
static void griddb_append_function_name(Oid funcid, deparse_expr_cxt *context);
static bool exist_in_function_list(char *funcname, const char **funclist);
static GSType convert_pgtyp_to_gstyp(Oid type_oid);

/* List of unique function of GridDB */
static const char *GridDBUniqueFunction[] = {
	"to_timestamp_ms",
	"to_epoch_ms",
	"array_length",
	"element",
	"griddb_timestamp",
	"timestampadd",
	"timestampdiff",
	"time_next",
	"time_next_only",
	"time_prev",
	"time_prev_only",
	"time_interpolated",
	"time_sampling",
	"max_rows",
	"min_rows",
	"griddb_now",
NULL};

/* List of common function of GridDB and PostgreSQL */
static const char *GridDBSupportedBuiltinFunction[] = {
	"char_length",
	"concat",
	"lower",
	"upper",
	"substr",
	"round",
	"ceiling",
	"ceil",
	"floor",
NULL};

/*
 * CastFunction
 * List of PostgreSQL cast functions, these functions can be skip cast.
 */
static const char *CastFunction[] = {
	"float4",
	"float8",
	"int2",
	"int4",
	"int8",
NULL};

/*
 * Deparse given targetlist and append it to context->buf.
 *
 * tlist is list of TargetEntry's which in turn contain Var nodes.
 *
 * retrieved_attrs is the list of continuously increasing integers starting
 * from 1. It has same number of entries as tlist.
 *
 * This is used for both SELECT and RETURNING targetlists; the is_returning
 * parameter is true only for a RETURNING targetlist.
 */
static void
griddb_deparse_explicit_targetList(List *tlist,
								   bool is_returning,
								   List **retrieved_attrs,
								   deparse_expr_cxt *context)
{
	ListCell   *lc;
	StringInfo	buf = context->buf;
	int			i = 0;

	*retrieved_attrs = NIL;

	foreach(lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (i > 0)
			appendStringInfoString(buf, ", ");
		else if (is_returning)
			appendStringInfoString(buf, " RETURNING ");

		griddb_deparse_expr((Expr *) tle->expr, context);

		*retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);
		i++;
	}

	if (i == 0 && !is_returning)
		appendStringInfoString(buf, "NULL");
}

/*
 * Append remote name of specified foreign table to buf.
 * Use value of table_name FDW option (if any) instead of relation's name.
 * Similarly, schema_name FDW option overrides schema name.
 */
static void
griddb_deparse_relation(StringInfo buf, Relation rel)
{
	ForeignTable *table;
	const char *relname = NULL;
	ListCell   *lc = NULL;

	/* obtain additional catalog information. */
	table = GetForeignTable(RelationGetRelid(rel));

	/*
	 * Use value of FDW options if any, instead of the name of object itself.
	 */
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, OPTION_TABLE) == 0)
			relname = defGetString(def);
	}

	if (relname == NULL)
		relname = RelationGetRelationName(rel);

	/* For GridDB, name space is not used. */
	appendStringInfo(buf, "%s", quote_identifier(relname));
}


/*
 * Deparse SELECT statement for given relation into buf.
 *
 * tlist contains the list of desired columns to be fetched from foreign server.
 * For a base relation fpinfo->attrs_used is used to construct SELECT clause,
 * hence the tlist is ignored for a base relation.
 *
 * remote_conds is the list of conditions to be deparsed as WHERE clause.
 *
 * If params_list is not NULL, it receives a list of Params and other-relation
 * Vars used in the clauses; these values must be transmitted to the remote
 * server as parameter values.
 *
 * If params_list is NULL, we're generating the query for EXPLAIN purposes,
 * so Params and other-relation Vars should be replaced by dummy values.
 *
 * pathkeys is the list of pathkeys to order the result by.
 *
 * List of columns selected is returned in retrieved_attrs.
 */
void
griddb_deparse_select(StringInfo buf,
					  PlannerInfo *root,
					  RelOptInfo *baserel,
					  List *remote_conds,
					  List *pathkeys,
					  List **retrieved_attrs,
					  List **params_list,
					  List *tlist,
					  bool has_limit)
{
	RangeTblEntry *rte;
	Relation	rel;
	GriddbFdwRelationInfo *fpinfo = (GriddbFdwRelationInfo *) baserel->fdw_private;
	deparse_expr_cxt context;
	GridDBAggref *aggref;
	List	   *quals;

	/* Fill portions of context common to join and base relation */
	context.buf = buf;
	context.root = root;
	context.foreignrel = baserel;
	context.params_list = params_list;
	context.scanrel = IS_UPPER_REL(baserel) ? fpinfo->outerrel : baserel;
	aggref = palloc0(sizeof(GridDBAggref));
	aggref->columnname = makeStringInfo();
	aggref->aggname = makeStringInfo();
	context.aggref = aggref;
	context.can_skip_cast = false;

	rte = planner_rt_fetch(context.scanrel->relid, root);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = table_open(rte->relid, NoLock);

	appendStringInfoString(buf, "SELECT ");
	if (IS_UPPER_REL(baserel) || fpinfo->is_tlist_func_pushdown == true)
	{
		griddb_deparse_explicit_targetList(tlist, false, retrieved_attrs, &context);
		fpinfo->aggref = context.aggref;
	}
	else
	{
		griddb_deparse_target_list(buf, root, baserel->relid, rel,
								   fpinfo->attrs_used, retrieved_attrs);
	}

	/*
	 * Construct FROM clause
	 */
	appendStringInfoString(buf, " FROM ");
	griddb_deparse_relation(buf, rel);

	/*
	 * For upper relations, the WHERE clause is built from the remote
	 * conditions of the underlying scan relation; otherwise, we can use the
	 * supplied list of remote conditions directly.
	 */
	if (IS_UPPER_REL(baserel))
	{
		GriddbFdwRelationInfo *ofpinfo;

		ofpinfo = (GriddbFdwRelationInfo *) fpinfo->outerrel->fdw_private;
		quals = ofpinfo->remote_conds;
	}
	else
		quals = remote_conds;

	/*
	 * Construct WHERE clause
	 */
	if (quals)
		griddb_append_where_clause(quals, &context);

	/* Add ORDER BY clause if we found any useful pathkeys */
	if (pathkeys)
		griddb_append_order_by_clause(pathkeys, &context);

	/* Add LIMIT clause if necessary */
	if (has_limit)
		griddb_append_limit_clause(&context);

	table_close(rel, NoLock);
}

/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 * This is used for SELECT targetlists.
 */
static void
griddb_deparse_target_list(StringInfo buf,
						   PlannerInfo *root,
						   Index rtindex,
						   Relation rel,
						   Bitmapset *attrs_used,
						   List **retrieved_attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		have_wholerow;
	int			i;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
								  attrs_used);

	*retrieved_attrs = NIL;
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (have_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}

	appendStringInfoString(buf, " * ");
}

/*
 * Deparse conditions from the provided list and append them to buf.
 *
 * The conditions in the list are assumed to be ANDed. This function is used to
 * deparse WHERE clauses.
 */
static void
griddb_append_where_clause(List *exprs, deparse_expr_cxt *context)
{
	ListCell   *lc;
	bool		is_first = true;
	StringInfo	buf = context->buf;

	foreach(lc, exprs)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (is_first)
			appendStringInfoString(buf, " WHERE ");
		else
			appendStringInfoString(buf, " AND ");

		appendStringInfoChar(buf, '(');
		griddb_deparse_expr(ri->clause, context);
		appendStringInfoChar(buf, ')');

		is_first = false;
	}
}

/*
 * Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 */
static void
griddb_deparse_column_ref(StringInfo buf, int varno, int varattno, deparse_expr_cxt *context)
{
	RangeTblEntry *rte;
	char	   *colname = NULL;
	List	   *options;
	ListCell   *lc;
	PlannerInfo *root = context->root;

	/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
	Assert(!IS_SPECIAL_VARNO(varno));

	/* Get RangeTblEntry from array in PlannerInfo. */
	rte = planner_rt_fetch(varno, root);

	/*
	 * If it's a column of a foreign table, and it has the column_name FDW
	 * option, use that value.
	 */
	options = GetForeignColumnOptions(rte->relid, varattno);
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "column_name") == 0)
		{
			colname = defGetString(def);
			break;
		}
	}

	/*
	 * If it's a column of a regular table or it doesn't have column_name FDW
	 * option, use attribute name.
	 */
	if (colname == NULL)
		colname = get_attname(rte->relid, varattno
#if (PG_VERSION_NUM >= 110000)
							  ,false
#endif
			);

	appendStringInfoString(buf, quote_identifier(colname));
	if (context->aggref)
	{
		if (strcmp(context->aggref->columnname->data, "") == 0)
			appendStringInfoString(context->aggref->columnname, colname);
	}
}


/*
 * Append a SQL string literal representing "val" to buf.
 */
static void
griddb_deparse_string_literal(StringInfo buf, const char *val)
{
	const char *valptr;

	appendStringInfoChar(buf, '\'');
	for (valptr = val; *valptr; valptr++)
	{
		char		ch = *valptr;

		if (SQL_STR_DOUBLE(ch, true))
			appendStringInfoChar(buf, ch);
		appendStringInfoChar(buf, ch);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 * Deparse given expression into context->buf.
 *
 * This function must support all the same node types that foreign_expr_walker
 * accepts.
 *
 * Note: unlike ruleutils.c, we just use a simple hard-wired parenthesization
 * scheme: anything more complex than a Var, Const, function call or cast
 * should be self-parenthesized.
 */
static void
griddb_deparse_expr(Expr *node, deparse_expr_cxt *context)
{
	bool		outer_can_skip_cast = context->can_skip_cast;

	if (node == NULL)
		return;

	switch (nodeTag(node))
	{
		case T_Var:
			griddb_deparse_var((Var *) node, context);
			break;
		case T_Const:
			griddb_deparse_const((Const *) node, context);
			break;
		case T_Param:
			/* Does not reach here because foreign_expr_walker returns false. */
			elog(ERROR, "Parameter is unsupported");
			Assert(false);
			break;
#if PG_VERSION_NUM < 120000
		case T_ArrayRef:
#else
		case T_SubscriptingRef:
#endif
			/* Does not reach here because foreign_expr_walker returns false. */
			elog(ERROR, "Array is unsupported");
			Assert(false);
			break;
		case T_FuncExpr:
			context->can_skip_cast = outer_can_skip_cast;
			griddb_deparse_func_expr((FuncExpr *) node, context);
			break;
		case T_OpExpr:
			griddb_deparse_op_expr((OpExpr *) node, context);
			break;
		case T_DistinctExpr:
			griddb_deparse_distinct_expr((DistinctExpr *) node, context);
			break;
		case T_ScalarArrayOpExpr:
			griddb_deparse_scalar_array_op_expr((ScalarArrayOpExpr *) node, context);
			break;
		case T_RelabelType:
			griddb_deparse_relabel_type((RelabelType *) node, context);
			break;
		case T_BoolExpr:
			griddb_deparse_bool_expr((BoolExpr *) node, context);
			break;
		case T_NullTest:
			griddb_deparse_null_test((NullTest *) node, context);
			break;
		case T_ArrayExpr:
			/* Does not reach here because foreign_expr_walker returns false. */
			elog(ERROR, "ARRAY[...] is not supported");
			Assert(false);
			break;
		case T_RowExpr:
			griddb_deparse_row_expr((RowExpr *) node, context);
			break;
		case T_FieldSelect:
			griddb_deparse_field_select_expr((FieldSelect *) node, context);
			break;
		case T_Aggref:
			griddb_deparse_aggref((Aggref *) node, context);
			break;
		default:
			elog(ERROR, "unsupported expression type for deparse: %d",
				 (int) nodeTag(node));
			break;
	}
}

/*
 * Deparse given Var node into context->buf.
 *
 * If the Var belongs to the foreign relation, just print its remote name.
 * Otherwise, it's effectively a Param (and will in fact be a Param at
 * run time).  Handle it the same way we handle plain Params --- it is
 * unsupported on GridDB.
 */
static void
griddb_deparse_var(Var *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Relids		relids = context->scanrel->relids;

	if (bms_is_member(node->varno, relids) && node->varlevelsup == 0)
	{
		/* Var belongs to foreign table */
		griddb_deparse_column_ref(buf, node->varno, node->varattno, context);
	}
	else
	{
		/* Does not reach here. */
		elog(ERROR, "Parameter is unsupported");
		Assert(false);
	}
}

/*
 * Deparse given constant value into context->buf.
 *
 * This function has to be kept in sync with ruleutils.c's get_const_expr.
 */
static void
griddb_deparse_const(Const *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;

	if (node->constisnull)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}

	getTypeOutputInfo(node->consttype,
					  &typoutput, &typIsVarlena);

	switch (node->consttype)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			{
				extval = OidOutputFunctionCall(typoutput, node->constvalue);

				/*
				 * No need to quote unless it's a special value such as 'NaN'.
				 * See comments in get_const_expr().
				 */
				if (strspn(extval, "0123456789+-eE.") == strlen(extval))
				{
					if (extval[0] == '+' || extval[0] == '-')
						appendStringInfo(buf, "(%s)", extval);
					else
						appendStringInfoString(buf, extval);
				}
				else
					appendStringInfo(buf, "'%s'", extval);
			}
			break;
		case BITOID:
		case VARBITOID:
			/* Does not reach here. */
			elog(ERROR, "VARBITOID/BITOID is unsupported");
			Assert(false);
			break;
		case BOOLOID:
			extval = OidOutputFunctionCall(typoutput, node->constvalue);
			if (strcmp(extval, "t") == 0)
				appendStringInfoString(buf, "true");
			else
				appendStringInfoString(buf, "false");
			break;
		case INTERVALOID:
			/* Does not reach here */
			elog(ERROR, "INTERVALOID is unsupported");
			Assert(false);
			break;
		case TIMESTAMPOID:
			{
				char		timestamp[MAXDATELEN + 1];

				griddb_convert_pg2gs_timestamp_string(node->constvalue, timestamp);
				appendStringInfoString(buf, "TIMESTAMP(");
				griddb_deparse_string_literal(buf, timestamp);
				appendStringInfoString(buf, ")");
				break;
			}
		default:
			extval = OidOutputFunctionCall(typoutput, node->constvalue);
			griddb_deparse_string_literal(buf, extval);
			break;
	}
}


/*
 * This possible that name of function in PostgreSQL and
 * GridDB differ, so return the GridDB equelent function name
 */
static char *
griddb_replace_function(char *in)
{
	if (strcmp(in, "ceil") == 0)
		return "ceiling";
	else if (strcmp(in, "griddb_timestamp") == 0)
		/* Change name to function TIMESTAMP of GridDB */
		return "timestamp";
	else if (strcmp(in, "substr") == 0)
		/* Change name to function SUBSTR of GridDB */
		return "substring";
	else if (strcmp(in, "griddb_now") == 0)
		/* Change name to function now() of GridDB */
		return "now";

	/* Explicit datatype conversion is unnecessary */
	if (exist_in_function_list(in, CastFunction))
		return "";

	return in;
}

/*
 * Deparse a function call.
 */
static void
griddb_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	char	   *proname;
	bool		use_variadic;
	bool		first;
	ListCell   *arg;

	/*
	 * Normal function: display as proname(args).
	 */
	proname = get_func_name(node->funcid);

	/* check NULL for proname */
	if (proname == NULL)
		elog(ERROR, "cache lookup failed for function %u", node->funcid);

	/* Check if need to print VARIADIC (cf. ruleutils.c) */
	use_variadic = node->funcvariadic;

	/* remove cast function if parent function is can handle without cast */
	if (context->can_skip_cast == true &&
		exist_in_function_list(proname, CastFunction))
	{
		arg = list_head(node->args);
		context->can_skip_cast = false;
		griddb_deparse_expr((Expr *) lfirst(arg), context);
		return;
	}

	/* Translate PostgreSQL function into GridDB function */
	proname = griddb_replace_function(proname);

	/* Deparse the function name ... */
	if (strcmp(proname, "time_next") == 0 ||
		strcmp(proname, "time_next_only") == 0 ||
		strcmp(proname, "time_prev") == 0 ||
		strcmp(proname, "time_prev_only") == 0 ||
		(strcmp(proname, "time_sampling") == 0 && list_length(node->args) == 4))
		/* Append * as input parameter */
		appendStringInfo(buf, "%s(*, ", proname);
	else
		appendStringInfo(buf, "%s(", proname);

	/* ... and all the arguments */
	first = true;
	foreach(arg, node->args)
	{
		if (!first)
			appendStringInfoString(buf, ", ");

#if (PG_VERSION_NUM >= 130000)
		if (use_variadic && lnext(node->args, arg) == NULL)
#else
		if (use_variadic && lnext(arg) == NULL)
#endif
			elog(ERROR, "VARIADIC is not supported");

		griddb_deparse_expr((Expr *) lfirst(arg), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse given operator expression.   To avoid problems around
 * priority of operations, we always parenthesize the arguments.
 */
static void
griddb_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Form_pg_operator form;
	char		oprkind;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprkind = form->oprkind;

	/* Sanity check. */
	Assert((oprkind == 'l' && list_length(node->args) == 1) ||
		   (oprkind == 'b' && list_length(node->args) == 2));

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	if (oprkind == 'b')
	{
		griddb_deparse_expr(linitial(node->args), context);
		appendStringInfoChar(buf, ' ');
	}

	/* Deparse operator name. */
	griddb_deparse_operator_name(buf, form);

	/* Deparse right operand. */
	appendStringInfoChar(buf, ' ');
	griddb_deparse_expr(llast(node->args), context);

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

/*
 * Print the name of an operator.
 */
static void
griddb_deparse_operator_name(StringInfo buf, Form_pg_operator opform)
{
	char	   *cur_opname;

	/* opname is not a SQL identifier, so we should not quote it. */
	cur_opname = NameStr(opform->oprname);

	/* Print schema name only if it's not pg_catalog */
	if (opform->oprnamespace != PG_CATALOG_NAMESPACE)
	{
		elog(ERROR, "OPERATOR is not supported");
	}
	else
	{
		if (strcmp(cur_opname, "~~") == 0)
		{
			appendStringInfoString(buf, "LIKE");
		}
		else if (strcmp(cur_opname, "!~~") == 0)
		{
			appendStringInfoString(buf, "NOT LIKE");
		}
		else if (strcmp(cur_opname, "~~*") == 0 ||
				 strcmp(cur_opname, "!~~*") == 0 ||
				 strcmp(cur_opname, "~") == 0 ||
				 strcmp(cur_opname, "!~") == 0 ||
				 strcmp(cur_opname, "~*") == 0 ||
				 strcmp(cur_opname, "!~*") == 0)
		{
			elog(ERROR, "OPERATOR is not supported");
		}
		else
		{
			appendStringInfoString(buf, cur_opname);
		}
	}
}

/*
 * Deparse IS DISTINCT FROM.
 * "expr IS DISTINCT FROM expr" is converted to "expr <> expr".
 */
static void
griddb_deparse_distinct_expr(DistinctExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	Assert(list_length(node->args) == 2);

	appendStringInfoChar(buf, '(');
	griddb_deparse_expr((Expr *) linitial(node->args), context);
	appendStringInfoString(buf, " <> ");
	griddb_deparse_expr((Expr *) lsecond(node->args), context);
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse if array of constants
 * Conditions concatenated by OR/AND will be deparsed, for examples:
 *  c1 = ANY(ARRAY(1, 2, 3)) => c1 == 1 OR c1 == 2 OR c1 == 3
 *  c1 = ALL(ARRAY(1, 2, 3)) => c1 <> 1 AND c1 <> 2 AND c1 <> 3
 */
static void
griddb_deparse_const_array(ScalarArrayOpExpr *node, deparse_expr_cxt *context, StringInfo buf, Const *c, const char *extval, bool isstr)
{
	const char *valptr;
	int			i = 0;
	bool		deparseLeft = true;
	Expr	   *arg1;
	char	   *opname;
	bool		inString = false;
	bool		isEscape = false;

	arg1 = linitial(node->args);
	opname = get_opname(node->opno);

	for (valptr = extval; *valptr; valptr++, i++)
	{
		char		ch = *valptr;

		/* Deparse left operand. */
		if (deparseLeft)
		{
			/* No need deparse bool column */
			if (c->consttype == BOOLARRAYOID)
			{
				deparseLeft = false;
				continue;
			}
			/* Deparse left operand */
			griddb_deparse_expr(arg1, context);
			/* Append operator */
			if (strcmp(opname, "=") == 0)
				appendStringInfo(buf, " == ");
			else
				appendStringInfo(buf, " %s ", opname);
			if (isstr)
				appendStringInfoChar(buf, '\'');
			deparseLeft = false;
		}

		/*
		 * Remove '{', '}' character from the string. Because this syntax is
		 * not recognize by the remote GridDB server.
		 */
		if ((ch == '{' && i == 0) || (ch == '}' && (i == (strlen(extval) - 1))))
			continue;

		/* Remove '\"' and process the next character. */
		if (ch == '\"' && !isEscape)
		{
			inString = !inString;
			continue;
		}
		/* Add escape character '\'' for '\'' */
		if (ch == '\'')
			appendStringInfoChar(buf, '\'');

		/* Remove character '\\' and process the next character. */
		if (ch == '\\' && !isEscape)
		{
			isEscape = true;
			continue;
		}
		isEscape = false;

		if (ch == ',' && !inString)
		{
			if (isstr)
				appendStringInfoChar(buf, '\'');
			if (node->useOr)
				appendStringInfo(buf, "  OR ");
			else
				appendStringInfo(buf, "  AND ");

			/* No need deparse bool column */
			if (c->consttype == BOOLARRAYOID)
			{
				deparseLeft = false;
				continue;
			}
			deparseLeft = true;
			continue;
		}

		/*
		 * When compare with timestamp column, need to convert and cast to
		 * TIMESTAMP
		 */
		if (c->consttype == TIMESTAMPARRAYOID || c->consttype == TIMESTAMPTZARRAYOID)
		{
			char		timestamp[MAXDATELEN + 1];
			char		chtime[MAXDATELEN + 1] = {0};
			struct tm	tm;
			int			j = 0;

			for (;; valptr++)
			{
				if (*valptr == '\"' && !isEscape)
				{
					inString = !inString;
					break;
				}
				chtime[j] = *valptr;
				j++;
			}
			i += j;

			/* Format of chtime is YYYY-MM-DD HH:MM:SS */
			strptime(chtime, "%Y-%m-%d %H:%M:%S", &tm);
			griddb_convert_pg2gs_timestamp_string(time_t_to_timestamptz(timegm(&tm)), timestamp);
			appendStringInfoString(buf, "TIMESTAMP(");
			griddb_deparse_string_literal(buf, timestamp);
			appendStringInfoString(buf, ")");
			continue;
		}

		/*
		 * GridDB not support compare bool column with true, false. Only
		 * support column or NOT column
		 */
		if (c->consttype == BOOLARRAYOID)
		{
			appendStringInfoChar(buf, '(');
			if (ch == 'f')
				appendStringInfoString(buf, "NOT ");

			griddb_deparse_expr(arg1, context);
			appendStringInfoChar(buf, ')');
			continue;
		}

		appendStringInfoChar(buf, ch);
	}

	if (isstr)
		appendStringInfoChar(buf, '\'');
}

/*
 * Deparse given ScalarArrayOpExpr expression.  To avoid problems
 * around priority of operations, we always parenthesize the arguments.
 * GridDB is not support IN.
 * Conditions concatenated by OR/AND will be deparsed, for examples:
 *  c1 = ANY(ARRAY(1, 2, 3)) => c1 == 1 OR c1 == 2 OR c1 == 3
 *  c1 = ANY(ARRAY(1, c2, c3 + 1)) => c1 == 1 OR c1 == c2 OR c1 == c3 + 1
 *  c1 <> ALL(ARRAY(1, 2, 3)) => c1 <> 1 AND c1 <> 2 AND c1 <> 3
 *  c1 <> ALL(ARRAY(1, c2, c3 + 1)) => c1 <> 1 AND c1 <> c2 AND c1 <> c3 + 1
 */
static void
griddb_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Expr	   *arg1;
	Expr	   *arg2;
	char	   *opname;
	Oid			typoutput;
	bool		typIsVarlena;
	bool		isstr;
	char	   *extval;

	opname = get_opname(node->opno);

	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	arg1 = linitial(node->args);
	arg2 = lsecond(node->args);

	switch (nodeTag((Node *) arg2))
	{
		case T_Const:
			{
				Const	   *c = (Const *) arg2;

				getTypeOutputInfo(c->consttype,
								  &typoutput, &typIsVarlena);
				extval = OidOutputFunctionCall(typoutput, c->constvalue);
				switch (c->consttype)
				{
					case BOOLARRAYOID:
					case INT8ARRAYOID:
					case INT2ARRAYOID:
					case INT4ARRAYOID:
					case OIDARRAYOID:
					case FLOAT4ARRAYOID:
					case FLOAT8ARRAYOID:
					case TIMESTAMPARRAYOID:
					case TIMESTAMPTZARRAYOID:
						isstr = false;
						break;
					default:
						isstr = true;
						break;
				}
				griddb_deparse_const_array(node, context, buf, c, extval, isstr);
				break;
			}
		case T_ArrayExpr:
			{
				bool		first = true;
				ListCell   *lc;
				ArrayExpr  *a = (ArrayExpr *) arg2;

				foreach(lc, a->elements)
				{
					if (!first)
					{
						if (node->useOr)
							appendStringInfoString(buf, " OR ");
						else
							appendStringInfoString(buf, " AND ");
					}
					/* Deparse left argument */
					appendStringInfoChar(buf, '(');
					griddb_deparse_expr(arg1, context);
					appendStringInfo(buf, " %s ", opname);

					/* Deparse each element in right argument */
					griddb_deparse_expr(lfirst(lc), context);
					appendStringInfoChar(buf, ')');
					first = false;
				}
				break;
			}
		default:
			{
				elog(ERROR, "unsupported expression type for deparse: %d", (int) nodeTag((Node *) arg2));
				break;
			}
	}
}

/*
 * Deparse a RelabelType (binary-compatible cast) node.
 */
static void
griddb_deparse_relabel_type(RelabelType *node, deparse_expr_cxt *context)
{
	griddb_deparse_expr(node->arg, context);
}

/*
 * Deparse a BoolExpr node.
 *
 * Note: by the time we get here, AND and OR expressions have been flattened
 * into N-argument form, so we'd better be prepared to deal with that.
 */
static void
griddb_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	const char *op = NULL;		/* keep compiler quiet */
	bool		first;
	ListCell   *lc;

	switch (node->boolop)
	{
		case AND_EXPR:
			op = "AND";
			break;
		case OR_EXPR:
			op = "OR";
			break;
		case NOT_EXPR:
			appendStringInfoString(buf, "(NOT ");
			griddb_deparse_expr((Expr *) linitial(node->args), context);
			appendStringInfoChar(buf, ')');
			return;
	}

	appendStringInfoChar(buf, '(');
	first = true;
	foreach(lc, node->args)
	{
		if (!first)
			appendStringInfo(buf, " %s ", op);
		griddb_deparse_expr((Expr *) lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse IS [NOT] NULL expression.
 */
static void
griddb_deparse_null_test(NullTest *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	appendStringInfoChar(buf, '(');
	griddb_deparse_expr(node->arg, context);

	if (node->nulltesttype == IS_NULL)
		appendStringInfoString(buf, " IS NULL)");
	else
		appendStringInfoString(buf, " IS NOT NULL)");
}


/*
 * Deparse a RowExpr node.
 *
 * For time-series function of GridDB, need to deparse Row type expression to build query to send to GridDB
 */
static void
griddb_deparse_row_expr(RowExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		first = true;
	ListCell   *lc;

	appendStringInfoChar(buf, '(');
	foreach(lc, node->colnames)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "%s", strVal(lfirst(lc)));
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse a FieldSelect node
 *
 * To be able to access to a value of a record type data, need to deparse syntax ((xxx(arguments)::text)::table_name).* to push down function only
 */
static void
griddb_deparse_field_select_expr(FieldSelect *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	appendStringInfoChar(buf, '(');
	if (IsA(node->arg, CoerceViaIO))
	{
		/* Handle casting to table name type (Example: ::time_series_tbl) */
		CoerceViaIO *cast_table = (CoerceViaIO *) node->arg;

		if (IsA(cast_table->arg, CoerceViaIO))
		{
			/* Handle casting to text type (Example: ::time_series_tbl) */
			CoerceViaIO *cast_text = (CoerceViaIO *) cast_table->arg;

			if (IsA(cast_text->arg, FuncExpr))
				griddb_deparse_func_expr((FuncExpr *) cast_text->arg, context);
		}
	}

	appendStringInfoChar(buf, ')');
}

/*
 * Return true if given object is one of PostgreSQL's built-in objects.
 *
 * We use FirstBootstrapObjectId as the cutoff, so that we only consider
 * objects with hand-assigned OIDs to be "built in", not for instance any
 * function or type defined in the information_schema.
 *
 * Our constraints for dealing with types are tighter than they are for
 * functions or operators: we want to accept only types that are in pg_catalog,
 * else format_type might incorrectly fail to schema-qualify their names.
 * (This could be fixed with some changes to format_type, but for now there's
 * no need.)  Thus we must exclude information_schema types.
 *
 * XXX there is a problem with this, which is that the set of built-in
 * objects expands over time.  Something that is built-in to us might not
 * be known to the remote server, if it's of an older version.  But keeping
 * track of that would be a huge exercise.
 */
bool
griddb_is_builtin(Oid oid)
{
#if PG_VERSION_NUM > 120000
	return (oid < FirstGenbkiObjectId);
#else
	return (oid < FirstBootstrapObjectId);
#endif
}

static bool
is_inequality(OpExpr *node)
{
	bool		res;
	HeapTuple	tuple;
	Form_pg_operator form;
	char	   *cur_opname;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);

	form = (Form_pg_operator) GETSTRUCT(tuple);
	cur_opname = NameStr(form->oprname);
	if (strcmp(cur_opname, "<") == 0 || strcmp(cur_opname, ">") == 0 ||
		strcmp(cur_opname, "<=") == 0 || strcmp(cur_opname, ">=") == 0)
		res = true;
	else
		res = false;

	ReleaseSysCache(tuple);
	return res;
}

/*
 * Check if inequality is safe to execute remotely, and return true if so.
 * GridDB does not support the inequality of GS_TYPE_STRING. The operation
 * should not execute remotely.
 */
static bool
foreign_inequality_walker(OpExpr *node)
{
	List	   *l = (List *) node->args;
	ListCell   *lc;

	if (!is_inequality(node))
		return true;

	/* Check data type of arguments. */
	foreach(lc, l)
	{
		Oid			type = exprType((Node *) lfirst(lc));

		if (type != INT2OID && type != INT4OID && type != INT8OID &&
			type != FLOAT4OID && type != FLOAT8OID && type != TIMESTAMPOID &&
			type != TIMESTAMPTZOID)
			return false;
	}

	return true;
}

/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (which we approximate
 * as being built-in), and that all collations used in the expression derive
 * from Vars of the foreign table.  Because of the latter, the logic is
 * pretty close to assign_collations_walker() in parse_collate.c, though we
 * can assume here that the given expression is valid.
 */
static bool
foreign_expr_walker(Node *node,
					foreign_glob_cxt *glob_cxt,
					foreign_loc_cxt *outer_cxt)
{
	bool		check_type = true;
	foreign_loc_cxt inner_cxt;
	Oid			collation;
	FDWCollateState state;
	HeapTuple	tuple;

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	/* Set up inner_cxt for possible recursion to child nodes */
	inner_cxt.collation = InvalidOid;
	inner_cxt.state = FDW_COLLATE_NONE;
	inner_cxt.can_skip_cast = false;
	inner_cxt.can_pushdown_stable = false;
	inner_cxt.can_pushdown_volatile = false;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				/*
				 * If the Var is from the foreign table, we consider its
				 * collation (if any) safe to use.  If it is from another
				 * table, we treat its collation the same way as we would a
				 * Param's collation, ie it's not safe for it to have a
				 * non-default collation.
				 */
				if (bms_is_member(var->varno, glob_cxt->relids) &&
					var->varlevelsup == 0 && var->varattno > 0)
				{
					/* Var belongs to foreign table */

					/*
					 * System columns other than ctid should not be sent to
					 * the remote, since we don't make any effort to ensure
					 * that local and remote values match (tableoid, in
					 * particular, almost certainly doesn't match).
					 */
					if (var->varattno < 0 &&
						var->varattno != SelfItemPointerAttributeNumber)
						return false;

					/* Else check the collation */
					collation = var->varcollid;
					state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
				}
				else
				{
					/* Parameter is unsupported */
					return false;
				}
			}
			break;
		case T_Const:
			{
				Const	   *c = (Const *) node;
				HeapTuple	tuple;

				if (c->consttype == INTERVALOID ||
					c->consttype == BITOID ||
					c->consttype == VARBITOID)
					return false;

				/*
				 * Get type name based on the const value. If the type name is
				 * "time_unit", allow it to push down to remote.
				 */
				tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(c->consttype));
				if (HeapTupleIsValid(tuple))
				{
					Form_pg_type type;
					char	   *type_name;

					type = (Form_pg_type) GETSTRUCT(tuple);
					type_name = (char *) type->typname.data;

					if (strcmp(type_name, "time_unit") == 0)
						check_type = false;

					ReleaseSysCache(tuple);
				}

				/*
				 * If the constant has nondefault collation, either it's of a
				 * non-builtin type, or it reflects folding of a CollateExpr.
				 * It's unsafe to send to the remote unless it's used in a
				 * non-collation-sensitive context.
				 */
				collation = c->constcollid;
				if (collation == InvalidOid ||
					collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_Param:
			{
				/* Parameter is unsupported */
				return false;
			}
			break;
#if PG_VERSION_NUM < 120000
		case T_ArrayRef:
#else
		case T_SubscriptingRef:
#endif
			{
				/* Array in condition is unsupported */
				return false;
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *fe = (FuncExpr *) node;
				char	   *proname;
				bool		is_cast_func = false;

				proname = get_func_name(fe->funcid);

				/* check NULL for proname */
				if (proname == NULL)
					elog(ERROR, "cache lookup failed for function %u", fe->funcid);

				if (exist_in_function_list(proname, CastFunction))
				{
					is_cast_func = true;
				}

				/* push down to GridDB */
				if (!(exist_in_function_list(proname, GridDBUniqueFunction) ||
					  exist_in_function_list(proname, GridDBSupportedBuiltinFunction)) &&
					is_cast_func != true)
				{
					return false;
				}

				/*
				 * The following functions are common function of GridDB and
				 * Postgresql. However, GridDB only support them in WHERE
				 * clause, so do not push down when they are in SELECT clause
				 */
				if ((exist_in_function_list(proname, GridDBSupportedBuiltinFunction)) &&
					glob_cxt->for_tlist == true)
				{
					return false;
				}

				/*
				 * Allow push down concat function even though volatility is
				 * stable
				 */
				if (strcmp(proname, "concat") == 0)
					outer_cxt->can_pushdown_stable = true;

				/*
				 * Allow push down griddb_now/timestampdiff/timestampadd
				 * function even though volatility is volatile
				 */
				if (strcmp(proname, "griddb_now") == 0 ||
					strcmp(proname, "timestampdiff") == 0 ||
					strcmp(proname, "timestampadd") == 0)
					outer_cxt->can_pushdown_volatile = true;

				/* inner function can skip cast if any */
				if (strcmp(proname, "to_timestamp_ms") == 0)
					inner_cxt.can_skip_cast = true;

				/* Accept type cast functions if outer is specific functions */
				if (is_cast_func)
				{
					if (outer_cxt->can_skip_cast == false)
						return false;
				}
				else
				{
					glob_cxt->is_inner_func = true;
				}

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) fe->args,
										 glob_cxt, &inner_cxt))
					return false;

				if (!is_cast_func)
					glob_cxt->is_inner_func = false;

				/*
				 * Transfer the can_pushdown_volatile in the composition
				 * T_Func->T_List->T_Func
				 */
				if (inner_cxt.can_pushdown_volatile == true)
					outer_cxt->can_pushdown_volatile = true;

				/* Skip collation check for time series function */
				if (strcmp(proname, "time_next") == 0 ||
					strcmp(proname, "time_next_only") == 0 ||
					strcmp(proname, "time_prev") == 0 ||
					strcmp(proname, "time_prev_only") == 0 ||
					strcmp(proname, "time_interpolated") == 0 ||
					strcmp(proname, "max_rows") == 0 ||
					strcmp(proname, "min_rows") == 0 ||
					strcmp(proname, "time_sampling") == 0)
				{
					collation = InvalidOid;
					state = FDW_COLLATE_NONE;
					check_type = false;
					outer_cxt->can_pushdown_stable = true;
				}
				else
				{
					/*
					 * If function's input collation is not derived from a
					 * foreign Var, it can't be sent to remote.
					 */
					if (fe->inputcollid == InvalidOid)
						 /* OK, inputs are all noncollatable */ ;
					else if (inner_cxt.state != FDW_COLLATE_SAFE ||
							 fe->inputcollid != inner_cxt.collation)
						return false;

					/*
					 * Detect whether node is introducing a collation not
					 * derived from a foreign Var.  (If so, we just mark it
					 * unsafe for now rather than immediately returning false,
					 * since the parent node might not care.)
					 */
					collation = fe->funccollid;
					if (collation == InvalidOid)
						state = FDW_COLLATE_NONE;
					else if (inner_cxt.state == FDW_COLLATE_SAFE &&
							 collation == inner_cxt.collation)
						state = FDW_COLLATE_SAFE;
					else
						state = FDW_COLLATE_UNSAFE;
				}
			}
			break;
		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
			{
				OpExpr	   *oe = (OpExpr *) node;

				/*
				 * Similarly, only built-in operators can be sent to remote.
				 * (If the operator is, surely its underlying function is
				 * too.)
				 */
				if (!griddb_is_builtin(oe->opno))
					return false;

				if (glob_cxt->for_tlist)
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt))
					return false;

				if (inner_cxt.can_pushdown_volatile == true)
					outer_cxt->can_pushdown_volatile = true;

				if (inner_cxt.can_pushdown_stable == false)
				{
					/*
					 * If operator's input collation is not derived from a
					 * foreign Var, it can't be sent to remote.
					 */
					if (oe->inputcollid == InvalidOid)
						 /* OK, inputs are all noncollatable */ ;
					else if (inner_cxt.state != FDW_COLLATE_SAFE ||
							 oe->inputcollid != inner_cxt.collation)
						return false;
				}
				else
				{
					outer_cxt->can_pushdown_stable = true;
				}

				/* Check inequality is safe to execute remotely. */
				if (!foreign_inequality_walker(oe))
					return false;

				if (inner_cxt.can_pushdown_stable == true)
				{
					state = FDW_COLLATE_NONE;
					collation = oe->opcollid;
				}
				else
				{
					/* Result-collation handling is same as for functions */
					collation = oe->opcollid;
					if (collation == InvalidOid)
						state = FDW_COLLATE_NONE;
					else if (inner_cxt.state == FDW_COLLATE_SAFE &&
							 collation == inner_cxt.collation)
						state = FDW_COLLATE_SAFE;
					else
						state = FDW_COLLATE_UNSAFE;
				}
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;

				/*
				 * Again, only built-in operators can be sent to remote.
				 */
				if (!griddb_is_builtin(oe->opno))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_RelabelType:
			{
				RelabelType *r = (RelabelType *) node;

				/*
				 * Recurse to input subexpression.
				 */
				if (!foreign_expr_walker((Node *) r->arg,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * RelabelType must not introduce a collation not derived from
				 * an input foreign Var.
				 */
				collation = r->resultcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) b->args,
										 glob_cxt, &inner_cxt))
					return false;

				/* Allow push down griddb_now in boolean condition */
				if (inner_cxt.can_pushdown_volatile == true)
					outer_cxt->can_pushdown_volatile = true;
				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) nt->arg,
										 glob_cxt, &inner_cxt))
					return false;

				/* Allow push down griddb_now with NULL test */
				if (inner_cxt.can_pushdown_volatile == true)
					outer_cxt->can_pushdown_volatile = true;
				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_ArrayExpr:
			{
				ArrayExpr  *a = (ArrayExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) a->elements,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * ArrayExpr must not introduce a collation not derived from
				 * an input foreign Var.
				 */
				collation = a->array_collid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_List:
			{
				List	   *l = (List *) node;
				ListCell   *lc;

				/* inherit can_skip_cast flag */
				inner_cxt.can_skip_cast = outer_cxt->can_skip_cast;
				inner_cxt.can_pushdown_stable = outer_cxt->can_pushdown_stable;

				/*
				 * Recurse to component subexpressions.
				 */
				foreach(lc, l)
				{
					if (!foreign_expr_walker((Node *) lfirst(lc),
											 glob_cxt, &inner_cxt))
						return false;
				}

				if (inner_cxt.can_pushdown_stable == true)
					outer_cxt->can_pushdown_stable = true;

				if (inner_cxt.can_pushdown_volatile == true)
					outer_cxt->can_pushdown_volatile = true;

				/*
				 * When processing a list, collation state just bubbles up
				 * from the list elements.
				 */

				collation = inner_cxt.collation;
				state = inner_cxt.state;

				/* Don't apply exprType() to the list. */
				check_type = false;
			}
			break;
		case T_RowExpr:			/* Allow pushdown RowExpr to support
								 * time-series functions */
		case T_FieldSelect:		/* Allow pushdown FieldSelect to support
								 * accessing value of record of time-series
								 * functions */
			{
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
				check_type = false;
			}
			break;
		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) node;
				ListCell   *lc;
				char	   *opername = NULL;
				bool		is_math_func = false;
				bool		is_selector_func = false;
				bool		is_count_func = false;

				/* get function name */
				tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(agg->aggfnoid));
				if (!HeapTupleIsValid(tuple))
				{
					elog(ERROR, "cache lookup failed for function %u", agg->aggfnoid);
				}
				opername = pstrdup(((Form_pg_proc) GETSTRUCT(tuple))->proname.data);
				ReleaseSysCache(tuple);

				/* these function can be passed to Griddb */
				if (strcmp(opername, "sum") == 0 ||
					strcmp(opername, "avg") == 0 ||
					strcmp(opername, "stddev") == 0 ||
					strcmp(opername, "variance") == 0 ||
					strcmp(opername, "time_avg") == 0)
					is_math_func = true;

				if (strcmp(opername, "max") == 0 ||
					strcmp(opername, "min") == 0)
					is_selector_func = true;

				if (strcmp(opername, "count") == 0)
					is_count_func = true;

				if (!(is_math_func || is_selector_func || is_count_func))
					return false;

				/* Not safe to pushdown when not in grouping context */
				if (glob_cxt->foreignrel->reloptkind != RELOPT_UPPER_REL)
					return false;

				/* Only non-split aggregates are pushable. */
				if (agg->aggsplit != AGGSPLIT_SIMPLE)
					return false;

				/*
				 * Recurse to input args. aggdirectargs, aggorder and
				 * aggdistinct are all present in args, so no need to check
				 * their shippability explicitly.
				 */
				foreach(lc, agg->args)
				{
					Node	   *n = (Node *) lfirst(lc);

					/* If TargetEntry, extract the expression from it */
					if (IsA(n, TargetEntry))
					{
						TargetEntry *tle = (TargetEntry *) n;
						Var		   *tmp_var;

						n = (Node *) tle->expr;
						tmp_var = (Var *) n;
						switch (tmp_var->vartype)
						{
							case INT2OID:
							case INT4OID:
							case INT8OID:
							case OIDOID:
							case FLOAT4OID:
							case FLOAT8OID:
							case NUMERICOID:
								{
									if (!(is_math_func || is_selector_func))
									{
										return false;
									}
									break;
								}
							case TIMESTAMPOID:
							case TIMESTAMPTZOID:
								{
									if (!is_selector_func)
									{
										return false;
									}
									break;
								}
							default:
								return false;
						}
					}
					else if (!(agg->aggstar == true && is_count_func))
						return false;

					if (!foreign_expr_walker(n, glob_cxt, &inner_cxt))
						return false;
				}

				if (agg->aggorder || agg->aggfilter)
				{
					return false;
				}

				/*
				 * If aggregate's input collation is not derived from a
				 * foreign Var, it can't be sent to remote.
				 */
				if (agg->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 agg->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
				 */
				collation = agg->aggcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		default:

			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support below.
			 */
			return false;
	}

	/*
	 * If result type of given expression is not built-in, it can't be sent to
	 * remote because it might have incompatible semantics on remote side.
	 */
	if (check_type && !griddb_is_builtin(exprType(node)))
		return false;

	/*
	 * Now, merge my collation information into my parent's state.
	 */
	if (state > outer_cxt->state)
	{
		/* Override previous parent state */
		outer_cxt->collation = collation;
		outer_cxt->state = state;
	}
	else if (state == outer_cxt->state)
	{
		/* Merge, or detect error if there's a collation conflict */
		switch (state)
		{
			case FDW_COLLATE_NONE:
				/* Nothing + nothing is still nothing */
				break;
			case FDW_COLLATE_SAFE:
				if (collation != outer_cxt->collation)
				{
					/*
					 * Non-default collation always beats default.
					 */
					if (outer_cxt->collation == DEFAULT_COLLATION_OID)
					{
						/* Override previous parent state */
						outer_cxt->collation = collation;
					}
					else if (collation != DEFAULT_COLLATION_OID)
					{
						/*
						 * Conflict; show state as indeterminate.  We don't
						 * want to "return false" right away, since parent
						 * node might not care about collation.
						 */
						outer_cxt->state = FDW_COLLATE_UNSAFE;
					}
				}
				break;
			case FDW_COLLATE_UNSAFE:
				/* We're still conflicted ... */
				break;
		}
	}

	/* It looks OK */
	return true;
}

/*
 * pull_func_clause_walker
 *
 * Recursively search for functions within a clause.
 */
static bool
pull_func_clause_walker(Node *node, pull_func_clause_context * context)
{
	if (node == NULL)
		return false;
	if (IsA(node, FuncExpr))
	{
		context->funclist = lappend(context->funclist, node);
		return false;
	}

	return expression_tree_walker(node, pull_func_clause_walker,
								  (void *) context);
}

/*
 * pull_func_clause
 *
 * Pull out function from a clause and then add to target list
 */
List *
griddb_pull_func_clause(Node *node)
{
	pull_func_clause_context context;

	context.funclist = NIL;

	pull_func_clause_walker(node, &context);

	return context.funclist;
}

/*
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
bool
griddb_is_foreign_expr(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Expr *expr,
					   bool for_tlist)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;
	GriddbFdwRelationInfo *fpinfo = (GriddbFdwRelationInfo *) (baserel->fdw_private);

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;

	/*
	 * For an upper relation, use relids from its underneath scan relation,
	 * because the upperrel's own relids currently aren't set to anything
	 * meaningful by the core code.  For other relation, use their own relids.
	 */
	if (IS_UPPER_REL(baserel))
		glob_cxt.relids = fpinfo->outerrel->relids;
	else
		glob_cxt.relids = baserel->relids;

	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;
	loc_cxt.can_skip_cast = false;
	loc_cxt.can_pushdown_stable = false;
	loc_cxt.can_pushdown_volatile = false;
	glob_cxt.for_tlist = for_tlist;
	glob_cxt.is_inner_func = false;

	if (!foreign_expr_walker((Node *) expr, &glob_cxt, &loc_cxt))
		return false;

	/*
	 * If the expression has a valid collation that does not arise from a
	 * foreign var, the expression can not be sent over.
	 */
	if (loc_cxt.state == FDW_COLLATE_UNSAFE)
		return false;

	/*
	 * An expression which includes any mutable functions can't be sent over
	 * because its result is not stable.  For example, sending now() remote
	 * side could cause confusion from clock offsets.  Future versions might
	 * be able to make this choice with more granularity.  (We check this last
	 * because it requires a lot of expensive catalog lookups.)
	 */
	if (loc_cxt.can_pushdown_volatile == true)
	{
		/* Does not perform additional check to push down griddb_now() */
	}
	else if (loc_cxt.can_pushdown_stable == true)
	{
		if (contain_volatile_functions((Node *) expr))
			return false;
	}
	else
	{
		if (contain_mutable_functions((Node *) expr))
			return false;
	}

	/* OK to evaluate on the remote server */
	return true;
}

/*
 * Deparse LIMIT/OFFSET clause.
 */
static void
griddb_append_limit_clause(deparse_expr_cxt *context)
{
	PlannerInfo *root = context->root;
	StringInfo	buf = context->buf;
	int			nestlevel;

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = griddb_set_transmission_modes();

	if (root->parse->limitCount)
	{
		appendStringInfoString(buf, " LIMIT ");
		griddb_deparse_expr((Expr *) root->parse->limitCount, context);
	}
	if (root->parse->limitOffset)
	{
		appendStringInfoString(buf, " OFFSET ");
		griddb_deparse_expr((Expr *) root->parse->limitOffset, context);
	}

	griddb_reset_transmission_modes(nestlevel);
}

/*
 * Deparse ORDER BY clause according to the given pathkeys for given base
 * relation. From given pathkeys expressions belonging entirely to the given
 * base relation are obtained and deparsed.
 */
static void
griddb_append_order_by_clause(List *pathkeys, deparse_expr_cxt *context)
{
	ListCell   *lcell;
	const char	   *delim = " ";
	StringInfo	buf = context->buf;

	appendStringInfo(buf, " ORDER BY");
	foreach(lcell, pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lcell);
		EquivalenceMember *em;
		Expr	   *em_expr;

		em = griddb_find_em_for_rel(context->root,
									pathkey->pk_eclass,
									context->scanrel);
		/*
		 * We don't expect any error here; it would mean that shippability
		 * wasn't verified earlier.  For the same reason, we don't recheck
		 * shippability of the sort operator.
		 */
		if (em == NULL)
			elog(ERROR, "could not find pathkey item to sort");

		em_expr = em->em_expr;

		appendStringInfoString(buf, delim);
		griddb_deparse_expr(em_expr, context);
		if (pathkey->pk_strategy == BTLessStrategyNumber)
			appendStringInfoString(buf, " ASC");
		else
			appendStringInfoString(buf, " DESC");

		delim = ", ";
	}
}


/*
 * Deparse the appropriate locking clause (FOR SELECT or FOR SHARE) for a
 * given relation (context->foreignrel).
 */
void
griddb_deparse_locking_clause(PlannerInfo *root, RelOptInfo *rel, int *for_update)
{
	int			relid = -1;

	while ((relid = bms_next_member(rel->relids, relid)) >= 0)
	{
		/*
		 * Add FOR UPDATE/SHARE if appropriate.  We apply locking during the
		 * initial row fetch, rather than later on as is done for local
		 * tables. The extra roundtrips involved in trying to duplicate the
		 * local semantics exactly don't seem worthwhile (see also comments
		 * for RowMarkType).
		 *
		 * Note: because we actually run the query as a cursor, this assumes
		 * that DECLARE CURSOR ... FOR UPDATE is supported, which it isn't
		 * before 8.3.
		 */
#if PG_VERSION_NUM < 140000
		if (relid == root->parse->resultRelation &&
#else
		if (bms_is_member(relid, root->all_result_relids) &&
#endif
			(root->parse->commandType == CMD_UPDATE ||
			 root->parse->commandType == CMD_DELETE))
		{
			/* Relation is UPDATE/DELETE target, so set FOR UPDATE flag */
			*for_update = 1;
		}
		else
		{
			PlanRowMark *rc = get_plan_rowmark(root->rowMarks, relid);

			if (rc)
			{
				/*
				 * Relation is specified as a FOR UPDATE/SHARE target, so
				 * handle that.  (But we could also see LCS_NONE, meaning this
				 * isn't a target relation after all.)
				 *
				 * For now, just ignore any [NO] KEY specification, since (a)
				 * it's not clear what that means for a remote table that we
				 * don't have complete information about, and (b) it wouldn't
				 * work anyway on older remote servers.  Likewise, we don't
				 * worry about NOWAIT.
				 */
				switch (rc->strength)
				{
					case LCS_NONE:
						/* No locking needed */
						break;
					case LCS_FORKEYSHARE:
					case LCS_FORSHARE:
						/* FOR SHARE */
						*for_update = 1;
						break;
					case LCS_FORNOKEYUPDATE:
					case LCS_FORUPDATE:
						/* FOR SHARE */
						*for_update = 1;
						break;
				}
			}
		}
	}
}

/*
 * Examine each qual clause in input_conds, and classify them into two groups,
 * which are returned as two lists:
 *	- remote_conds contains expressions that can be evaluated remotely
 *	- local_conds contains expressions that can't be evaluated remotely
 */
void
griddb_classify_conditions(PlannerInfo *root,
						   RelOptInfo *baserel,
						   List *input_conds,
						   List **remote_conds,
						   List **local_conds)
{
	ListCell   *lc;

	*remote_conds = NIL;
	*local_conds = NIL;

	foreach(lc, input_conds)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		if (griddb_is_foreign_expr(root, baserel, ri->clause, false))
			*remote_conds = lappend(*remote_conds, ri);
		else
			*local_conds = lappend(*local_conds, ri);
	}
}

/*****************************************************************************
 *		Check clauses for immutable functions
 *****************************************************************************/

/*
 * contain_immutable_functions
 *	  Recursively search for immutable functions within a clause.
 *
 * Returns true if any immutable function (or operator implemented by a
 * immutable function) is found.
 *
 * We will recursively look into TargetEntry exprs.
 */
static bool
griddb_contain_functions(Node *clause)
{
	return griddb_contain_functions_walker(clause, NULL);
}

static bool
griddb_contain_functions_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	/* Check for functions in node itself */
	if (nodeTag(node) == T_FuncExpr)
	{
		return true;
	}

	/*
	 * It should be safe to treat MinMaxExpr as immutable, because it will
	 * depend on a non-cross-type btree comparison function, and those should
	 * always be immutable.  Treating XmlExpr as immutable is more dubious,
	 * and treating CoerceToDomain as immutable is outright dangerous.  But we
	 * have done so historically, and changing this would probably cause more
	 * problems than it would fix.  In practice, if you have a non-immutable
	 * domain constraint you are in for pain anyhow.
	 */

	/* Recurse to check arguments */
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		return query_tree_walker((Query *) node,
								 griddb_contain_functions_walker,
								 context, 0);
	}
	return expression_tree_walker(node, griddb_contain_functions_walker,
								  context);
}

/*
 * Returns true if given tlist is safe to evaluate on the foreign server.
 */
bool
griddb_is_foreign_function_tlist(PlannerInfo *root,
								 RelOptInfo *baserel,
								 List *tlist)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;
	ListCell   *lc;
	bool		is_contain_function;

	if (!(baserel->reloptkind == RELOPT_BASEREL ||
		  baserel->reloptkind == RELOPT_OTHER_MEMBER_REL))
		return false;

	/*
	 * Check that the expression consists of any immutable function.
	 */
	is_contain_function = false;
	foreach(lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (griddb_contain_functions((Node *) tle->expr))
		{
			is_contain_function = true;
			break;
		}
	}

	if (!is_contain_function)
		return false;

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	foreach(lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		glob_cxt.root = root;
		glob_cxt.foreignrel = baserel;
		glob_cxt.relids = baserel->relids;
		glob_cxt.for_tlist = true;
		loc_cxt.collation = InvalidOid;
		loc_cxt.state = FDW_COLLATE_NONE;
		loc_cxt.can_skip_cast = false;
		loc_cxt.can_pushdown_stable = false;
		loc_cxt.can_pushdown_volatile = false;
		if (!foreign_expr_walker((Node *) tle->expr, &glob_cxt, &loc_cxt))
			return false;

		/*
		 * If the expression has a valid collation that does not arise from a
		 * foreign var, the expression can not be sent over.
		 */
		if (loc_cxt.state == FDW_COLLATE_UNSAFE)
			return false;

		/*
		 * An expression which includes any mutable functions can't be sent
		 * over because its result is not stable.  For example, sending now()
		 * remote side could cause confusion from clock offsets.  Future
		 * versions might be able to make this choice with more granularity.
		 * (We check this last because it requires a lot of expensive catalog
		 * lookups.) Do not check mutable function if expression is
		 * FieldSelect
		 */
		if (!IsA(tle->expr, FieldSelect))
		{
			if (loc_cxt.can_pushdown_volatile == true)
				return true;

			if (loc_cxt.can_pushdown_stable == true)
			{
				if (contain_volatile_functions((Node *) tle->expr))
					return false;
			}
			else
			{
				if (contain_mutable_functions((Node *) tle->expr))
					return false;
			}
		}
	}

	/* OK for the target list with functions to evaluate on the remote server */
	return true;
}

/*
 * Build the targetlist for given relation to be deparsed as SELECT clause.
 *
 * The output targetlist contains the columns that need to be fetched from the
 * foreign server for the given relation.  If foreignrel is an upper relation,
 * then the output targetlist can also contain expressions to be evaluated on
 * foreign server.
 */
List *
griddb_build_tlist_to_deparse(RelOptInfo *foreignrel)
{
	List	   *tlist = NIL;
	GriddbFdwRelationInfo *fpinfo = (GriddbFdwRelationInfo *) foreignrel->fdw_private;
	ListCell   *lc;

	/*
	 * For an upper relation, we have already built the target list while
	 * checking shippability, so just return that.
	 */
	if (IS_UPPER_REL(foreignrel))
		return fpinfo->grouped_tlist;

	/*
	 * We require columns specified in foreignrel->reltarget->exprs and those
	 * required for evaluating the local conditions.
	 */
	tlist = add_to_flat_tlist(tlist,
							  pull_var_clause((Node *) foreignrel->reltarget->exprs,
											  PVC_RECURSE_PLACEHOLDERS));
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		tlist = add_to_flat_tlist(tlist,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_RECURSE_PLACEHOLDERS));
	}

	return tlist;
}

/*
 * Deparse an Aggref node.
 */
static void
griddb_deparse_aggref(Aggref *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		use_variadic;
	GridDBAggref *aggref = context->aggref;

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	initStringInfo(aggref->aggname);
	initStringInfo(aggref->columnname);

	/* Only basic, non-split aggregation accepted. */
	Assert(node->aggsplit == AGGSPLIT_SIMPLE);

	/* Check if need to print VARIADIC (cf. ruleutils.c) */
	use_variadic = node->aggvariadic;

	/* Find aggregate name from aggfnoid which is a pg_proc entry */
	griddb_append_function_name(node->aggfnoid, context);
	appendStringInfoChar(buf, '(');

	/* Add DISTINCT */
	appendStringInfoString(buf, (node->aggdistinct != NIL) ? "DISTINCT " : "");

	if (AGGKIND_IS_ORDERED_SET(node->aggkind))
	{
		/* Add WITHIN GROUP (ORDER BY ..) */
		ListCell   *arg;
		bool		first = true;

		Assert(!node->aggvariadic);
		Assert(node->aggorder != NIL);

		foreach(arg, node->aggdirectargs)
		{
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			griddb_deparse_expr((Expr *) lfirst(arg), context);
		}

		appendStringInfoString(buf, ") WITHIN GROUP (ORDER BY ");
		griddb_append_agg_order_by(node->aggorder, node->args, context);
	}
	else
	{
		/* aggstar can be set only in zero-argument aggregates */
		if (node->aggstar)
		{
			appendStringInfoChar(buf, '*');
		}
		else
		{
			ListCell   *arg;
			bool		first = true;

			/* Add all the arguments */
			foreach(arg, node->args)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(arg);
				Node	   *n = (Node *) tle->expr;

				if (tle->resjunk)
					continue;

				if (!first)
					appendStringInfoString(buf, ", ");
				first = false;

				/* Add VARIADIC */
#if PG_VERSION_NUM < 130000
				if (use_variadic && lnext(arg) == NULL)
#else
				if (use_variadic && lnext(node->args, arg) == NULL)
#endif
					appendStringInfoString(buf, "VARIADIC ");

				griddb_deparse_expr((Expr *) n, context);
			}
		}

		/* Add ORDER BY */
		if (node->aggorder != NIL)
		{
			appendStringInfoString(buf, " ORDER BY ");
			griddb_append_agg_order_by(node->aggorder, node->args, context);
		}
	}

	/* Add FILTER (WHERE ..) */
	if (node->aggfilter != NULL)
	{
		appendStringInfoString(buf, ") FILTER (WHERE ");
		griddb_deparse_expr((Expr *) node->aggfilter, context);
	}

	appendStringInfoChar(buf, ')');
}

/*
 * Append ORDER BY within aggregate function.
 */
static void
griddb_append_agg_order_by(List *orderList, List *targetList, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	ListCell   *lc;
	bool		first = true;

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	foreach(lc, orderList)
	{
		SortGroupClause *srt = (SortGroupClause *) lfirst(lc);
		Node	   *sortexpr;

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		/* Deparse the sort expression proper. */
		sortexpr = griddb_deparse_sort_group_clause(srt->tleSortGroupRef, targetList,
													false, context);

		/* Add decoration as needed. */
		griddb_append_order_by_suffix(srt->sortop, exprType(sortexpr), srt->nulls_first,
									  context);
	}
}

/*
 * Append the ASC, DESC, USING <OPERATOR> and NULLS FIRST / NULLS LAST parts
 * of an ORDER BY clause.
 */
static void
griddb_append_order_by_suffix(Oid sortop, Oid sortcoltype, bool nulls_first,
							  deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	TypeCacheEntry *typentry;

	/* See whether operator is default < or > for datatype */
	typentry = lookup_type_cache(sortcoltype,
								 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
	if (sortop == typentry->lt_opr)
		appendStringInfoString(buf, " ASC");
	else if (sortop == typentry->gt_opr)
		appendStringInfoString(buf, " DESC");
	else
	{
		HeapTuple	opertup;
		Form_pg_operator operform;

		appendStringInfoString(buf, " USING ");

		/* Append operator name. */
		opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(sortop));
		if (!HeapTupleIsValid(opertup))
			elog(ERROR, "cache lookup failed for operator %u", sortop);
		operform = (Form_pg_operator) GETSTRUCT(opertup);
		griddb_deparse_operator_name(buf, operform);
		ReleaseSysCache(opertup);
	}

	if (nulls_first)
		appendStringInfoString(buf, " NULLS FIRST");
	else
		appendStringInfoString(buf, " NULLS LAST");
}

/*
 * Appends a sort or group clause.
 *
 * Like get_rule_sortgroupclause(), returns the expression tree, so caller
 * need not find it again.
 */
static Node *
griddb_deparse_sort_group_clause(Index ref, List *tlist, bool force_colno,
								 deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	TargetEntry *tle;
	Expr	   *expr;

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	tle = get_sortgroupref_tle(ref, tlist);
	expr = tle->expr;

	if (force_colno)
	{
		/* Use column-number form when requested by caller. */
		Assert(!tle->resjunk);
		appendStringInfo(buf, "%d", tle->resno);
	}
	else if (expr && IsA(expr, Const))
	{
		/*
		 * Force a typecast here so that we don't emit something like "GROUP
		 * BY 2", which will be misconstrued as a column position rather than
		 * a constant.
		 */
		griddb_deparse_const((Const *) expr, context);
	}
	else if (!expr || IsA(expr, Var))
		griddb_deparse_expr(expr, context);
	else
	{
		/* Always parenthesize the expression. */
		appendStringInfoChar(buf, '(');
		griddb_deparse_expr(expr, context);
		appendStringInfoChar(buf, ')');
	}

	return (Node *) expr;
}

/*
 * griddb_append_function_name
 *		Deparses function name from given function oid.
 */
static void
griddb_append_function_name(Oid funcid, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	proctup;
	Form_pg_proc procform;
	const char *proname;

	elog(DEBUG1, "griddb_fdw: %s", __func__);

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	/* Always print the function name */
	proname = NameStr(procform->proname);
	appendStringInfoString(buf, quote_identifier(proname));
	if (context->aggref)
		appendStringInfoString(context->aggref->aggname, quote_identifier(proname));

	ReleaseSysCache(proctup);
}

/*
 * Returns true if given expr is something we'd have to send the value of
 * to the foreign server.
 *
 * This should return true when the expression is a shippable node that
 * griddb_deparse_expr would add to context->params_list.  Note that we don't care
 * if the expression *contains* such a node, only whether one appears at top
 * level.  We need this to detect cases where setrefs.c would recognize a
 * false match between an fdw_exprs item (which came from the params_list)
 * and an entry in fdw_scan_tlist (which we're considering putting the given
 * expression into).
 */
bool
griddb_is_foreign_param(PlannerInfo *root,
						RelOptInfo *baserel,
						Expr *expr)
{
	elog(DEBUG1, "griddb_fdw: %s", __func__);

	if (expr == NULL)
		return false;

	switch (nodeTag(expr))
	{
		case T_Var:
			{
				/* It would have to be sent unless it's a foreign Var */
				Var		   *var = (Var *) expr;
				GriddbFdwRelationInfo *fpinfo = (GriddbFdwRelationInfo *) (baserel->fdw_private);
				Relids		relids;

				if (IS_UPPER_REL(baserel))
					relids = fpinfo->outerrel->relids;
				else
					relids = baserel->relids;

				if (bms_is_member(var->varno, relids) && var->varlevelsup == 0)
					return false;	/* foreign Var, so not a param */
				else
					return true;	/* it'd have to be a param */
				break;
			}
		case T_Param:
			/* Params always have to be sent to the foreign server */
			return true;
		default:
			break;
	}
	return false;
}

/*
 * Returns true if it's safe to push down the sort expression described by
 * 'pathkey' to the foreign server.
 */
bool
griddb_is_foreign_pathkey(PlannerInfo *root,
						  RelOptInfo *baserel,
						  PathKey *pathkey)
{
	EquivalenceClass *pathkey_ec = pathkey->pk_eclass;

	/*
	 * griddb_is_foreign_expr would detect volatile expressions as well, but checking
	 * ec_has_volatile here saves some cycles.
	 */
	if (pathkey_ec->ec_has_volatile)
		return false;
	
	/* Can't push down the sort if the EC's opfamily is not built-in. */
	if (!griddb_is_builtin(linitial_oid(pathkey_ec->ec_opfamilies)))
		return false;

	/* can push if a suitable EC member exists */
	return (griddb_find_em_for_rel(root, pathkey_ec, baserel) != NULL);
}

/*
 * Return true if function name existed in list of function
 */
static bool
exist_in_function_list(char *funcname, const char **funclist)
{
	int			i;

	for (i = 0; funclist[i]; i++)
	{
		if (strcmp(funcname, funclist[i]) == 0)
			return true;
	}
	return false;
}

/*
 * Convert type OID info of Postgresql table into a type name of Griddb.
 */
static GSType
convert_pgtyp_to_gstyp(Oid type_oid)
{
	switch (type_oid)
	{
		case TEXTOID:
		case CHAROID:
		case VARCHAROID:
			return GS_TYPE_STRING;
		case BOOLOID:
			return GS_TYPE_BOOL;
		case INT2OID:
			return GS_TYPE_SHORT;
		case INT4OID:
			return GS_TYPE_INTEGER;
		case INT8OID:
			return GS_TYPE_LONG;
		case FLOAT4OID:
			return GS_TYPE_FLOAT;
		case FLOAT8OID:
			return GS_TYPE_DOUBLE;
		case TIMESTAMPOID:
			return GS_TYPE_TIMESTAMP;
		case BYTEAOID:
			return GS_TYPE_BLOB;
		case TEXTARRAYOID:
			return GS_TYPE_STRING_ARRAY;
		case BOOLARRAYOID:
			return GS_TYPE_BOOL_ARRAY;
		case INT2ARRAYOID:
			return GS_TYPE_SHORT_ARRAY;
		case INT4ARRAYOID:
			return GS_TYPE_INTEGER_ARRAY;
		case INT8ARRAYOID:
			return GS_TYPE_LONG_ARRAY;
		case FLOAT4ARRAYOID:
			return GS_TYPE_FLOAT_ARRAY;
		case FLOAT8ARRAYOID:
			return GS_TYPE_DOUBLE_ARRAY;
		case TIMESTAMPARRAYOID:
			return GS_TYPE_TIMESTAMP_ARRAY;
		default:
			elog(ERROR, "cannot convert %d to GSType", type_oid);
	}
}

/*
 * Construct Container Infomation for CREATING
 */
GSContainerInfo
griddb_set_container_info(GSContainerInfo containerinfo, Relation rel)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			i;
	int column_count = 0;
	GSContainerInfo container_info;
	GSColumnInfo	columnInfo;
	GSColumnInfo *columnInfoList;

	container_info = (GSContainerInfo) GS_CONTAINER_INFO_INITIALIZER;

	/* count number of columns */
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		/* Ignore dropped columns. */
		if (att->attisdropped)
			continue;

		column_count++;
	}

	columnInfoList = palloc0(column_count * sizeof(GSColumnInfo));

	/* deparse column */
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		/* Ignore dropped columns. */
		if (att->attisdropped)
			continue;

		/* Use attribute name */
		columnInfo.name = NameStr(att->attname);
		columnInfo.type = convert_pgtyp_to_gstyp(att->atttypid);
		if (att->attnotnull)
			columnInfo.options = GS_TYPE_OPTION_NOT_NULL;

		columnInfoList[i] = columnInfo;
	}

	container_info.columnInfoList = columnInfoList;
	container_info.columnCount = column_count;

	return container_info;
}
