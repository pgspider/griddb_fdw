/*
 * GridDB Foreign Data Wrapper
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
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
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "time.h"

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
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
} foreign_loc_cxt;

/*
 * Context for deparseExpr
 */
typedef struct deparse_expr_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	StringInfo	buf;			/* output buffer to append to */
	List	  **params_list;	/* exprs that will become remote Params */
} deparse_expr_cxt;


/*
 * Functions to construct string representation of a node tree.
 */
static void deparseExpr(Expr *expr, deparse_expr_cxt *context);
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




static void griddb_deparse_relation(StringInfo buf, Relation rel);
static void griddb_deparse_target_list(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel,
						   Bitmapset *attrs_used, List **retrieved_attrs);
static void griddb_deparse_column_ref(StringInfo buf, int varno, int varattno,
						  PlannerInfo *root);

static void griddb_append_order_by_clause(List *pathkeys, deparse_expr_cxt *context);
static void griddb_append_where_clause(List *exprs, deparse_expr_cxt *context);
static bool is_griddb_func(FuncExpr *fe);


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
					  List **params_list)
{
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	Relation	rel;
	GriddbFdwRelationInfo *fpinfo = (GriddbFdwRelationInfo *) baserel->fdw_private;
	deparse_expr_cxt context;

	/* Fill portions of context common to join and base relation */
	context.buf = buf;
	context.root = root;
	context.foreignrel = baserel;
	context.params_list = params_list;

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = table_open(rte->relid, NoLock);

	appendStringInfoString(buf, "SELECT ");
	griddb_deparse_target_list(buf, root, baserel->relid, rel,
							   fpinfo->attrs_used, retrieved_attrs);

	/*
	 * Construct FROM clause
	 */
	appendStringInfoString(buf, " FROM ");
	griddb_deparse_relation(buf, rel);

	/*
	 * Construct WHERE clause
	 */
	if (remote_conds)
		griddb_append_where_clause(remote_conds, &context);

	/* Add ORDER BY clause if we found any useful pathkeys */
	if (pathkeys)
		griddb_append_order_by_clause(pathkeys, &context);

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
		deparseExpr(ri->clause, context);
		appendStringInfoChar(buf, ')');

		is_first = false;
	}
}

/*
 * Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 */
static void
griddb_deparse_column_ref(StringInfo buf, int varno, int varattno, PlannerInfo *root)
{
	RangeTblEntry *rte;
	char	   *colname = NULL;
	List	   *options;
	ListCell   *lc;

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
deparseExpr(Expr *node, deparse_expr_cxt *context)
{
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

	if (node->varno == context->foreignrel->relid &&
		node->varlevelsup == 0)
	{
		/* Var belongs to foreign table */
		griddb_deparse_column_ref(buf, node->varno, node->varattno, context->root);
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
		case TIMESTAMPOID: {
			char timestamp[MAXDATELEN + 1];
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

	/* Explicit datatype conversion is unnecessary */
	if (strcmp(in, "int4") == 0 ||
		strcmp(in, "int8") == 0 ||
		strcmp(in, "float8") == 0)
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
	HeapTuple	proctup;
	Form_pg_proc procform;
	const char *proname;
	bool		use_variadic;
	bool		first;
	ListCell   *arg;

	/*
	 * Normal function: display as proname(args).
	 */
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(node->funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", node->funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	/* Check if need to print VARIADIC (cf. ruleutils.c) */
	use_variadic = node->funcvariadic;

	/* Translate PostgreSQL function into GridDB function */
	proname = griddb_replace_function(NameStr(procform->proname));

	/* Deparse the function name ... */
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
		deparseExpr((Expr *) lfirst(arg), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
	ReleaseSysCache(proctup);
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
	ListCell   *arg;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprkind = form->oprkind;

	/* Sanity check. */
	Assert((oprkind == 'r' && list_length(node->args) == 1) ||
		   (oprkind == 'l' && list_length(node->args) == 1) ||
		   (oprkind == 'b' && list_length(node->args) == 2));

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	if (oprkind == 'r' || oprkind == 'b')
	{
		arg = list_head(node->args);
		deparseExpr((Expr *) lfirst(arg), context);
		appendStringInfoChar(buf, ' ');
	}

	/* Deparse operator name. */
	griddb_deparse_operator_name(buf, form);

	/* Deparse right operand. */
	if (oprkind == 'l' || oprkind == 'b')
	{
		arg = list_tail(node->args);
		appendStringInfoChar(buf, ' ');
		deparseExpr((Expr *) lfirst(arg), context);
	}

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
	deparseExpr((Expr *) linitial(node->args), context);
	appendStringInfoString(buf, " <> ");
	deparseExpr((Expr *) lsecond(node->args), context);
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse given ScalarArrayOpExpr expression.  To avoid problems
 * around priority of operations, we always parenthesize the arguments.
 * GridDB is not support IN.
 * Conditions concatenated by OR will be created.
 * expr IN (c1, c2, c3) => expr == c1 OR expr == c2 OR expr == c3
 * expr NOT IN (c1, c2, c3) => expr <> c1 AND expr <> c2 AND expr <> c3
 */
static void
griddb_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Expr	   *arg1;
	Expr	   *arg2;
	Form_pg_operator form;
	char	   *opname;
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;
	bool		notIn;
	Const	   *c;
	bool		isstr;
	const char *valptr;
	int			i = -1;
	bool		deparseLeft;
	bool		inString;
	bool		isEscape;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);

	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	opname = NameStr(form->oprname);

	notIn = false;
	if (strcmp(opname, "<>") == 0)
		notIn = true;

	arg1 = linitial(node->args);
	arg2 = lsecond(node->args);
	c = (Const *) arg2;
	Assert(nodeTag((Node *) arg2) == T_Const || c->constisnull);

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

	/* Deparse right operand. */
	deparseLeft = true;
	inString = false;
	isEscape = false;

	for (valptr = extval; *valptr; valptr++)
	{
		char		ch = *valptr;

		i++;

		/* Deparse left operand. */
		if (deparseLeft)
		{
			/* No need deparse bool column */
			if (c->consttype == BOOLARRAYOID)
			{
				deparseLeft = false;
				continue;
			}
			deparseExpr(arg1, context);
			if (notIn)
				appendStringInfo(buf, " <> ");
			else
				appendStringInfo(buf, " == ");
			if (isstr)
				appendStringInfoChar(buf, '\'');
			deparseLeft = false;
		}

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
			if (notIn)
				appendStringInfo(buf, "  AND ");
			else
				appendStringInfo(buf, "  OR ");

			/* No need deparse bool column */
			if (c->consttype == BOOLARRAYOID)
			{
				deparseLeft = false;
				continue;
			}

			deparseLeft = true;
			continue;
		}

		/* When compare with timestamp column, need to convert and cast to TIMESTAMP */
		if (c->consttype == TIMESTAMPARRAYOID || c->consttype == TIMESTAMPTZARRAYOID)
		{
			char		timestamp[MAXDATELEN + 1];
			char		chtime[MAXDATELEN + 1] = {0};
			struct tm	tm;
			int 		j = 0;

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
		 * GridDB not support compare bool column with true, false.
		 * Only support column or NOT column
		 */
		if (c->consttype == BOOLARRAYOID)
		{
			appendStringInfoChar(buf, '(');
			if (ch == 'f')
				appendStringInfoString(buf, "NOT ");

			deparseExpr(arg1, context);
			appendStringInfoChar(buf, ')');
			continue;
		}

		appendStringInfoChar(buf, ch);
	}
	if (isstr)
		appendStringInfoChar(buf, '\'');
	ReleaseSysCache(tuple);
}

/*
 * Deparse a RelabelType (binary-compatible cast) node.
 */
static void
griddb_deparse_relabel_type(RelabelType *node, deparse_expr_cxt *context)
{
	deparseExpr(node->arg, context);
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
			deparseExpr((Expr *) linitial(node->args), context);
			appendStringInfoChar(buf, ')');
			return;
	}

	appendStringInfoChar(buf, '(');
	first = true;
	foreach(lc, node->args)
	{
		if (!first)
			appendStringInfo(buf, " %s ", op);
		deparseExpr((Expr *) lfirst(lc), context);
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
	deparseExpr(node->arg, context);

	if (node->nulltesttype == IS_NULL)
		appendStringInfoString(buf, " IS NULL)");
	else
		appendStringInfoString(buf, " IS NOT NULL)");
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
static bool
is_builtin(Oid oid)
{
	return (oid < FirstBootstrapObjectId);
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

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	/* Set up inner_cxt for possible recursion to child nodes */
	inner_cxt.collation = InvalidOid;
	inner_cxt.state = FDW_COLLATE_NONE;

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
				if (var->varno == glob_cxt->foreignrel->relid &&
					var->varlevelsup == 0 && var->varattno > 0)
				{
					/* Var belongs to foreign table */
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

				if (c->consttype == INTERVALOID ||
					c->consttype == BITOID ||
					c->consttype == VARBITOID)
					return false;

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

				/*
				 * If function used by the expression is not built-in, it
				 * can't be sent to remote because it might have incompatible
				 * semantics on remote side.
				 */
				if (!is_builtin(fe->funcid))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) fe->args,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If function's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (fe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 fe->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * Check if GridDB can use the function.
				 */
				if (!is_griddb_func(fe))
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
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
				if (!is_builtin(oe->opno))
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

				/* Check inequality is safe to execute remotely. */
				if (!foreign_inequality_walker(oe))
					return false;

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
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;
				Expr	   *arg2 = (Expr *) lsecond(oe->args);
				Const	   *c = (Const *) lsecond(oe->args);

				/*
				 * Again, only built-in operators can be sent to remote.
				 */
				if (!is_builtin(oe->opno))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt))
					return false;

				/* Support only [NOT] IN (const array) */
				if (nodeTag((Node *) arg2) != T_Const || c->constisnull)
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

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_ArrayExpr:
			{
				/* Array expr is unsupported */
				return false;
			}
			break;
		case T_List:
			{
				List	   *l = (List *) node;
				ListCell   *lc;

				/*
				 * Recurse to component subexpressions.
				 */
				foreach(lc, l)
				{
					if (!foreign_expr_walker((Node *) lfirst(lc),
											 glob_cxt, &inner_cxt))
						return false;
				}

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
	if (check_type && !is_builtin(exprType(node)))
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
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
bool
griddb_is_foreign_expr(PlannerInfo *root,
				RelOptInfo *baserel,
				Expr *expr)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;
	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;
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
	if (contain_mutable_functions((Node *) expr))
		return false;

	/* OK to evaluate on the remote server */
	return true;
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
	char	   *delim = " ";
	RelOptInfo *baserel = context->foreignrel;
	StringInfo	buf = context->buf;

	appendStringInfo(buf, " ORDER BY");
	foreach(lcell, pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lcell);
		Expr	   *em_expr;

		em_expr = griddb_find_em_expr_for_rel(pathkey->pk_eclass, baserel);
		Assert(em_expr != NULL);

		appendStringInfoString(buf, delim);
		deparseExpr(em_expr, context);
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
		if (relid == root->parse->resultRelation &&
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

		if (griddb_is_foreign_expr(root, baserel, ri->clause))
			*remote_conds = lappend(*remote_conds, ri);
		else
			*local_conds = lappend(*local_conds, ri);
	}
}

bool
is_griddb_func(FuncExpr *fe)
{
	HeapTuple	proctup;
	Form_pg_proc procform;
	const char *proname;
	bool		ret = false;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(fe->funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", fe->funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	/* Get PostgreSQL function name */
	proname = NameStr(procform->proname);

	if (strcmp(proname, "char_length") == 0 ||
		strcmp(proname, "concat") == 0 ||
		strcmp(proname, "lower") == 0 ||
		strcmp(proname, "upper") == 0 ||
		strcmp(proname, "substring") == 0 ||
		strcmp(proname, "round") == 0 ||
		strcmp(proname, "ceiling") == 0 ||
		strcmp(proname, "ceil") == 0 ||
		strcmp(proname, "floor") == 0 ||
		strcmp(proname, "int4") == 0 ||
		strcmp(proname, "int8") == 0 ||
		strcmp(proname, "float8") == 0 ||
		strcmp(proname, "now") == 0
		)
		ret = true;

	ReleaseSysCache(proctup);
	return ret;
}
