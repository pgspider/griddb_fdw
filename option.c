/*
 * GridDB Foreign Data Wrapper
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  option.c
 *
 */

#include "postgres.h"

#include "griddb_fdw.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"

/*
 * Describes the valid options for objects that use this wrapper.
 */
typedef struct GridDBFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
}			GridDBFdwOption;


/*
 * Valid options for mysql_fdw.
 *
 */
static GridDBFdwOption griddb_options[] =
{
	/* Connection options */
	{
		OPTION_HOST, ForeignServerRelationId
	},
	{
		OPTION_PORT, ForeignServerRelationId
	},
	{
		OPTION_CLUSTER, ForeignServerRelationId
	},
	{
		OPTION_DATABASE, ForeignServerRelationId
	},
	{
		OPTION_MEMBER, ForeignServerRelationId
	},
	{
		OPTION_USER, UserMappingRelationId
	},
	{
		OPTION_PWD, UserMappingRelationId
	},
	{
		OPTION_TABLE, ForeignTableRelationId
	},
	{
		OPTION_COLUMN, AttributeRelationId
	},
	{
		OPTION_ROWKEY, AttributeRelationId
	},
	/* updatable is available on both server and table */
	{
		OPTION_UPDATABLE, ForeignServerRelationId
	},
	{
		OPTION_UPDATABLE, ForeignTableRelationId
	},
	/* cost factors */
	{
		OPTION_STARTUP_COST, ForeignServerRelationId
	},
	{
		OPTION_TUPLE_COST, ForeignServerRelationId
	},
	/* batch_size available on both server and table */
	{
		OPTION_BATCH_SIZE, ForeignTableRelationId
	},
	{
		OPTION_BATCH_SIZE, ForeignServerRelationId
	},
	/* keep connection */
	{
		OPTION_KEEP_CONN, ForeignServerRelationId
	},
	/* Sentinel */
	{
		NULL, InvalidOid
	}
};

extern Datum griddb_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(griddb_fdw_validator);


/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses griddb_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
griddb_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;

	/*
	 * Check that only options supported by griddb_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!griddb_is_valid_option(def->defname, catalog))
		{
			struct GridDBFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = griddb_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s", buf.len ? buf.data : "<none>")
					 ));
		}

		/*
		 * Validate option value, when we can do so without any context.
		 */
		if (strcmp(def->defname, OPTION_KEEP_CONN) == 0)
		{
			/* these accept only boolean values */
			(void) defGetBoolean(def);
		}

		if (strcmp(def->defname, OPTION_BATCH_SIZE) == 0)
		{
			int			fetch_size;

			fetch_size = strtol(defGetString(def), NULL, 10);
			if (fetch_size <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("%s requires a non-negative integer value",
								def->defname)));
		}
	}
	PG_RETURN_VOID();
}


/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
bool
griddb_is_valid_option(const char *option, Oid context)
{
	struct GridDBFdwOption *opt;

	for (opt = griddb_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a griddb_fdw foreign table.
 */
griddb_opt *
griddb_get_options(Oid foreignoid)
{
	ForeignTable *f_table = NULL;
	ForeignServer *f_server = NULL;
	UserMapping *f_mapping;
	List	   *options;
	ListCell   *lc;
	griddb_opt *opt;

	opt = (griddb_opt *) palloc(sizeof(griddb_opt));
	memset(opt, 0, sizeof(griddb_opt));

	/*
	 * Extract options from FDW objects.
	 */
	PG_TRY();
	{
		f_table = GetForeignTable(foreignoid);
		f_server = GetForeignServer(f_table->serverid);
	}
	PG_CATCH();
	{
		f_table = NULL;
		f_server = GetForeignServer(foreignoid);
	}
	PG_END_TRY();

	f_mapping = GetUserMapping(GetUserId(), f_server->serverid);

	options = NIL;
	if (f_table)
		options = list_concat(options, f_table->options);
	options = list_concat(options, f_server->options);
	options = list_concat(options, f_mapping->options);

	opt->use_remote_estimate = false;
	opt->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	opt->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;

	/*
	 * By default, all the connections to any foreign servers are kept open.
	 */
	opt->keep_connections = true;

	/* Loop through the options, and get the server/port */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, OPTION_HOST) == 0)
			opt->svr_address = defGetString(def);

		if (strcmp(def->defname, OPTION_PORT) == 0)
			opt->svr_port = defGetString(def);

		if (strcmp(def->defname, OPTION_USER) == 0)
			opt->svr_username = defGetString(def);

		if (strcmp(def->defname, OPTION_PWD) == 0)
			opt->svr_password = defGetString(def);

		if (strcmp(def->defname, OPTION_CLUSTER) == 0)
			opt->svr_clustername = defGetString(def);

		if (strcmp(def->defname, OPTION_DATABASE) == 0)
			opt->svr_database = defGetString(def);

		if (strcmp(def->defname, OPTION_MEMBER) == 0)
			opt->svr_notification_member = defGetString(def);

		if (strcmp(def->defname, OPTION_REMOTE_ESTIMATE) == 0)
			opt->use_remote_estimate = defGetBoolean(def);

		if (strcmp(def->defname, OPTION_STARTUP_COST) == 0)
			opt->use_remote_estimate = strtod(defGetString(def), NULL);

		if (strcmp(def->defname, OPTION_TUPLE_COST) == 0)
			opt->use_remote_estimate = strtod(defGetString(def), NULL);

		if (strcmp(def->defname, OPTION_KEEP_CONN) == 0)
			opt->keep_connections = defGetBoolean(def);
	}
	/* Default values, if required */
	if (!opt->svr_address)
		opt->svr_address = DEFAULT_GRIDDB_IP_ADDRESS;

	if (!opt->svr_port)
		opt->svr_port = DEFAULT_GRIDDB_PORT;

	return opt;
}
