/*
 * GridDB Foreign Data Wrapper
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  compare.c
 *
 */
#include "postgres.h"

#include "griddb_fdw.h"

#include "executor/executor.h"
#include "executor/tuptable.h"
#include "utils/datetime.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "catalog/pg_type.h"

#define COMPARE_RETURN(a,b) \
    do{ \
	    if (a == b) \
		    return 0; \
	    else if (a > b) \
		    return 1; \
	    else \
		    return -1; \
    } while(0)

/*
 * Comparison functions for pg_qsort.
 * Arguments are a pointer of tuple. The tuple is an arrary of Datum.
 * They are compared by the 1st column.
 *   | TupleA[0] | TupleA[1] | ... | TupleA[N-1] |
 *   | TupleB[0] | TupleB[1] | ... | TupleB[N-1] |
 * Retuen 0 if TupleA[0] and TupleB[0] are same.
 * Retuen 1 if TupleA[0] is greater than TupleB[0].
 * Retuen -1 if TupleA[0] is less than TupleB[0].
 * Tuple[1] ... Tuple[N-1] are ignored for the comparison.
 */

static int
griddb_compare_tuplekey_string(const void *a, const void *b)
{
	Datum	   *val1 = *(Datum **) a;
	Datum	   *val2 = *(Datum **) b;
	char	   *textVal1;
	char	   *textVal2;
	Oid			outputFunctionId;
	bool		typeVarLength;

	getTypeOutputInfo(TEXTOID, &outputFunctionId, &typeVarLength);
	textVal1 = OidOutputFunctionCall(outputFunctionId, val1[0]);
	textVal2 = OidOutputFunctionCall(outputFunctionId, val2[0]);

	return strcmp((const char *) textVal1, (const char *) textVal2);
}

static int
griddb_compare_tuplekey_integer(const void *a, const void *b)
{
	Datum	   *val1 = *(Datum **) a;
	Datum	   *val2 = *(Datum **) b;
	int32		intVal1 = DatumGetInt32(val1[0]);
	int32		intVal2 = DatumGetInt32(val2[0]);

	COMPARE_RETURN(intVal1, intVal2);
}

static int
griddb_compare_tuplekey_long(const void *a, const void *b)
{
	Datum	   *val1 = *(Datum **) a;
	Datum	   *val2 = *(Datum **) b;
	int64		longVal1 = DatumGetInt64(val1[0]);
	int64		longVal2 = DatumGetInt64(val2[0]);

	COMPARE_RETURN(longVal1, longVal2);
}

static int
griddb_compare_tuplekey_timestamp(const void *a, const void *b)
{
	Datum	   *val1 = *(Datum **) a;
	Datum	   *val2 = *(Datum **) b;
	Timestamp	timestamp1 = DatumGetTimestamp(val1[0]);
	Timestamp	timestamp2 = DatumGetTimestamp(val2[0]);

	COMPARE_RETURN(timestamp1, timestamp2);
}

/* Return comparator based on gs_type for pg_qsort used in griddb_fdw. */
int			(*
			 griddb_get_comparator_tuplekey(GSType gs_type)) (const void *, const void *)
{
	switch (gs_type)
	{
		case GS_TYPE_STRING:
			return griddb_compare_tuplekey_string;

		case GS_TYPE_INTEGER:
			return griddb_compare_tuplekey_integer;

		case GS_TYPE_LONG:
			return griddb_compare_tuplekey_long;

		case GS_TYPE_TIMESTAMP:
			return griddb_compare_tuplekey_timestamp;

		default:

			/*
			 * Should not happen, we have already checked rowkey is assigned.
			 * GridDB support rowkey for column of only GS_TYPE_STRING,
			 * GS_TYPE_INTEGER, GS_TYPE_LONG and GS_TYPE_TIMESTAMP type.
			 */
			elog(ERROR, "Cannot compare rowkey type(GS) %d", gs_type);
			return NULL;		/* keep compiler quiet */
	}
}

/*
 * Comparison functions for hash key.
 * Arguments are a pointer of Datum.
 */

static int
griddb_compare_datum_string(const void *a, const void *b)
{
	Datum		val1 = *(Datum *) a;
	Datum		val2 = *(Datum *) b;
	char	   *textVal1;
	char	   *textVal2;
	Oid			outputFunctionId;
	bool		typeVarLength;

	getTypeOutputInfo(TEXTOID, &outputFunctionId, &typeVarLength);
	textVal1 = OidOutputFunctionCall(outputFunctionId, val1);
	textVal2 = OidOutputFunctionCall(outputFunctionId, val2);

	return strcmp((const char *) textVal1, (const char *) textVal2);
}

static int
griddb_compare_datum_integer(const void *a, const void *b)
{
	Datum		val1 = *(Datum *) a;
	Datum		val2 = *(Datum *) b;
	int32		intVal1 = DatumGetInt32(val1);
	int32		intVal2 = DatumGetInt32(val2);

	COMPARE_RETURN(intVal1, intVal2);
}

static int
griddb_compare_datum_long(const void *a, const void *b)
{
	Datum		val1 = *(Datum *) a;
	Datum		val2 = *(Datum *) b;
	int64		longVal1 = DatumGetInt64(val1);
	int64		longVal2 = DatumGetInt64(val2);

	COMPARE_RETURN(longVal1, longVal2);
}

static int
griddb_compare_datum_timestamp(const void *a, const void *b)
{
	Datum		val1 = *(Datum *) a;
	Datum		val2 = *(Datum *) b;
	Timestamp	timestamp1 = DatumGetTimestamp(val1);
	Timestamp	timestamp2 = DatumGetTimestamp(val2);

	COMPARE_RETURN(timestamp1, timestamp2);
}

/* Return comparator based on gs_type for hash used in griddb_fdw. */
int			(*
			 griddb_get_comparator_datum(GSType gs_type)) (const void *, const void *)
{
	switch (gs_type)
	{
		case GS_TYPE_STRING:
			return griddb_compare_datum_string;

		case GS_TYPE_INTEGER:
			return griddb_compare_datum_integer;

		case GS_TYPE_LONG:
			return griddb_compare_datum_long;

		case GS_TYPE_TIMESTAMP:
			return griddb_compare_datum_timestamp;

		default:

			/*
			 * Should not happen, we have already checked rowkey is assigned.
			 * GridDB support rowkey for column of only GS_TYPE_STRING,
			 * GS_TYPE_INTEGER, GS_TYPE_LONG and GS_TYPE_TIMESTAMP type.
			 */
			elog(ERROR, "Cannot compare rowkey type(GS) %d", gs_type);
			return NULL;		/* keep compiler quiet */
	}
}
