/*
 * GridDB Foreign Data Wrapper
 *
 * Portions Copyright (c) 2019, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  store.c
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

/*
 * griddb_update_rows_init
 *		Initialize area for storing information of modified rows.
 *		Size is INITIAL_TARGET_VALUE_ROWS rows.
 */
void
griddb_modify_target_init(GridDBFdwModifiedRows * modified_rows, int attnum)
{
	int			i;
	int			nField = attnum + 1;	/* +1 is key column. */

	Assert(attnum > 0);

	modified_rows->target_values = (Datum **) palloc0(sizeof(Datum *) * INITIAL_TARGET_VALUE_ROWS);
	for (i = 0; i < INITIAL_TARGET_VALUE_ROWS; i++)
		modified_rows->target_values[i] = (Datum *) palloc0(sizeof(Datum) * nField);

	modified_rows->field_num = nField;
	modified_rows->num_target = 0;
	modified_rows->max_target = INITIAL_TARGET_VALUE_ROWS;
}

/*
 * griddb_modify_target_expand
 *		Expand the area for storing information of modified rows.
 *		Size is increased to double.
 */
void
griddb_modify_target_expand(GridDBFdwModifiedRows * modified_rows)
{
	int			i;

	Assert(modified_rows->field_num > 0);
	modified_rows->target_values = (Datum **) repalloc(modified_rows->target_values, sizeof(Datum *) * modified_rows->max_target * 2);
	for (i = modified_rows->max_target; i < modified_rows->max_target * 2; i++)
		modified_rows->target_values[i] = (Datum *) palloc0(sizeof(Datum) * modified_rows->field_num);
	modified_rows->max_target *= 2;
}

/*
 * griddb_modify_target_fini
 *		Free memory of modified rows information.
 */
void
griddb_modify_target_fini(GridDBFdwModifiedRows * modified_rows)
{
	int			i;

	for (i = 0; i < modified_rows->max_target; i++)
		pfree(modified_rows->target_values[i]);
	pfree(modified_rows->target_values);
	memset(modified_rows, 0, sizeof(GridDBFdwModifiedRows));
}

/*
 * griddb_modify_target_insert
 *		Store values of updated/deleted row information.
 *		Values are stored in an array.
 *		1st element is key column.
 */
void
griddb_modify_target_insert(GridDBFdwModifiedRows * modified_rows, TupleTableSlot *slot,
							TupleTableSlot *planSlot, AttrNumber junk_att_no,
							List *target_attrs, GridDBFdwFieldInfo * field_info)
{
	ListCell   *lc;
	Datum	   *target_values;
	int			pindex = 0;
	Datum		value;
	bool		isnull;
	Form_pg_attribute attr;

	/* Expand the space if necessary. */
	if (modified_rows->num_target >= modified_rows->max_target)
		griddb_modify_target_expand(modified_rows);

	target_values = modified_rows->target_values[modified_rows->num_target];

	/* Firstly, store a value of rowkey column which is the 1st column. */
	value = ExecGetJunkAttribute(planSlot, junk_att_no, &isnull);
	Assert(isnull == false);
	attr = TupleDescAttr(planSlot->tts_tupleDescriptor, junk_att_no - 1);
	target_values[pindex] = datumCopy(value, attr->attbyval, attr->attlen);
	pindex++;

	/* Store modified column values. */
	foreach(lc, target_attrs)
	{
		int			attnum = lfirst_int(lc);

		if (attnum < 0)
			continue;

		griddb_check_slot_type(slot, attnum, field_info);

		value = slot_getattr(slot, attnum, &isnull);
		Assert(isnull == false);
		attr = TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1);
		target_values[pindex] = datumCopy(value, attr->attbyval, attr->attlen);
		pindex++;
	}
	modified_rows->num_target++;
}

/*
 * target_values are stored in 2 dimentional array.
 * 1st column is storing row key value.
 * This function sorts target_values about row key value by pg_qsort().
 * The comparator functions compare records which are rows in target_values about 1st element.
 *
 */
void
griddb_modify_target_sort(GridDBFdwModifiedRows * modified_rows, GridDBFdwFieldInfo * field_info)
{
	int			(*comparator) (const void *, const void *);
	GSType		gs_type = field_info->column_types[ROWKEY_ATTNO - 1];

	comparator = griddb_get_comparator(gs_type);
	pg_qsort(modified_rows->target_values, modified_rows->num_target, sizeof(Datum *), comparator);
}

/* Create TQL for fetching UPDATE/DELETE targets. */
static void
griddb_modify_target_tql(GridDBFdwModifiedRows * modified_rows, char *cont_name,
						 GridDBFdwFieldInfo * field_info, uint64_t iStart, uint64 iEnd,
						 StringInfo buf)
{
	char	   *rowkey = (char *) field_info->column_names[ROWKEY_ATTNO - 1];
	Datum	  **target_values = modified_rows->target_values;
	Oid			outputFunctionId;
	bool		typeVarLength;
	uint64_t	i;

	Assert(iStart < iEnd);
	Assert(iEnd <= modified_rows->num_target);

	/* Construct SELECT statement for fetching update targets. */
	appendStringInfo(buf, "SELECT * FROM %s WHERE ", cont_name);

	switch (field_info->column_types[ROWKEY_ATTNO - 1])
	{
		case GS_TYPE_INTEGER:
			for (i = iStart; i < iEnd; i++)
			{
				Datum	   *values = target_values[i];
				int			intVal = DatumGetInt32(values[ROWKEY_IDX]);

				if (i != 0)
					appendStringInfo(buf, " OR ");

				appendStringInfo(buf, "%s = %d", rowkey, intVal);
			}
			break;

		case GS_TYPE_LONG:
			for (i = iStart; i < iEnd; i++)
			{
				Datum	   *values = target_values[i];
				int64		longVal = DatumGetInt64(values[ROWKEY_IDX]);

				if (i != 0)
					appendStringInfo(buf, " OR ");

				appendStringInfo(buf, "%s = %lld", rowkey, (long long int) longVal);
			}
			break;

		case GS_TYPE_TIMESTAMP:
			for (i = iStart; i < iEnd; i++)
			{
				Datum	   *values = target_values[i];
				char		timestampVal[MAXDATELEN + 1] = {0};
				Timestamp	timestamp = DatumGetTimestamp(values[ROWKEY_IDX]);

				griddb_convert_pg2gs_timestamp_string(timestamp, timestampVal);

				if (i != 0)
					appendStringInfo(buf, " OR ");

				appendStringInfo(buf, "%s = TIMESTAMP('%s')", rowkey, timestampVal);
			}
			break;

		case GS_TYPE_STRING:
			getTypeOutputInfo(TEXTOID, &outputFunctionId, &typeVarLength);
			for (i = iStart; i < iEnd; i++)
			{
				Datum	   *values = target_values[i];
				char	   *stringVal = OidOutputFunctionCall(outputFunctionId, values[ROWKEY_IDX]);

				if (i != 0)
					appendStringInfo(buf, " OR ");

				appendStringInfo(buf, "%s = '%s'", rowkey, stringVal);
			}
			break;

		default:
			/* Should not happen. */
			Assert(false);
			break;				/* keep compiler quiet */
	}

	appendStringInfo(buf, " ORDER BY %s", rowkey);
}

/*
 * Operate UPDATE or DELETE to GridDB.
 * The record information is stored modified_rows->target_values.
 * Firstly, fetch update/delete target rows from GridDB.
 * Then modify result set in case of UPDATE.
 * And update or delete the current row which cursor is pointing.
 */
void
griddb_modify_targets_apply(GridDBFdwModifiedRows * modified_rows, char *cont_name,
							GSContainer * cont, List *target_attrs,
							GridDBFdwFieldInfo * field_info, Oid pgkeytype,
							CmdType operation)
{
	StringInfoData buf;
	GSResult	ret;
	uint64		iStart = 0;
	GSRow	   *row;
	int			(*comparator) (const void *, const void *) = griddb_get_comparator(field_info->column_types[ROWKEY_ATTNO - 1]);

	if (modified_rows->num_target == 0)
		return;

	initStringInfo(&buf);

	ret = gsCreateRowByContainer(cont, &row);
	if (!GS_SUCCEEDED(ret))
		griddb_REPORT_ERROR(ERROR, ret, cont);

	while (iStart < modified_rows->num_target)
	{
		GSQuery    *query;
		GSRowSet   *row_set;
		uint64		i;
		uint64_t	iEnd = Min(modified_rows->num_target, iStart + BULK_ROWS_COUNT);

		/* Create TQL */
		resetStringInfo(&buf);
		griddb_modify_target_tql(modified_rows, cont_name, field_info, iStart, iEnd, &buf);
		elog(DEBUG1, "TQL for modification: %s", buf.data);

		/* Execute TQL */
		ret = gsQuery(cont, buf.data, &query);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, cont);

		/* Fetch result set for modification */
		ret = gsFetch(query, GS_TRUE, &row_set);
		if (!GS_SUCCEEDED(ret))
			griddb_REPORT_ERROR(ERROR, ret, query);

		for (i = iStart; i < iEnd; i++)
		{
			Datum		rowkey;
			Datum	   *rowValues[1];
			Datum	   *targets = modified_rows->target_values[i];

			Assert(gsHasNextRow(row_set) == GS_TRUE);
			ret = gsGetNextRow(row_set, row);
			if (!GS_SUCCEEDED(ret))
				griddb_REPORT_ERROR(ERROR, ret, row_set);

			/* Check the cursor is pointing the target. */
			rowkey = griddb_make_datum_from_row(row, ROWKEY_IDX,
												field_info->column_types[ROWKEY_ATTNO - 1],
												pgkeytype);
			rowValues[0] = &rowkey;
			if (comparator((const void *) &targets, (const void *) &rowValues))
			{
				Assert(false);
				ereport(WARNING, (errmsg("Fetched rowkey is not same as expected")));
				continue;
			}

			/* Do operation. */
			if (operation == CMD_UPDATE)
			{
				int			pindex = ROWKEY_IDX + 1;
				ListCell   *lc;

				foreach(lc, target_attrs)
				{
					int			attnum = lfirst_int(lc);

					if (attnum < 0)
						continue;

					griddb_set_row_field(row, targets[pindex], field_info->column_types[attnum - 1], attnum - 1);
					pindex++;
				}

				/* Do UPDATE */
				ret = gsUpdateCurrentRow(row_set, row);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row_set);
			}
			else
			{
				/* Do DELETE */
				ret = gsDeleteCurrentRow(row_set);
				if (!GS_SUCCEEDED(ret))
					griddb_REPORT_ERROR(ERROR, ret, row_set);
			}
		}

		gsCloseRowSet(&row_set);
		gsCloseQuery(&query);

		/* For the next loop. */
		iStart = iEnd;
	}

	gsCloseRow(&row);
}
