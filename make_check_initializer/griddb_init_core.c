#include "gridstore.h"
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>

#define STRING_MAX_LENGTH 1000

typedef struct {
  GSContainer *container;
  GSContainerInfo info;
} table_info;

/**
 * Create table info
 * Arguments: GridStore instance, table name, table info, number of column, [ column1_name, column1_type, column1_options, column2_name, column2_type, column2_options,...
 */
void set_tableInfo (GSGridStore *store,
                    const GSChar *tbl_name,
                    table_info *tbl_info,
                    size_t column_count,...)
{
  GSResult ret;
  tbl_info->info = (GSContainerInfo)GS_CONTAINER_INFO_INITIALIZER;
  tbl_info->info.type = GS_CONTAINER_COLLECTION;
  tbl_info->info.name = tbl_name;
  tbl_info->info.columnCount = column_count;
  /* Set column info */
  GSColumnInfo column_info = GS_COLUMN_INFO_INITIALIZER;
  GSColumnInfo *column_info_list = calloc(column_count, sizeof(GSColumnInfo));
  int i;
  va_list valist;
  const GSChar *rowkey;
  va_start(valist, column_count);
  for (i = 0; i < column_count; i++) {
    column_info.name = va_arg(valist, GSChar*);
    if (i == 0) {
      rowkey = column_info.name;
    }
    column_info.type = va_arg(valist, GSType);
    column_info.options = va_arg(valist, GSTypeOption);
    column_info_list[i] = column_info;
  }
  va_end(valist);
  tbl_info->info.columnInfoList = column_info_list;
  tbl_info->info.rowKeyAssigned = GS_TRUE;
  /* Drop the old container if it existed */
  gsDropContainer(store, tbl_info->info.name);
  /* Create a Collection (Delete if schema setting is NULL) */
  ret = gsPutContainerGeneral(store, NULL, &(tbl_info->info), GS_FALSE, &(tbl_info->container));
  if (ret != GS_RESULT_OK) {
    printf("CREATE CONTAINER FAILED %s\n", tbl_name);
  }
  /* Set the autocommit mode to OFF */
  gsSetAutoCommit(tbl_info->container, GS_FALSE);
  /* Set an index on the Row-key Column */
  gsCreateIndex(tbl_info->container, rowkey, GS_INDEX_FLAG_DEFAULT);
}

/**
 * Insert records from TSV file
 * Arguments: GridStore instance, table info, TSV file path
 */
void insertRecordsFromTSV (GSGridStore *store, table_info *tbl_info, char* file_path)
{
  int i;
  // Create array to save a record
  char** record_cols = (char**) malloc(tbl_info->info.columnCount * sizeof(char*));

  for (i = 0; i < tbl_info->info.columnCount; i++) {
    record_cols[i] = (char*) malloc(STRING_MAX_LENGTH * sizeof(char));
  }

  // Open .data file (tab-separated values file)
  char line[STRING_MAX_LENGTH];
  char* data;
  int offset;
  FILE *infile;
  GSRow *row;
  GSResult ret;
  infile = fopen(file_path, "r");

  if (!infile) {
    printf("Couldn't open %s for reading\n", file_path);
    return;
  }

  while(fgets(line, sizeof(line), infile) != NULL) {
    data = line;
    i = 0;
    while (sscanf(data, " %[^\t^\n]%n", record_cols[i], &offset) == 1) {
      data += offset;
      i++;
    }

    /* Prepare data for a Row */
    {
      gsCreateRowByStore(store, &(tbl_info->info), &row);
      for (i = 0; i < tbl_info->info.columnCount; i++) {
        switch (tbl_info->info.columnInfoList[i].type) {
          case GS_TYPE_STRING:
            gsSetRowFieldByString(row, i, record_cols[i]);
            break;
          case GS_TYPE_BOOL:
            gsSetRowFieldByBool(row, i, atoi(record_cols[i]));
            break;
          case GS_TYPE_BYTE:
            gsSetRowFieldByByte(row, i, (int8_t)atoi(record_cols[i]));
            break;
          case GS_TYPE_SHORT:
            gsSetRowFieldByShort(row, i, (int16_t)atoi(record_cols[i]));
            break;
          case GS_TYPE_INTEGER:
            gsSetRowFieldByInteger(row, i, (int32_t)atoi(record_cols[i]));
            break;
          case GS_TYPE_LONG:
            gsSetRowFieldByLong(row, i, atol(record_cols[i]));
            break;
          case GS_TYPE_FLOAT:
            gsSetRowFieldByFloat(row, i, strtof(record_cols[i], NULL));
            break;
          case GS_TYPE_DOUBLE:
            gsSetRowFieldByDouble(row, i, strtod(record_cols[i], NULL));
            break;
          default:
            break;
          // if needed
          // case GS_TYPE_TIMESTAMP:
          //   gsSetRowFieldByTimestamp(row, i, );
          //   break;
          // case GS_TYPE_GEOMETRY:
          //   gsSetRowFieldByGeometry(row, i, );
          //   break;
          // case GS_TYPE_BLOB:
          //   gsSetRowFieldByBlob(row, i, );
          //   break;
        }
      }
    }

    /* Adding row */
    ret = gsPutRow(tbl_info->container, NULL, row, NULL);
    if (ret != GS_RESULT_OK)
    {
      printf("ADDING ROW FAILED\n");
      return;
    }

    gsCloseRow(&row);
  }

  /* Commit the transaction (Release the lock) */
  ret = gsCommit(tbl_info->container);

  return;
}

/**
 * Connect to GridDB cluster and insert data to the database
 * Arguments: IP address, port, cluster name, username, password
 */
int griddb_preparation (const char *addr,
                        const char *port,
                        const char *cluster_name,
                        const char *user,
                        const char *passwd)
{
  static const GSBool update = GS_TRUE;
  GSColumnInfo* columnInfoList;
  GSGridStore *store;
  GSRow *row;
  GSQuery *query;
  GSRowSet *rs;
  GSResult ret;
  int count;
  int32_t id;
  const GSPropertyEntry props[] = {
      {"notificationAddress", addr},
      {"notificationPort", port},
      {"clusterName", cluster_name},
      {"user", user},
      {"password", passwd}};
  const size_t prop_count = sizeof(props) / sizeof(*props);
  /* Create a GridStore instance */
  gsGetGridStore(gsGetDefaultFactory(), props, prop_count, &store);

  table_info FLOAT4_TBL, FLOAT8_TBL, INT4_TBL, INT8_TBL,
             INT2_TBL, test_having, onek, onek2, tenk1, tenk2,
             aggtest, student, person, road, foo, bitwise_test,
             bool_test, minmaxtest, agg_t1, agg_t2, multi_arg_agg,
             VARCHAR_TBL, bytea_test_table, num_data, num_exp_add,
             num_exp_sub, num_exp_div, num_exp_mul, num_exp_sqrt,
             num_exp_ln, num_exp_log10, num_exp_power_10_ln,
             num_result, ceil_floor_round, width_bucket_test,
             num_input_test, J1_TBL, J2_TBL, t11, t21, t31, x, y, t12, t22,
             t32, tt1, tt2, tt3, tt4, tt4x, tt5, tt6, xx, yy, zt1, zt2, zt3,
             a1, b1, a2, b2, c2, nt1, nt2, nt3, TEXT_TBL, a3, b3, c3, d3,
             parent, child, a4, b4, innertab, uniquetbl, join_pt1, fkest,
             fkest1, j11, j21, j31, j12, j22, j32, inserttest01, inserttest,
             update_test, upsert_test;

  set_tableInfo(store, "FLOAT4_TBL", &FLOAT4_TBL,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "f1", GS_TYPE_FLOAT, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "FLOAT8_TBL", &FLOAT8_TBL,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "f1", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "INT4_TBL", &INT4_TBL,
                  1,
                  "f1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL);

  set_tableInfo(store, "INT8_TBL", &INT8_TBL,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "q1", GS_TYPE_LONG, GS_TYPE_OPTION_NULLABLE,
                  "q2", GS_TYPE_LONG, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "INT2_TBL", &INT2_TBL,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "f1", GS_TYPE_SHORT, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "test_having", &test_having,
                  4,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "d", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "onek", &onek,
                  16,
                  "unique1",      GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "unique2",      GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "two",          GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "four",         GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "ten",          GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "twenty",       GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "hundred",      GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "thousand",     GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "twothousand",  GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "fivethous",    GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "tenthous",     GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "odd",          GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "even",         GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "stringu1",     GS_TYPE_STRING,  GS_TYPE_OPTION_NULLABLE,
                  "stringu2",     GS_TYPE_STRING,  GS_TYPE_OPTION_NULLABLE,
                  "string4",      GS_TYPE_STRING,  GS_TYPE_OPTION_NULLABLE);
  
  set_tableInfo(store, "onek2", &onek2,
                  16,
                  "unique1",      GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "unique2",      GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "two",          GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "four",         GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "ten",          GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "twenty",       GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "hundred",      GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "thousand",     GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "twothousand",  GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "fivethous",    GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "tenthous",     GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "odd",          GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "even",         GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "stringu1",     GS_TYPE_STRING,  GS_TYPE_OPTION_NULLABLE,
                  "stringu2",     GS_TYPE_STRING,  GS_TYPE_OPTION_NULLABLE,
                  "string4",      GS_TYPE_STRING,  GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "tenk1", &tenk1,
                  16,
                  "unique1",      GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "unique2",      GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "two",          GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "four",         GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "ten",          GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "twenty",       GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "hundred",      GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "thousand",     GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "twothousand",  GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "fivethous",    GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "tenthous",     GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "odd",          GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "even",         GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "stringu1",     GS_TYPE_STRING,  GS_TYPE_OPTION_NULLABLE,
                  "stringu2",     GS_TYPE_STRING,  GS_TYPE_OPTION_NULLABLE,
                  "string4",      GS_TYPE_STRING,  GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "tenk2", &tenk2,
                  16,
                  "unique1",      GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "unique2",      GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "two",          GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "four",         GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "ten",          GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "twenty",       GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "hundred",      GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "thousand",     GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "twothousand",  GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "fivethous",    GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "tenthous",     GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "odd",          GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "even",         GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "stringu1",     GS_TYPE_STRING,  GS_TYPE_OPTION_NULLABLE,
                  "stringu2",     GS_TYPE_STRING,  GS_TYPE_OPTION_NULLABLE,
                  "string4",      GS_TYPE_STRING,  GS_TYPE_OPTION_NULLABLE);


  set_tableInfo(store, "aggtest", &aggtest,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_SHORT, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_FLOAT, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "student", &student,
                  4,
                  "name", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "age", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "location", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "gpa", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "person", &person,
                  3,
                  "name", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "age", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "location", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "road", &road,
                  2,
                  "name", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "path", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  insertRecordsFromTSV (store, &INT2_TBL, "/tmp/int2.data");
  insertRecordsFromTSV (store, &onek, "/tmp/onek.data");
  insertRecordsFromTSV (store, &onek2, "/tmp/onek.data");
  insertRecordsFromTSV (store, &tenk1, "/tmp/tenk.data");
  insertRecordsFromTSV (store, &tenk2, "/tmp/tenk.data");
  insertRecordsFromTSV (store, &aggtest, "/tmp/agg.data");
  insertRecordsFromTSV (store, &student, "/tmp/student.data");
  insertRecordsFromTSV (store, &person, "/tmp/person.data");
  insertRecordsFromTSV (store, &road, "/tmp/streets.data");

  set_tableInfo(store, "bitwise_test", &bitwise_test,
                  6,
                  "i4", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "i2", GS_TYPE_SHORT, GS_TYPE_OPTION_NULLABLE,
                  "i8", GS_TYPE_LONG, GS_TYPE_OPTION_NULLABLE,
                  "i", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "x", GS_TYPE_SHORT, GS_TYPE_OPTION_NULLABLE,
                  "y", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);


  set_tableInfo(store, "bool_test", &bool_test,
                  5,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b1", GS_TYPE_BOOL, GS_TYPE_OPTION_NULLABLE,
                  "b2", GS_TYPE_BOOL, GS_TYPE_OPTION_NULLABLE,
                  "b3", GS_TYPE_BOOL, GS_TYPE_OPTION_NULLABLE,
                  "b4", GS_TYPE_BOOL, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "minmaxtest", &minmaxtest,
                  1,
                  "f1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL);

  set_tableInfo(store, "agg_t1", &agg_t1,
                  4,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "d", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "agg_t2", &agg_t2,
                  3,
                  "x", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "y", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "z", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "multi_arg_agg", &multi_arg_agg,
                  3,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "VARCHAR_TBL", &VARCHAR_TBL,
                  1,
                  "f1", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL);

  insertRecordsFromTSV (store, &VARCHAR_TBL, "/tmp/varchar_tbl.data");

  set_tableInfo(store, "bytea_test_table", &bytea_test_table,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "v", GS_TYPE_BLOB, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "num_data", &num_data,
                  3,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "val", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "num_exp_add", &num_exp_add,
                  4,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "id2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "expected", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "num_exp_sub", &num_exp_sub,
                  4,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "id2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "expected", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "num_exp_div", &num_exp_div,
                  4,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "id2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "expected", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "num_exp_mul", &num_exp_mul,
                  4,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "id2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "expected", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "num_exp_sqrt", &num_exp_sqrt,
                  3,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "expected", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "num_exp_ln", &num_exp_ln,
                  3,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "expected", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "num_exp_log10", &num_exp_log10,
                  3,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "expected", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "num_exp_power_10_ln", &num_exp_power_10_ln,
                  3,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "expected", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "num_result", &num_result,
                  4,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "id2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "result", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "ceil_floor_round", &ceil_floor_round,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "width_bucket_test", &width_bucket_test,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "operand_num", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE,
                  "operand_f8", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "num_input_test", &num_input_test,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "n1", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "foo", &foo,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "f1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "J1_TBL", &J1_TBL,
                  4,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "i", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "j", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "t", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "J2_TBL", &J2_TBL,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "i", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "k", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "t11", &t11,
                  2,
                  "name", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "n", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "t21", &t21,
                  2,
                  "name", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "n", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "t31", &t31,
                  2,
                  "name", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "n", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "x", &x,
                  2,
                  "x1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "x2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "y", &y,
                  2,
                  "y1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "y2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);


  set_tableInfo(store, "t12", &t12,
                  2,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "t22", &t22,
                  2,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "t32", &t32,
                  2,
                  "x", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "y", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "tt1", &tt1,
                  2,
                  "tt1_id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "joincol", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "tt2", &tt2,
                  2,
                  "tt2_id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "joincol", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "tt3", &tt3,
                  2,
                  "tt1_id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "joincol", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "tt4", &tt4,
                  1,
                  "f1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL);

  set_tableInfo(store, "tt4x", &tt4x,
                  1,
                  "c1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "c2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c3", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "tt5", &tt5,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "f1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "f2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "tt6", &tt6,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "f1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "f2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "xx", &xx,
                  1,
                  "pkxx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL);

  set_tableInfo(store, "yy", &yy,
                  2,
                  "pkyy", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "pkxx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "zt1", &zt1,
                  1,
                  "f1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL);

  set_tableInfo(store, "zt2", &zt2,
                  1,
                  "f2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL);

  set_tableInfo(store, "zt3", &zt3,
                  1,
                  "f3", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL);

  set_tableInfo(store, "a1", &a1,
                  1,
                  "i", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL);


  set_tableInfo(store, "b1", &b1,
                  2,
                  "x", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "y", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "a2", &a2,
                  1,
                  "code", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL);

  set_tableInfo(store, "a2", &a2,
                  1,
                  "code", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL);

  set_tableInfo(store, "b2", &b2,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "num", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL);

  set_tableInfo(store, "c2", &c2,
                  2,
                  "name", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "nt1", &nt1,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a1", GS_TYPE_BOOL, GS_TYPE_OPTION_NULLABLE,
                  "a2", GS_TYPE_BOOL, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "nt2", &nt2,
                  4,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "nt1_id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "b1", GS_TYPE_BOOL, GS_TYPE_OPTION_NULLABLE,
                  "b2", GS_TYPE_BOOL, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "nt3", &nt3,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "nt2_id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c1", GS_TYPE_BOOL, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "TEXT_TBL", &TEXT_TBL,
                  1,
                  "f1", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL);

  insertRecordsFromTSV (store, &TEXT_TBL, "/tmp/text_tbl.data");

  set_tableInfo(store, "a3", &a3,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b_id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "b3", &b3,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "c_id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "c3", &c3,
                  1,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL);

  set_tableInfo(store, "d3", &d3,
                  2,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "parent", &parent,
                  2,
                  "k", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "pd", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "child", &child,
                  2,
                  "k", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "cd", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "a4", &a4,
                  1,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL);

  set_tableInfo(store, "b4", &b4,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a_id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "innertab", &innertab,
                  2,
                  "id", GS_TYPE_LONG, GS_TYPE_OPTION_NOT_NULL,
                  "dat1", GS_TYPE_LONG, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "uniquetbl", &uniquetbl,
                  1,
                  "f1", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL);

  set_tableInfo(store, "join_pt1", &join_pt1,
                  3,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "fkest", &fkest,
                  4,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "fkest1", &fkest1,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "j11", &j11,
                  2,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "j21", &j21,
                  2,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "j31", &j31,
                  2,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "j12", &j12,
                  3,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "id2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "j22", &j22,
                  3,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "id2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "j32", &j32,
                  3,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "id2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "inserttest01", &inserttest01,
                  4,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "col1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "col2", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "col3", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "inserttest", &inserttest,
                  4,
                  "f1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "f2", GS_TYPE_INTEGER_ARRAY, GS_TYPE_OPTION_NULLABLE,
                  "if1", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "if2", GS_TYPE_STRING_ARRAY , GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "update_test", &update_test,
                  4,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "upsert_test", &upsert_test,
                  2,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  table_info part_b_20_b_30, part_b_1_b_10, part_a_10_a_20, part_a_1_a_10,
             part_d_1_15, part_d_15_20, part_c_1_100, list_part1, list_default,
             sub_parted, sub_part1, sub_part2, list_part11, hpart1, hpart2, hpart3, hpart4;

  set_tableInfo(store, "part_b_20_b_30", &part_b_20_b_30,
                  6,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_LONG, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE,
                  "d", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "e", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_b_1_b_10", &part_b_1_b_10,
                  6,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_LONG, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE,
                  "d", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "e", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_a_10_a_20", &part_a_10_a_20,
                  6,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_LONG, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE,
                  "d", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "e", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_a_1_a_10", &part_a_1_a_10,
                  6,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_LONG, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE,
                  "d", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "e", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_d_1_15", &part_d_1_15,
                  6,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_LONG, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE,
                  "d", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "e", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_d_15_20", &part_d_15_20,
                  6,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_LONG, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE,
                  "d", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "e", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_c_1_100", &part_c_1_100,
                  6,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_LONG, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE,
                  "d", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "e", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "list_part1", &list_part1,
                  2,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "list_default", &list_default,
                  2,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "sub_parted", &sub_parted,
                  3,
                  "c", GS_TYPE_LONG, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "a", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "sub_part1", &sub_part1,
                  3,
                  "c", GS_TYPE_LONG, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "a", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "sub_part2", &sub_part2,
                  3,
                  "c", GS_TYPE_LONG, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "a", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "list_part11", &list_part11,
                  3,
                  "c", GS_TYPE_LONG, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "a", GS_TYPE_DOUBLE, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "hpart1", &hpart1,
                  2,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "hpart2", &hpart2,
                  2,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "hpart3", &hpart3,
                  2,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "hpart4", &hpart4,
                  2,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  // join.sql
  table_info join_pt1p2, join_pt1p1p1, simple, bigger_than_it_looks, extremely_skewed,
              wide, join_foo, join_bar;

  set_tableInfo(store, "join_pt1p2", &join_pt1p2,
                  3,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "join_pt1p1p1", &join_pt1p1p1,
                  3,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "simple", &simple,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "t", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "bigger_than_it_looks", &bigger_than_it_looks,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "t", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "extremely_skewed", &extremely_skewed,
                  3,
                  "idx", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "t", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "wide", &wide,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "t", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "join_foo", &join_foo,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "t", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "join_bar", &join_bar,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "t", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  // insert.sql
  table_info part1, part2, part3, part4, part_aa_bb, part_cc_dd, part_null, part_ee_ff1, part_ee_ff2,
             part_xx_yy_p1, part_xx_yy_defpart, part_default_p1, part_default_p2, part_def,
             hpart10, hpart11, hpart12, hpart13, part_default, mcrparted0, mcrparted1, mcrparted2,
             mcrparted3, mcrparted4, mcrparted5, brtrigpartcon1, donothingbrtrig_test1, donothingbrtrig_test2,
             mcrparted1_lt_b, mcrparted2_b, mcrparted3_c_to_common, mcrparted4_common_lt_0,
             mcrparted5_common_0_to_10, mcrparted6_common_ge_10, mcrparted7_gt_common_lt_d, mcrparted8_ge_d;

  set_tableInfo(store, "part1", &part1,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part2", &part2,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part3", &part3,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part4", &part4,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_aa_bb", &part_aa_bb,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_cc_dd", &part_cc_dd,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_null", &part_null,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_ee_ff1", &part_ee_ff1,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_ee_ff2", &part_ee_ff2,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_xx_yy_p1", &part_xx_yy_p1,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_xx_yy_defpart", &part_xx_yy_defpart,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_default_p1", &part_default_p1,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_default_p2", &part_default_p2,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_def", &part_def,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "hpart10", &hpart10,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "hpart11", &hpart11,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "hpart12", &hpart12,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "hpart13", &hpart13,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "part_default", &part_default,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted0", &mcrparted0,
                  4,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted1", &mcrparted1,
                  4,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted2", &mcrparted2,
                  4,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted3", &mcrparted3,
                  4,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted4", &mcrparted4,
                  4,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted5", &mcrparted5,
                  4,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "c", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "brtrigpartcon1", &brtrigpartcon1,
                  3,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE,
                  "b", GS_TYPE_STRING, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "donothingbrtrig_test1", &donothingbrtrig_test1,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "donothingbrtrig_test2", &donothingbrtrig_test2,
                  2,
                  "id", GS_TYPE_INTEGER, GS_TYPE_OPTION_NOT_NULL,
                  "a", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted1_lt_b", &mcrparted1_lt_b,
                  2,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted2_b", &mcrparted2_b,
                  2,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted3_c_to_common", &mcrparted3_c_to_common,
                  2,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted4_common_lt_0", &mcrparted4_common_lt_0,
                  2,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted5_common_0_to_10", &mcrparted5_common_0_to_10,
                  2,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted6_common_ge_10", &mcrparted6_common_ge_10,
                  2,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted7_gt_common_lt_d", &mcrparted7_gt_common_lt_d,
                  2,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  set_tableInfo(store, "mcrparted8_ge_d", &mcrparted8_ge_d,
                  2,
                  "a", GS_TYPE_STRING, GS_TYPE_OPTION_NOT_NULL,
                  "b", GS_TYPE_INTEGER, GS_TYPE_OPTION_NULLABLE);

  /* Release the resource */
  gsCloseGridStore(&store, GS_TRUE);
}

/* Main funtion */
void main(int argc, char *argv[])
{
  griddb_preparation(argv[1], argv[2], argv[3], argv[4], argv[5]);
}