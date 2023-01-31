GridDB Foreign Data Wrapper for PostgreSQL
==========================================

This is a foreign data wrapper (FDW) to connect [PostgreSQL](https://www.postgresql.org/)
to [GridDB](https://github.com/griddb).


Contents
--------

1. [Features](#features)
2. [Supported platforms](#supported-platforms)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Functions](#functions)
6. [Identifier case handling](#identifier-case-handling)
7. [Generated columns](#generated-columns)
8. [Character set handling](#character-set-handling)
9. [Examples](#examples)
10. [Limitations](#limitations)
11. [Contributing](#contributing)
12. [Useful links](#useful-links)
13. [License](#license)

Features
--------
### Common features

- Support `SELECT`. For `SELECT`, you can enable partial execution mode as follows: `SET griddbfdw.enable_partial_execution TO TRUE;`

- Support `INSERT`, `DELETE`, `UPDATE`

For PostgreSQL version 14 or later:
- Support bulk `INSERT` by using `batch_size` option.

- Support list cached connections to foreign servers by using function `griddb_get_connection()`.
- Support discard cached connections to foreign servers by using function `griddb_disconnect('server_name')`, `griddb_disconnect_all()`.
- Support deparse `ANY`/`ALL ARRAY` with the argument is constant.

### Pushdowning

- Support function push down in `WHERE` clause: 
    * Common functions of Postgres and GridDB (same as for PostgreSQL): `char_length`, `concat`, `lower`, `upper`, `substr`, `round`, `ceiling`, `ceil`, `floor`.
    * Unique function of Griddb: `to_timestamp_ms`, `to_epoch_ms`, `array_length`, `element`, `timestamp`, `timestampadd`, `timestampdiff`, `time_next`, `time_next_only`, `time_prev`, `time_prev_only`, `time_interpolated`, `time_sampling`, `max_rows`, `min_rows`, `now`.
    * For the push down of `now()` and `timestamp()` function, please add prefix `griddb_` for these functions.
      Example: `now()` -> `griddb_now()`

- Support `LIMIT...OFFSET` pushdown when having `LIMIT` clause only or both `LIMIT` and `OFFSET`.
- Support `ORDER BY` pushdown with column, except `ORDER BY column ASC NULLS FIRST`, `ORDER BY column DESC NULLS LAST` and `ORDER BY` functions/formulas
- Support aggregation push down:
    * Common functions of Postgres and Griddb are pushed down: `min`, `max`, `count(*)`, `sum`, `avg`, `variance`, `stddev`.
    * Unique function of Griddb are pushed down: `time_avg`<br>
- Support push down `ANY`/`ALL ARRAY` with the argument is constant.

### Notes about features

Supported platforms
-------------------

`griddb_fdw` was developed on Linux, and should run on any
reasonably POSIX-compliant system.

`griddb_fdw` is designed to be compatible with PostgreSQL 10 ~ 15.

`griddb_fdw` is confirmed in GridDB 5.0.0.


Installation
------------

### Source installation

Prerequisites:

GridDB's C client library. This library can be downloaded from the [GridDB](https://github.com/griddb) website on github.

1. Download GridDB's C client and unpack it into `griddb_fdw` directory as griddb.
    Build GridDB's C client
    
    * `gridstore.h` should be in `griddb_fdw/griddb/client/c/include`.
    * `libgridstore.so` should be in `griddb/bin`.

2. Build and install griddb_fdw
    Change into the griddb_fdw source directory.
<pre>
$ make
$ make install
</pre>

If you want to build griddb_fdw in a source tree of PostgreSQL, use

<pre>
$ make NO_PGXS=1
</pre>

Usage
-----

## CREATE SERVER options

`griddb_fdw` accepts the following options via the `CREATE SERVER` command:

- **keep_connections** as *boolean*, optional

  Controls whether griddb_fdw keeps the connections to the foreign server open so that the subsequent queries can re-use them. The default is on. If set to off, all connections to the foreign server will be discarded at the end of transaction. Losed connections will be re-established when they are necessary by future queries using a foreign table.

- **host** as *string*, optional

  GridDB notification address.

- **port** as *interger*, optional

  GridDB notification port.

- **database** as *string*, optional

  GridDB database name. Default is `public` if skipped.

- **clustername** as *boolean*, optional

  GridDB cluster name.
  
- **notification_member** as *string*, optional

  If you want to use fixed list mode instead of multicast mode, you have to specify `notification_member` instead of `host` and `port`:
`notification_member '10.18.18.18:10001'`

- **updatable** as *boolean*, optional

- **batch_size** as *integer*, optional

## CREATE USER MAPPING options

`griddb_fdw` accepts the following options via the `CREATE USER MAPPING`
command:

- **username**

  The griddb username to connect as.

- **password**

  The griddb user's password.


## CREATE FOREIGN TABLE options

`griddb_fdw` accepts the following table-level options via the
`CREATE FOREIGN TABLE` command.

- **table_name** as *string*

- **updatable** as *boolean*, optional

- **use_remote_estimate** as *boolean*, optional

- **batch_size** as *integer*, optional

- **fdw_startup_cost** as *interger*, optional

- **fdw_tuple_cost** as *integer*, optional

- **rowkey** as *string*, optional


The following column-level options are available:

- **column_name** as *string*, optional

- **rowkey** as *boolean*, optional


## IMPORT FOREIGN SCHEMA options

`griddb_fdw` supports [IMPORT FOREIGN SCHEMA](https://www.postgresql.org/docs/current/sql-importforeignschema.html)
(when running with PostgreSQL 9.5 or later) and accepts the following custom options:

- **recreate** as *boolean*, optional

  If 'true', table schema will be updated.


## TRUNCATE support

`griddb_fdw` yet **don't support** the foreign data wrapper `TRUNCATE` API, available
from PostgreSQL 14.

Functions
---------

Functions from this FDW in PostgreSQL catalog are **yet not described**.


Identifier case handling
------------------------

PostgreSQL folds identifiers to lower case by default.
Rules and problems with griddb identifiers **yet not tested and described**.

Generated columns
-----------------

Behavoiur within generated columns **yet not tested and described**. 

For more details on generated columns see:

- [Generated Columns](https://www.postgresql.org/docs/current/ddl-generated-columns.html)
- [CREATE FOREIGN TABLE](https://www.postgresql.org/docs/current/sql-createforeigntable.html)


Character set handling
----------------------

**Yet not described**.

Examples
--------

Install the extension:
```sql
	CREATE EXTENSION griddb_fdw;
```

Create a foreign server with appropriate configuration:

```sql
	CREATE SERVER griddb_svr
	FOREIGN DATA WRAPPER griddb_fdw
	OPTIONS(
	  host '239.0.0.1',
	  port '31999',
	  clustername 'ktymCluster',
	  database 'public'
	  );
```
Create an appropriate user mapping:
```sql
    	CREATE USER MAPPING
	FOR CURRENT_USER
	SERVER griddb_svr 
    	OPTIONS (
	  username 'username',
	  password 'password'
	  );
```
Create a foreign table referencing the griddb table `fdw_test`:
```sql
	CREATE FOREIGN TABLE ft1 (
	  c1 text,
	  c2 float,
	  c3 integer
	  )
	SERVER griddb_svr;
```
Query the foreign table.
```sql
	SELECT * FROM ft1;
```
The **container must have rowkey on GridDB in order to execute update and delete query**.	

Import a griddb schema:

```sql
	IMPORT FOREIGN SCHEMA public
	FROM SERVER griddb_svr
	INTO public
	OPTIONS (
	 recreate 'true'
	 );
```

After schema is imported, we can access tables.
To use `CREATE FOREIGN TABLE` is not recommended.

Limitations
-----------
### SQL commands
#### Record is updated by `INSERT` command if a record with same rowkey as new record exists in GridDB.
In this case, `griddb_fdw` raises the warning.

```sql
INSERT INTO ft1 VALUES(100, 'AAA');
INSERT INTO ft1 VALUES(100, 'BBB'); -- Same as "UPDATE ft1 SET b = 'BBB' WHERE a = 100;"
```
#### Don't support ON CONFLICT.
PostgreSQL has upsert (update or insert) feature. When inserting a new row into the table, PostgreSQL will update the row if it already exists, otherwise, PostgreSQL inserts the new row.
However, `griddb_fdw` does not support the upsert feature now. For example, it shows an error message if user executes the following query.

```sql
INSERT INTO ft1(c1, c2) VALUES(11, 12) ON CONFLICT DO NOTHING;
```

#### Don't support `RETURNING`.
Returning is way to obtain data if rows are modified by `INSERT`, `UPDATE`, and `DELETE` commands. `griddb_fdw` does not support this feature. 

```sql
INSERT INTO ft1 (c0, c1) VALUES (1, 2) RETURNING c0, c1;
```

#### Don't support DIRECT MODIFICATION
#### Don't support `IMPORT FOREIGN SCHEMA` with option `import_generated` or `LIMIT TO` clause
#### Don't support `SAVEPOINT`. 
Savepoint does not work. **Warning is returned**.

### Other

#### Do not push down `ANY`/`ALL` `ARRAY` with the argument is subquery. 

#### GridDB does not support `numeric` data type as PostgreSQL.
Therefore, it can not store numbers with too high precision and scale.

#### Limitations related in rowkey-column attribute.
GridDB can set a rowkey attribute to the 1st column.
`griddb_fdw` uses it for identifying a record.
- It is required that a container have the rowkey attribute for executing `UPDATE` or `DELETE`.
- It is not supported to update a rowkey-column.


#### Don't support the query execution which is satisfied with all of following conditions:  
- It requires to get record locks in a transaction.
- Records are locked by the other foreign table which links the same container in GridDB.

If such query is executed, it is no response.

Example1:
Foreign table `ft1` and `ft2` are linked to same container in GridDB.

```sql
BEGIN;
SELECT * FROM ft1 FOR UPDATE;
SELECT * FROM ft2 FOR UPDATE; -- No response
```
Example2:

```sql
BEGIN;
SELECT * FROM ft1, ft2 WHERE ft1.a = ft2.a FOR UPDATE; -- No response
```
This is because GridDB manages a transaction by container unit and griddb_fdw creates GSContainer instances for each foreign tables even if the container is same in GridDB.

#### Don't support an arbitrary column mapping.
`griddb_fdw` is assumed that a column structure on PostgreSQL is same as that of griddb.
It is recommended to create a schema on PostgreSQL by `IMPORT FOREIGN SCHEMA`.
`griddb_fdw` might return an error when DML is executed.

For example, container on GridDB has 3 columns. The 1st column is `"c1"` as integer, the 2nd is `"c2"` as float, and the 3rd is `"c3"` as text.
Schema must be created as

```sql
	CREATE FOREIGN TABLE ft1 (
	  c1 integer,
	  c2 float,
	  c3 text 
	  )
	SERVER griddb_svr;
```

You should not execute following queries.

Types are not match.

```sql
	CREATE FOREIGN TABLE ft1 (
	  c1 text,
	  c2 float,
	  c3 integer
	  )
	SERVER griddb_svr;
```
There is unknown column.
Even if unknown column is dropped, griddb cannot access `ft1` correctly.

```sql
	CREATE FOREIGN TABLE ft1 (
	  c0 integer,
	  c1 integer,
	  c2 float,
	  c3 text
	  )
	SERVER griddb_svr;
	
	ALTER FOREIGN TABLE ft1 DROP COLUMN c0;
```

Contributing
------------
Opening issues and pull requests on GitHub are welcome.

Useful links
------------

### General FDW Documentation

 - https://www.postgresql.org/docs/current/ddl-foreign-data.html
 - https://www.postgresql.org/docs/current/sql-createforeigndatawrapper.html
 - https://www.postgresql.org/docs/current/sql-createforeigntable.html
 - https://www.postgresql.org/docs/current/sql-importforeignschema.html
 - https://www.postgresql.org/docs/current/fdwhandler.html
 - https://www.postgresql.org/docs/current/postgres-fdw.html

### Other FDWs

 - https://wiki.postgresql.org/wiki/Fdw
 - https://pgxn.org/tag/fdw/
 
License
-------
Copyright (c) 2018, TOSHIBA CORPORATION
Copyright (c) 2011-2016, EnterpriseDB Corporation

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement is
hereby granted, provided that the above copyright notice and this paragraph and
the following two paragraphs appear in all copies.

See the [`LICENSE`][1] file for full details.

[1]: LICENSE.md
