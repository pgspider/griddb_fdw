# GridDB Foreign Data Wrapper for PostgreSQL

This PostgreSQL extension is a Foreign Data Wrapper (FDW) for [GridDB][1].  
This version of griddb_fdw can work for PostgreSQL 10, 11, 12, 13 and 14. It is confirmed in GridDB 4.6.1.

## 1. Installation
griddb_fdw requires GridDB's C client library. This library can be downloaded from the [GridDB][1] website on github[1].

1. Preapre GridDB's C client
    Download GridDB's C client and unpack it into griddb_fdw directory as griddb.  
    Build GridDB's C client  
    -> gridstore.h should be in griddb_fdw/griddb/client/c/include.  
    -> libgridstore.so should be in griddb/bin.  

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

## 2. Usage
### load extension
```
CREATE EXTENSION griddb_fdw;
```
### create server object
```
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS(host '239.0.0.1',  
port '31999', clustername 'ktymCluster', database 'public');
```

We have to specify the following parameters for a [GridDB][1] foreign server:

host : GridDB notification addres.  
port : GridDB notification port  
clustername : GridDB cluster name  
database : GridDB database name (can be skipped if you want to connect `public` database) 

If you want to use fixed list mode instead of multicast mode, you have to specify `notification_member` instead of `host` and `port`:
```
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS(notification_member '10.18.18.164:10001',
clustername 'griddbfdwTestCluster');
```

### create user mapping
```
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS(username 'admin', password 'testadmin');
```

We have to specify the following parameters for a user mapping:  
username : GridDB username  
password : GridDB password  

### import schema
```
IMPORT FOREIGN SCHEMA griddb_schema FROM SERVER griddb_svr INTO public;
```

We can use the following parameters for import schema:  
recreate : 'true' or 'false'. If 'true', table schema will be updated.  

After schema is imported, we can access tables.
To use CREATE FOREIGN TABLE is not recommended.

### update and delete

The container must have rowkey on [GridDB][1] in order to execute update and delete query.


## 3. Features
- Support SELECT

For SELECT, you can enable partial execution mode as follows:

```
SET griddbfdw.enable_partial_execution TO TRUE;
```

- Support INSERT, DELETE, UPDATE
- Support function push down in WHERE clause: 
    * Common functions of Postgres and GridDB (same as for PostgreSQL): char_length, concat, lower, upper, substr, round, ceiling, ceil, floor.
    * Unique function of Griddb: to_timestamp_ms, to_epoch_ms, array_length, element, timestamp, timestampadd, timestampdiff,       
                                 time_next, time_next_only, time_prev, time_prev_only, time_interpolated, time_sampling, max_rows, min_rows, now.
    * For the push down of now() and timestamp() function, please add prefix `griddb_` for these functions.     
      Example: now() -> griddb_now()

- Support LIMIT...OFFSET pushdown when having LIMIT clause only or both LIMIT and OFFSET.
- Support ORDER BY pushdown with column, except ORDER BY column ASC NULLS FIRST, ORDER BY column DESC NULLS LAST and ORDER BY functions/formulas
- Support aggregation push down:
    * Common functions of Postgres and Griddb are pushed down: min, max, count(*), sum, avg, variance, stddev.
    * Unique function of Griddb are pushed down: time_avg<br>

For PostgreSQL version 14 or later:
- Support bulk INSERT by using batch_size option.
- Support keep connections open by use keep_connections option:
    * keep_connections option that controls whether griddb_fdw keeps the connections to the foreign server open so that the subsequent queries can re-use them. This option can only be specified for a foreign server. The default is on. If set to off, all connections to the foreign server will be discarded at the end of transaction. losed connections will be re-established when they are necessary by future queries using a foreign table.
- Support list cached connections to foreign servers by using function griddb_get_connection().
- Support discard cached connections to foreign servers by using function griddb_disconnect('server_name'), griddb_disconnect_all().

## 4. Limitations
#### Record is updated by INSERT command if a record with same rowkey as new record exists in GridDB.
In this case, griddb_fdw raises the warning.

```
INSERT INTO ft1 VALUES(100, 'AAA');
INSERT INTO ft1 VALUES(100, 'BBB'); -- Same as "UPDATE ft1 SET b = 'BBB' WHERE a = 100;"
```

#### Limitations related in rowkey-column attribute.
GridDB can set a rowkey attribute to the 1st column.
griddb_fdw uses it for identifying a record.
- It is required that a container have the rowkey attribute for executing UPDATE or DELETE.
- It is not supported to update a rowkey-column.

#### Don't support SAVEPOINT.
Savepoint does not work. Warning is returned.

#### Don't support the query execution which is satisfied with all of following conditions:  
- It requires to get record locks in a transaction.
- Records are locked by the other foreign table which links the same container in GridDB.

If such query is executed, it is no response.

Example1:  
Foreign table ft1 and ft2 are linked to same container in GridDB.

```
BEGIN;
SELECT * FROM ft1 FOR UPDATE;
SELECT * FROM ft2 FOR UPDATE; -- No response
```
Example2:

```
BEGIN;
SELECT * FROM ft1, ft2 WHERE ft1.a = ft2.a FOR UPDATE; -- No response
```
This is because GridDB manages a transaction by container unit and griddb_fdw creates GSContainer instances for each foreign tables even if the container is same in GridDB.

#### Don't support an arbitrary column mapping.
griddb_fdw is assumed that a column structure on PostgreSQL is same as that of griddb.
It is recommended to create a schema on PostgreSQL by IMPORT FOREIGN SCHEMA.
griddb_fdw might return an error when DML is executed.

For example, container on GridDB has 3 columns. The 1st column is "c1" as integer, the 2nd is "c2" as float, and the 3rd is "c3" as text.  
Schema must be created as

```
CREATE FOREIGN TABLE ft1 (c1 integer, c2 float, c3 text) SERVER griddb_svr;
```

You should not execute following queries.  

Types are not match.

```
CREATE FOREIGN TABLE ft1 (c1 text, c2 float, c3 integer) SERVER griddb_svr;
```
There is unknown column.
Even if unknown column is dropped, griddb cannot access ft1 correctly.

```
CREATE FOREIGN TABLE ft1 (c0 integer, c1 integer, c2 float, c3 text) SERVER griddb_svr;
ALTER FOREIGN TABLE ft1 DROP COLUMN c0;
```
#### Don't support ON CONFLICT.
PostgreSQL has upsert (update or insert) feature. When inserting a new row into the table, PostgreSQL will update the row if it already exists, otherwise, PostgreSQL inserts the new row.
However, griddb_fdw does not support the upsert feature now. For example, it shows an error message if user executes the following query.

```
INSERT INTO ft1(c1, c2) VALUES(11, 12) ON CONFLICT DO NOTHING;
```

#### Don't support RETURNING.
Returning is way to obtain data if rows are modified by INSERT, UPDATE, and DELETE commands. griddb_fdw does not support this feature.

```
INSERT INTO ft1 (c0, c1) VALUES (1, 2) RETURNING c0, c1;
```

#### Don't support DIRECT MODIFICATION
#### Don't support IMPORT FOREIGN SCHEMA with option import_generated

## 5. License
Copyright (c) 2017-2021, TOSHIBA Corporation  
Copyright (c) 2011-2016, EnterpriseDB Corporation

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement is
hereby granted, provided that the above copyright notice and this paragraph and
the following two paragraphs appear in all copies.

See the [`LICENSE`][2] file for full details.

[1]: https://github.com/griddb
[2]: LICENSE

