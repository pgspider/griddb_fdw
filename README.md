# GridDB Foreign Data Wrapper for PostgreSQL

This PostgreSQL extension is a Foreign Data Wrapper (FDW) for [GridDB][1].  
This version of griddb_fdw can work for PostgreSQL 9.6 and 10.  


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
--------
### load extension
```
CREATE EXTENSION griddb_fdw;
```
### create server object
```
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS(host '239.0.0.1',  
port '31999', clustername 'ktymCluster');
```

We have to specify the following parameters for a [GridDB][1] foreign server:

host : GridDB notification addres.  
port : GridDB notification port  
clustername : GridDB cluster name  

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

### update and delete

The container must have rowkey on [GridDB][1] in order to execute update and delete query.


## 3. Features
-----------
- Supprt SELECT and INSERT
- Supprt UPDATE and DELETE
- WHERE clauses are pushdowned

## 4. Limitations
--------------
Nothing.

## 5. License
----------
Copyright (c) 2017-2018, TOSHIBA Corporation  
Copyright (c) 2011 - 2016, EnterpriseDB Corporation

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement is
hereby granted, provided that the above copyright notice and this paragraph and
the following two paragraphs appear in all copies.

See the [`LICENSE`][2] file for full details.

[1]: https://github.com/griddb
[2]: LICENSE


