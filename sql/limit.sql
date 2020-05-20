--
-- LIMIT
-- Check the LIMIT/OFFSET feature of SELECT
--
CREATE EXTENSION griddb_fdw;
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS(host '239.0.0.1', port '31999', clustername 'griddbfdwTestCluster');
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS(username 'admin', password 'testadmin');
CREATE FOREIGN TABLE onek(
  unique1     int4 OPTIONS (rowkey 'true'),
  unique2     int4,
  two         int4,
  four        int4,
  ten         int4,
  twenty      int4,
  hundred     int4,
  thousand    int4,
  twothousand int4,
  fivethous   int4,
  tenthous    int4,
  odd         int4,
  even        int4,
  stringu1    text,
  stringu2    text,
  string4     text
) SERVER griddb_svr;

CREATE FOREIGN TABLE int8_tbl(id serial OPTIONS (rowkey 'true'), q1 int8, q2 int8) SERVER griddb_svr;

CREATE FOREIGN TABLE tenk1 (
  unique1     int4 OPTIONS (rowkey 'true'),
  unique2     int4,
  two         int4,
  four        int4,
  ten         int4,
  twenty      int4,
  hundred     int4,
  thousand    int4,
  twothousand int4,
  fivethous   int4,
  tenthous    int4,
  odd         int4,
  even        int4,
  stringu1    text,
  stringu2    text,
  string4     text
) SERVER griddb_svr;

SELECT ''::text AS two, unique1, unique2, stringu1
    FROM onek WHERE unique1 > 50
    ORDER BY unique1 LIMIT 2;
SELECT ''::text AS five, unique1, unique2, stringu1
    FROM onek WHERE unique1 > 60
    ORDER BY unique1 LIMIT 5;
SELECT ''::text AS two, unique1, unique2, stringu1
    FROM onek WHERE unique1 > 60 AND unique1 < 63
    ORDER BY unique1 LIMIT 5;
SELECT ''::text AS three, unique1, unique2, stringu1
    FROM onek WHERE unique1 > 100
    ORDER BY unique1 LIMIT 3 OFFSET 20;
SELECT ''::text AS zero, unique1, unique2, stringu1
    FROM onek WHERE unique1 < 50
    ORDER BY unique1 DESC LIMIT 8 OFFSET 99;
SELECT ''::text AS eleven, unique1, unique2, stringu1
    FROM onek WHERE unique1 < 50
    ORDER BY unique1 DESC LIMIT 20 OFFSET 39;
SELECT ''::text AS ten, unique1, unique2, stringu1
    FROM onek
    ORDER BY unique1 OFFSET 990;
SELECT ''::text AS five, unique1, unique2, stringu1
    FROM onek
    ORDER BY unique1 OFFSET 990 LIMIT 5;
SELECT ''::text AS five, unique1, unique2, stringu1
    FROM onek
    ORDER BY unique1 LIMIT 5 OFFSET 900;

-- Test null limit and offset.  The planner would discard a simple null
-- constant, so to ensure executor is exercised, do this:
select * from int8_tbl limit (case when random() < 0.5 then null::bigint end);
select * from int8_tbl offset (case when random() < 0.5 then null::bigint end);

-- Test assorted cases involving backwards fetch from a LIMIT plan node
begin;

declare c1 scroll cursor for select * from int8_tbl limit 10;
fetch all in c1;
fetch 1 in c1;
fetch backward 1 in c1;
fetch backward all in c1;
fetch backward 1 in c1;
fetch all in c1;

declare c2 scroll cursor for select * from int8_tbl limit 3;
fetch all in c2;
fetch 1 in c2;
fetch backward 1 in c2;
fetch backward all in c2;
fetch backward 1 in c2;
fetch all in c2;

declare c3 scroll cursor for select * from int8_tbl offset 3;
fetch all in c3;
fetch 1 in c3;
fetch backward 1 in c3;
fetch backward all in c3;
fetch backward 1 in c3;
fetch all in c3;

declare c4 scroll cursor for select * from int8_tbl offset 10;
fetch all in c4;
fetch 1 in c4;
fetch backward 1 in c4;
fetch backward all in c4;
fetch backward 1 in c4;
fetch all in c4;

rollback;

-- Stress test for variable LIMIT in conjunction with bounded-heap sorting
BEGIN;
DELETE FROM INT8_TBL;
INSERT INTO INT8_TBL(q1) SELECT q1 FROM generate_series(1,10) q1;
SELECT
  (SELECT s.q1 
     FROM (VALUES (1)) AS x,
          (SELECT q1 FROM INT8_TBL as n 
             ORDER BY q1 LIMIT 1 OFFSET s.q1-1) AS y) AS z
  FROM INT8_TBL AS s;
ROLLBACK;

--
-- Test behavior of volatile and set-returning functions in conjunction
-- with ORDER BY and LIMIT.
--

create temp sequence testseq;

explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 limit 10;

select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 limit 10;

select currval('testseq');

explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by tenthous limit 10;

select unique1, unique2, nextval('testseq')
  from tenk1 order by tenthous limit 10;

select currval('testseq');

explain (verbose, costs off)
select unique1, unique2, generate_series(1,10)
  from tenk1 order by unique2 limit 7;

select unique1, unique2, generate_series(1,10)
  from tenk1 order by unique2 limit 7;

explain (verbose, costs off)
select unique1, unique2, generate_series(1,10)
  from tenk1 order by tenthous limit 7;

select unique1, unique2, generate_series(1,10)
  from tenk1 order by tenthous limit 7;

-- use of random() is to keep planner from folding the expressions together
BEGIN;
DELETE FROM INT8_TBL;
INSERT INTO INT8_TBL(q1, q2) VALUES (generate_series(0,2), generate_series((random()*.1)::int,2));

explain (verbose, costs off)
select q1, q2 from INT8_TBL;

select q1, q2 from INT8_TBL;

explain (verbose, costs off)
select q1, q2 from INT8_TBL order by q2 desc;

select q1, q2 from INT8_TBL order by q2 desc;
ROLLBACK;

-- test for failure to set all aggregates' aggtranstype
explain (verbose, costs off)
select sum(tenthous) as s1, sum(tenthous) + random()*0 as s2
  from tenk1 group by thousand order by thousand limit 3;

select sum(tenthous) as s1, sum(tenthous) + random()*0 as s2
  from tenk1 group by thousand order by thousand limit 3;

DROP FOREIGN TABLE onek;
DROP FOREIGN TABLE int8_tbl;
DROP FOREIGN TABLE tenk1;
DROP USER MAPPING FOR public SERVER griddb_svr;
DROP SERVER griddb_svr;
DROP EXTENSION griddb_fdw CASCADE;
