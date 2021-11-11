--
-- LIMIT
-- Check the LIMIT/OFFSET feature of SELECT
--
\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 57:
CREATE EXTENSION griddb_fdw;
--Testcase 58:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');
--Testcase 59:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);
--Testcase 60:
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

--Testcase 61:
CREATE FOREIGN TABLE int8_tbl(id serial OPTIONS (rowkey 'true'), q1 int8, q2 int8) SERVER griddb_svr;

--Testcase 62:
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

--Testcase 1:
SELECT ''::text AS two, unique1, unique2, stringu1
    FROM onek WHERE unique1 > 50
    ORDER BY unique1 LIMIT 2;
--Testcase 2:
SELECT ''::text AS five, unique1, unique2, stringu1
    FROM onek WHERE unique1 > 60
    ORDER BY unique1 LIMIT 5;
--Testcase 3:
SELECT ''::text AS two, unique1, unique2, stringu1
    FROM onek WHERE unique1 > 60 AND unique1 < 63
    ORDER BY unique1 LIMIT 5;
--Testcase 4:
SELECT ''::text AS three, unique1, unique2, stringu1
    FROM onek WHERE unique1 > 100
    ORDER BY unique1 LIMIT 3 OFFSET 20;
--Testcase 5:
SELECT ''::text AS zero, unique1, unique2, stringu1
    FROM onek WHERE unique1 < 50
    ORDER BY unique1 DESC LIMIT 8 OFFSET 99;
--Testcase 6:
SELECT ''::text AS eleven, unique1, unique2, stringu1
    FROM onek WHERE unique1 < 50
    ORDER BY unique1 DESC LIMIT 20 OFFSET 39;
--Testcase 7:
SELECT ''::text AS ten, unique1, unique2, stringu1
    FROM onek
    ORDER BY unique1 OFFSET 990;
--Testcase 8:
SELECT ''::text AS five, unique1, unique2, stringu1
    FROM onek
    ORDER BY unique1 OFFSET 990 LIMIT 5;
--Testcase 9:
SELECT ''::text AS five, unique1, unique2, stringu1
    FROM onek
    ORDER BY unique1 LIMIT 5 OFFSET 900;

-- Test null limit and offset.  The planner would discard a simple null
-- constant, so to ensure executor is exercised, do this:
--Testcase 10:
select q1, q2 from int8_tbl limit (case when random() < 0.5 then null::bigint end);
--Testcase 11:
select q1, q2 from int8_tbl offset (case when random() < 0.5 then null::bigint end);

-- Test assorted cases involving backwards fetch from a LIMIT plan node
begin;

declare c1 scroll cursor for select q1, q2 from int8_tbl limit 10;
--Testcase 12:
fetch all in c1;
--Testcase 13:
fetch 1 in c1;
--Testcase 14:
fetch backward 1 in c1;
--Testcase 15:
fetch backward all in c1;
--Testcase 16:
fetch backward 1 in c1;
--Testcase 17:
fetch all in c1;

declare c2 scroll cursor for select q1, q2 from int8_tbl limit 3;
--Testcase 18:
fetch all in c2;
--Testcase 19:
fetch 1 in c2;
--Testcase 20:
fetch backward 1 in c2;
--Testcase 21:
fetch backward all in c2;
--Testcase 22:
fetch backward 1 in c2;
--Testcase 23:
fetch all in c2;

declare c3 scroll cursor for select q1, q2 from int8_tbl offset 3;
--Testcase 24:
fetch all in c3;
--Testcase 25:
fetch 1 in c3;
--Testcase 26:
fetch backward 1 in c3;
--Testcase 27:
fetch backward all in c3;
--Testcase 28:
fetch backward 1 in c3;
--Testcase 29:
fetch all in c3;

declare c4 scroll cursor for select q1, q2 from int8_tbl offset 10;
--Testcase 30:
fetch all in c4;
--Testcase 31:
fetch 1 in c4;
--Testcase 32:
fetch backward 1 in c4;
--Testcase 33:
fetch backward all in c4;
--Testcase 34:
fetch backward 1 in c4;
--Testcase 35:
fetch all in c4;

declare c5 scroll cursor for select q1, q2 from int8_tbl order by q1 fetch first 2 rows with ties;
--Testcase 63:
fetch all in c5;
--Testcase 64:
fetch 1 in c5;
--Testcase 65:
fetch backward 1 in c5;
--Testcase 66:
fetch backward 1 in c5;
--Testcase 67:
fetch all in c5;
--Testcase 68:
fetch backward all in c5;
--Testcase 69:
fetch all in c5;
--Testcase 70:
fetch backward all in c5;

rollback;

-- Stress test for variable LIMIT in conjunction with bounded-heap sorting
BEGIN;
--Testcase 36:
DELETE FROM INT8_TBL;
--Testcase 37:
INSERT INTO INT8_TBL(q1) SELECT q1 FROM generate_series(1,10) q1;
--Testcase 38:
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

--Testcase 71:
create temp sequence testseq;

--Testcase 39:
explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 limit 10;

--Testcase 40:
select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 limit 10;

--Testcase 41:
select currval('testseq');

--Testcase 42:
explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by tenthous limit 10;

--Testcase 43:
select unique1, unique2, nextval('testseq')
  from tenk1 order by tenthous limit 10;

--Testcase 44:
select currval('testseq');

--Testcase 45:
explain (verbose, costs off)
select unique1, unique2, generate_series(1,10)
  from tenk1 order by unique2 limit 7;

--Testcase 46:
select unique1, unique2, generate_series(1,10)
  from tenk1 order by unique2 limit 7;

--Testcase 47:
explain (verbose, costs off)
select unique1, unique2, generate_series(1,10)
  from tenk1 order by tenthous limit 7;

--Testcase 48:
select unique1, unique2, generate_series(1,10)
  from tenk1 order by tenthous limit 7;

-- use of random() is to keep planner from folding the expressions together
BEGIN;
--Testcase 49:
DELETE FROM INT8_TBL;
--Testcase 50:
INSERT INTO INT8_TBL(q1, q2) VALUES (generate_series(0,2), generate_series((random()*.1)::int,2));

--Testcase 51:
explain (verbose, costs off)
select q1, q2 from INT8_TBL;

--Testcase 52:
select q1, q2 from INT8_TBL;

--Testcase 53:
explain (verbose, costs off)
select q1, q2 from INT8_TBL order by q2 desc;

--Testcase 54:
select q1, q2 from INT8_TBL order by q2 desc;
ROLLBACK;

-- test for failure to set all aggregates' aggtranstype
--Testcase 55:
explain (verbose, costs off)
select sum(tenthous) as s1, sum(tenthous) + random()*0 as s2
  from tenk1 group by thousand order by thousand limit 3;

--Testcase 56:
select sum(tenthous) as s1, sum(tenthous) + random()*0 as s2
  from tenk1 group by thousand order by thousand limit 3;

--
-- FETCH FIRST
-- Check the WITH TIES clause
--

--Testcase 72:
SELECT  thousand
		FROM onek WHERE thousand < 5
		ORDER BY thousand FETCH FIRST 2 ROW WITH TIES;

--Testcase 73:
SELECT  thousand
		FROM onek WHERE thousand < 5
		ORDER BY thousand FETCH FIRST ROWS WITH TIES;

--Testcase 74:
SELECT  thousand
		FROM onek WHERE thousand < 5
		ORDER BY thousand FETCH FIRST 1 ROW WITH TIES;

--Testcase 75:
SELECT  thousand
		FROM onek WHERE thousand < 5
		ORDER BY thousand FETCH FIRST 2 ROW ONLY;

-- should fail
--Testcase 76:
SELECT ''::text AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 50
		FETCH FIRST 2 ROW WITH TIES;

-- test ruleutils
--Testcase 77:
CREATE VIEW limit_thousand_v_1 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand FETCH FIRST 5 ROWS WITH TIES OFFSET 10;
--Testcase 78:
\d+ limit_thousand_v_1
--Testcase 79:
CREATE VIEW limit_thousand_v_2 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand OFFSET 10 FETCH FIRST 5 ROWS ONLY;
--Testcase 80:
\d+ limit_thousand_v_2
--Testcase 81:
CREATE VIEW limit_thousand_v_3 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand FETCH FIRST NULL ROWS WITH TIES;		-- fails
--Testcase 82:
CREATE VIEW limit_thousand_v_3 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand FETCH FIRST (NULL+1) ROWS WITH TIES;
--Testcase 83:
\d+ limit_thousand_v_3
--Testcase 84:
CREATE VIEW limit_thousand_v_4 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand FETCH FIRST NULL ROWS ONLY;
--Testcase 85:
\d+ limit_thousand_v_4
-- leave these views
--Testcase 86:
DROP VIEW limit_thousand_v_1;
--Testcase 87:
DROP VIEW limit_thousand_v_2;
--Testcase 88:
DROP VIEW limit_thousand_v_3;
--Testcase 89:
DROP VIEW limit_thousand_v_4;

--Testcase 90:
DROP FOREIGN TABLE onek;
--Testcase 91:
DROP FOREIGN TABLE int8_tbl;
--Testcase 92:
DROP FOREIGN TABLE tenk1;
--Testcase 93:
DROP USER MAPPING FOR public SERVER griddb_svr;
--Testcase 94:
DROP SERVER griddb_svr;
--Testcase 95:
DROP EXTENSION griddb_fdw CASCADE;
