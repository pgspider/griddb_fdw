--
-- AGGREGATES
--
\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 345:
CREATE EXTENSION griddb_fdw;
--Testcase 346:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');
--Testcase 347:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);
--Testcase 348:
CREATE FOREIGN TABLE onek(
  unique1   int4 OPTIONS (rowkey 'true'),
  unique2   int4,
  two     int4,
  four    int4,
  ten     int4,
  twenty    int4,
  hundred   int4,
  thousand  int4,
  twothousand int4,
  fivethous int4,
  tenthous  int4,
  odd     int4,
  even    int4,
  stringu1  text,
  stringu2  text,
  string4   text
) SERVER griddb_svr;

--Testcase 349:
CREATE FOREIGN TABLE aggtest (
  id      int4,
  a       int2,
  b     float4
) SERVER griddb_svr;

--Testcase 350:
CREATE FOREIGN TABLE student (
  name    text,
  age     int4,
  location  text,
  gpa     float8
) SERVER griddb_svr;

--Testcase 351:
CREATE FOREIGN TABLE tenk1 (
  unique1   int4,
  unique2   int4,
  two     int4,
  four    int4,
  ten     int4,
  twenty    int4,
  hundred   int4,
  thousand  int4,
  twothousand int4,
  fivethous int4,
  tenthous  int4,
  odd     int4,
  even    int4,
  stringu1  text,
  stringu2  text,
  string4   text
) SERVER griddb_svr;

--Testcase 352:
CREATE FOREIGN TABLE multi_arg_agg (a int OPTIONS (rowkey 'true'), b int, c text) SERVER griddb_svr;

--Testcase 353:
CREATE FOREIGN TABLE INT4_TBL(id serial OPTIONS (rowkey 'true'), f1 int4) SERVER griddb_svr; 

--Testcase 354:
CREATE FOREIGN TABLE INT8_TBL(id serial OPTIONS (rowkey 'true'), q1 int8 , q2 int8) SERVER griddb_svr; 

--Testcase 355:
CREATE FOREIGN TABLE VARCHAR_TBL(f1 text OPTIONS (rowkey 'true')) SERVER griddb_svr;

--Testcase 356:
CREATE FOREIGN TABLE FLOAT8_TBL(id serial OPTIONS (rowkey 'true'), f1 float8) SERVER griddb_svr;

--Testcase 357:
CREATE FOREIGN TABLE FLOAT8_TMP(id serial OPTIONS (rowkey 'true'), f1 float8, f2 float8) SERVER griddb_svr;

-- avoid bit-exact output here because operations may not be bit-exact.
--Testcase 535:
SET extra_float_digits = 0;

--Testcase 1:
SELECT avg(four) AS avg_1 FROM onek;

--Testcase 2:
SELECT avg(a) AS avg_32 FROM aggtest WHERE a < 100;

-- In 7.1, avg(float4) is computed using float8 arithmetic.
-- Round the result to 3 digits to avoid platform-specific results.

--Testcase 3:
SELECT avg(b)::numeric(10,3) AS avg_107_943 FROM aggtest;

--Testcase 4:
SELECT avg(gpa) AS avg_3_4 FROM ONLY student;


--Testcase 5:
SELECT sum(four) AS sum_1500 FROM onek;
--Testcase 6:
SELECT sum(a) AS sum_198 FROM aggtest;
--Testcase 7:
SELECT sum(b) AS avg_431_773 FROM aggtest;
--Testcase 8:
SELECT sum(gpa) AS avg_6_8 FROM ONLY student;

--Testcase 9:
SELECT max(four) AS max_3 FROM onek;
--Testcase 10:
SELECT max(a) AS max_100 FROM aggtest;
--Testcase 11:
SELECT max(aggtest.b) AS max_324_78 FROM aggtest;
--Testcase 12:
SELECT max(student.gpa) AS max_3_7 FROM student;

--Testcase 13:
SELECT stddev_pop(b) FROM aggtest;
--Testcase 14:
SELECT stddev_samp(b) FROM aggtest;
--Testcase 15:
SELECT var_pop(b) FROM aggtest;
--Testcase 16:
SELECT var_samp(b) FROM aggtest;

--Testcase 17:
SELECT stddev_pop(b::numeric) FROM aggtest;
--Testcase 18:
SELECT stddev_samp(b::numeric) FROM aggtest;
--Testcase 19:
SELECT var_pop(b::numeric) FROM aggtest;
--Testcase 20:
SELECT var_samp(b::numeric) FROM aggtest;

-- population variance is defined for a single tuple, sample variance
-- is not
BEGIN;
--Testcase 358:
DELETE FROM FLOAT8_TBL;
--Testcase 359:
INSERT INTO FLOAT8_TBL(f1) VALUES (1.0::float8);
--Testcase 360:
SELECT var_pop(f1::float8), var_samp(f1::float8) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 361:
DELETE FROM FLOAT8_TBL;
--Testcase 362:
INSERT INTO FLOAT8_TBL(f1) VALUES (3.0::float8);
--Testcase 363:
SELECT stddev_pop(f1::float8), stddev_samp(f1::float8) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 364:
DELETE FROM FLOAT8_TBL;
--Testcase 365:
INSERT INTO FLOAT8_TBL(f1) VALUES ('inf'::float8);
--Testcase 366:
SELECT var_pop(f1::float8), var_samp(f1::float8) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 367:
DELETE FROM FLOAT8_TBL;
--Testcase 368:
INSERT INTO FLOAT8_TBL(f1) VALUES ('inf'::float8);
--Testcase 369:
SELECT stddev_pop(f1::float8), stddev_samp(f1::float8) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 370:
DELETE FROM FLOAT8_TBL;
--Testcase 371:
INSERT INTO FLOAT8_TBL(f1) VALUES ('nan'::float8);
--Testcase 372:
SELECT var_pop(f1::float8), var_samp(f1::float8) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 373:
DELETE FROM FLOAT8_TBL;
--Testcase 374:
INSERT INTO FLOAT8_TBL(f1) VALUES ('nan'::float8);
--Testcase 375:
SELECT stddev_pop(f1::float8), stddev_samp(f1::float8) FROM FLOAT8_TBL;
ROLLBACK;

--Testcase 376:
CREATE FOREIGN TABLE FLOAT4_TBL(id serial OPTIONS (rowkey 'true'), f1 float4) SERVER griddb_svr;

BEGIN;
--Testcase 377:
DELETE FROM FLOAT4_TBL;
--Testcase 378:
INSERT INTO FLOAT4_TBL(f1) VALUES (1.0::float4);
--Testcase 379:
SELECT var_pop(f1::float4), var_samp(f1::float4) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 380:
DELETE FROM FLOAT4_TBL;
--Testcase 381:
INSERT INTO FLOAT4_TBL(f1) VALUES (3.0::float4);
--Testcase 382:
SELECT stddev_pop(f1::float4), stddev_samp(f1::float4) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 383:
DELETE FROM FLOAT4_TBL;
--Testcase 384:
INSERT INTO FLOAT4_TBL(f1) VALUES ('inf'::float4);
--Testcase 385:
SELECT var_pop(f1::float4), var_samp(f1::float4) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 386:
DELETE FROM FLOAT4_TBL;
--Testcase 387:
INSERT INTO FLOAT4_TBL(f1) VALUES ('inf'::float4);
--Testcase 388:
SELECT stddev_pop(f1::float4), stddev_samp(f1::float4) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 389:
DELETE FROM FLOAT4_TBL;
--Testcase 390:
INSERT INTO FLOAT4_TBL(f1) VALUES ('nan'::float4);
--Testcase 391:
SELECT var_pop(f1::float4), var_samp(f1::float4) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 392:
DELETE FROM FLOAT4_TBL;
--Testcase 393:
INSERT INTO FLOAT4_TBL(f1) VALUES ('nan'::float4);
--Testcase 394:
SELECT stddev_pop(f1::float4), stddev_samp(f1::float4) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 395:
DELETE FROM FLOAT4_TBL;
--Testcase 396:
INSERT INTO FLOAT4_TBL(f1) VALUES (1.0);
--Testcase 397:
SELECT var_pop(f1::numeric), var_samp(f1::numeric) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 398:
DELETE FROM FLOAT4_TBL;
--Testcase 399:
INSERT INTO FLOAT4_TBL(f1) VALUES (3.0);
--Testcase 400:
SELECT stddev_pop(f1::numeric), stddev_samp(f1::numeric) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 401:
DELETE FROM FLOAT4_TBL;
--Testcase 402:
INSERT INTO FLOAT4_TBL(f1) VALUES ('nan');
--Testcase 403:
SELECT var_pop(f1::numeric), var_samp(f1::numeric) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 404:
DELETE FROM FLOAT4_TBL;
--Testcase 405:
INSERT INTO FLOAT4_TBL(f1) VALUES ('nan');
--Testcase 406:
SELECT stddev_pop(f1::numeric), stddev_samp(f1::numeric) FROM FLOAT4_TBL;
ROLLBACK;

--Testcase 407:
DROP FOREIGN TABLE FLOAT4_TBL;

-- verify correct results for null and NaN inputs
begin;
--Testcase 21:
delete from INT4_TBL;
--Testcase 22:
insert into INT4_TBL select * from generate_series(1,3);
--Testcase 23:
select sum(null::int4) from INT4_TBL;
--Testcase 24:
select sum(null::int8) from INT4_TBL;
--Testcase 25:
select sum(null::numeric) from INT4_TBL;
--Testcase 26:
select sum(null::float8) from INT4_TBL;
--Testcase 27:
select avg(null::int4) from INT4_TBL;
--Testcase 28:
select avg(null::int8) from INT4_TBL;
--Testcase 29:
select avg(null::numeric) from INT4_TBL;
--Testcase 30:
select avg(null::float8) from INT4_TBL;
--Testcase 31:
select sum('NaN'::numeric) from INT4_TBL;
--Testcase 32:
select avg('NaN'::numeric) from INT4_TBL;
rollback;

-- verify correct results for infinite inputs
BEGIN;
--Testcase 33:
DELETE FROM FLOAT8_TBL;
--Testcase 34:
INSERT INTO FLOAT8_TBL(f1) VALUES ('1'), ('infinity');
--Testcase 35:
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 36:
DELETE FROM FLOAT8_TBL;
--Testcase 37:
INSERT INTO FLOAT8_TBL(f1) VALUES ('infinity'), ('1');
--Testcase 38:
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 39:
DELETE FROM FLOAT8_TBL;
--Testcase 40:
INSERT INTO FLOAT8_TBL(f1) VALUES ('infinity'), ('infinity');
--Testcase 41:
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 42:
DELETE FROM FLOAT8_TBL;
--Testcase 43:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-infinity'), ('infinity');
--Testcase 44:
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

-- test accuracy with a large input offset
BEGIN;
--Testcase 45:
DELETE FROM FLOAT8_TBL;
--Testcase 46:
INSERT INTO FLOAT8_TBL(f1) VALUES ('100000003'), ('100000004'), 
				('100000006'), ('100000007');
--Testcase 47:
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 48:
DELETE FROM FLOAT8_TBL;
--Testcase 49:
INSERT INTO FLOAT8_TBL(f1) VALUES ('7000000000005'), ('7000000000007');
--Testcase 50:
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

-- SQL2003 binary aggregates
--Testcase 51:
SELECT regr_count(b, a) FROM aggtest;
--Testcase 52:
SELECT regr_sxx(b, a) FROM aggtest;
--Testcase 53:
SELECT regr_syy(b, a) FROM aggtest;
--Testcase 54:
SELECT regr_sxy(b, a) FROM aggtest;
--Testcase 55:
SELECT regr_avgx(b, a), regr_avgy(b, a) FROM aggtest;
--Testcase 56:
SELECT regr_r2(b, a) FROM aggtest;
--Testcase 57:
SELECT regr_slope(b, a), regr_intercept(b, a) FROM aggtest;
--Testcase 58:
SELECT covar_pop(b, a), covar_samp(b, a) FROM aggtest;
--Testcase 59:
SELECT corr(b, a) FROM aggtest;

-- check single-tuple behavior
BEGIN;
--Testcase 408:
DELETE FROM FLOAT8_TMP;
--Testcase 409:
INSERT INTO FLOAT8_TMP(f1, f2) VALUES (1::float8,2::float8);
--Testcase 410:
SELECT covar_pop(f1::float8, f2::float8), covar_samp(f1::float8, f2::float8) FROM FLOAT8_TMP;
ROLLBACK;

BEGIN;
--Testcase 411:
DELETE FROM FLOAT8_TMP;
--Testcase 412:
INSERT INTO FLOAT8_TMP(f1, f2) VALUES (1::float8, 'inf'::float8);
--Testcase 413:
SELECT covar_pop(f1::float8, f2::float8), covar_samp(f1::float8, f2::float8) FROM FLOAT8_TMP;
ROLLBACK;

BEGIN;
--Testcase 414:
DELETE FROM FLOAT8_TMP;
--Testcase 415:
INSERT INTO FLOAT8_TMP(f1, f2) VALUES (1::float8, 'nan'::float8);
--Testcase 416:
SELECT covar_pop(f1::float8, f2::float8), covar_samp(f1::float8, f2::float8) FROM FLOAT8_TMP;
ROLLBACK;

-- test accum and combine functions directly
--Testcase 417:
CREATE FOREIGN TABLE regr_test (id serial OPTIONS (rowkey 'true'), x int4, y int4) SERVER griddb_svr;
--Testcase 60:
INSERT INTO regr_test(x, y) VALUES (10,150),(20,250),(30,350),(80,540),(100,200);
--Testcase 61:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (10,20,30,80);
--Testcase 62:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test;

--Testcase 418:
CREATE FOREIGN TABLE regr_test_array (id serial OPTIONS (rowkey 'true'), x float8[], y float8[]) SERVER griddb_svr;
BEGIN;
--Testcase 63:
INSERT INTO regr_test_array(x) VALUES ('{4,140,2900}'::float8[]);
--Testcase 64:
SELECT float8_accum(x, 100) FROM regr_test_array;
ROLLBACK;

BEGIN;
--Testcase 65:
INSERT INTO regr_test_array(x) VALUES ('{4,140,2900,1290,83075,15050}'::float8[]);
--Testcase 66:
SELECT float8_regr_accum(x, 200, 100) FROM regr_test_array;
ROLLBACK;

--Testcase 67:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (10,20,30);
--Testcase 68:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (80,100);

BEGIN;
--Testcase 69:
INSERT INTO regr_test_array(x,y) VALUES ('{3,60,200}'::float8[], '{0,0,0}'::float8[]);
--Testcase 70:
SELECT float8_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;
--Testcase 71:
INSERT INTO regr_test_array(x,y) VALUES ('{0,0,0}'::float8[], '{2,180,200}'::float8[]);
--Testcase 72:
SELECT float8_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;
--Testcase 73:
INSERT INTO regr_test_array(x,y) VALUES ('{3,60,200}'::float8[], '{2,180,200}'::float8[]);
--Testcase 74:
SELECT float8_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;
--Testcase 75:
INSERT INTO regr_test_array(x,y) VALUES ('{3,60,200,750,20000,2000}'::float8[],
                           '{0,0,0,0,0,0}'::float8[]);
--Testcase 76:
SELECT float8_regr_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;
--Testcase 77:
INSERT INTO regr_test_array(x,y) VALUES ('{0,0,0,0,0,0}'::float8[],
                           '{2,180,200,740,57800,-3400}'::float8[]);
--Testcase 78:
SELECT float8_regr_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;
--Testcase 79:
INSERT INTO regr_test_array(x,y) VALUES ('{3,60,200,750,20000,2000}'::float8[],
                           '{2,180,200,740,57800,-3400}'::float8[]);
--Testcase 80:
SELECT float8_regr_combine(x, y) FROM regr_test_array;
ROLLBACK;

--Testcase 419:
DROP FOREIGN TABLE regr_test;
--Testcase 420:
DROP FOREIGN TABLE regr_test_array;

-- test count, distinct
--Testcase 81:
SELECT count(four) AS cnt_1000 FROM onek;
--Testcase 82:
SELECT count(DISTINCT four) AS cnt_4 FROM onek;

--Testcase 83:
select ten, count(*), sum(four) from onek
group by ten order by ten;

--Testcase 84:
select ten, count(four), sum(DISTINCT four) from onek
group by ten order by ten;

-- user-defined aggregates
--Testcase 421:
CREATE AGGREGATE newavg (
   sfunc = int4_avg_accum, basetype = int4, stype = _int8,
   finalfunc = int8_avg,
   initcond1 = '{0,0}'
);

--Testcase 422:
CREATE AGGREGATE newsum (
   sfunc1 = int4pl, basetype = int4, stype1 = int4,
   initcond1 = '0'
);

--Testcase 423:
CREATE AGGREGATE newcnt (*) (
   sfunc = int8inc, stype = int8,
   initcond = '0', parallel = safe
);

--Testcase 424:
CREATE AGGREGATE newcnt ("any") (
   sfunc = int8inc_any, stype = int8,
   initcond = '0'
);

--Testcase 425:
CREATE AGGREGATE oldcnt (
   sfunc = int8inc, basetype = 'ANY', stype = int8,
   initcond = '0'
);

--Testcase 426:
create function sum3(int8,int8,int8) returns int8 as
'select $1 + $2 + $3' language sql strict immutable;

--Testcase 427:
create aggregate sum2(int8,int8) (
   sfunc = sum3, stype = int8,
   initcond = '0'
);

--Testcase 85:
SELECT newavg(four) AS avg_1 FROM onek;
--Testcase 86:
SELECT newsum(four) AS sum_1500 FROM onek;
--Testcase 87:
SELECT newcnt(four) AS cnt_1000 FROM onek;
--Testcase 88:
SELECT newcnt(*) AS cnt_1000 FROM onek;
--Testcase 89:
SELECT oldcnt(*) AS cnt_1000 FROM onek;
--Testcase 90:
SELECT sum2(q1,q2) FROM int8_tbl;

-- test for outer-level aggregates

-- this should work
--Testcase 91:
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

-- this should fail because subquery has an agg of its own in WHERE
--Testcase 92:
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b
               where sum(distinct a.four + b.four) = b.four);

-- Test handling of sublinks within outer-level aggregates.
-- Per bug report from Daniel Grace.
--Testcase 93:
select
  (select max((select i.unique2 from tenk1 i where i.unique1 = o.unique1)))
from tenk1 o;

-- Test handling of Params within aggregate arguments in hashed aggregation.
-- Per bug report from Jeevan Chalke.
BEGIN;
--Testcase 94:
DELETE FROM INT4_TBL;
--Testcase 95:
INSERT INTO INT4_TBL(f1) values (generate_series(1, 3));
--Testcase 96:
explain (verbose, costs off)
select s1.f1, ss.f1, sm
from INT4_TBL s1,
     lateral (select s2.f1, sum(s1.f1 + s2.f1) sm
              from INT4_TBL s2 group by s2.f1) ss
order by 1, 2;
--Testcase 97:
select s1.f1, ss.f1, sm
from INT4_TBL s1,
     lateral (select s2.f1, sum(s1.f1 + s2.f1) sm
              from INT4_TBL s2 group by s2.f1) ss
order by 1, 2;

--Testcase 98:
explain (verbose, costs off)
select array(select sum(x.f1+y.f1) s
            from INT4_TBL y group by y.f1 order by s)
  from INT4_TBL x;
--Testcase 99:
select array(select sum(x.f1+y.f1) s
            from INT4_TBL y group by y.f1 order by s)
  from INT4_TBL x;
ROLLBACK;

--
-- test for bitwise integer aggregates
--
--Testcase 428:
CREATE FOREIGN TABLE bitwise_test(
  id serial OPTIONS (rowkey 'true'),
  i2 INT2,
  i4 INT4,
  i8 INT8,
  i INTEGER,
  x INT2,
  y text
) SERVER griddb_svr;

-- empty case
--Testcase 100:
SELECT
  BIT_AND(i2) AS "?",
  BIT_OR(i4)  AS "?"
FROM bitwise_test;

--Testcase 101:
INSERT INTO bitwise_test(i2, i4, i8, i, x, y) VALUES
  (1, 1, 1, 1, 1, B'0101'),
  (3, 3, 3, null, 2, B'0100'),
  (7, 7, 7, 3, 4, B'1100');

--Testcase 102:
SELECT
  BIT_AND(i2) AS "1",
  BIT_AND(i4) AS "1",
  BIT_AND(i8) AS "1",
  BIT_AND(i)  AS "?",
  BIT_AND(x)  AS "0",
  BIT_AND(y::bit(4))  AS "0100",

  BIT_OR(i2)  AS "7",
  BIT_OR(i4)  AS "7",
  BIT_OR(i8)  AS "7",
  BIT_OR(i)   AS "?",
  BIT_OR(x)   AS "7",
  BIT_OR(y::bit(4))   AS "1101"
FROM bitwise_test;

--
-- test boolean aggregates
--
-- first test all possible transition and final states

--Testcase 429:
CREATE FOREIGN TABLE bool_test_a(
  id serial OPTIONS (rowkey 'true'),
  a1 BOOL,
  a2 BOOL,
  a3 BOOL,
  a4 BOOL,
  a5 BOOL,
  a6 BOOL,
  a7 BOOL,
  a8 BOOL,
  a9 BOOL
) SERVER griddb_svr;

--Testcase 430:
CREATE FOREIGN TABLE bool_test_b(
  id serial OPTIONS (rowkey 'true'),
  b1 BOOL,
  b2 BOOL,
  b3 BOOL,
  b4 BOOL,
  b5 BOOL,
  b6 BOOL,
  b7 BOOL,
  b8 BOOL,
  b9 BOOL
) SERVER griddb_svr;

--Testcase 103:
INSERT INTO bool_test_a(a1, a2, a3, a4, a5, a6, a7, a8, a9) VALUES 
(NULL, TRUE, FALSE, NULL, NULL, TRUE, TRUE, FALSE, FALSE);
--Testcase 104:
INSERT INTO bool_test_b(b1, b2, b3, b4, b5, b6, b7, b8, b9) VALUES 
(NULL, NULL, NULL, TRUE, FALSE, TRUE, FALSE, TRUE, FALSE);

--Testcase 105:
SELECT
  -- boolean or transitions
  -- null because strict
  boolor_statefunc(a.a1, b.b1)  IS NULL AS "t",
  boolor_statefunc(a.a2, b.b2)  IS NULL AS "t",
  boolor_statefunc(a.a3, b.b3) IS NULL AS "t",
  boolor_statefunc(a.a4, b.b4)  IS NULL AS "t",
  boolor_statefunc(a.a5, b.b5) IS NULL AS "t",
  -- actual computations
  boolor_statefunc(a.a6, b.b6) AS "t",
  boolor_statefunc(a.a7, b.b7) AS "t",
  boolor_statefunc(a.a8, b.b8) AS "t",
  NOT boolor_statefunc(a.a9, b.b9) AS "t" FROM bool_test_a a, bool_test_b b;

--
-- test boolean aggregates
--

--Testcase 431:
CREATE FOREIGN TABLE bool_test(
  id serial OPTIONS (rowkey 'true'),
  b1 BOOL,
  b2 BOOL,
  b3 BOOL,
  b4 BOOL
) SERVER griddb_svr;

-- empty case
--Testcase 106:
SELECT
  BOOL_AND(b1)   AS "n",
  BOOL_OR(b3)    AS "n"
FROM bool_test;

--Testcase 107:
INSERT INTO bool_test(b1, b2, b3, b4) VALUES
  (TRUE, null, FALSE, null),
  (FALSE, TRUE, null, null),
  (null, TRUE, FALSE, null);

--Testcase 108:
SELECT
  BOOL_AND(b1)     AS "f",
  BOOL_AND(b2)     AS "t",
  BOOL_AND(b3)     AS "f",
  BOOL_AND(b4)     AS "n",
  BOOL_AND(NOT b2) AS "f",
  BOOL_AND(NOT b3) AS "t"
FROM bool_test;

--Testcase 109:
SELECT
  EVERY(b1)     AS "f",
  EVERY(b2)     AS "t",
  EVERY(b3)     AS "f",
  EVERY(b4)     AS "n",
  EVERY(NOT b2) AS "f",
  EVERY(NOT b3) AS "t"
FROM bool_test;

--Testcase 110:
SELECT
  BOOL_OR(b1)      AS "t",
  BOOL_OR(b2)      AS "t",
  BOOL_OR(b3)      AS "f",
  BOOL_OR(b4)      AS "n",
  BOOL_OR(NOT b2)  AS "f",
  BOOL_OR(NOT b3)  AS "t"
FROM bool_test;

--
-- Test cases that should be optimized into indexscans instead of
-- the generic aggregate implementation.
--

-- Basic cases
--Testcase 111:
explain (costs off)
  select min(unique1) from tenk1;
--Testcase 112:
select min(unique1) from tenk1;
--Testcase 113:
explain (costs off)
  select max(unique1) from tenk1;
--Testcase 114:
select max(unique1) from tenk1;
--Testcase 115:
explain (costs off)
  select max(unique1) from tenk1 where unique1 < 42;
--Testcase 116:
select max(unique1) from tenk1 where unique1 < 42;
--Testcase 117:
explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42;
--Testcase 118:
select max(unique1) from tenk1 where unique1 > 42;

-- the planner may choose a generic aggregate here if parallel query is
-- enabled, since that plan will be parallel safe and the "optimized"
-- plan, which has almost identical cost, will not be.  we want to test
-- the optimized plan, so temporarily disable parallel query.
begin;
--Testcase 536:
set local max_parallel_workers_per_gather = 0;
--Testcase 119:
explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42000;
--Testcase 120:
select max(unique1) from tenk1 where unique1 > 42000;
rollback;

-- multi-column index (uses tenk1_thous_tenthous)
--Testcase 121:
explain (costs off)
  select max(tenthous) from tenk1 where thousand = 33;
--Testcase 122:
select max(tenthous) from tenk1 where thousand = 33;
--Testcase 123:
explain (costs off)
  select min(tenthous) from tenk1 where thousand = 33;
--Testcase 124:
select min(tenthous) from tenk1 where thousand = 33;

-- check parameter propagation into an indexscan subquery
--Testcase 125:
explain (costs off)
  select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
    from int4_tbl;
--Testcase 126:
select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
  from int4_tbl;

-- check some cases that were handled incorrectly in 8.3.0
--Testcase 127:
explain (costs off)
  select distinct max(unique2) from tenk1;
--Testcase 128:
select distinct max(unique2) from tenk1;
--Testcase 129:
explain (costs off)
  select max(unique2) from tenk1 order by 1;
--Testcase 130:
select max(unique2) from tenk1 order by 1;
--Testcase 131:
explain (costs off)
  select max(unique2) from tenk1 order by max(unique2);
--Testcase 132:
select max(unique2) from tenk1 order by max(unique2);
--Testcase 133:
explain (costs off)
  select max(unique2) from tenk1 order by max(unique2)+1;
--Testcase 134:
select max(unique2) from tenk1 order by max(unique2)+1;
--Testcase 135:
explain (costs off)
  select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;
--Testcase 136:
select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;

-- interesting corner case: constant gets optimized into a seqscan
--Testcase 137:
explain (costs off)
  select max(100) from tenk1;
--Testcase 138:
select max(100) from tenk1;

-- try it on an inheritance tree
--Testcase 432:
create foreign table minmaxtest(f1 int) server griddb_svr;;
--Testcase 433:
create table minmaxtest1() inherits (minmaxtest);
--Testcase 434:
create table minmaxtest2() inherits (minmaxtest);
--Testcase 435:
create table minmaxtest3() inherits (minmaxtest);
--Testcase 436:
create index minmaxtest1i on minmaxtest1(f1);
--Testcase 437:
create index minmaxtest2i on minmaxtest2(f1 desc);
--Testcase 438:
create index minmaxtest3i on minmaxtest3(f1) where f1 is not null;

--Testcase 139:
insert into minmaxtest values(11), (12);
--Testcase 140:
insert into minmaxtest1 values(13), (14);
--Testcase 141:
insert into minmaxtest2 values(15), (16);
--Testcase 142:
insert into minmaxtest3 values(17), (18);

--Testcase 143:
explain (costs off)
  select min(f1), max(f1) from minmaxtest;
--Testcase 144:
select min(f1), max(f1) from minmaxtest;

-- DISTINCT doesn't do anything useful here, but it shouldn't fail
--Testcase 145:
explain (costs off)
  select distinct min(f1), max(f1) from minmaxtest;
--Testcase 146:
select distinct min(f1), max(f1) from minmaxtest;

--Testcase 439:
drop foreign table minmaxtest cascade;

-- check for correct detection of nested-aggregate errors
--Testcase 147:
select max(min(unique1)) from tenk1;
--Testcase 148:
select (select max(min(unique1)) from int8_tbl) from tenk1;

--
-- Test removal of redundant GROUP BY columns
--

--Testcase 440:
create foreign table agg_t1 (a int OPTIONS (rowkey 'true'), b int, c int, d int) server griddb_svr;
--Testcase 441:
create foreign table agg_t2 (x int OPTIONS (rowkey 'true'), y int, z int) server griddb_svr;
-- GridDB does not support deferable for primary key
-- Skip this test
-- create foreign table t3 (a int, b int, c int, primary key(a, b) deferrable);

-- Non-primary-key columns can be removed from GROUP BY
--Testcase 149:
explain (costs off) select * from agg_t1 group by a,b,c,d;

-- No removal can happen if the complete PK is not present in GROUP BY
--Testcase 150:
explain (costs off) select a,c from agg_t1 group by a,c,d;

-- Test removal across multiple relations
--Testcase 151:
explain (costs off) select *
from agg_t1 inner join agg_t2 on agg_t1.a = agg_t2.x and agg_t1.b = agg_t2.y
group by agg_t1.a,agg_t1.b,agg_t1.c,agg_t1.d,agg_t2.x,agg_t2.y,agg_t2.z;

-- Test case where agg_t1 can be optimized but not agg_t2
--Testcase 152:
explain (costs off) select agg_t1.*,agg_t2.x,agg_t2.z
from agg_t1 inner join agg_t2 on agg_t1.a = agg_t2.x and agg_t1.b = agg_t2.y
group by agg_t1.a,agg_t1.b,agg_t1.c,agg_t1.d,agg_t2.x,agg_t2.z;

-- skip this test
-- Cannot optimize when PK is deferrable
--explain (costs off) select * from t3 group by a,b,c;

--create temp table t1c () inherits (t1);

-- Ensure we don't remove any columns when t1 has a child table
--Testcase 153:
explain (costs off) select * from agg_t1 group by a,b,c,d;

-- Okay to remove columns if we're only querying the parent.
--Testcase 154:
explain (costs off) select * from only agg_t1 group by a,b,c,d;

--create temp table p_t1 (
--  a int,
--  b int,
--  c int,
-- d int,
--  primary key(a,b)
--) partition by list(a);
--create temp table p_t1_1 partition of p_t1 for values in(1);
--create temp table p_t1_2 partition of p_t1 for values in(2);

-- Ensure we can remove non-PK columns for partitioned tables.
--explain (costs off) select * from p_t1 group by a,b,c,d;

--drop table p_t1;

--
-- Test combinations of DISTINCT and/or ORDER BY
--
begin;
--Testcase 155:
delete from INT8_TBL;
--Testcase 156:
insert into INT8_TBL(q1,q2) values (1,4),(2,3),(3,1),(4,2);
--Testcase 157:
select array_agg(q1 order by q2)
  from INT8_TBL;
--Testcase 158:
select array_agg(q1 order by q1)
  from INT8_TBL;
--Testcase 159:
select array_agg(q1 order by q1 desc)
  from INT8_TBL;
--Testcase 160:
select array_agg(q2 order by q1 desc)
  from INT8_TBL;

--Testcase 161:
delete from INT8_TBL;
--Testcase 162:
insert into INT8_TBL(q1) values (1),(2),(1),(3),(null),(2);
--Testcase 163:
select array_agg(distinct q1)
  from INT8_TBL;
--Testcase 164:
select array_agg(distinct q1 order by q1)
  from INT8_TBL;
--Testcase 165:
select array_agg(distinct q1 order by q1 desc)
  from INT8_TBL;
--Testcase 166:
select array_agg(distinct q1 order by q1 desc nulls last)
  from INT8_TBL;
rollback;

-- multi-arg aggs, strict/nonstrict, distinct/order by
--Testcase 442:
create type aggtype as (a integer, b integer, c text);

--Testcase 443:
create function aggf_trans(aggtype[],integer,integer,text) returns aggtype[]
as 'select array_append($1,ROW($2,$3,$4)::aggtype)'
language sql strict immutable;

--Testcase 444:
create function aggfns_trans(aggtype[],integer,integer,text) returns aggtype[]
as 'select array_append($1,ROW($2,$3,$4)::aggtype)'
language sql immutable;

--Testcase 445:
create aggregate aggfstr(integer,integer,text) (
   sfunc = aggf_trans, stype = aggtype[],
   initcond = '{}'
);

--Testcase 446:
create aggregate aggfns(integer,integer,text) (
   sfunc = aggfns_trans, stype = aggtype[], sspace = 10000,
   initcond = '{}'
);

begin;
--Testcase 167:
insert into multi_arg_agg values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz');
--Testcase 168:
select aggfstr(a,b,c) from multi_arg_agg;
--Testcase 169:
select aggfns(a,b,c) from multi_arg_agg;

--Testcase 170:
select aggfstr(distinct a,b,c) from multi_arg_agg, generate_series(1,3) i;
--Testcase 171:
select aggfns(distinct a,b,c) from multi_arg_agg, generate_series(1,3) i;

--Testcase 172:
select aggfstr(distinct a,b,c order by b) from multi_arg_agg, generate_series(1,3) i;
--Testcase 173:
select aggfns(distinct a,b,c order by b) from multi_arg_agg, generate_series(1,3) i;

-- test specific code paths

--Testcase 174:
select aggfns(distinct a,a,c order by c using ~<~,a) from multi_arg_agg, generate_series(1,2) i;
--Testcase 175:
select aggfns(distinct a,a,c order by c using ~<~) from multi_arg_agg, generate_series(1,2) i;
--Testcase 176:
select aggfns(distinct a,a,c order by a) from multi_arg_agg, generate_series(1,2) i;
--Testcase 177:
select aggfns(distinct a,b,c order by a,c using ~<~,b) from multi_arg_agg, generate_series(1,2) i;

-- check node I/O via view creation and usage, also deparsing logic

--Testcase 447:
create view agg_view1 as
  select aggfns(a,b,c) from multi_arg_agg;

--Testcase 178:
select * from agg_view1;
--Testcase 179:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 448:
create or replace view agg_view1 as
  select aggfns(distinct a,b,c) from multi_arg_agg, generate_series(1,3) i;

--Testcase 180:
select * from agg_view1;
--Testcase 181:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 449:
create or replace view agg_view1 as
  select aggfns(distinct a,b,c order by b) from multi_arg_agg, generate_series(1,3) i;

--Testcase 182:
select * from agg_view1;
--Testcase 183:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 450:
create or replace view agg_view1 as
  select aggfns(a,b,c order by b+1) from multi_arg_agg;

--Testcase 184:
select * from agg_view1;
--Testcase 185:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 451:
create or replace view agg_view1 as
  select aggfns(a,a,c order by b) from multi_arg_agg;

--Testcase 186:
select * from agg_view1;
--Testcase 187:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 452:
create or replace view agg_view1 as
  select aggfns(a,b,c order by c using ~<~) from multi_arg_agg;

--Testcase 188:
select * from agg_view1;
--Testcase 189:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 453:
create or replace view agg_view1 as
  select aggfns(distinct a,b,c order by a,c using ~<~,b) from multi_arg_agg, generate_series(1,2) i;

--Testcase 190:
select * from agg_view1;
--Testcase 191:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 454:
drop view agg_view1;
rollback;

-- incorrect DISTINCT usage errors

--Testcase 192:
insert into multi_arg_agg values (1,1,'foo');
--Testcase 193:
select aggfns(distinct a,b,c order by i) from multi_arg_agg, generate_series(1,2) i;
--Testcase 194:
select aggfns(distinct a,b,c order by a,b+1) from multi_arg_agg, generate_series(1,2) i;
--Testcase 195:
select aggfns(distinct a,b,c order by a,b,i,c) from multi_arg_agg, generate_series(1,2) i;
--Testcase 196:
select aggfns(distinct a,a,c order by a,b) from multi_arg_agg, generate_series(1,2) i;

-- string_agg tests
begin;
--Testcase 197:
delete from multi_arg_agg;
--Testcase 198:
insert into multi_arg_agg(a,c) values (1,'aaaa'),(2,'bbbb'),(3,'cccc');
--Testcase 199:
select string_agg(c,',') from multi_arg_agg;

--Testcase 200:
delete from multi_arg_agg;
--Testcase 201:
insert into multi_arg_agg(a,c) values (1,'aaaa'),(2,null),(3,'bbbb'),(4,'cccc');
--Testcase 202:
select string_agg(c,',') from multi_arg_agg;

--Testcase 203:
delete from multi_arg_agg;
--Testcase 204:
insert into multi_arg_agg(a,c) values (1,null),(2,null),(3,'bbbb'),(4,'cccc');
--Testcase 205:
select string_agg(c,'AB') from multi_arg_agg;

--Testcase 206:
delete from multi_arg_agg;
--Testcase 207:
insert into multi_arg_agg(a,c) values (1,null),(2,null);
--Testcase 208:
select string_agg(c,',') from multi_arg_agg;
rollback;

-- check some implicit casting cases, as per bug #5564

--Testcase 209:
select string_agg(distinct f1, ',' order by f1) from varchar_tbl;  -- ok
--Testcase 210:
select string_agg(distinct f1::varchar, ',' order by f1) from varchar_tbl;  -- not ok
--Testcase 211:
select string_agg(distinct f1, ',' order by f1::varchar) from varchar_tbl;  -- not ok
--Testcase 212:
select string_agg(distinct f1::varchar, ',' order by f1::varchar) from varchar_tbl;  -- ok

-- string_agg bytea tests
--Testcase 455:
create foreign table bytea_test_table(id serial, v bytea) server griddb_svr;

--Testcase 213:
select string_agg(v, '') from bytea_test_table;

--Testcase 214:
insert into bytea_test_table(v) values(decode('ff','hex'));

--Testcase 215:
select string_agg(v, '') from bytea_test_table;

--Testcase 216:
insert into bytea_test_table(v) values(decode('aa','hex'));

--Testcase 217:
select string_agg(v, '') from bytea_test_table;
--Testcase 218:
select string_agg(v, NULL) from bytea_test_table;
--Testcase 219:
select string_agg(v, decode('ee', 'hex')) from bytea_test_table;

--Testcase 456:
drop foreign table bytea_test_table;

-- FILTER tests

--Testcase 220:
select min(unique1) filter (where unique1 > 100) from tenk1;

--Testcase 221:
select sum(1/ten) filter (where ten > 0) from tenk1;

--Testcase 222:
select ten, sum(distinct four) filter (where four::text ~ '123') from onek a
group by ten;

--Testcase 223:
select ten, sum(distinct four) filter (where four > 10) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

--Testcase 457:
create foreign table agg_t0(foo text, bar text) server griddb_svr;
--Testcase 224:
insert into agg_t0 values ('a', 'b');
--Testcase 225:
select max(foo COLLATE "C") filter (where (bar collate "POSIX") > '0')
from agg_t0;

-- outer reference in FILTER (PostgreSQL extension)
--Testcase 458:
create foreign table agg_t3 (inner_c int) server griddb_svr;
--Testcase 459:
create foreign table agg_t4 (outer_c int) server griddb_svr;

--Testcase 226:
insert into agg_t3 values (1);
--Testcase 227:
insert into agg_t4 values (2), (3);

--Testcase 228:
select (select count(*) from agg_t3) from agg_t4; -- inner query is aggregation query
--Testcase 229:
select (select count(*) filter (where outer_c <> 0) from agg_t3)
from agg_t4; -- outer query is aggregation query
--Testcase 230:
select (select count(inner_c) filter (where outer_c <> 0) from agg_t3)
from agg_t4; -- inner query is aggregation query
--Testcase 231:
select
  (select max((select i.unique2 from tenk1 i where i.unique1 = o.unique1))
     filter (where o.unique1 < 10))
from tenk1 o;					-- outer query is aggregation query

-- subquery in FILTER clause (PostgreSQL extension)
--Testcase 232:
select sum(unique1) FILTER (WHERE
  unique1 IN (SELECT unique1 FROM onek where unique1 < 100)) FROM tenk1;

-- exercise lots of aggregate parts with FILTER
begin;
--Testcase 233:
delete from multi_arg_agg;
--Testcase 234:
insert into multi_arg_agg values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz');
--Testcase 235:
select aggfns(distinct a,b,c order by a,c using ~<~,b) filter (where a > 1) from multi_arg_agg, generate_series(1,2) i;
rollback;

-- ordered-set aggregates

begin;
--Testcase 236:
delete from FLOAT8_TBL;
--Testcase 237:
insert into FLOAT8_TBL(f1) values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);
--Testcase 238:
select f1, percentile_cont(f1) within group (order by x::float8)
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

begin;
--Testcase 239:
delete from FLOAT8_TBL;
--Testcase 240:
insert into FLOAT8_TBL(f1) values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);
--Testcase 241:
select f1, percentile_cont(f1 order by f1) within group (order by x)  -- error
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

begin;
--Testcase 242:
delete from FLOAT8_TBL;
--Testcase 243:
insert into FLOAT8_TBL(f1) values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);
--Testcase 244:
select f1, sum() within group (order by x::float8)  -- error
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

begin;
--Testcase 245:
delete from FLOAT8_TBL;
--Testcase 246:
insert into FLOAT8_TBL(f1) values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);
--Testcase 247:
select f1, percentile_cont(f1,f1)  -- error
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

--Testcase 248:
select percentile_cont(0.5) within group (order by b) from aggtest;
--Testcase 249:
select percentile_cont(0.5) within group (order by b), sum(b) from aggtest;
--Testcase 250:
select percentile_cont(0.5) within group (order by thousand) from tenk1;
--Testcase 251:
select percentile_disc(0.5) within group (order by thousand) from tenk1;

begin;
--Testcase 252:
delete from INT8_TBL;
--Testcase 253:
insert into INT8_TBL(q1) values (1),(1),(2),(2),(3),(3),(4);
--Testcase 254:
select rank(3) within group (order by q1) from INT8_TBL;
--Testcase 255:
select cume_dist(3) within group (order by q1) from INT8_TBL;
rollback;
begin;
--Testcase 256:
delete from INT8_TBL;
--Testcase 257:
insert into INT8_TBL(q1) values (1),(1),(2),(2),(3),(3),(4),(5);
--Testcase 258:
select percent_rank(3) within group (order by q1) from INT8_TBL;
rollback;
begin;
--Testcase 259:
delete from INT8_TBL;
--Testcase 260:
insert into INT8_TBL(q1) values (1),(1),(2),(2),(3),(3),(4);
--Testcase 261:
select dense_rank(3) within group (order by q1) from INT8_TBL;
rollback;

--Testcase 262:
select percentile_disc(array[0,0.1,0.25,0.5,0.75,0.9,1]) within group (order by thousand)
from tenk1;
--Testcase 263:
select percentile_cont(array[0,0.25,0.5,0.75,1]) within group (order by thousand)
from tenk1;
--Testcase 264:
select percentile_disc(array[[null,1,0.5],[0.75,0.25,null]]) within group (order by thousand)
from tenk1;

--Testcase 460:
create foreign table agg_t5 (x int) server griddb_svr;
begin;
--Testcase 265:
insert into agg_t5 select * from generate_series(1,6);
--Testcase 266:
select percentile_cont(array[0,1,0.25,0.75,0.5,1,0.3,0.32,0.35,0.38,0.4]) within group (order by x)
from agg_t5;
rollback;

--Testcase 267:
select ten, mode() within group (order by string4) from tenk1 group by ten;

--Testcase 461:
create foreign table agg_t6 (id serial OPTIONS (rowkey 'true'), x text) server griddb_svr;
begin;
--Testcase 268:
insert into agg_t6(x) values (unnest('{fred,jim,fred,jack,jill,fred,jill,jim,jim,sheila,jim,sheila}'::text[]));
--Testcase 269:
select percentile_disc(array[0.25,0.5,0.75]) within group (order by x)
from agg_t6;
rollback;

-- check collation propagates up in suitable cases:
begin;
--Testcase 270:
insert into agg_t6(x) values ('fred'), ('jim');
--Testcase 271:
select pg_collation_for(percentile_disc(1) within group (order by x collate "POSIX"))
  from agg_t6;
rollback;
-- ordered-set aggs created with CREATE 
--Testcase 462:
create aggregate my_percentile_disc(float8 ORDER BY anyelement) (
  stype = internal,
  sfunc = ordered_set_transition,
  finalfunc = percentile_disc_final,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);
--Testcase 463:
create aggregate my_rank(VARIADIC "any" ORDER BY VARIADIC "any") (
  stype = internal,
  sfunc = ordered_set_transition_multi,
  finalfunc = rank_final,
  finalfunc_extra = true,
  hypothetical
);
--Testcase 537:
alter aggregate my_percentile_disc(float8 ORDER BY anyelement)
  rename to test_percentile_disc;
--Testcase 538:
alter aggregate my_rank(VARIADIC "any" ORDER BY VARIADIC "any")
  rename to test_rank;

begin;
--Testcase 272:
delete from INT8_TBL;
--Testcase 273:
insert into INT8_TBL(q1) values (1),(1),(2),(2),(3),(3),(4);
--Testcase 274:
select test_rank(3) within group (order by q1) from INT8_TBL;
rollback;

--Testcase 275:
select test_percentile_disc(0.5) within group (order by thousand) from tenk1;

-- ordered-set aggs can't use ungrouped vars in direct args:
begin;
--Testcase 276:
insert into agg_t5(x) select * from generate_series(1,5);
--Testcase 277:
select rank(x) within group (order by x) from agg_t5;
rollback;

-- outer-level agg can't use a grouped arg of a lower level, either:

begin;
--Testcase 278:
insert into agg_t5(x) select * from generate_series(1,5);
--Testcase 279:
select array(select percentile_disc(a) within group (order by x)
               from (values (0.3),(0.7)) v(a) group by a)
  from agg_t5;
rollback;

-- agg in the direct args is a grouping violation, too:
begin;
--Testcase 280:
insert into agg_t5(x) select * from generate_series(1,5);
--Testcase 281:
select rank(sum(x)) within group (order by x) from agg_t5;
rollback;

-- hypothetical-set type unification and argument-count failures:
begin;
--Testcase 282:
insert into agg_t6(x) values ('fred'), ('jim');
--Testcase 283:
select rank(3) within group (order by x) from agg_t6;
rollback;

--Testcase 284:
select rank(3) within group (order by stringu1,stringu2) from tenk1;

begin;
--Testcase 285:
insert into agg_t5 select * from generate_series(1,5);
--Testcase 286:
select rank('fred') within group (order by x) from agg_t5;
rollback;

begin;
--Testcase 287:
insert into agg_t6(x) values ('fred'), ('jim');
--Testcase 288:
select rank('adam'::text collate "C") within group (order by x collate "POSIX")
  from agg_t6;
rollback;

-- hypothetical-set type unification successes:
begin;
--Testcase 289:
insert into agg_t6(x) values ('fred'), ('jim');
--Testcase 290:
select rank('adam'::varchar) within group (order by x) from agg_t6;
rollback;

begin;
--Testcase 291:
insert into agg_t5 select * from generate_series(1,5);
--Testcase 292:
select rank('3') within group (order by x) from agg_t5;
rollback;

-- divide by zero check
begin;
--Testcase 293:
insert into agg_t5 select * from generate_series(1,5);
--Testcase 294:
select percent_rank(0) within group (order by x) from agg_t5;
rollback;


-- deparse and multiple features:
--Testcase 464:
create view aggordview1 as
select ten,
       percentile_disc(0.5) within group (order by thousand) as p50,
       percentile_disc(0.5) within group (order by thousand) filter (where hundred=1) as px,
       rank(5,'AZZZZ',50) within group (order by hundred, string4 desc, hundred)
  from tenk1
 group by ten order by ten;

--Testcase 296:
select pg_get_viewdef('aggordview1');
--Testcase 297:
select * from aggordview1 order by ten;
--Testcase 465:
drop view aggordview1;

-- variadic aggregates
--Testcase 466:
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';

--Testcase 467:
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);

--Testcase 468:
create function cleast_accum(anycompatible, variadic anycompatiblearray)
returns anycompatible language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';

--Testcase 469:
create aggregate cleast_agg(variadic items anycompatiblearray) (
  stype = anycompatible, sfunc = cleast_accum
);

--Testcase 298:
select least_agg(q1,q2) from int8_tbl;
--Testcase 299:
select least_agg(variadic array[q1,q2]) from int8_tbl;

--Testcase 470:
select cleast_agg(q1,q2) from int8_tbl;
--Testcase 471:
select cleast_agg(4.5,f1) from int4_tbl;
--Testcase 472:
select cleast_agg(variadic array[4.5,f1]) from int4_tbl;
--Testcase 473:
select pg_typeof(cleast_agg(variadic array[4.5,f1])) from int4_tbl;

--Testcase 474:
drop aggregate least_agg(variadic items anyarray);
--Testcase 475:
drop function least_accum(anyelement, variadic anyarray);

-- test aggregates with common transition functions share the same states
begin work;

--Testcase 476:
create type avg_state as (total bigint, count bigint);

--Testcase 477:
create or replace function avg_transfn(state avg_state, n bigint) returns avg_state as
$$
declare new_state avg_state;
begin
	raise notice 'avg_transfn called with %', n;
	if state is null then
		if n is not null then
			new_state.total := n;
			new_state.count := 1;
			return new_state;
		end if;
		return null;
	elsif n is not null then
		state.total := state.total + n;
		state.count := state.count + 1;
		return state;
	end if;

	return null;
end
$$ language plpgsql;

--Testcase 478:
create function avg_finalfn(state avg_state) returns bigint as
$$
begin
	if state is null then
		return NULL;
	else
		return state.total / state.count;
	end if;
end
$$ language plpgsql;

--Testcase 479:
create function sum_finalfn(state avg_state) returns bigint as
$$
begin
	if state is null then
		return NULL;
	else
		return state.total;
	end if;
end
$$ language plpgsql;

--Testcase 480:
create aggregate my_avg(bigint)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = avg_finalfn
);

--Testcase 481:
create aggregate my_sum(bigint)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = sum_finalfn
);

-- aggregate state should be shared as aggs are the same.
--Testcase 300:
delete from int8_tbl;
--Testcase 301:
insert into int8_tbl(q1) values (1),(3);
--Testcase 302:
select my_avg(q1),my_avg(q1) from int8_tbl;

-- aggregate state should be shared as transfn is the same for both aggs.
--Testcase 303:
delete from int8_tbl;
--Testcase 304:
insert into int8_tbl(q1) values (1),(3);
--Testcase 305:
select my_avg(q1),my_sum(q1) from int8_tbl;

-- same as previous one, but with DISTINCT, which requires sorting the input.
--Testcase 306:
delete from int8_tbl;
--Testcase 307:
insert into int8_tbl(q1) values (1),(3),(1);
--Testcase 308:
select my_avg(distinct q1),my_sum(distinct q1) from int8_tbl;

-- shouldn't share states due to the distinctness not matching.
--Testcase 309:
delete from int8_tbl;
--Testcase 310:
insert into int8_tbl(q1) values (1),(3);
--Testcase 311:
select my_avg(distinct q1),my_sum(q1) from int8_tbl;

-- shouldn't share states due to the filter clause not matching.
--Testcase 312:
delete from int8_tbl;
--Testcase 313:
insert into int8_tbl(q1) values (1),(3);
--Testcase 314:
select my_avg(q1) filter (where q1 > 1),my_sum(q1) from int8_tbl;

-- this should not share the state due to different input columns.
--Testcase 315:
delete from int8_tbl;
--Testcase 316:
insert into int8_tbl(q1,q2) values (1,2),(3,4);
--Testcase 317:
select my_avg(q1),my_sum(q2) from int8_tbl;

-- exercise cases where OSAs share state
--Testcase 318:
delete from int8_tbl;
--Testcase 319:
insert into int8_tbl(q1) values (1::float8),(3),(5),(7);
--Testcase 320:
select
  percentile_cont(0.5) within group (order by q1),
  percentile_disc(0.5) within group (order by q1)
from int8_tbl;

--Testcase 321:
select
  percentile_cont(0.25) within group (order by q1),
  percentile_disc(0.5) within group (order by q1)
from int8_tbl;

-- these can't share state currently
--Testcase 322:
select
  rank(4) within group (order by q1),
  dense_rank(4) within group (order by q1)
from int8_tbl;

-- test that aggs with the same sfunc and initcond share the same agg state
--Testcase 482:
create aggregate my_sum_init(int8)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = sum_finalfn,
   initcond = '(10,0)'
);

--Testcase 483:
create aggregate my_avg_init(int8)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = avg_finalfn,
   initcond = '(10,0)'
);

--Testcase 484:
create aggregate my_avg_init2(int8)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = avg_finalfn,
   initcond = '(4,0)'
);

-- state should be shared if INITCONDs are matching
--Testcase 323:
delete from int8_tbl;
--Testcase 324:
insert into int8_tbl(q1) values (1),(3);
--Testcase 325:
select my_sum_init(q1),my_avg_init(q1) from int8_tbl;

-- Varying INITCONDs should cause the states not to be shared.
--Testcase 326:
select my_sum_init(q1),my_avg_init2(q1) from int8_tbl;

rollback;

-- test aggregate state sharing to ensure it works if one aggregate has a
-- finalfn and the other one has none.
begin work;

--Testcase 485:
create or replace function sum_transfn(state int8, n int8) returns int8 as
$$
declare new_state int8;
begin
	raise notice 'sum_transfn called with %', n;
	if state is null then
		if n is not null then
			new_state := n;
			return new_state;
		end if;
		return null;
	elsif n is not null then
		state := state + n;
		return state;
	end if;

	return null;
end
$$ language plpgsql;

--Testcase 486:
create function halfsum_finalfn(state int8) returns int8 as
$$
begin
	if state is null then
		return NULL;
	else
		return state / 2;
	end if;
end
$$ language plpgsql;

--Testcase 487:
create aggregate my_sum(int8)
(
   stype = int8,
   sfunc = sum_transfn
);

--Testcase 488:
create aggregate my_half_sum(int8)
(
   stype = int8,
   sfunc = sum_transfn,
   finalfunc = halfsum_finalfn
);

-- Agg state should be shared even though my_sum has no finalfn
--Testcase 327:
delete from int8_tbl;
--Testcase 328:
insert into int8_tbl(q1) values (1),(2),(3),(4);
--Testcase 329:
select my_sum(q1),my_half_sum(q1) from int8_tbl;

rollback;


-- test that the aggregate transition logic correctly handles
-- transition / combine functions returning NULL

-- First test the case of a normal transition function returning NULL
BEGIN;
--Testcase 489:
CREATE FUNCTION balkifnull(int8, int4)
RETURNS int8
STRICT
LANGUAGE plpgsql AS $$
BEGIN
    IF $1 IS NULL THEN
       RAISE 'erroneously called with NULL argument';
    END IF;
    RETURN NULL;
END$$;

--Testcase 490:
CREATE AGGREGATE balk(int4)
(
    SFUNC = balkifnull(int8, int4),
    STYPE = int8,
    PARALLEL = SAFE,
    INITCOND = '0'
);

--Testcase 330:
SELECT balk(hundred) FROM tenk1;

ROLLBACK;

-- Secondly test the case of a parallel aggregate combiner function
-- returning NULL. For that use normal transition function, but a
-- combiner function returning NULL.
BEGIN ISOLATION LEVEL REPEATABLE READ;
--Testcase 491:
CREATE FUNCTION balkifnull(int8, int8)
RETURNS int8
PARALLEL SAFE
STRICT
LANGUAGE plpgsql AS $$
BEGIN
    IF $1 IS NULL THEN
       RAISE 'erroneously called with NULL argument';
    END IF;
    RETURN NULL;
END$$;

--Testcase 492:
CREATE AGGREGATE balk(int4)
(
    SFUNC = int4_sum(int8, int4),
    STYPE = int8,
    COMBINEFUNC = balkifnull(int8, int8),
    PARALLEL = SAFE,
    INITCOND = '0'
);

-- Skip this test case, cannot alter tenk1.
-- force use of parallelism
-- ALTER TABLE tenk1 set (parallel_workers = 4);
-- SET LOCAL parallel_setup_cost=0;
--SET LOCAL max_parallel_workers_per_gather=4;

--EXPLAIN (COSTS OFF) SELECT balk(hundred) FROM tenk1;
--SELECT balk(hundred) FROM tenk1;

ROLLBACK;

-- test coverage for aggregate combine/serial/deserial functions
BEGIN ISOLATION LEVEL REPEATABLE READ;

--Testcase 539:
SET parallel_setup_cost = 0;
--Testcase 540:
SET parallel_tuple_cost = 0;
--Testcase 541:
SET min_parallel_table_scan_size = 0;
--Testcase 542:
SET max_parallel_workers_per_gather = 4;
--Testcase 543:
SET parallel_leader_participation = off;
--Testcase 544:
SET enable_indexonlyscan = off;

-- variance(int4) covers numeric_poly_combine
-- sum(int8) covers int8_avg_combine
-- regr_count(float8, float8) covers int8inc_float8_float8 and aggregates with > 1 arg
--Testcase 331:
EXPLAIN (COSTS OFF, VERBOSE)
  SELECT variance(unique1::int4), sum(unique1::int8), regr_count(unique1::float8, unique1::float8)
  FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

--Testcase 332:
SELECT variance(unique1::int4), sum(unique1::int8), regr_count(unique1::float8, unique1::float8)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

-- variance(int8) covers numeric_combine
-- avg(numeric) covers numeric_avg_combine
--Testcase 493:
EXPLAIN (COSTS OFF, VERBOSE)
SELECT variance(unique1::int8), avg(unique1::numeric)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

--Testcase 494:
SELECT variance(unique1::int8), avg(unique1::numeric)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;
ROLLBACK;

-- test coverage for dense_rank
BEGIN;
--Testcase 333:
DELETE FROM INT8_TBL;
--Testcase 334:
INSERT INTO INT8_TBL(q1) VALUES (1),(1),(2),(2),(3),(3);
--Testcase 335:
SELECT dense_rank(q1) WITHIN GROUP (ORDER BY q1) FROM INT8_TBL GROUP BY (q1) ORDER BY 1;
ROLLBACK;

-- Ensure that the STRICT checks for aggregates does not take NULLness
-- of ORDER BY columns into account. See bug report around
-- 2a505161-2727-2473-7c46-591ed108ac52@email.cz
begin;
--Testcase 336:
insert into INT8_TBL(q1, q2) values (1, NULL);
--Testcase 337:
SELECT min(x ORDER BY y) FROM INT8_TBL AS d(x,y);
rollback;

begin;
--Testcase 338:
insert into INT8_TBL(q1, q2) values (1, 2);
--Testcase 339:
SELECT min(x ORDER BY y) FROM INT8_TBL AS d(x,y);
rollback;

-- check collation-sensitive matching between grouping expressions
begin;
--Testcase 340:
insert into agg_t6(x) values (unnest(array['a','b']));
--Testcase 341:
select x||'a', case x||'a' when 'aa' then 1 else 0 end, count(*)
  from agg_t6 group by x||'a' order by 1;
rollback;

begin;
--Testcase 342:
insert into agg_t6(x) values (unnest(array['a','b']));
--Testcase 343:
select x||'a', case when x||'a' = 'aa' then 1 else 0 end, count(*)
  from agg_t6 group by x||'a' order by 1;
rollback;

-- Make sure that generation of HashAggregate for uniqification purposes
-- does not lead to array overflow due to unexpected duplicate hash keys
-- see CAFeeJoKKu0u+A_A9R9316djW-YW3-+Gtgvy3ju655qRHR3jtdA@mail.gmail.com
--Testcase 344:
explain (costs off)
  select 1 from tenk1
   where (hundred, thousand) in (select twothousand, twothousand from onek);

--
-- Hash Aggregation Spill tests
--

--Testcase 545:
set enable_sort=false;
--Testcase 546:
set work_mem='64kB';

--Testcase 495:
select unique1, count(*), sum(twothousand) from tenk1
group by unique1
having sum(fivethous) > 4975
order by sum(twothousand);

--Testcase 547:
set work_mem to default;
--Testcase 548:
set enable_sort to default;

--
-- Compare results between plans using sorting and plans using hash
-- aggregation. Force spilling in both cases by setting work_mem low.
--

--Testcase 549:
set work_mem='64kB';

--Testcase 496:
create foreign table agg_data_2k (g int) server griddb_svr;
--Testcase 497:
create foreign table agg_data_20k (g int) server griddb_svr;

--Testcase 498:
create foreign table agg_group_1(id serial OPTIONS (rowkey 'true'), c1 int, c2 float8, c3 int) server griddb_svr;
--Testcase 499:
create foreign table agg_group_2(id serial OPTIONS (rowkey 'true'), a int, c1 float8, c2 text, c3 int) server griddb_svr;
--Testcase 500:
create foreign table agg_group_3(id serial OPTIONS (rowkey 'true'), c1 float8, c2 int4, c3 int) server griddb_svr;
--Testcase 501:
create foreign table agg_group_4(id serial OPTIONS (rowkey 'true'), c1 float8, c2 text, c3 int) server griddb_svr;

--Testcase 502:
create foreign table agg_hash_1(id serial OPTIONS (rowkey 'true'), c1 int, c2 float8, c3 int) server griddb_svr;
--Testcase 503:
create foreign table agg_hash_2(id serial OPTIONS (rowkey 'true'), a int, c1 float8, c2 text, c3 int) server griddb_svr;
--Testcase 504:
create foreign table agg_hash_3(id serial OPTIONS (rowkey 'true'), c1 float8, c2 int4, c3 int) server griddb_svr;
--Testcase 505:
create foreign table agg_hash_4(id serial OPTIONS (rowkey 'true'), c1 float8, c2 text, c3 int) server griddb_svr;

--Testcase 506:
insert into agg_data_2k select g from generate_series(0, 1999) g;
-- analyze agg_data_2k;

--Testcase 507:
insert into agg_data_20k select g from generate_series(0, 19999) g;
-- analyze agg_data_20k;

-- Produce results with sorting.

--Testcase 550:
set enable_hashagg = false;

--Testcase 551:
set jit_above_cost = 0;

--Testcase 508:
explain (costs off)
select g%10000 as c1, sum(g::float8) as c2, count(*) as c3
  from agg_data_20k group by g%10000;

--Testcase 509:
insert into agg_group_1(c1, c2, c3)
select g%10000 as c1, sum(g::float8) as c2, count(*) as c3
  from agg_data_20k group by g%10000;

--Testcase 510:
insert into agg_group_2(a, c1, c2, c3)
select * from
  (values (100), (300), (500)) as r(a),
  lateral (
    select (g/2)::float8 as c1,
           array_agg(g::float8) as c2,
	   count(*) as c3
    from agg_data_2k
    where g < r.a
    group by g/2) as s;

--Testcase 511:
insert into agg_group_3(c1, c2, c3)
select (g/2)::float8 as c1, sum(7::int4) as c2, count(*) as c3
  from agg_data_2k group by g/2;

--Testcase 512:
insert into agg_group_4(c1, c2, c3)
select (g/2)::float8 as c1, array_agg(g::float8) as c2, count(*) as c3
  from agg_data_2k group by g/2;

-- Produce results with hash aggregation

--Testcase 552:
set enable_hashagg = true;
--Testcase 553:
set enable_sort = false;

--Testcase 554:
set jit_above_cost = 0;

--Testcase 513:
explain (costs off)
select g%10000 as c1, sum(g::float8) as c2, count(*) as c3
  from agg_data_20k group by g%10000;

--Testcase 514:
insert into agg_hash_1(c1, c2, c3)
select g%10000 as c1, sum(g::float8) as c2, count(*) as c3
  from agg_data_20k group by g%10000;

--Testcase 515:
insert into agg_hash_2(a, c1, c2, c3)
select * from
  (values (100), (300), (500)) as r(a),
  lateral (
    select (g/2)::float8 as c1,
           array_agg(g::float8) as c2,
	   count(*) as c3
    from agg_data_2k
    where g < r.a
    group by g/2) as s;

--Testcase 555:
set jit_above_cost to default;

--Testcase 516:
insert into agg_hash_3(c1, c2, c3)
select (g/2)::float8 as c1, sum(7::int4) as c2, count(*) as c3
  from agg_data_2k group by g/2;

--Testcase 517:
insert into agg_hash_4(c1, c2, c3)
select (g/2)::float8 as c1, array_agg(g::float8) as c2, count(*) as c3
  from agg_data_2k group by g/2;

--Testcase 556:
set enable_sort = true;
--Testcase 557:
set work_mem to default;

-- Compare group aggregation results to hash aggregation results

--Testcase 518:
(select c1, c2, c3 from agg_hash_1 except select c1, c2, c3 from agg_group_1)
  union all
(select c1, c2, c3 from agg_group_1 except select c1, c2, c3 from agg_hash_1);

--Testcase 519:
(select a, c1, c2, c3 from agg_hash_2 except select a, c1, c2, c3 from agg_group_2)
  union all
(select a, c1, c2, c3 from agg_group_2 except select a, c1, c2, c3 from agg_hash_2);

--Testcase 520:
(select c1, c2, c3 from agg_hash_3 except select c1, c2, c3 from agg_group_3)
  union all
(select c1, c2, c3 from agg_group_3 except select c1, c2, c3 from agg_hash_3);

--Testcase 521:
(select c1, c2, c3 from agg_hash_4 except select c1, c2, c3 from agg_group_4)
  union all
(select c1, c2, c3 from agg_group_4 except select c1, c2, c3 from agg_hash_4);

--Testcase 522:
drop foreign table agg_data_2k;
--Testcase 523:
drop foreign table agg_data_20k;
--Testcase 524:
drop foreign table agg_group_1;
--Testcase 525:
drop foreign table agg_group_2;
--Testcase 526:
drop foreign table agg_group_3;
--Testcase 527:
drop foreign table agg_group_4;
--Testcase 528:
drop foreign table agg_hash_1;
--Testcase 529:
drop foreign table agg_hash_2;
--Testcase 530:
drop foreign table agg_hash_3;
--Testcase 531:
drop foreign table agg_hash_4;

DO $d$
declare
  l_rec record;
begin
  for l_rec in (select foreign_table_schema, foreign_table_name 
                from information_schema.foreign_tables) loop
     execute format('drop foreign table %I.%I cascade;', l_rec.foreign_table_schema, l_rec.foreign_table_name);
  end loop;
end;
$d$;
--Testcase 532:
DROP USER MAPPING FOR public SERVER griddb_svr;
--Testcase 533:
DROP SERVER griddb_svr;
--Testcase 534:
DROP EXTENSION griddb_fdw CASCADE;
