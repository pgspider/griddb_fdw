--
-- AGGREGATES
--
CREATE EXTENSION griddb_fdw;
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS(host '239.0.0.1', port '31999', clustername 'griddbfdwTestCluster');
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS(username 'admin', password 'testadmin');
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

CREATE FOREIGN TABLE aggtest (
  id      int4,
  a       int2,
  b     float4
) SERVER griddb_svr;

CREATE FOREIGN TABLE student (
  name    text,
  age     int4,
  location  text,
  gpa     float8
) SERVER griddb_svr;

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

CREATE FOREIGN TABLE multi_arg_agg (a int OPTIONS (rowkey 'true'), b int, c text) SERVER griddb_svr;

CREATE FOREIGN TABLE INT4_TBL(id serial OPTIONS (rowkey 'true'), f1 int4) SERVER griddb_svr; 

CREATE FOREIGN TABLE INT8_TBL(id serial OPTIONS (rowkey 'true'), q1 int8 , q2 int8) SERVER griddb_svr; 

CREATE FOREIGN TABLE VARCHAR_TBL(f1 text OPTIONS (rowkey 'true')) SERVER griddb_svr;

CREATE FOREIGN TABLE FLOAT8_TBL(id serial OPTIONS (rowkey 'true'), f1 float8) SERVER griddb_svr;

CREATE FOREIGN TABLE FLOAT8_TMP(id serial OPTIONS (rowkey 'true'), f1 float8, f2 float8) SERVER griddb_svr;

-- avoid bit-exact output here because operations may not be bit-exact.
SET extra_float_digits = 0;

SELECT avg(four) AS avg_1 FROM onek;

SELECT avg(a) AS avg_32 FROM aggtest WHERE a < 100;

-- In 7.1, avg(float4) is computed using float8 arithmetic.
-- Round the result to 3 digits to avoid platform-specific results.

SELECT avg(b)::numeric(10,3) AS avg_107_943 FROM aggtest;

SELECT avg(gpa) AS avg_3_4 FROM ONLY student;


SELECT sum(four) AS sum_1500 FROM onek;
SELECT sum(a) AS sum_198 FROM aggtest;
SELECT sum(b) AS avg_431_773 FROM aggtest;
SELECT sum(gpa) AS avg_6_8 FROM ONLY student;

SELECT max(four) AS max_3 FROM onek;
SELECT max(a) AS max_100 FROM aggtest;
SELECT max(aggtest.b) AS max_324_78 FROM aggtest;
SELECT max(student.gpa) AS max_3_7 FROM student;

SELECT stddev_pop(b) FROM aggtest;
SELECT stddev_samp(b) FROM aggtest;
SELECT var_pop(b) FROM aggtest;
SELECT var_samp(b) FROM aggtest;

SELECT stddev_pop(b::numeric) FROM aggtest;
SELECT stddev_samp(b::numeric) FROM aggtest;
SELECT var_pop(b::numeric) FROM aggtest;
SELECT var_samp(b::numeric) FROM aggtest;

-- skip this test, the above tests already covered
-- population variance is defined for a single tuple, sample variance
-- is not
--SELECT var_pop(1.0), var_samp(2.0);
--SELECT stddev_pop(3.0::numeric), stddev_samp(4.0::numeric);

-- verify correct results for null and NaN inputs
begin;
delete from INT4_TBL;
insert into INT4_TBL select * from generate_series(1,3);
select sum(null::int4) from INT4_TBL;
select sum(null::int8) from INT4_TBL;
select sum(null::numeric) from INT4_TBL;
select sum(null::float8) from INT4_TBL;
select avg(null::int4) from INT4_TBL;
select avg(null::int8) from INT4_TBL;
select avg(null::numeric) from INT4_TBL;
select avg(null::float8) from INT4_TBL;
select sum('NaN'::numeric) from INT4_TBL;
select avg('NaN'::numeric) from INT4_TBL;
rollback;

-- verify correct results for infinite inputs
BEGIN;
DELETE FROM FLOAT8_TBL;
INSERT INTO FLOAT8_TBL(f1) VALUES ('1'), ('infinity');
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
DELETE FROM FLOAT8_TBL;
INSERT INTO FLOAT8_TBL(f1) VALUES ('infinity'), ('1');
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
DELETE FROM FLOAT8_TBL;
INSERT INTO FLOAT8_TBL(f1) VALUES ('infinity'), ('infinity');
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
DELETE FROM FLOAT8_TBL;
INSERT INTO FLOAT8_TBL(f1) VALUES ('-infinity'), ('infinity');
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

-- test accuracy with a large input offset
BEGIN;
DELETE FROM FLOAT8_TBL;
INSERT INTO FLOAT8_TBL(f1) VALUES ('100000003'), ('100000004'), 
				('100000006'), ('100000007');
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
DELETE FROM FLOAT8_TBL;
INSERT INTO FLOAT8_TBL(f1) VALUES ('7000000000005'), ('7000000000007');
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

-- SQL2003 binary aggregates
SELECT regr_count(b, a) FROM aggtest;
SELECT regr_sxx(b, a) FROM aggtest;
SELECT regr_syy(b, a) FROM aggtest;
SELECT regr_sxy(b, a) FROM aggtest;
SELECT regr_avgx(b, a), regr_avgy(b, a) FROM aggtest;
SELECT regr_r2(b, a) FROM aggtest;
SELECT regr_slope(b, a), regr_intercept(b, a) FROM aggtest;
SELECT covar_pop(b, a), covar_samp(b, a) FROM aggtest;
SELECT corr(b, a) FROM aggtest;

-- test accum and combine functions directly
CREATE FOREIGN TABLE regr_test (id serial OPTIONS (rowkey 'true'), x int4, y int4) SERVER griddb_svr;
INSERT INTO regr_test(x, y) VALUES (10,150),(20,250),(30,350),(80,540),(100,200);
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (10,20,30,80);
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test;

CREATE FOREIGN TABLE regr_test_array (id serial OPTIONS (rowkey 'true'), x float8[], y float8[]) SERVER griddb_svr;
BEGIN;
INSERT INTO regr_test_array(x) VALUES ('{4,140,2900}'::float8[]);
SELECT float8_accum(x, 100) FROM regr_test_array;
ROLLBACK;

BEGIN;
INSERT INTO regr_test_array(x) VALUES ('{4,140,2900,1290,83075,15050}'::float8[]);
SELECT float8_regr_accum(x, 200, 100) FROM regr_test_array;
ROLLBACK;

SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (10,20,30);
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (80,100);

BEGIN;
INSERT INTO regr_test_array(x,y) VALUES ('{3,60,200}'::float8[], '{0,0,0}'::float8[]);
SELECT float8_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;
INSERT INTO regr_test_array(x,y) VALUES ('{0,0,0}'::float8[], '{2,180,200}'::float8[]);
SELECT float8_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;
INSERT INTO regr_test_array(x,y) VALUES ('{3,60,200}'::float8[], '{2,180,200}'::float8[]);
SELECT float8_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;
INSERT INTO regr_test_array(x,y) VALUES ('{3,60,200,750,20000,2000}'::float8[],
                           '{0,0,0,0,0,0}'::float8[]);
SELECT float8_regr_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;
INSERT INTO regr_test_array(x,y) VALUES ('{0,0,0,0,0,0}'::float8[],
                           '{2,180,200,740,57800,-3400}'::float8[]);
SELECT float8_regr_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;
INSERT INTO regr_test_array(x,y) VALUES ('{3,60,200,750,20000,2000}'::float8[],
                           '{2,180,200,740,57800,-3400}'::float8[]);
SELECT float8_regr_combine(x, y) FROM regr_test_array;
ROLLBACK;

DROP FOREIGN TABLE regr_test;
DROP FOREIGN TABLE regr_test_array;

-- test count, distinct
SELECT count(four) AS cnt_1000 FROM onek;
SELECT count(DISTINCT four) AS cnt_4 FROM onek;

select ten, count(*), sum(four) from onek
group by ten order by ten;

select ten, count(four), sum(DISTINCT four) from onek
group by ten order by ten;

-- user-defined aggregates
CREATE AGGREGATE newavg (
   sfunc = int4_avg_accum, basetype = int4, stype = _int8,
   finalfunc = int8_avg,
   initcond1 = '{0,0}'
);

CREATE AGGREGATE newsum (
   sfunc1 = int4pl, basetype = int4, stype1 = int4,
   initcond1 = '0'
);

CREATE AGGREGATE newcnt (*) (
   sfunc = int8inc, stype = int8,
   initcond = '0', parallel = safe
);

CREATE AGGREGATE newcnt ("any") (
   sfunc = int8inc_any, stype = int8,
   initcond = '0'
);

CREATE AGGREGATE oldcnt (
   sfunc = int8inc, basetype = 'ANY', stype = int8,
   initcond = '0'
);

create function sum3(int8,int8,int8) returns int8 as
'select $1 + $2 + $3' language sql strict immutable;

create aggregate sum2(int8,int8) (
   sfunc = sum3, stype = int8,
   initcond = '0'
);

SELECT newavg(four) AS avg_1 FROM onek;
SELECT newsum(four) AS sum_1500 FROM onek;
SELECT newcnt(four) AS cnt_1000 FROM onek;
SELECT newcnt(*) AS cnt_1000 FROM onek;
SELECT oldcnt(*) AS cnt_1000 FROM onek;
SELECT sum2(q1,q2) FROM int8_tbl;

-- test for outer-level aggregates

-- this should work
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

-- this should fail because subquery has an agg of its own in WHERE
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b
               where sum(distinct a.four + b.four) = b.four);

-- Test handling of sublinks within outer-level aggregates.
-- Per bug report from Daniel Grace.
select
  (select max((select i.unique2 from tenk1 i where i.unique1 = o.unique1)))
from tenk1 o;

-- Test handling of Params within aggregate arguments in hashed aggregation.
-- Per bug report from Jeevan Chalke.
BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) values (generate_series(1, 3));
explain (verbose, costs off)
select s1.f1, ss.f1, sm
from INT4_TBL s1,
     lateral (select s2.f1, sum(s1.f1 + s2.f1) sm
              from INT4_TBL s2 group by s2.f1) ss
order by 1, 2;
select s1.f1, ss.f1, sm
from INT4_TBL s1,
     lateral (select s2.f1, sum(s1.f1 + s2.f1) sm
              from INT4_TBL s2 group by s2.f1) ss
order by 1, 2;

explain (verbose, costs off)
select array(select sum(x.f1+y.f1) s
            from INT4_TBL y group by y.f1 order by s)
  from INT4_TBL x;
select array(select sum(x.f1+y.f1) s
            from INT4_TBL y group by y.f1 order by s)
  from INT4_TBL x;
ROLLBACK;

--
-- test for bitwise integer aggregates
--
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
SELECT
  BIT_AND(i2) AS "?",
  BIT_OR(i4)  AS "?"
FROM bitwise_test;

INSERT INTO bitwise_test(i2, i4, i8, i, x, y) VALUES
  (1, 1, 1, 1, 1, B'0101'),
  (3, 3, 3, null, 2, B'0100'),
  (7, 7, 7, 3, 4, B'1100');

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

INSERT INTO bool_test_a(a1, a2, a3, a4, a5, a6, a7, a8, a9) VALUES 
(NULL, TRUE, FALSE, NULL, NULL, TRUE, TRUE, FALSE, FALSE);
INSERT INTO bool_test_b(b1, b2, b3, b4, b5, b6, b7, b8, b9) VALUES 
(NULL, NULL, NULL, TRUE, FALSE, TRUE, FALSE, TRUE, FALSE);

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

CREATE FOREIGN TABLE bool_test(
  id serial OPTIONS (rowkey 'true'),
  b1 BOOL,
  b2 BOOL,
  b3 BOOL,
  b4 BOOL
) SERVER griddb_svr;

-- empty case
SELECT
  BOOL_AND(b1)   AS "n",
  BOOL_OR(b3)    AS "n"
FROM bool_test;

INSERT INTO bool_test(b1, b2, b3, b4) VALUES
  (TRUE, null, FALSE, null),
  (FALSE, TRUE, null, null),
  (null, TRUE, FALSE, null);

SELECT
  BOOL_AND(b1)     AS "f",
  BOOL_AND(b2)     AS "t",
  BOOL_AND(b3)     AS "f",
  BOOL_AND(b4)     AS "n",
  BOOL_AND(NOT b2) AS "f",
  BOOL_AND(NOT b3) AS "t"
FROM bool_test;

SELECT
  EVERY(b1)     AS "f",
  EVERY(b2)     AS "t",
  EVERY(b3)     AS "f",
  EVERY(b4)     AS "n",
  EVERY(NOT b2) AS "f",
  EVERY(NOT b3) AS "t"
FROM bool_test;

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
explain (costs off)
  select min(unique1) from tenk1;
select min(unique1) from tenk1;
explain (costs off)
  select max(unique1) from tenk1;
select max(unique1) from tenk1;
explain (costs off)
  select max(unique1) from tenk1 where unique1 < 42;
select max(unique1) from tenk1 where unique1 < 42;
explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42;
select max(unique1) from tenk1 where unique1 > 42;

-- the planner may choose a generic aggregate here if parallel query is
-- enabled, since that plan will be parallel safe and the "optimized"
-- plan, which has almost identical cost, will not be.  we want to test
-- the optimized plan, so temporarily disable parallel query.
begin;
set local max_parallel_workers_per_gather = 0;
explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42000;
select max(unique1) from tenk1 where unique1 > 42000;
rollback;

-- multi-column index (uses tenk1_thous_tenthous)
explain (costs off)
  select max(tenthous) from tenk1 where thousand = 33;
select max(tenthous) from tenk1 where thousand = 33;
explain (costs off)
  select min(tenthous) from tenk1 where thousand = 33;
select min(tenthous) from tenk1 where thousand = 33;

-- check parameter propagation into an indexscan subquery
explain (costs off)
  select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
    from int4_tbl;
select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
  from int4_tbl;

-- check some cases that were handled incorrectly in 8.3.0
explain (costs off)
  select distinct max(unique2) from tenk1;
select distinct max(unique2) from tenk1;
explain (costs off)
  select max(unique2) from tenk1 order by 1;
select max(unique2) from tenk1 order by 1;
explain (costs off)
  select max(unique2) from tenk1 order by max(unique2);
select max(unique2) from tenk1 order by max(unique2);
explain (costs off)
  select max(unique2) from tenk1 order by max(unique2)+1;
select max(unique2) from tenk1 order by max(unique2)+1;
explain (costs off)
  select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;
select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;

-- interesting corner case: constant gets optimized into a seqscan
explain (costs off)
  select max(100) from tenk1;
select max(100) from tenk1;

-- try it on an inheritance tree
create foreign table minmaxtest(f1 int) server griddb_svr;;
create table minmaxtest1() inherits (minmaxtest);
create table minmaxtest2() inherits (minmaxtest);
create table minmaxtest3() inherits (minmaxtest);
create index minmaxtest1i on minmaxtest1(f1);
create index minmaxtest2i on minmaxtest2(f1 desc);
create index minmaxtest3i on minmaxtest3(f1) where f1 is not null;

insert into minmaxtest values(11), (12);
insert into minmaxtest1 values(13), (14);
insert into minmaxtest2 values(15), (16);
insert into minmaxtest3 values(17), (18);

explain (costs off)
  select min(f1), max(f1) from minmaxtest;
select min(f1), max(f1) from minmaxtest;

-- DISTINCT doesn't do anything useful here, but it shouldn't fail
explain (costs off)
  select distinct min(f1), max(f1) from minmaxtest;
select distinct min(f1), max(f1) from minmaxtest;

drop foreign table minmaxtest cascade;

-- check for correct detection of nested-aggregate errors
select max(min(unique1)) from tenk1;
select (select max(min(unique1)) from int8_tbl) from tenk1;

--
-- Test removal of redundant GROUP BY columns
--

create foreign table agg_t1 (a int OPTIONS (rowkey 'true'), b int, c int, d int) server griddb_svr;
create foreign table agg_t2 (x int OPTIONS (rowkey 'true'), y int, z int) server griddb_svr;
-- GridDB does not support deferable for primary key
-- Skip this test
-- create foreign table t3 (a int, b int, c int, primary key(a, b) deferrable);

-- Non-primary-key columns can be removed from GROUP BY
explain (costs off) select * from agg_t1 group by a,b,c,d;

-- No removal can happen if the complete PK is not present in GROUP BY
explain (costs off) select a,c from agg_t1 group by a,c,d;

-- Test removal across multiple relations
explain (costs off) select *
from agg_t1 inner join agg_t2 on agg_t1.a = agg_t2.x and agg_t1.b = agg_t2.y
group by agg_t1.a,agg_t1.b,agg_t1.c,agg_t1.d,agg_t2.x,agg_t2.y,agg_t2.z;

-- Test case where agg_t1 can be optimized but not agg_t2
explain (costs off) select agg_t1.*,agg_t2.x,agg_t2.z
from agg_t1 inner join agg_t2 on agg_t1.a = agg_t2.x and agg_t1.b = agg_t2.y
group by agg_t1.a,agg_t1.b,agg_t1.c,agg_t1.d,agg_t2.x,agg_t2.z;

-- skip this test
-- Cannot optimize when PK is deferrable
--explain (costs off) select * from t3 group by a,b,c;

--create temp table t1c () inherits (t1);

-- Ensure we don't remove any columns when t1 has a child table
explain (costs off) select * from agg_t1 group by a,b,c,d;

-- Okay to remove columns if we're only querying the parent.
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
delete from INT8_TBL;
insert into INT8_TBL(q1,q2) values (1,4),(2,3),(3,1),(4,2);
select array_agg(q1 order by q2)
  from INT8_TBL;
select array_agg(q1 order by q1)
  from INT8_TBL;
select array_agg(q1 order by q1 desc)
  from INT8_TBL;
select array_agg(q2 order by q1 desc)
  from INT8_TBL;

delete from INT8_TBL;
insert into INT8_TBL(q1) values (1),(2),(1),(3),(null),(2);
select array_agg(distinct q1)
  from INT8_TBL;
select array_agg(distinct q1 order by q1)
  from INT8_TBL;
select array_agg(distinct q1 order by q1 desc)
  from INT8_TBL;
select array_agg(distinct q1 order by q1 desc nulls last)
  from INT8_TBL;
rollback;

-- multi-arg aggs, strict/nonstrict, distinct/order by
create type aggtype as (a integer, b integer, c text);

create function aggf_trans(aggtype[],integer,integer,text) returns aggtype[]
as 'select array_append($1,ROW($2,$3,$4)::aggtype)'
language sql strict immutable;

create function aggfns_trans(aggtype[],integer,integer,text) returns aggtype[]
as 'select array_append($1,ROW($2,$3,$4)::aggtype)'
language sql immutable;

create aggregate aggfstr(integer,integer,text) (
   sfunc = aggf_trans, stype = aggtype[],
   initcond = '{}'
);

create aggregate aggfns(integer,integer,text) (
   sfunc = aggfns_trans, stype = aggtype[], sspace = 10000,
   initcond = '{}'
);

begin;
insert into multi_arg_agg values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz');
select aggfstr(a,b,c) from multi_arg_agg;
select aggfns(a,b,c) from multi_arg_agg;

select aggfstr(distinct a,b,c) from multi_arg_agg, generate_series(1,3) i;
select aggfns(distinct a,b,c) from multi_arg_agg, generate_series(1,3) i;

select aggfstr(distinct a,b,c order by b) from multi_arg_agg, generate_series(1,3) i;
select aggfns(distinct a,b,c order by b) from multi_arg_agg, generate_series(1,3) i;

-- test specific code paths

select aggfns(distinct a,a,c order by c using ~<~,a) from multi_arg_agg, generate_series(1,2) i;
select aggfns(distinct a,a,c order by c using ~<~) from multi_arg_agg, generate_series(1,2) i;
select aggfns(distinct a,a,c order by a) from multi_arg_agg, generate_series(1,2) i;
select aggfns(distinct a,b,c order by a,c using ~<~,b) from multi_arg_agg, generate_series(1,2) i;

-- check node I/O via view creation and usage, also deparsing logic

create view agg_view1 as
  select aggfns(a,b,c) from multi_arg_agg;

select * from agg_view1;
select pg_get_viewdef('agg_view1'::regclass);

create or replace view agg_view1 as
  select aggfns(distinct a,b,c) from multi_arg_agg, generate_series(1,3) i;

select * from agg_view1;
select pg_get_viewdef('agg_view1'::regclass);

create or replace view agg_view1 as
  select aggfns(distinct a,b,c order by b) from multi_arg_agg, generate_series(1,3) i;

select * from agg_view1;
select pg_get_viewdef('agg_view1'::regclass);

create or replace view agg_view1 as
  select aggfns(a,b,c order by b+1) from multi_arg_agg;

select * from agg_view1;
select pg_get_viewdef('agg_view1'::regclass);

create or replace view agg_view1 as
  select aggfns(a,a,c order by b) from multi_arg_agg;

select * from agg_view1;
select pg_get_viewdef('agg_view1'::regclass);

create or replace view agg_view1 as
  select aggfns(a,b,c order by c using ~<~) from multi_arg_agg;

select * from agg_view1;
select pg_get_viewdef('agg_view1'::regclass);

create or replace view agg_view1 as
  select aggfns(distinct a,b,c order by a,c using ~<~,b) from multi_arg_agg, generate_series(1,2) i;

select * from agg_view1;
select pg_get_viewdef('agg_view1'::regclass);

drop view agg_view1;
rollback;

-- incorrect DISTINCT usage errors

insert into multi_arg_agg values (1,1,'foo');
select aggfns(distinct a,b,c order by i) from multi_arg_agg, generate_series(1,2) i;
select aggfns(distinct a,b,c order by a,b+1) from multi_arg_agg, generate_series(1,2) i;
select aggfns(distinct a,b,c order by a,b,i,c) from multi_arg_agg, generate_series(1,2) i;
select aggfns(distinct a,a,c order by a,b) from multi_arg_agg, generate_series(1,2) i;

-- string_agg tests
begin;
delete from multi_arg_agg;
insert into multi_arg_agg(a,c) values (1,'aaaa'),(2,'bbbb'),(3,'cccc');
select string_agg(c,',') from multi_arg_agg;

delete from multi_arg_agg;
insert into multi_arg_agg(a,c) values (1,'aaaa'),(2,null),(3,'bbbb'),(4,'cccc');
select string_agg(c,',') from multi_arg_agg;

delete from multi_arg_agg;
insert into multi_arg_agg(a,c) values (1,null),(2,null),(3,'bbbb'),(4,'cccc');
select string_agg(c,'AB') from multi_arg_agg;

delete from multi_arg_agg;
insert into multi_arg_agg(a,c) values (1,null),(2,null);
select string_agg(c,',') from multi_arg_agg;
rollback;

-- check some implicit casting cases, as per bug #5564

select string_agg(distinct f1, ',' order by f1) from varchar_tbl;  -- ok
select string_agg(distinct f1::varchar, ',' order by f1) from varchar_tbl;  -- not ok
select string_agg(distinct f1, ',' order by f1::varchar) from varchar_tbl;  -- not ok
select string_agg(distinct f1::varchar, ',' order by f1::varchar) from varchar_tbl;  -- ok

-- string_agg bytea tests
create foreign table bytea_test_table(id serial, v bytea) server griddb_svr;

select string_agg(v, '') from bytea_test_table;

insert into bytea_test_table(v) values(decode('ff','hex'));

select string_agg(v, '') from bytea_test_table;

insert into bytea_test_table(v) values(decode('aa','hex'));

select string_agg(v, '') from bytea_test_table;
select string_agg(v, NULL) from bytea_test_table;
select string_agg(v, decode('ee', 'hex')) from bytea_test_table;

drop foreign table bytea_test_table;

-- FILTER tests

select min(unique1) filter (where unique1 > 100) from tenk1;

select sum(1/ten) filter (where ten > 0) from tenk1;

select ten, sum(distinct four) filter (where four::text ~ '123') from onek a
group by ten;

select ten, sum(distinct four) filter (where four > 10) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

create foreign table agg_t0(foo text, bar text) server griddb_svr;
insert into agg_t0 values ('a', 'b');
select max(foo COLLATE "C") filter (where (bar collate "POSIX") > '0')
from agg_t0;

-- outer reference in FILTER (PostgreSQL extension)
create foreign table agg_t3 (inner_c int) server griddb_svr;
create foreign table agg_t4 (outer_c int) server griddb_svr;

insert into agg_t3 values (1);
insert into agg_t4 values (2), (3);

select (select count(*) from agg_t3) from agg_t4; -- inner query is aggregation query
select (select count(*) filter (where outer_c <> 0) from agg_t3)
from agg_t4; -- outer query is aggregation query
select (select count(inner_c) filter (where outer_c <> 0) from agg_t3)
from agg_t4; -- inner query is aggregation query
select
  (select max((select i.unique2 from tenk1 i where i.unique1 = o.unique1))
     filter (where o.unique1 < 10))
from tenk1 o;					-- outer query is aggregation query

-- subquery in FILTER clause (PostgreSQL extension)
select sum(unique1) FILTER (WHERE
  unique1 IN (SELECT unique1 FROM onek where unique1 < 100)) FROM tenk1;

-- exercise lots of aggregate parts with FILTER
begin;
delete from multi_arg_agg;
insert into multi_arg_agg values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz');
select aggfns(distinct a,b,c order by a,c using ~<~,b) filter (where a > 1) from multi_arg_agg, generate_series(1,2) i;
rollback;

-- ordered-set aggregates

begin;
delete from FLOAT8_TBL;
insert into FLOAT8_TBL(f1) values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);
select f1, percentile_cont(f1) within group (order by x::float8)
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

begin;
delete from FLOAT8_TBL;
insert into FLOAT8_TBL(f1) values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);
select f1, percentile_cont(f1 order by f1) within group (order by x)  -- error
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

begin;
delete from FLOAT8_TBL;
insert into FLOAT8_TBL(f1) values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);
select f1, sum() within group (order by x::float8)  -- error
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

begin;
delete from FLOAT8_TBL;
insert into FLOAT8_TBL(f1) values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);
select f1, percentile_cont(f1,f1)  -- error
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

select percentile_cont(0.5) within group (order by b) from aggtest;
select percentile_cont(0.5) within group (order by b), sum(b) from aggtest;
select percentile_cont(0.5) within group (order by thousand) from tenk1;
select percentile_disc(0.5) within group (order by thousand) from tenk1;

begin;
delete from INT8_TBL;
insert into INT8_TBL(q1) values (1),(1),(2),(2),(3),(3),(4);
select rank(3) within group (order by q1) from INT8_TBL;
select cume_dist(3) within group (order by q1) from INT8_TBL;
rollback;
begin;
delete from INT8_TBL;
insert into INT8_TBL(q1) values (1),(1),(2),(2),(3),(3),(4),(5);
select percent_rank(3) within group (order by q1) from INT8_TBL;
rollback;
begin;
delete from INT8_TBL;
insert into INT8_TBL(q1) values (1),(1),(2),(2),(3),(3),(4);
select dense_rank(3) within group (order by q1) from INT8_TBL;
rollback;

select percentile_disc(array[0,0.1,0.25,0.5,0.75,0.9,1]) within group (order by thousand)
from tenk1;
select percentile_cont(array[0,0.25,0.5,0.75,1]) within group (order by thousand)
from tenk1;
select percentile_disc(array[[null,1,0.5],[0.75,0.25,null]]) within group (order by thousand)
from tenk1;

create foreign table agg_t5 (x int) server griddb_svr;
begin;
insert into agg_t5 select * from generate_series(1,6);
select percentile_cont(array[0,1,0.25,0.75,0.5,1,0.3,0.32,0.35,0.38,0.4]) within group (order by x)
from agg_t5;
rollback;

select ten, mode() within group (order by string4) from tenk1 group by ten;

create foreign table agg_t6 (id serial OPTIONS (rowkey 'true'), x text) server griddb_svr;
begin;
insert into agg_t6(x) values (unnest('{fred,jim,fred,jack,jill,fred,jill,jim,jim,sheila,jim,sheila}'::text[]));
select percentile_disc(array[0.25,0.5,0.75]) within group (order by x)
from agg_t6;
rollback;

-- check collation propagates up in suitable cases:
begin;
insert into agg_t6(x) values ('fred'), ('jim');
select pg_collation_for(percentile_disc(1) within group (order by x collate "POSIX"))
  from agg_t6;
rollback;
-- ordered-set aggs created with CREATE 
create aggregate my_percentile_disc(float8 ORDER BY anyelement) (
  stype = internal,
  sfunc = ordered_set_transition,
  finalfunc = percentile_disc_final,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);
create aggregate my_rank(VARIADIC "any" ORDER BY VARIADIC "any") (
  stype = internal,
  sfunc = ordered_set_transition_multi,
  finalfunc = rank_final,
  finalfunc_extra = true,
  hypothetical
);
alter aggregate my_percentile_disc(float8 ORDER BY anyelement)
  rename to test_percentile_disc;
alter aggregate my_rank(VARIADIC "any" ORDER BY VARIADIC "any")
  rename to test_rank;

begin;
delete from INT8_TBL;
insert into INT8_TBL(q1) values (1),(1),(2),(2),(3),(3),(4);
select test_rank(3) within group (order by q1) from INT8_TBL;
rollback;

select test_percentile_disc(0.5) within group (order by thousand) from tenk1;

-- ordered-set aggs can't use ungrouped vars in direct args:
begin;
insert into agg_t5(x) select * from generate_series(1,5);
select rank(x) within group (order by x) from agg_t5;
rollback;

-- outer-level agg can't use a grouped arg of a lower level, either:

begin;
insert into agg_t5(x) select * from generate_series(1,5);
select array(select percentile_disc(a) within group (order by x)
               from (values (0.3),(0.7)) v(a) group by a)
  from agg_t5;
rollback;

-- agg in the direct args is a grouping violation, too:
begin;
insert into agg_t5(x) select * from generate_series(1,5);
select rank(sum(x)) within group (order by x) from agg_t5;
rollback;

-- hypothetical-set type unification and argument-count failures:
begin;
insert into agg_t6(x) values ('fred'), ('jim');
select rank(3) within group (order by x) from agg_t6;
rollback;

select rank(3) within group (order by stringu1,stringu2) from tenk1;

begin;
insert into agg_t5 select * from generate_series(1,5);
select rank('fred') within group (order by x) from agg_t5;
rollback;

begin;
insert into agg_t6(x) values ('fred'), ('jim');
select rank('adam'::text collate "C") within group (order by x collate "POSIX")
  from agg_t6;
rollback;

-- hypothetical-set type unification successes:
begin;
insert into agg_t6(x) values ('fred'), ('jim');
select rank('adam'::varchar) within group (order by x) from agg_t6;
rollback;

begin;
insert into agg_t5 select * from generate_series(1,5);
select rank('3') within group (order by x) from agg_t5;
rollback;

-- divide by zero check
begin;
insert into agg_t5 select * from generate_series(1,5);
select percent_rank(0) within group (order by x) from agg_t5;
rollback;


-- deparse and multiple features:
create view aggordview1 as
select ten,
       percentile_disc(0.5) within group (order by thousand) as p50,
       percentile_disc(0.5) within group (order by thousand) filter (where hundred=1) as px,
       rank(5,'AZZZZ',50) within group (order by hundred, string4 desc, hundred)
  from tenk1
 group by ten order by ten;

select pg_get_viewdef('aggordview1');
select * from aggordview1 order by ten;
drop view aggordview1;

-- variadic aggregates
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';

create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);
select least_agg(q1,q2) from int8_tbl;
select least_agg(variadic array[q1,q2]) from int8_tbl;

drop aggregate least_agg(variadic items anyarray);
drop function least_accum(anyelement, variadic anyarray);

-- test aggregates with common transition functions share the same states
begin work;

create type avg_state as (total bigint, count bigint);

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

create aggregate my_avg(bigint)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = avg_finalfn
);

create aggregate my_sum(bigint)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = sum_finalfn
);

-- aggregate state should be shared as aggs are the same.
delete from int8_tbl;
insert into int8_tbl(q1) values (1),(3);
select my_avg(q1),my_avg(q1) from int8_tbl;

-- aggregate state should be shared as transfn is the same for both aggs.
delete from int8_tbl;
insert into int8_tbl(q1) values (1),(3);
select my_avg(q1),my_sum(q1) from int8_tbl;

-- same as previous one, but with DISTINCT, which requires sorting the input.
delete from int8_tbl;
insert into int8_tbl(q1) values (1),(3),(1);
select my_avg(distinct q1),my_sum(distinct q1) from int8_tbl;

-- shouldn't share states due to the distinctness not matching.
delete from int8_tbl;
insert into int8_tbl(q1) values (1),(3);
select my_avg(distinct q1),my_sum(q1) from int8_tbl;

-- shouldn't share states due to the filter clause not matching.
delete from int8_tbl;
insert into int8_tbl(q1) values (1),(3);
select my_avg(q1) filter (where q1 > 1),my_sum(q1) from int8_tbl;

-- this should not share the state due to different input columns.
delete from int8_tbl;
insert into int8_tbl(q1,q2) values (1,2),(3,4);
select my_avg(q1),my_sum(q2) from int8_tbl;

-- exercise cases where OSAs share state
delete from int8_tbl;
insert into int8_tbl(q1) values (1::float8),(3),(5),(7);
select
  percentile_cont(0.5) within group (order by q1),
  percentile_disc(0.5) within group (order by q1)
from int8_tbl;

select
  percentile_cont(0.25) within group (order by q1),
  percentile_disc(0.5) within group (order by q1)
from int8_tbl;

-- these can't share state currently
select
  rank(4) within group (order by q1),
  dense_rank(4) within group (order by q1)
from int8_tbl;

-- test that aggs with the same sfunc and initcond share the same agg state
create aggregate my_sum_init(int8)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = sum_finalfn,
   initcond = '(10,0)'
);

create aggregate my_avg_init(int8)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = avg_finalfn,
   initcond = '(10,0)'
);

create aggregate my_avg_init2(int8)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = avg_finalfn,
   initcond = '(4,0)'
);

-- state should be shared if INITCONDs are matching
delete from int8_tbl;
insert into int8_tbl(q1) values (1),(3);
select my_sum_init(q1),my_avg_init(q1) from int8_tbl;

-- Varying INITCONDs should cause the states not to be shared.
select my_sum_init(q1),my_avg_init2(q1) from int8_tbl;

rollback;

-- test aggregate state sharing to ensure it works if one aggregate has a
-- finalfn and the other one has none.
begin work;

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

create aggregate my_sum(int8)
(
   stype = int8,
   sfunc = sum_transfn
);

create aggregate my_half_sum(int8)
(
   stype = int8,
   sfunc = sum_transfn,
   finalfunc = halfsum_finalfn
);

-- Agg state should be shared even though my_sum has no finalfn
delete from int8_tbl;
insert into int8_tbl(q1) values (1),(2),(3),(4);
select my_sum(q1),my_half_sum(q1) from int8_tbl;

rollback;


-- test that the aggregate transition logic correctly handles
-- transition / combine functions returning NULL

-- First test the case of a normal transition function returning NULL
BEGIN;
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

CREATE AGGREGATE balk(int4)
(
    SFUNC = balkifnull(int8, int4),
    STYPE = int8,
    PARALLEL = SAFE,
    INITCOND = '0'
);

SELECT balk(hundred) FROM tenk1;

ROLLBACK;

-- Secondly test the case of a parallel aggregate combiner function
-- returning NULL. For that use normal transition function, but a
-- combiner function returning NULL.
BEGIN ISOLATION LEVEL REPEATABLE READ;
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

SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 4;
SET enable_indexonlyscan = off;

-- variance(int4) covers numeric_poly_combine
-- sum(int8) covers int8_avg_combine
-- regr_count(float8, float8) covers int8inc_float8_float8 and aggregates with > 1 arg
EXPLAIN (COSTS OFF, VERBOSE)
  SELECT variance(unique1::int4), sum(unique1::int8), regr_count(unique1::float8, unique1::float8) FROM tenk1;

SELECT variance(unique1::int4), sum(unique1::int8), regr_count(unique1::float8, unique1::float8) FROM tenk1;

ROLLBACK;

-- test coverage for dense_rank
BEGIN;
DELETE FROM INT8_TBL;
INSERT INTO INT8_TBL(q1) VALUES (1),(1),(2),(2),(3),(3);
SELECT dense_rank(q1) WITHIN GROUP (ORDER BY q1) FROM INT8_TBL GROUP BY (q1) ORDER BY 1;
ROLLBACK;

-- Ensure that the STRICT checks for aggregates does not take NULLness
-- of ORDER BY columns into account. See bug report around
-- 2a505161-2727-2473-7c46-591ed108ac52@email.cz
begin;
insert into INT8_TBL(q1, q2) values (1, NULL);
SELECT min(x ORDER BY y) FROM INT8_TBL AS d(x,y);
rollback;

begin;
insert into INT8_TBL(q1, q2) values (1, 2);
SELECT min(x ORDER BY y) FROM INT8_TBL AS d(x,y);
rollback;

-- check collation-sensitive matching between grouping expressions
begin;
insert into agg_t6(x) values (unnest(array['a','b']));
select x||'a', case x||'a' when 'aa' then 1 else 0 end, count(*)
  from agg_t6 group by x||'a' order by 1;
rollback;

begin;
insert into agg_t6(x) values (unnest(array['a','b']));
select x||'a', case when x||'a' = 'aa' then 1 else 0 end, count(*)
  from agg_t6 group by x||'a' order by 1;
rollback;

-- Make sure that generation of HashAggregate for uniqification purposes
-- does not lead to array overflow due to unexpected duplicate hash keys
-- see CAFeeJoKKu0u+A_A9R9316djW-YW3-+Gtgvy3ju655qRHR3jtdA@mail.gmail.com
explain (costs off)
  select 1 from tenk1
   where (hundred, thousand) in (select twothousand, twothousand from onek);

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
DROP USER MAPPING FOR public SERVER griddb_svr;
DROP SERVER griddb_svr;
DROP EXTENSION griddb_fdw CASCADE;
