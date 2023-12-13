--
-- AGGREGATES
--
\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 1:
CREATE EXTENSION griddb_fdw;

--Testcase 2:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');

--Testcase 3:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

--Testcase 4:
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

--Testcase 5:
CREATE FOREIGN TABLE aggtest (
  id      int4,
  a       int2,
  b     float4
) SERVER griddb_svr;

--Testcase 6:
CREATE FOREIGN TABLE student (
  name    text,
  age     int4,
  location  text,
  gpa     float8
) SERVER griddb_svr;

--Testcase 7:
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

--Testcase 8:
CREATE FOREIGN TABLE multi_arg_agg (a int OPTIONS (rowkey 'true'), b int, c text) SERVER griddb_svr;

--Testcase 9:
CREATE FOREIGN TABLE INT4_TBL(id serial OPTIONS (rowkey 'true'), f1 int4) SERVER griddb_svr; 

--Testcase 10:
CREATE FOREIGN TABLE INT8_TBL(id serial OPTIONS (rowkey 'true'), q1 int8 , q2 int8) SERVER griddb_svr; 

--Testcase 11:
CREATE FOREIGN TABLE VARCHAR_TBL(f1 text OPTIONS (rowkey 'true')) SERVER griddb_svr;

--Testcase 12:
CREATE FOREIGN TABLE FLOAT8_TBL(id serial OPTIONS (rowkey 'true'), f1 float8) SERVER griddb_svr;

--Testcase 13:
CREATE FOREIGN TABLE FLOAT8_TMP(id serial OPTIONS (rowkey 'true'), f1 float8, f2 float8) SERVER griddb_svr;

-- avoid bit-exact output here because operations may not be bit-exact.

--Testcase 14:
SET extra_float_digits = 0;

--Testcase 15:
SELECT avg(four) AS avg_1 FROM onek;

--Testcase 16:
SELECT avg(a) AS avg_32 FROM aggtest WHERE a < 100;

CREATE FOREIGN TABLE v1 (id int OPTIONS (rowkey 'true'), v int) SERVER griddb_svr OPTIONS (table_name 'v1');
CREATE FOREIGN TABLE v2 (id int OPTIONS (rowkey 'true'), v text[]) SERVER griddb_svr OPTIONS (table_name 'v2');

INSERT INTO v1 (id, v) VALUES (1, 1), (2, 2), (3, 3);
SELECT any_value(v) FROM v1;
DELETE FROM v1;

INSERT INTO v1 (id, v) VALUES (4, NULL);
SELECT any_value(v) FROM v1;
DELETE FROM v1;

INSERT INTO v1 (id, v) VALUES (5, NULL), (6, 1), (7, 2);
SELECT any_value(v) FROM v1;
DELETE FROM v1;

INSERT INTO v2 (id, v) VALUES (8, array['hello', 'world']);
SELECT any_value(v) FROM v2;
DELETE FROM v1;

-- In 7.1, avg(float4) is computed using float8 arithmetic.
-- Round the result to 3 digits to avoid platform-specific results.

--Testcase 17:
SELECT avg(b)::numeric(10,3) AS avg_107_943 FROM aggtest;

--Testcase 18:
SELECT avg(gpa) AS avg_3_4 FROM ONLY student;


--Testcase 19:
SELECT sum(four) AS sum_1500 FROM onek;

--Testcase 20:
SELECT sum(a) AS sum_198 FROM aggtest;

--Testcase 21:
SELECT sum(b) AS avg_431_773 FROM aggtest;

--Testcase 22:
SELECT sum(gpa) AS avg_6_8 FROM ONLY student;

--Testcase 23:
SELECT max(four) AS max_3 FROM onek;

--Testcase 24:
SELECT max(a) AS max_100 FROM aggtest;

--Testcase 25:
SELECT max(aggtest.b) AS max_324_78 FROM aggtest;

--Testcase 26:
SELECT max(student.gpa) AS max_3_7 FROM student;

--Testcase 27:
SELECT stddev_pop(b) FROM aggtest;

--Testcase 28:
SELECT stddev_samp(b) FROM aggtest;

--Testcase 29:
SELECT var_pop(b) FROM aggtest;

--Testcase 30:
SELECT var_samp(b) FROM aggtest;

--Testcase 31:
SELECT stddev_pop(b::numeric) FROM aggtest;

--Testcase 32:
SELECT stddev_samp(b::numeric) FROM aggtest;

--Testcase 33:
SELECT var_pop(b::numeric) FROM aggtest;

--Testcase 34:
SELECT var_samp(b::numeric) FROM aggtest;

-- population variance is defined for a single tuple, sample variance
-- is not
BEGIN;

--Testcase 35:
DELETE FROM FLOAT8_TBL;

--Testcase 36:
INSERT INTO FLOAT8_TBL(f1) VALUES (1.0::float8);

--Testcase 37:
SELECT var_pop(f1::float8), var_samp(f1::float8) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 38:
DELETE FROM FLOAT8_TBL;

--Testcase 39:
INSERT INTO FLOAT8_TBL(f1) VALUES (3.0::float8);

--Testcase 40:
SELECT stddev_pop(f1::float8), stddev_samp(f1::float8) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 41:
DELETE FROM FLOAT8_TBL;

--Testcase 42:
INSERT INTO FLOAT8_TBL(f1) VALUES ('inf'::float8);

--Testcase 43:
SELECT var_pop(f1::float8), var_samp(f1::float8) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 44:
DELETE FROM FLOAT8_TBL;

--Testcase 45:
INSERT INTO FLOAT8_TBL(f1) VALUES ('inf'::float8);

--Testcase 46:
SELECT stddev_pop(f1::float8), stddev_samp(f1::float8) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 47:
DELETE FROM FLOAT8_TBL;

--Testcase 48:
INSERT INTO FLOAT8_TBL(f1) VALUES ('nan'::float8);

--Testcase 49:
SELECT var_pop(f1::float8), var_samp(f1::float8) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 50:
DELETE FROM FLOAT8_TBL;

--Testcase 51:
INSERT INTO FLOAT8_TBL(f1) VALUES ('nan'::float8);

--Testcase 52:
SELECT stddev_pop(f1::float8), stddev_samp(f1::float8) FROM FLOAT8_TBL;
ROLLBACK;

--Testcase 53:
CREATE FOREIGN TABLE FLOAT4_TBL(id serial OPTIONS (rowkey 'true'), f1 float4) SERVER griddb_svr;

BEGIN;

--Testcase 54:
DELETE FROM FLOAT4_TBL;

--Testcase 55:
INSERT INTO FLOAT4_TBL(f1) VALUES (1.0::float4);

--Testcase 56:
SELECT var_pop(f1::float4), var_samp(f1::float4) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 57:
DELETE FROM FLOAT4_TBL;

--Testcase 58:
INSERT INTO FLOAT4_TBL(f1) VALUES (3.0::float4);

--Testcase 59:
SELECT stddev_pop(f1::float4), stddev_samp(f1::float4) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 60:
DELETE FROM FLOAT4_TBL;

--Testcase 61:
INSERT INTO FLOAT4_TBL(f1) VALUES ('inf'::float4);

--Testcase 62:
SELECT var_pop(f1::float4), var_samp(f1::float4) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 63:
DELETE FROM FLOAT4_TBL;

--Testcase 64:
INSERT INTO FLOAT4_TBL(f1) VALUES ('inf'::float4);

--Testcase 65:
SELECT stddev_pop(f1::float4), stddev_samp(f1::float4) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 66:
DELETE FROM FLOAT4_TBL;

--Testcase 67:
INSERT INTO FLOAT4_TBL(f1) VALUES ('nan'::float4);

--Testcase 68:
SELECT var_pop(f1::float4), var_samp(f1::float4) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 69:
DELETE FROM FLOAT4_TBL;

--Testcase 70:
INSERT INTO FLOAT4_TBL(f1) VALUES ('nan'::float4);

--Testcase 71:
SELECT stddev_pop(f1::float4), stddev_samp(f1::float4) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 72:
DELETE FROM FLOAT4_TBL;

--Testcase 73:
INSERT INTO FLOAT4_TBL(f1) VALUES (1.0);

--Testcase 74:
SELECT var_pop(f1::numeric), var_samp(f1::numeric) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 75:
DELETE FROM FLOAT4_TBL;

--Testcase 76:
INSERT INTO FLOAT4_TBL(f1) VALUES (3.0);

--Testcase 77:
SELECT stddev_pop(f1::numeric), stddev_samp(f1::numeric) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 78:
DELETE FROM FLOAT4_TBL;

--Testcase 79:
INSERT INTO FLOAT4_TBL(f1) VALUES ('inf');

--Testcase 80:
SELECT var_pop(f1::numeric), var_samp(f1::numeric) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 81:
DELETE FROM FLOAT4_TBL;

--Testcase 82:
INSERT INTO FLOAT4_TBL(f1) VALUES ('inf');

--Testcase 83:
SELECT stddev_pop(f1::numeric), stddev_samp(f1::numeric) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 84:
DELETE FROM FLOAT4_TBL;

--Testcase 85:
INSERT INTO FLOAT4_TBL(f1) VALUES ('nan');

--Testcase 86:
SELECT var_pop(f1::numeric), var_samp(f1::numeric) FROM FLOAT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 87:
DELETE FROM FLOAT4_TBL;

--Testcase 88:
INSERT INTO FLOAT4_TBL(f1) VALUES ('nan');

--Testcase 89:
SELECT stddev_pop(f1::numeric), stddev_samp(f1::numeric) FROM FLOAT4_TBL;
ROLLBACK;

--Testcase 90:
DROP FOREIGN TABLE FLOAT4_TBL;

-- verify correct results for null and NaN inputs
begin;

--Testcase 91:
delete from INT4_TBL;

--Testcase 92:
insert into INT4_TBL select * from generate_series(1,3);

--Testcase 93:
select sum(null::int4) from INT4_TBL;

--Testcase 94:
select sum(null::int8) from INT4_TBL;

--Testcase 95:
select sum(null::numeric) from INT4_TBL;

--Testcase 96:
select sum(null::float8) from INT4_TBL;

--Testcase 97:
select avg(null::int4) from INT4_TBL;

--Testcase 98:
select avg(null::int8) from INT4_TBL;

--Testcase 99:
select avg(null::numeric) from INT4_TBL;

--Testcase 100:
select avg(null::float8) from INT4_TBL;

--Testcase 101:
select sum('NaN'::numeric) from INT4_TBL;

--Testcase 102:
select avg('NaN'::numeric) from INT4_TBL;
rollback;

-- verify correct results for infinite inputs
BEGIN;

--Testcase 103:
DELETE FROM FLOAT8_TBL;

--Testcase 104:
INSERT INTO FLOAT8_TBL(f1) VALUES ('1'), ('infinity');

--Testcase 105:
SELECT sum(f1), avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 106:
DELETE FROM FLOAT8_TBL;

--Testcase 107:
INSERT INTO FLOAT8_TBL(f1) VALUES ('infinity'), ('1');

--Testcase 108:
SELECT sum(f1), avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 109:
DELETE FROM FLOAT8_TBL;

--Testcase 110:
INSERT INTO FLOAT8_TBL(f1) VALUES ('infinity'), ('infinity');

--Testcase 111:
SELECT sum(f1), avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 112:
DELETE FROM FLOAT8_TBL;

--Testcase 113:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-infinity'), ('infinity');

--Testcase 114:
SELECT sum(f1), avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 115:
DELETE FROM FLOAT8_TBL;

--Testcase 116:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-infinity'), ('-infinity');

--Testcase 117:
SELECT sum(f1), avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 118:
DELETE FROM FLOAT8_TBL;

--Testcase 119:
INSERT INTO FLOAT8_TBL(f1) VALUES ('1'), ('infinity');

--Testcase 120:
SELECT sum(f1::numeric), avg(f1::numeric), var_pop(f1::numeric) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 121:
DELETE FROM FLOAT8_TBL;

--Testcase 122:
INSERT INTO FLOAT8_TBL(f1) VALUES ('infinity'), ('1');

--Testcase 123:
SELECT sum(f1::numeric), avg(f1::numeric), var_pop(f1::numeric) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 124:
DELETE FROM FLOAT8_TBL;

--Testcase 125:
INSERT INTO FLOAT8_TBL(f1) VALUES ('infinity'), ('infinity');

--Testcase 126:
SELECT sum(f1::numeric), avg(f1::numeric), var_pop(f1::numeric) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 127:
DELETE FROM FLOAT8_TBL;

--Testcase 128:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-infinity'), ('infinity');

--Testcase 129:
SELECT sum(f1::numeric), avg(f1::numeric), var_pop(f1::numeric) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 130:
DELETE FROM FLOAT8_TBL;

--Testcase 131:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-infinity'), ('-infinity');

--Testcase 132:
SELECT sum(f1::numeric), avg(f1::numeric), var_pop(f1::numeric) FROM FLOAT8_TBL;
ROLLBACK;

-- test accuracy with a large input offset
BEGIN;

--Testcase 133:
DELETE FROM FLOAT8_TBL;

--Testcase 134:
INSERT INTO FLOAT8_TBL(f1) VALUES ('100000003'), ('100000004'), 
				('100000006'), ('100000007');

--Testcase 135:
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 136:
DELETE FROM FLOAT8_TBL;

--Testcase 137:
INSERT INTO FLOAT8_TBL(f1) VALUES ('7000000000005'), ('7000000000007');

--Testcase 138:
SELECT avg(f1), var_pop(f1) FROM FLOAT8_TBL;
ROLLBACK;

-- SQL2003 binary aggregates

--Testcase 139:
SELECT regr_count(b, a) FROM aggtest;

--Testcase 140:
SELECT regr_sxx(b, a) FROM aggtest;

--Testcase 141:
SELECT regr_syy(b, a) FROM aggtest;

--Testcase 142:
SELECT regr_sxy(b, a) FROM aggtest;

--Testcase 143:
SELECT regr_avgx(b, a), regr_avgy(b, a) FROM aggtest;

--Testcase 144:
SELECT regr_r2(b, a) FROM aggtest;

--Testcase 145:
SELECT regr_slope(b, a), regr_intercept(b, a) FROM aggtest;

--Testcase 146:
SELECT covar_pop(b, a), covar_samp(b, a) FROM aggtest;

--Testcase 147:
SELECT corr(b, a) FROM aggtest;

-- check single-tuple behavior
BEGIN;

--Testcase 148:
DELETE FROM FLOAT8_TMP;

--Testcase 149:
INSERT INTO FLOAT8_TMP(f1, f2) VALUES (1::float8,2::float8);

--Testcase 150:
SELECT covar_pop(f1::float8, f2::float8), covar_samp(f1::float8, f2::float8) FROM FLOAT8_TMP;
ROLLBACK;

BEGIN;

--Testcase 151:
DELETE FROM FLOAT8_TMP;

--Testcase 152:
INSERT INTO FLOAT8_TMP(f1, f2) VALUES (1::float8, 'inf'::float8);

--Testcase 153:
SELECT covar_pop(f1::float8, f2::float8), covar_samp(f1::float8, f2::float8) FROM FLOAT8_TMP;
ROLLBACK;

BEGIN;

--Testcase 154:
DELETE FROM FLOAT8_TMP;

--Testcase 155:
INSERT INTO FLOAT8_TMP(f1, f2) VALUES (1::float8, 'nan'::float8);

--Testcase 156:
SELECT covar_pop(f1::float8, f2::float8), covar_samp(f1::float8, f2::float8) FROM FLOAT8_TMP;
ROLLBACK;

-- test accum and combine functions directly

--Testcase 157:
CREATE FOREIGN TABLE regr_test (id serial OPTIONS (rowkey 'true'), x int4, y int4) SERVER griddb_svr;

--Testcase 158:
INSERT INTO regr_test(x, y) VALUES (10,150),(20,250),(30,350),(80,540),(100,200);

--Testcase 159:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (10,20,30,80);

--Testcase 160:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test;

--Testcase 161:
CREATE FOREIGN TABLE regr_test_array (id serial OPTIONS (rowkey 'true'), x float8[], y float8[]) SERVER griddb_svr;
BEGIN;

--Testcase 162:
INSERT INTO regr_test_array(x) VALUES ('{4,140,2900}'::float8[]);

--Testcase 163:
SELECT float8_accum(x, 100) FROM regr_test_array;
ROLLBACK;

BEGIN;

--Testcase 164:
INSERT INTO regr_test_array(x) VALUES ('{4,140,2900,1290,83075,15050}'::float8[]);

--Testcase 165:
SELECT float8_regr_accum(x, 200, 100) FROM regr_test_array;
ROLLBACK;

--Testcase 166:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (10,20,30);

--Testcase 167:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (80,100);

BEGIN;

--Testcase 168:
INSERT INTO regr_test_array(x,y) VALUES ('{3,60,200}'::float8[], '{0,0,0}'::float8[]);

--Testcase 169:
SELECT float8_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;

--Testcase 170:
INSERT INTO regr_test_array(x,y) VALUES ('{0,0,0}'::float8[], '{2,180,200}'::float8[]);

--Testcase 171:
SELECT float8_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;

--Testcase 172:
INSERT INTO regr_test_array(x,y) VALUES ('{3,60,200}'::float8[], '{2,180,200}'::float8[]);

--Testcase 173:
SELECT float8_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;

--Testcase 174:
INSERT INTO regr_test_array(x,y) VALUES ('{3,60,200,750,20000,2000}'::float8[],
                           '{0,0,0,0,0,0}'::float8[]);

--Testcase 175:
SELECT float8_regr_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;

--Testcase 176:
INSERT INTO regr_test_array(x,y) VALUES ('{0,0,0,0,0,0}'::float8[],
                           '{2,180,200,740,57800,-3400}'::float8[]);

--Testcase 177:
SELECT float8_regr_combine(x, y) FROM regr_test_array;
ROLLBACK;

BEGIN;

--Testcase 178:
INSERT INTO regr_test_array(x,y) VALUES ('{3,60,200,750,20000,2000}'::float8[],
                           '{2,180,200,740,57800,-3400}'::float8[]);

--Testcase 179:
SELECT float8_regr_combine(x, y) FROM regr_test_array;
ROLLBACK;

--Testcase 180:
DROP FOREIGN TABLE regr_test;

--Testcase 181:
DROP FOREIGN TABLE regr_test_array;

-- test count, distinct

--Testcase 182:
SELECT count(four) AS cnt_1000 FROM onek;

--Testcase 183:
SELECT count(DISTINCT four) AS cnt_4 FROM onek;

--Testcase 184:
select ten, count(*), sum(four) from onek
group by ten order by ten;

--Testcase 185:
select ten, count(four), sum(DISTINCT four) from onek
group by ten order by ten;

-- user-defined aggregates

--Testcase 186:
CREATE AGGREGATE newavg (
   sfunc = int4_avg_accum, basetype = int4, stype = _int8,
   finalfunc = int8_avg,
   initcond1 = '{0,0}'
);

--Testcase 187:
CREATE AGGREGATE newsum (
   sfunc1 = int4pl, basetype = int4, stype1 = int4,
   initcond1 = '0'
);

--Testcase 188:
CREATE AGGREGATE newcnt (*) (
   sfunc = int8inc, stype = int8,
   initcond = '0', parallel = safe
);

--Testcase 189:
CREATE AGGREGATE newcnt ("any") (
   sfunc = int8inc_any, stype = int8,
   initcond = '0'
);

--Testcase 190:
CREATE AGGREGATE oldcnt (
   sfunc = int8inc, basetype = 'ANY', stype = int8,
   initcond = '0'
);

--Testcase 191:
create function sum3(int8,int8,int8) returns int8 as
'select $1 + $2 + $3' language sql strict immutable;

--Testcase 192:
create aggregate sum2(int8,int8) (
   sfunc = sum3, stype = int8,
   initcond = '0'
);

--Testcase 193:
SELECT newavg(four) AS avg_1 FROM onek;

--Testcase 194:
SELECT newsum(four) AS sum_1500 FROM onek;

--Testcase 195:
SELECT newcnt(four) AS cnt_1000 FROM onek;

--Testcase 196:
SELECT newcnt(*) AS cnt_1000 FROM onek;

--Testcase 197:
SELECT oldcnt(*) AS cnt_1000 FROM onek;

--Testcase 198:
SELECT sum2(q1,q2) FROM int8_tbl;

-- test for outer-level aggregates

-- this should work

--Testcase 199:
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

-- this should fail because subquery has an agg of its own in WHERE

--Testcase 200:
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b
               where sum(distinct a.four + b.four) = b.four);

-- Test handling of sublinks within outer-level aggregates.
-- Per bug report from Daniel Grace.

--Testcase 201:
select
  (select max((select i.unique2 from tenk1 i where i.unique1 = o.unique1)))
from tenk1 o;

-- Test handling of Params within aggregate arguments in hashed aggregation.
-- Per bug report from Jeevan Chalke.
BEGIN;

--Testcase 202:
DELETE FROM INT4_TBL;

--Testcase 203:
INSERT INTO INT4_TBL(f1) values (generate_series(1, 3));

--Testcase 204:
explain (verbose, costs off)
select s1.f1, ss.f1, sm
from INT4_TBL s1,
     lateral (select s2.f1, sum(s1.f1 + s2.f1) sm
              from INT4_TBL s2 group by s2.f1) ss
order by 1, 2;

--Testcase 205:
select s1.f1, ss.f1, sm
from INT4_TBL s1,
     lateral (select s2.f1, sum(s1.f1 + s2.f1) sm
              from INT4_TBL s2 group by s2.f1) ss
order by 1, 2;

--Testcase 206:
explain (verbose, costs off)
select array(select sum(x.f1+y.f1) s
            from INT4_TBL y group by y.f1 order by s)
  from INT4_TBL x;

--Testcase 207:
select array(select sum(x.f1+y.f1) s
            from INT4_TBL y group by y.f1 order by s)
  from INT4_TBL x;
ROLLBACK;

--
-- test for bitwise integer aggregates
--

--Testcase 208:
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

--Testcase 209:
SELECT
  BIT_AND(i2) AS "?",
  BIT_OR(i4)  AS "?",
  BIT_OR(i8)  AS "?"
FROM bitwise_test;

--Testcase 210:
INSERT INTO bitwise_test(i2, i4, i8, i, x, y) VALUES
  (1, 1, 1, 1, 1, B'0101'),
  (3, 3, 3, null, 2, B'0100'),
  (7, 7, 7, 3, 4, B'1100');

--Testcase 211:
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
  BIT_OR(y::bit(4))   AS "1101",

  BIT_XOR(i2) AS "5",
  BIT_XOR(i4) AS "5",
  BIT_XOR(i8) AS "5",
  BIT_XOR(i)  AS "?",
  BIT_XOR(x)  AS "7",
  BIT_XOR(y::bit(4))  AS "1101"
FROM bitwise_test;

--
-- test boolean aggregates
--
-- first test all possible transition and final states

--Testcase 212:
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

--Testcase 213:
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

--Testcase 214:
INSERT INTO bool_test_a(a1, a2, a3, a4, a5, a6, a7, a8, a9) VALUES 
(NULL, TRUE, FALSE, NULL, NULL, TRUE, TRUE, FALSE, FALSE);

--Testcase 215:
INSERT INTO bool_test_b(b1, b2, b3, b4, b5, b6, b7, b8, b9) VALUES 
(NULL, NULL, NULL, TRUE, FALSE, TRUE, FALSE, TRUE, FALSE);

--Testcase 216:
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

--Testcase 217:
CREATE FOREIGN TABLE bool_test(
  id serial OPTIONS (rowkey 'true'),
  b1 BOOL,
  b2 BOOL,
  b3 BOOL,
  b4 BOOL
) SERVER griddb_svr;

-- empty case

--Testcase 218:
SELECT
  BOOL_AND(b1)   AS "n",
  BOOL_OR(b3)    AS "n"
FROM bool_test;

--Testcase 219:
INSERT INTO bool_test(b1, b2, b3, b4) VALUES
  (TRUE, null, FALSE, null),
  (FALSE, TRUE, null, null),
  (null, TRUE, FALSE, null);

--Testcase 220:
SELECT
  BOOL_AND(b1)     AS "f",
  BOOL_AND(b2)     AS "t",
  BOOL_AND(b3)     AS "f",
  BOOL_AND(b4)     AS "n",
  BOOL_AND(NOT b2) AS "f",
  BOOL_AND(NOT b3) AS "t"
FROM bool_test;

--Testcase 221:
SELECT
  EVERY(b1)     AS "f",
  EVERY(b2)     AS "t",
  EVERY(b3)     AS "f",
  EVERY(b4)     AS "n",
  EVERY(NOT b2) AS "f",
  EVERY(NOT b3) AS "t"
FROM bool_test;

--Testcase 222:
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

--Testcase 223:
explain (costs off)
  select min(unique1) from tenk1;

--Testcase 224:
select min(unique1) from tenk1;

--Testcase 225:
explain (costs off)
  select max(unique1) from tenk1;

--Testcase 226:
select max(unique1) from tenk1;

--Testcase 227:
explain (costs off)
  select max(unique1) from tenk1 where unique1 < 42;

--Testcase 228:
select max(unique1) from tenk1 where unique1 < 42;

--Testcase 229:
explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42;

--Testcase 230:
select max(unique1) from tenk1 where unique1 > 42;

-- the planner may choose a generic aggregate here if parallel query is
-- enabled, since that plan will be parallel safe and the "optimized"
-- plan, which has almost identical cost, will not be.  we want to test
-- the optimized plan, so temporarily disable parallel query.
begin;

--Testcase 231:
set local max_parallel_workers_per_gather = 0;

--Testcase 232:
explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42000;

--Testcase 233:
select max(unique1) from tenk1 where unique1 > 42000;
rollback;

-- multi-column index (uses tenk1_thous_tenthous)

--Testcase 234:
explain (costs off)
  select max(tenthous) from tenk1 where thousand = 33;

--Testcase 235:
select max(tenthous) from tenk1 where thousand = 33;

--Testcase 236:
explain (costs off)
  select min(tenthous) from tenk1 where thousand = 33;

--Testcase 237:
select min(tenthous) from tenk1 where thousand = 33;

-- check parameter propagation into an indexscan subquery

--Testcase 238:
explain (costs off)
  select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
    from int4_tbl;

--Testcase 239:
select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
  from int4_tbl;

-- check some cases that were handled incorrectly in 8.3.0

--Testcase 240:
explain (costs off)
  select distinct max(unique2) from tenk1;

--Testcase 241:
select distinct max(unique2) from tenk1;

--Testcase 242:
explain (costs off)
  select max(unique2) from tenk1 order by 1;

--Testcase 243:
select max(unique2) from tenk1 order by 1;

--Testcase 244:
explain (costs off)
  select max(unique2) from tenk1 order by max(unique2);

--Testcase 245:
select max(unique2) from tenk1 order by max(unique2);

--Testcase 246:
explain (costs off)
  select max(unique2) from tenk1 order by max(unique2)+1;

--Testcase 247:
select max(unique2) from tenk1 order by max(unique2)+1;

--Testcase 248:
explain (costs off)
  select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;

--Testcase 249:
select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;

-- interesting corner case: constant gets optimized into a seqscan

--Testcase 250:
explain (costs off)
  select max(100) from tenk1;

--Testcase 251:
select max(100) from tenk1;

-- try it on an inheritance tree

--Testcase 252:
create foreign table minmaxtest(f1 int) server griddb_svr;;

--Testcase 253:
create table minmaxtest1() inherits (minmaxtest);

--Testcase 254:
create table minmaxtest2() inherits (minmaxtest);

--Testcase 255:
create table minmaxtest3() inherits (minmaxtest);

--Testcase 256:
create index minmaxtest1i on minmaxtest1(f1);

--Testcase 257:
create index minmaxtest2i on minmaxtest2(f1 desc);

--Testcase 258:
create index minmaxtest3i on minmaxtest3(f1) where f1 is not null;

--Testcase 259:
insert into minmaxtest values(11), (12);

--Testcase 260:
insert into minmaxtest1 values(13), (14);

--Testcase 261:
insert into minmaxtest2 values(15), (16);

--Testcase 262:
insert into minmaxtest3 values(17), (18);

--Testcase 263:
explain (costs off)
  select min(f1), max(f1) from minmaxtest;

--Testcase 264:
select min(f1), max(f1) from minmaxtest;

-- DISTINCT doesn't do anything useful here, but it shouldn't fail

--Testcase 265:
explain (costs off)
  select distinct min(f1), max(f1) from minmaxtest;

--Testcase 266:
select distinct min(f1), max(f1) from minmaxtest;

--Testcase 267:
drop foreign table minmaxtest cascade;

-- check for correct detection of nested-aggregate errors

--Testcase 268:
select max(min(unique1)) from tenk1;

--Testcase 269:
select (select max(min(unique1)) from int8_tbl) from tenk1;

--Testcase 589:
select avg((select avg(a1.col1 order by (select avg(a2.col2) from tenk1 a3))
            from tenk1 a1(col1)))
from tenk1 a2(col2);

--
-- Test removal of redundant GROUP BY columns
--

--Testcase 270:
create foreign table agg_t1 (a int OPTIONS (rowkey 'true'), b int, c int, d int) server griddb_svr;

--Testcase 271:
create foreign table agg_t2 (x int OPTIONS (rowkey 'true'), y int, z int) server griddb_svr;
-- GridDB does not support deferable for primary key
-- Skip this test
-- create foreign table t3 (a int, b int, c int, primary key(a, b) deferrable);

-- Non-primary-key columns can be removed from GROUP BY

--Testcase 272:
explain (costs off) select * from agg_t1 group by a,b,c,d;

-- No removal can happen if the complete PK is not present in GROUP BY

--Testcase 273:
explain (costs off) select a,c from agg_t1 group by a,c,d;

-- Test removal across multiple relations

--Testcase 274:
explain (costs off) select *
from agg_t1 inner join agg_t2 on agg_t1.a = agg_t2.x and agg_t1.b = agg_t2.y
group by agg_t1.a,agg_t1.b,agg_t1.c,agg_t1.d,agg_t2.x,agg_t2.y,agg_t2.z;

-- Test case where agg_t1 can be optimized but not agg_t2

--Testcase 275:
explain (costs off) select agg_t1.*,agg_t2.x,agg_t2.z
from agg_t1 inner join agg_t2 on agg_t1.a = agg_t2.x and agg_t1.b = agg_t2.y
group by agg_t1.a,agg_t1.b,agg_t1.c,agg_t1.d,agg_t2.x,agg_t2.z;

-- skip this test
-- Cannot optimize when PK is deferrable
--explain (costs off) select * from t3 group by a,b,c;

--create temp table t1c () inherits (t1);

-- Ensure we don't remove any columns when t1 has a child table

--Testcase 276:
explain (costs off) select * from agg_t1 group by a,b,c,d;

-- Okay to remove columns if we're only querying the parent.

--Testcase 277:
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
-- Test GROUP BY matching of join columns that are type-coerced due to USING
--

CREATE FOREIGN TABLE t1 (f1 int OPTIONS (rowkey 'true'), f2 int) SERVER griddb_svr OPTIONS (table_name 't1_agg');
CREATE FOREIGN TABLE t2 (f1 bigint OPTIONS (rowkey 'true'), f2 oid) SERVER griddb_svr OPTIONS (table_name 't2_agg');

-- check case where we have to inject nullingrels into coerced join alias
select f1, count(*) from
t1 x(x0,x1) left join (t1 left join t2 using(f1)) on (x0 = 0)
group by f1;

-- same, for a RelabelType coercion
select f2, count(*) from
t1 x(x0,x1) left join (t1 left join t2 using(f2)) on (x0 = 0)
group by f2;

--
-- Test planner's selection of pathkeys for ORDER BY aggregates
--

-- Ensure we order by four.  This suits the most aggregate functions.
explain (costs off)
select sum(two order by two),max(four order by four), min(four order by four)
from tenk1;

-- Ensure we order by two.  It's a tie between ordering by two and four but
-- we tiebreak on the aggregate's position.
explain (costs off)
select
  sum(two order by two), max(four order by four),
  min(four order by four), max(two order by two)
from tenk1;

-- Similar to above, but tiebreak on ordering by four
explain (costs off)
select
  max(four order by four), sum(two order by two),
  min(four order by four), max(two order by two)
from tenk1;

-- Ensure this one orders by ten since there are 3 aggregates that require ten
-- vs two that suit two and four.
explain (costs off)
select
  max(four order by four), sum(two order by two),
  min(four order by four), max(two order by two),
  sum(ten order by ten), min(ten order by ten), max(ten order by ten)
from tenk1;

-- Try a case involving a GROUP BY clause where the GROUP BY column is also
-- part of an aggregate's ORDER BY clause.  We want a sort order that works
-- for the GROUP BY along with the first and the last aggregate.
explain (costs off)
select
  sum(unique1 order by ten, two), sum(unique1 order by four),
  sum(unique1 order by two, four)
from tenk1
group by ten;

-- Ensure that we never choose to provide presorted input to an Aggref with
-- a volatile function in the ORDER BY / DISTINCT clause.  We want to ensure
-- these sorts are performed individually rather than at the query level.
explain (costs off)
select
  sum(unique1 order by two), sum(unique1 order by four),
  sum(unique1 order by four, two), sum(unique1 order by two, random()),
  sum(unique1 order by two, random(), random() + 1)
from tenk1
group by ten;

-- Ensure consecutive NULLs are properly treated as distinct from each other
select array_agg(distinct val)
from (select null as val from generate_series(1, 2));

-- Ensure no ordering is requested when enable_presorted_aggregate is off
set enable_presorted_aggregate to off;
explain (costs off)
select sum(two order by two) from tenk1;
reset enable_presorted_aggregate;

--
-- Test combinations of DISTINCT and/or ORDER BY
--
begin;

--Testcase 278:
delete from INT8_TBL;

--Testcase 279:
insert into INT8_TBL(q1,q2) values (1,4),(2,3),(3,1),(4,2);

--Testcase 280:
select array_agg(q1 order by q2)
  from INT8_TBL;

--Testcase 281:
select array_agg(q1 order by q1)
  from INT8_TBL;

--Testcase 282:
select array_agg(q1 order by q1 desc)
  from INT8_TBL;

--Testcase 283:
select array_agg(q2 order by q1 desc)
  from INT8_TBL;

--Testcase 284:
delete from INT8_TBL;

--Testcase 285:
insert into INT8_TBL(q1) values (1),(2),(1),(3),(null),(2);

--Testcase 286:
select array_agg(distinct q1)
  from INT8_TBL;

--Testcase 287:
select array_agg(distinct q1 order by q1)
  from INT8_TBL;

--Testcase 288:
select array_agg(distinct q1 order by q1 desc)
  from INT8_TBL;

--Testcase 289:
select array_agg(distinct q1 order by q1 desc nulls last)
  from INT8_TBL;
rollback;

-- multi-arg aggs, strict/nonstrict, distinct/order by

--Testcase 290:
create type aggtype as (a integer, b integer, c text);

--Testcase 291:
create function aggf_trans(aggtype[],integer,integer,text) returns aggtype[]
as 'select array_append($1,ROW($2,$3,$4)::aggtype)'
language sql strict immutable;

--Testcase 292:
create function aggfns_trans(aggtype[],integer,integer,text) returns aggtype[]
as 'select array_append($1,ROW($2,$3,$4)::aggtype)'
language sql immutable;

--Testcase 293:
create aggregate aggfstr(integer,integer,text) (
   sfunc = aggf_trans, stype = aggtype[],
   initcond = '{}'
);

--Testcase 294:
create aggregate aggfns(integer,integer,text) (
   sfunc = aggfns_trans, stype = aggtype[], sspace = 10000,
   initcond = '{}'
);

begin;

--Testcase 295:
insert into multi_arg_agg values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz');

--Testcase 296:
select aggfstr(a,b,c) from multi_arg_agg;

--Testcase 297:
select aggfns(a,b,c) from multi_arg_agg;

--Testcase 298:
select aggfstr(distinct a,b,c) from multi_arg_agg, generate_series(1,3) i;

--Testcase 299:
select aggfns(distinct a,b,c) from multi_arg_agg, generate_series(1,3) i;

--Testcase 300:
select aggfstr(distinct a,b,c order by b) from multi_arg_agg, generate_series(1,3) i;

--Testcase 301:
select aggfns(distinct a,b,c order by b) from multi_arg_agg, generate_series(1,3) i;

-- test specific code paths

--Testcase 302:
select aggfns(distinct a,a,c order by c using ~<~,a) from multi_arg_agg, generate_series(1,2) i;

--Testcase 303:
select aggfns(distinct a,a,c order by c using ~<~) from multi_arg_agg, generate_series(1,2) i;

--Testcase 304:
select aggfns(distinct a,a,c order by a) from multi_arg_agg, generate_series(1,2) i;

--Testcase 305:
select aggfns(distinct a,b,c order by a,c using ~<~,b) from multi_arg_agg, generate_series(1,2) i;

-- check node I/O via view creation and usage, also deparsing logic

--Testcase 306:
create view agg_view1 as
  select aggfns(a,b,c) from multi_arg_agg;

--Testcase 307:
select * from agg_view1;

--Testcase 308:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 309:
create or replace view agg_view1 as
  select aggfns(distinct a,b,c) from multi_arg_agg, generate_series(1,3) i;

--Testcase 310:
select * from agg_view1;

--Testcase 311:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 312:
create or replace view agg_view1 as
  select aggfns(distinct a,b,c order by b) from multi_arg_agg, generate_series(1,3) i;

--Testcase 313:
select * from agg_view1;

--Testcase 314:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 315:
create or replace view agg_view1 as
  select aggfns(a,b,c order by b+1) from multi_arg_agg;

--Testcase 316:
select * from agg_view1;

--Testcase 317:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 318:
create or replace view agg_view1 as
  select aggfns(a,a,c order by b) from multi_arg_agg;

--Testcase 319:
select * from agg_view1;

--Testcase 320:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 321:
create or replace view agg_view1 as
  select aggfns(a,b,c order by c using ~<~) from multi_arg_agg;

--Testcase 322:
select * from agg_view1;

--Testcase 323:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 324:
create or replace view agg_view1 as
  select aggfns(distinct a,b,c order by a,c using ~<~,b) from multi_arg_agg, generate_series(1,2) i;

--Testcase 325:
select * from agg_view1;

--Testcase 326:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 327:
drop view agg_view1;
rollback;

-- incorrect DISTINCT usage errors

--Testcase 328:
insert into multi_arg_agg values (1,1,'foo');

--Testcase 329:
select aggfns(distinct a,b,c order by i) from multi_arg_agg, generate_series(1,2) i;

--Testcase 330:
select aggfns(distinct a,b,c order by a,b+1) from multi_arg_agg, generate_series(1,2) i;

--Testcase 331:
select aggfns(distinct a,b,c order by a,b,i,c) from multi_arg_agg, generate_series(1,2) i;

--Testcase 332:
select aggfns(distinct a,a,c order by a,b) from multi_arg_agg, generate_series(1,2) i;

-- string_agg tests
begin;

--Testcase 333:
delete from multi_arg_agg;

--Testcase 334:
insert into multi_arg_agg(a,c) values (1,'aaaa'),(2,'bbbb'),(3,'cccc');

--Testcase 335:
select string_agg(c,',') from multi_arg_agg;

--Testcase 336:
delete from multi_arg_agg;

--Testcase 337:
insert into multi_arg_agg(a,c) values (1,'aaaa'),(2,null),(3,'bbbb'),(4,'cccc');

--Testcase 338:
select string_agg(c,',') from multi_arg_agg;

--Testcase 339:
delete from multi_arg_agg;

--Testcase 340:
insert into multi_arg_agg(a,c) values (1,null),(2,null),(3,'bbbb'),(4,'cccc');

--Testcase 341:
select string_agg(c,'AB') from multi_arg_agg;

--Testcase 342:
delete from multi_arg_agg;

--Testcase 343:
insert into multi_arg_agg(a,c) values (1,null),(2,null);

--Testcase 344:
select string_agg(c,',') from multi_arg_agg;
rollback;

-- check some implicit casting cases, as per bug #5564

--Testcase 345:
select string_agg(distinct f1, ',' order by f1) from varchar_tbl;  -- ok

--Testcase 346:
select string_agg(distinct f1::varchar, ',' order by f1) from varchar_tbl;  -- not ok

--Testcase 347:
select string_agg(distinct f1, ',' order by f1::varchar) from varchar_tbl;  -- not ok

--Testcase 348:
select string_agg(distinct f1::varchar, ',' order by f1::varchar) from varchar_tbl;  -- ok

-- string_agg bytea tests

--Testcase 349:
create foreign table bytea_test_table(id serial, v bytea) server griddb_svr;

--Testcase 350:
select string_agg(v, '') from bytea_test_table;

--Testcase 351:
insert into bytea_test_table(v) values(decode('ff','hex'));

--Testcase 352:
select string_agg(v, '') from bytea_test_table;

--Testcase 353:
insert into bytea_test_table(v) values(decode('aa','hex'));

--Testcase 354:
select string_agg(v, '') from bytea_test_table;

--Testcase 355:
select string_agg(v, NULL) from bytea_test_table;

--Testcase 356:
select string_agg(v, decode('ee', 'hex')) from bytea_test_table;

--Testcase 357:
drop foreign table bytea_test_table;

-- Test parallel string_agg and array_agg
--Testcase 590:
create foreign table pagg_test (id serial OPTIONS (rowkey 'true'), x int, y int) SERVER griddb_svr;
insert into pagg_test (x, y)
select (case z % 4 when 1 then null else z end), z % 10
from generate_series(1,5000) z;

set parallel_setup_cost TO 0;
set parallel_tuple_cost TO 0;
set parallel_leader_participation TO 0;
set min_parallel_table_scan_size = 0;
set bytea_output = 'escape';
set max_parallel_workers_per_gather = 2;

-- create a view as we otherwise have to repeat this query a few times.
create view v_pagg_test AS
select
	y,
	min(t) AS tmin,max(t) AS tmax,count(distinct t) AS tndistinct,
	min(b) AS bmin,max(b) AS bmax,count(distinct b) AS bndistinct,
	min(a) AS amin,max(a) AS amax,count(distinct a) AS andistinct,
	min(aa) AS aamin,max(aa) AS aamax,count(distinct aa) AS aandistinct
from (
	select
		y,
		unnest(regexp_split_to_array(a1.t, ','))::int AS t,
		unnest(regexp_split_to_array(a1.b::text, ',')) AS b,
		unnest(a1.a) AS a,
		unnest(a1.aa) AS aa
	from (
		select
			y,
			string_agg(x::text, ',') AS t,
			string_agg(x::text::bytea, ',') AS b,
			array_agg(x) AS a,
			array_agg(ARRAY[x]) AS aa
		from pagg_test
		group by y
	) a1
) a2
group by y;

-- Ensure results are correct.
select * from v_pagg_test order by y;

-- Ensure parallel aggregation is actually being used.
explain (costs off) select * from v_pagg_test order by y;

set max_parallel_workers_per_gather = 0;

-- Ensure results are the same without parallel aggregation.
select * from v_pagg_test order by y;

-- Clean up
reset max_parallel_workers_per_gather;
reset bytea_output;
reset min_parallel_table_scan_size;
reset parallel_leader_participation;
reset parallel_tuple_cost;
reset parallel_setup_cost;

drop view v_pagg_test;
drop foreign table pagg_test;

-- FILTER tests

--Testcase 358:
select min(unique1) filter (where unique1 > 100) from tenk1;

--Testcase 359:
select sum(1/ten) filter (where ten > 0) from tenk1;

--Testcase 360:
select ten, sum(distinct four) filter (where four::text ~ '123') from onek a
group by ten;

--Testcase 361:
select ten, sum(distinct four) filter (where four > 10) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

--Testcase 362:
create foreign table agg_t0(foo text, bar text) server griddb_svr;

--Testcase 363:
insert into agg_t0 values ('a', 'b');

--Testcase 364:
select max(foo COLLATE "C") filter (where (bar collate "POSIX") > '0')
from agg_t0;

create foreign table agg_t7(v int) server griddb_svr;
insert into agg_t7 values (1), (2), (3);
select any_value(v) filter (where v > 2) from agg_t7;

-- outer reference in FILTER (PostgreSQL extension)

--Testcase 365:
create foreign table agg_t3 (inner_c int) server griddb_svr;

--Testcase 366:
create foreign table agg_t4 (outer_c int) server griddb_svr;

--Testcase 367:
insert into agg_t3 values (1);

--Testcase 368:
insert into agg_t4 values (2), (3);

--Testcase 369:
select (select count(*) from agg_t3) from agg_t4; -- inner query is aggregation query

--Testcase 370:
select (select count(*) filter (where outer_c <> 0) from agg_t3)
from agg_t4; -- outer query is aggregation query

--Testcase 371:
select (select count(inner_c) filter (where outer_c <> 0) from agg_t3)
from agg_t4; -- inner query is aggregation query

--Testcase 372:
select
  (select max((select i.unique2 from tenk1 i where i.unique1 = o.unique1))
     filter (where o.unique1 < 10))
from tenk1 o;					-- outer query is aggregation query

-- subquery in FILTER clause (PostgreSQL extension)

--Testcase 373:
select sum(unique1) FILTER (WHERE
  unique1 IN (SELECT unique1 FROM onek where unique1 < 100)) FROM tenk1;

-- exercise lots of aggregate parts with FILTER
begin;

--Testcase 374:
delete from multi_arg_agg;

--Testcase 375:
insert into multi_arg_agg values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz');

--Testcase 376:
select aggfns(distinct a,b,c order by a,c using ~<~,b) filter (where a > 1) from multi_arg_agg, generate_series(1,2) i;
rollback;

-- check handling of bare boolean Var in FILTER
--Testcase 583:
select max(0) filter (where b1) from bool_test;
--Testcase 584:
select (select max(0) filter (where b1)) from bool_test;

-- check for correct detection of nested-aggregate errors in FILTER
--Testcase 585:
select max(unique1) filter (where sum(ten) > 0) from tenk1;
--Testcase 586:
select (select max(unique1) filter (where sum(ten) > 0) from int8_tbl) from tenk1;
--Testcase 587:
select max(unique1) filter (where bool_or(ten > 0)) from tenk1;
--Testcase 588:
select (select max(unique1) filter (where bool_or(ten > 0)) from int8_tbl) from tenk1;

-- ordered-set aggregates

begin;

--Testcase 377:
delete from FLOAT8_TBL;

--Testcase 378:
insert into FLOAT8_TBL(f1) values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);

--Testcase 379:
select f1, percentile_cont(f1) within group (order by x::float8)
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

begin;

--Testcase 380:
delete from FLOAT8_TBL;

--Testcase 381:
insert into FLOAT8_TBL(f1) values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);

--Testcase 382:
select f1, percentile_cont(f1 order by f1) within group (order by x)  -- error
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

begin;

--Testcase 383:
delete from FLOAT8_TBL;

--Testcase 384:
insert into FLOAT8_TBL(f1) values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);

--Testcase 385:
select f1, sum() within group (order by x::float8)  -- error
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

begin;

--Testcase 386:
delete from FLOAT8_TBL;

--Testcase 387:
insert into FLOAT8_TBL(f1) values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);

--Testcase 388:
select f1, percentile_cont(f1,f1)  -- error
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

--Testcase 389:
select percentile_cont(0.5) within group (order by b) from aggtest;

--Testcase 390:
select percentile_cont(0.5) within group (order by b), sum(b) from aggtest;

--Testcase 391:
select percentile_cont(0.5) within group (order by thousand) from tenk1;

--Testcase 392:
select percentile_disc(0.5) within group (order by thousand) from tenk1;

begin;

--Testcase 393:
delete from INT8_TBL;

--Testcase 394:
insert into INT8_TBL(q1) values (1),(1),(2),(2),(3),(3),(4);

--Testcase 395:
select rank(3) within group (order by q1) from INT8_TBL;

--Testcase 396:
select cume_dist(3) within group (order by q1) from INT8_TBL;
rollback;
begin;

--Testcase 397:
delete from INT8_TBL;

--Testcase 398:
insert into INT8_TBL(q1) values (1),(1),(2),(2),(3),(3),(4),(5);

--Testcase 399:
select percent_rank(3) within group (order by q1) from INT8_TBL;
rollback;
begin;

--Testcase 400:
delete from INT8_TBL;

--Testcase 401:
insert into INT8_TBL(q1) values (1),(1),(2),(2),(3),(3),(4);

--Testcase 402:
select dense_rank(3) within group (order by q1) from INT8_TBL;
rollback;

--Testcase 403:
select percentile_disc(array[0,0.1,0.25,0.5,0.75,0.9,1]) within group (order by thousand)
from tenk1;

--Testcase 404:
select percentile_cont(array[0,0.25,0.5,0.75,1]) within group (order by thousand)
from tenk1;

--Testcase 405:
select percentile_disc(array[[null,1,0.5],[0.75,0.25,null]]) within group (order by thousand)
from tenk1;

--Testcase 406:
create foreign table agg_t5 (x int) server griddb_svr;
begin;

--Testcase 407:
insert into agg_t5 select * from generate_series(1,6);

--Testcase 408:
select percentile_cont(array[0,1,0.25,0.75,0.5,1,0.3,0.32,0.35,0.38,0.4]) within group (order by x)
from agg_t5;
rollback;

--Testcase 409:
select ten, mode() within group (order by string4) from tenk1 group by ten;

--Testcase 410:
create foreign table agg_t6 (id serial OPTIONS (rowkey 'true'), x text) server griddb_svr;
begin;

--Testcase 411:
insert into agg_t6(x) values (unnest('{fred,jim,fred,jack,jill,fred,jill,jim,jim,sheila,jim,sheila}'::text[]));

--Testcase 412:
select percentile_disc(array[0.25,0.5,0.75]) within group (order by x)
from agg_t6;
rollback;

-- check collation propagates up in suitable cases:
begin;

--Testcase 413:
insert into agg_t6(x) values ('fred'), ('jim');

--Testcase 414:
select pg_collation_for(percentile_disc(1) within group (order by x collate "POSIX"))
  from agg_t6;
rollback;
-- ordered-set aggs created with CREATE 

--Testcase 415:
create aggregate my_percentile_disc(float8 ORDER BY anyelement) (
  stype = internal,
  sfunc = ordered_set_transition,
  finalfunc = percentile_disc_final,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

--Testcase 416:
create aggregate my_rank(VARIADIC "any" ORDER BY VARIADIC "any") (
  stype = internal,
  sfunc = ordered_set_transition_multi,
  finalfunc = rank_final,
  finalfunc_extra = true,
  hypothetical
);

--Testcase 417:
alter aggregate my_percentile_disc(float8 ORDER BY anyelement)
  rename to test_percentile_disc;

--Testcase 418:
alter aggregate my_rank(VARIADIC "any" ORDER BY VARIADIC "any")
  rename to test_rank;

begin;

--Testcase 419:
delete from INT8_TBL;

--Testcase 420:
insert into INT8_TBL(q1) values (1),(1),(2),(2),(3),(3),(4);

--Testcase 421:
select test_rank(3) within group (order by q1) from INT8_TBL;
rollback;

--Testcase 422:
select test_percentile_disc(0.5) within group (order by thousand) from tenk1;

-- ordered-set aggs can't use ungrouped vars in direct args:
begin;

--Testcase 423:
insert into agg_t5(x) select * from generate_series(1,5);

--Testcase 424:
select rank(x) within group (order by x) from agg_t5;
rollback;

-- outer-level agg can't use a grouped arg of a lower level, either:

begin;

--Testcase 425:
insert into agg_t5(x) select * from generate_series(1,5);

--Testcase 426:
select array(select percentile_disc(a) within group (order by x)
               from (values (0.3),(0.7)) v(a) group by a)
  from agg_t5;
rollback;

-- agg in the direct args is a grouping violation, too:
begin;

--Testcase 427:
insert into agg_t5(x) select * from generate_series(1,5);

--Testcase 428:
select rank(sum(x)) within group (order by x) from agg_t5;
rollback;

-- hypothetical-set type unification and argument-count failures:
begin;

--Testcase 429:
insert into agg_t6(x) values ('fred'), ('jim');

--Testcase 430:
select rank(3) within group (order by x) from agg_t6;
rollback;

--Testcase 431:
select rank(3) within group (order by stringu1,stringu2) from tenk1;

begin;

--Testcase 432:
insert into agg_t5 select * from generate_series(1,5);

--Testcase 433:
select rank('fred') within group (order by x) from agg_t5;
rollback;

begin;

--Testcase 434:
insert into agg_t6(x) values ('fred'), ('jim');

--Testcase 435:
select rank('adam'::text collate "C") within group (order by x collate "POSIX")
  from agg_t6;
rollback;

-- hypothetical-set type unification successes:
begin;

--Testcase 436:
insert into agg_t6(x) values ('fred'), ('jim');

--Testcase 437:
select rank('adam'::varchar) within group (order by x) from agg_t6;
rollback;

begin;

--Testcase 438:
insert into agg_t5 select * from generate_series(1,5);

--Testcase 439:
select rank('3') within group (order by x) from agg_t5;
rollback;

-- divide by zero check
begin;

--Testcase 440:
insert into agg_t5 select * from generate_series(1,5);

--Testcase 441:
select percent_rank(0) within group (order by x) from agg_t5;
rollback;

-- deparse and multiple features:

--Testcase 442:
create view aggordview1 as
select ten,
       percentile_disc(0.5) within group (order by thousand) as p50,
       percentile_disc(0.5) within group (order by thousand) filter (where hundred=1) as px,
       rank(5,'AZZZZ',50) within group (order by hundred, string4 desc, hundred)
  from tenk1
 group by ten order by ten;

--Testcase 443:
select pg_get_viewdef('aggordview1');

--Testcase 444:
select * from aggordview1 order by ten;

--Testcase 445:
drop view aggordview1;

-- variadic aggregates

--Testcase 446:
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';

--Testcase 447:
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);

--Testcase 448:
create function cleast_accum(anycompatible, variadic anycompatiblearray)
returns anycompatible language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';

--Testcase 449:
create aggregate cleast_agg(variadic items anycompatiblearray) (
  stype = anycompatible, sfunc = cleast_accum
);

--Testcase 450:
select least_agg(q1,q2) from int8_tbl;

--Testcase 451:
select least_agg(variadic array[q1,q2]) from int8_tbl;

--Testcase 452:
select cleast_agg(q1,q2) from int8_tbl;

--Testcase 453:
select cleast_agg(4.5,f1) from int4_tbl;

--Testcase 454:
select cleast_agg(variadic array[4.5,f1]) from int4_tbl;

--Testcase 455:
select pg_typeof(cleast_agg(variadic array[4.5,f1])) from int4_tbl;

--Testcase 456:
drop aggregate least_agg(variadic items anyarray);

--Testcase 457:
drop function least_accum(anyelement, variadic anyarray);

-- test aggregates with common transition functions share the same states
begin work;

--Testcase 458:
create type avg_state as (total bigint, count bigint);

--Testcase 459:
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

--Testcase 460:
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

--Testcase 461:
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

--Testcase 462:
create aggregate my_avg(bigint)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = avg_finalfn
);

--Testcase 463:
create aggregate my_sum(bigint)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = sum_finalfn
);

-- aggregate state should be shared as aggs are the same.

--Testcase 464:
delete from int8_tbl;

--Testcase 465:
insert into int8_tbl(q1) values (1),(3);

--Testcase 466:
select my_avg(q1),my_avg(q1) from int8_tbl;

-- aggregate state should be shared as transfn is the same for both aggs.

--Testcase 467:
delete from int8_tbl;

--Testcase 468:
insert into int8_tbl(q1) values (1),(3);

--Testcase 469:
select my_avg(q1),my_sum(q1) from int8_tbl;

-- same as previous one, but with DISTINCT, which requires sorting the input.

--Testcase 470:
delete from int8_tbl;

--Testcase 471:
insert into int8_tbl(q1) values (1),(3),(1);

--Testcase 472:
select my_avg(distinct q1),my_sum(distinct q1) from int8_tbl;

-- shouldn't share states due to the distinctness not matching.

--Testcase 473:
delete from int8_tbl;

--Testcase 474:
insert into int8_tbl(q1) values (1),(3);

--Testcase 475:
select my_avg(distinct q1),my_sum(q1) from int8_tbl;

-- shouldn't share states due to the filter clause not matching.

--Testcase 476:
delete from int8_tbl;

--Testcase 477:
insert into int8_tbl(q1) values (1),(3);

--Testcase 478:
select my_avg(q1) filter (where q1 > 1),my_sum(q1) from int8_tbl;

-- this should not share the state due to different input columns.

--Testcase 479:
delete from int8_tbl;

--Testcase 480:
insert into int8_tbl(q1,q2) values (1,2),(3,4);

--Testcase 481:
select my_avg(q1),my_sum(q2) from int8_tbl;

-- exercise cases where OSAs share state

--Testcase 482:
delete from int8_tbl;

--Testcase 483:
insert into int8_tbl(q1) values (1::float8),(3),(5),(7);

--Testcase 484:
select
  percentile_cont(0.5) within group (order by q1),
  percentile_disc(0.5) within group (order by q1)
from int8_tbl;

--Testcase 485:
select
  percentile_cont(0.25) within group (order by q1),
  percentile_disc(0.5) within group (order by q1)
from int8_tbl;

-- these can't share state currently

--Testcase 486:
select
  rank(4) within group (order by q1),
  dense_rank(4) within group (order by q1)
from int8_tbl;

-- test that aggs with the same sfunc and initcond share the same agg state

--Testcase 487:
create aggregate my_sum_init(int8)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = sum_finalfn,
   initcond = '(10,0)'
);

--Testcase 488:
create aggregate my_avg_init(int8)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = avg_finalfn,
   initcond = '(10,0)'
);

--Testcase 489:
create aggregate my_avg_init2(int8)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = avg_finalfn,
   initcond = '(4,0)'
);

-- state should be shared if INITCONDs are matching

--Testcase 490:
delete from int8_tbl;

--Testcase 491:
insert into int8_tbl(q1) values (1),(3);

--Testcase 492:
select my_sum_init(q1),my_avg_init(q1) from int8_tbl;

-- Varying INITCONDs should cause the states not to be shared.

--Testcase 493:
select my_sum_init(q1),my_avg_init2(q1) from int8_tbl;

rollback;

-- test aggregate state sharing to ensure it works if one aggregate has a
-- finalfn and the other one has none.
begin work;

--Testcase 494:
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

--Testcase 495:
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

--Testcase 496:
create aggregate my_sum(int8)
(
   stype = int8,
   sfunc = sum_transfn
);

--Testcase 497:
create aggregate my_half_sum(int8)
(
   stype = int8,
   sfunc = sum_transfn,
   finalfunc = halfsum_finalfn
);

-- Agg state should be shared even though my_sum has no finalfn

--Testcase 498:
delete from int8_tbl;

--Testcase 499:
insert into int8_tbl(q1) values (1),(2),(3),(4);

--Testcase 500:
select my_sum(q1),my_half_sum(q1) from int8_tbl;

rollback;

-- test that the aggregate transition logic correctly handles
-- transition / combine functions returning NULL

-- First test the case of a normal transition function returning NULL
BEGIN;

--Testcase 501:
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

--Testcase 502:
CREATE AGGREGATE balk(int4)
(
    SFUNC = balkifnull(int8, int4),
    STYPE = int8,
    PARALLEL = SAFE,
    INITCOND = '0'
);

--Testcase 503:
SELECT balk(hundred) FROM tenk1;

ROLLBACK;

-- Secondly test the case of a parallel aggregate combiner function
-- returning NULL. For that use normal transition function, but a
-- combiner function returning NULL.
BEGIN;

--Testcase 504:
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

--Testcase 505:
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

-- test multiple usage of an aggregate whose finalfn returns a R/W datum
BEGIN;

CREATE FUNCTION rwagg_sfunc(x anyarray, y anyarray) RETURNS anyarray
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RETURN array_fill(y[1], ARRAY[4]);
END;
$$;

CREATE FUNCTION rwagg_finalfunc(x anyarray) RETURNS anyarray
LANGUAGE plpgsql STRICT IMMUTABLE AS $$
DECLARE
    res x%TYPE;
BEGIN
    -- assignment is essential for this test, it expands the array to R/W
    res := array_fill(x[1], ARRAY[4]);
    RETURN res;
END;
$$;

CREATE AGGREGATE rwagg(anyarray) (
    STYPE = anyarray,
    SFUNC = rwagg_sfunc,
    FINALFUNC = rwagg_finalfunc
);

CREATE FUNCTION eatarray(x real[]) RETURNS real[]
LANGUAGE plpgsql STRICT IMMUTABLE AS $$
BEGIN
    x[1] := x[1] + 1;
    RETURN x;
END;
$$;

CREATE FOREIGN TABLE float_tb(id int OPTIONS(rowkey 'true'), f real) SERVER griddb_svr OPTIONS (table_name 'float_tb');
INSERT INTO float_tb(id, f) VALUES (1, 1.0);
SELECT eatarray(rwagg(ARRAY[f::real])), eatarray(rwagg(ARRAY[f::real])) FROM float_tb;

ROLLBACK;

-- test coverage for aggregate combine/serial/deserial functions
BEGIN;

--Testcase 506:
SET parallel_setup_cost = 0;

--Testcase 507:
SET parallel_tuple_cost = 0;

--Testcase 508:
SET min_parallel_table_scan_size = 0;

--Testcase 509:
SET max_parallel_workers_per_gather = 4;

--Testcase 510:
SET parallel_leader_participation = off;

--Testcase 511:
SET enable_indexonlyscan = off;

-- variance(int4) covers numeric_poly_combine
-- sum(int8) covers int8_avg_combine
-- regr_count(float8, float8) covers int8inc_float8_float8 and aggregates with > 1 arg

--Testcase 512:
EXPLAIN (COSTS OFF, VERBOSE)
  SELECT variance(unique1::int4), sum(unique1::int8), regr_count(unique1::float8, unique1::float8)
  FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

--Testcase 513:
SELECT variance(unique1::int4), sum(unique1::int8), regr_count(unique1::float8, unique1::float8)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

-- variance(int8) covers numeric_combine
-- avg(numeric) covers numeric_avg_combine

--Testcase 514:
EXPLAIN (COSTS OFF, VERBOSE)
SELECT variance(unique1::int8), avg(unique1::numeric)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

--Testcase 515:
SELECT variance(unique1::int8), avg(unique1::numeric)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;
ROLLBACK;

-- test coverage for dense_rank
BEGIN;

--Testcase 516:
DELETE FROM INT8_TBL;

--Testcase 517:
INSERT INTO INT8_TBL(q1) VALUES (1),(1),(2),(2),(3),(3);

--Testcase 518:
SELECT dense_rank(q1) WITHIN GROUP (ORDER BY q1) FROM INT8_TBL GROUP BY (q1) ORDER BY 1;
ROLLBACK;

-- Ensure that the STRICT checks for aggregates does not take NULLness
-- of ORDER BY columns into account. See bug report around
-- 2a505161-2727-2473-7c46-591ed108ac52@email.cz
begin;

--Testcase 519:
insert into INT8_TBL(q1, q2) values (1, NULL);

--Testcase 520:
SELECT min(x ORDER BY y) FROM INT8_TBL AS d(x,y);
rollback;

begin;

--Testcase 521:
insert into INT8_TBL(q1, q2) values (1, 2);

--Testcase 522:
SELECT min(x ORDER BY y) FROM INT8_TBL AS d(x,y);
rollback;

-- check collation-sensitive matching between grouping expressions
begin;

--Testcase 523:
insert into agg_t6(x) values (unnest(array['a','b']));

--Testcase 524:
select x||'a', case x||'a' when 'aa' then 1 else 0 end, count(*)
  from agg_t6 group by x||'a' order by 1;
rollback;

begin;

--Testcase 525:
insert into agg_t6(x) values (unnest(array['a','b']));

--Testcase 526:
select x||'a', case when x||'a' = 'aa' then 1 else 0 end, count(*)
  from agg_t6 group by x||'a' order by 1;
rollback;

-- Make sure that generation of HashAggregate for uniqification purposes
-- does not lead to array overflow due to unexpected duplicate hash keys
-- see CAFeeJoKKu0u+A_A9R9316djW-YW3-+Gtgvy3ju655qRHR3jtdA@mail.gmail.com

--Testcase 527:
set enable_memoize to off;

--Testcase 528:
explain (costs off)
  select 1 from tenk1
   where (hundred, thousand) in (select twothousand, twothousand from onek);

--Testcase 529:
reset enable_memoize;

--
-- Hash Aggregation Spill tests
--

--Testcase 530:
set enable_sort=false;

--Testcase 531:
set work_mem='64kB';

--Testcase 532:
select unique1, count(*), sum(twothousand) from tenk1
group by unique1
having sum(fivethous) > 4975
order by sum(twothousand);

--Testcase 533:
set work_mem to default;

--Testcase 534:
set enable_sort to default;

--
-- Compare results between plans using sorting and plans using hash
-- aggregation. Force spilling in both cases by setting work_mem low.
--

--Testcase 535:
set work_mem='64kB';

--Testcase 536:
create foreign table agg_data_2k (g int) server griddb_svr;

--Testcase 537:
create foreign table agg_data_20k (g int) server griddb_svr;

--Testcase 538:
create foreign table agg_group_1(id serial OPTIONS (rowkey 'true'), c1 int, c2 float8, c3 int) server griddb_svr;

--Testcase 539:
create foreign table agg_group_2(id serial OPTIONS (rowkey 'true'), a int, c1 float8, c2 text, c3 int) server griddb_svr;

--Testcase 540:
create foreign table agg_group_3(id serial OPTIONS (rowkey 'true'), c1 float8, c2 int4, c3 int) server griddb_svr;

--Testcase 541:
create foreign table agg_group_4(id serial OPTIONS (rowkey 'true'), c1 float8, c2 text, c3 int) server griddb_svr;

--Testcase 542:
create foreign table agg_hash_1(id serial OPTIONS (rowkey 'true'), c1 int, c2 float8, c3 int) server griddb_svr;

--Testcase 543:
create foreign table agg_hash_2(id serial OPTIONS (rowkey 'true'), a int, c1 float8, c2 text, c3 int) server griddb_svr;

--Testcase 544:
create foreign table agg_hash_3(id serial OPTIONS (rowkey 'true'), c1 float8, c2 int4, c3 int) server griddb_svr;

--Testcase 545:
create foreign table agg_hash_4(id serial OPTIONS (rowkey 'true'), c1 float8, c2 text, c3 int) server griddb_svr;

--Testcase 546:
insert into agg_data_2k select g from generate_series(0, 1999) g;
-- analyze agg_data_2k;

--Testcase 547:
insert into agg_data_20k select g from generate_series(0, 19999) g;
-- analyze agg_data_20k;

-- Produce results with sorting.

--Testcase 548:
set enable_hashagg = false;

--Testcase 549:
set jit_above_cost = 0;

--Testcase 550:
explain (costs off)
select g%10000 as c1, sum(g::float8) as c2, count(*) as c3
  from agg_data_20k group by g%10000;

--Testcase 551:
insert into agg_group_1(c1, c2, c3)
select g%10000 as c1, sum(g::float8) as c2, count(*) as c3
  from agg_data_20k group by g%10000;

--Testcase 552:
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

--Testcase 553:
insert into agg_group_3(c1, c2, c3)
select (g/2)::float8 as c1, sum(7::int4) as c2, count(*) as c3
  from agg_data_2k group by g/2;

--Testcase 554:
insert into agg_group_4(c1, c2, c3)
select (g/2)::float8 as c1, array_agg(g::float8) as c2, count(*) as c3
  from agg_data_2k group by g/2;

-- Produce results with hash aggregation

--Testcase 555:
set enable_hashagg = true;

--Testcase 556:
set enable_sort = false;

--Testcase 557:
set jit_above_cost = 0;

--Testcase 558:
explain (costs off)
select g%10000 as c1, sum(g::float8) as c2, count(*) as c3
  from agg_data_20k group by g%10000;

--Testcase 559:
insert into agg_hash_1(c1, c2, c3)
select g%10000 as c1, sum(g::float8) as c2, count(*) as c3
  from agg_data_20k group by g%10000;

--Testcase 560:
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

--Testcase 561:
set jit_above_cost to default;

--Testcase 562:
insert into agg_hash_3(c1, c2, c3)
select (g/2)::float8 as c1, sum(7::int4) as c2, count(*) as c3
  from agg_data_2k group by g/2;

--Testcase 563:
insert into agg_hash_4(c1, c2, c3)
select (g/2)::float8 as c1, array_agg(g::float8) as c2, count(*) as c3
  from agg_data_2k group by g/2;

--Testcase 564:
set enable_sort = true;

--Testcase 565:
set work_mem to default;

-- Compare group aggregation results to hash aggregation results

--Testcase 566:
(select c1, c2, c3 from agg_hash_1 except select c1, c2, c3 from agg_group_1)
  union all
(select c1, c2, c3 from agg_group_1 except select c1, c2, c3 from agg_hash_1);

--Testcase 567:
(select a, c1, c2, c3 from agg_hash_2 except select a, c1, c2, c3 from agg_group_2)
  union all
(select a, c1, c2, c3 from agg_group_2 except select a, c1, c2, c3 from agg_hash_2);

--Testcase 568:
(select c1, c2, c3 from agg_hash_3 except select c1, c2, c3 from agg_group_3)
  union all
(select c1, c2, c3 from agg_group_3 except select c1, c2, c3 from agg_hash_3);

--Testcase 569:
(select c1, c2, c3 from agg_hash_4 except select c1, c2, c3 from agg_group_4)
  union all
(select c1, c2, c3 from agg_group_4 except select c1, c2, c3 from agg_hash_4);

--Testcase 570:
drop foreign table agg_data_2k;

--Testcase 571:
drop foreign table agg_data_20k;

--Testcase 572:
drop foreign table agg_group_1;

--Testcase 573:
drop foreign table agg_group_2;

--Testcase 574:
drop foreign table agg_group_3;

--Testcase 575:
drop foreign table agg_group_4;

--Testcase 576:
drop foreign table agg_hash_1;

--Testcase 577:
drop foreign table agg_hash_2;

--Testcase 578:
drop foreign table agg_hash_3;

--Testcase 579:
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

--Testcase 580:
DROP USER MAPPING FOR public SERVER griddb_svr;

--Testcase 581:
DROP SERVER griddb_svr;

--Testcase 582:
DROP EXTENSION griddb_fdw CASCADE;
