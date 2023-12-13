--
-- JOIN
-- Test JOIN clauses
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
CREATE FOREIGN TABLE J1_TBL (
  id serial OPTIONS (rowkey 'true'),
  i integer,
  j integer,
  t text
) SERVER griddb_svr; 

--Testcase 5:
CREATE FOREIGN TABLE J2_TBL (
  id serial OPTIONS (rowkey 'true'),
  i integer,
  k integer
) SERVER griddb_svr;

--Testcase 6:
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

--Testcase 7:
CREATE FOREIGN TABLE tenk2 (
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

--Testcase 772:
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

--Testcase 8:
CREATE FOREIGN TABLE INT4_TBL(id int4 OPTIONS (rowkey 'true'), f1 int4) SERVER griddb_svr;

--Testcase 9:
CREATE FOREIGN TABLE FLOAT8_TBL(id int4 OPTIONS (rowkey 'true'), f1 float8) SERVER griddb_svr;

--Testcase 10:
CREATE FOREIGN TABLE INT8_TBL(id int4 OPTIONS (rowkey 'true'), q1 int8, q2 int8) SERVER griddb_svr;

--Testcase 11:
CREATE FOREIGN TABLE INT2_TBL(id int4 OPTIONS (rowkey 'true'), f1 int2) SERVER griddb_svr;

--Testcase 12:
INSERT INTO J1_TBL(i, j, t) VALUES (1, 4, 'one');

--Testcase 13:
INSERT INTO J1_TBL(i, j, t) VALUES (2, 3, 'two');

--Testcase 14:
INSERT INTO J1_TBL(i, j, t) VALUES (3, 2, 'three');

--Testcase 15:
INSERT INTO J1_TBL(i, j, t) VALUES (4, 1, 'four');

--Testcase 16:
INSERT INTO J1_TBL(i, j, t) VALUES (5, 0, 'five');

--Testcase 17:
INSERT INTO J1_TBL(i, j, t) VALUES (6, 6, 'six');

--Testcase 18:
INSERT INTO J1_TBL(i, j, t) VALUES (7, 7, 'seven');

--Testcase 19:
INSERT INTO J1_TBL(i, j, t) VALUES (8, 8, 'eight');

--Testcase 20:
INSERT INTO J1_TBL(i, j, t) VALUES (0, NULL, 'zero');

--Testcase 21:
INSERT INTO J1_TBL(i, j, t) VALUES (NULL, NULL, 'null');

--Testcase 22:
INSERT INTO J1_TBL(i, j, t) VALUES (NULL, 0, 'zero');

--Testcase 23:
INSERT INTO J2_TBL(i, k) VALUES (1, -1);

--Testcase 24:
INSERT INTO J2_TBL(i, k) VALUES (2, 2);

--Testcase 25:
INSERT INTO J2_TBL(i, k) VALUES (3, -3);

--Testcase 26:
INSERT INTO J2_TBL(i, k) VALUES (2, 4);

--Testcase 27:
INSERT INTO J2_TBL(i, k) VALUES (5, -5);

--Testcase 28:
INSERT INTO J2_TBL(i, k) VALUES (5, -5);

--Testcase 29:
INSERT INTO J2_TBL(i, k) VALUES (0, NULL);

--Testcase 30:
INSERT INTO J2_TBL(i, k) VALUES (NULL, NULL);

--Testcase 31:
INSERT INTO J2_TBL(i, k) VALUES (NULL, 0);

-- useful in some tests below

--Testcase 32:
create temp table onerow();

--Testcase 33:
insert into onerow default values;
--
-- CORRELATION NAMES
-- Make sure that table/column aliases are supported
-- before diving into more complex join syntax.
--

--Testcase 34:
SELECT i, j, t
  FROM J1_TBL AS tx;

--Testcase 35:
SELECT i, j, t
  FROM J1_TBL tx;

--Testcase 36:
SELECT a, b, c
  FROM J1_TBL AS t1 (id, a, b, c);

--Testcase 37:
SELECT a, b, c
  FROM J1_TBL t1 (id, a, b, c);

--Testcase 38:
SELECT a, b, c, d, e
  FROM J1_TBL t1 (id, a, b, c), J2_TBL t2 (id, d, e);

--Testcase 39:
SELECT t1.a, t2.e
  FROM J1_TBL t1 (id, a, b, c), J2_TBL t2 (id, d, e)
  WHERE t1.a = t2.d;

--
-- CROSS JOIN
-- Qualifications are not allowed on cross joins,
-- which degenerate into a standard unqualified inner join.
--

--Testcase 40:
SELECT i, j, t, i1 as i, k
  FROM J1_TBL t1 (id, i, j, t) CROSS JOIN J2_TBL t2 (id, i1, k);

-- ambiguous column

--Testcase 41:
SELECT i, k, t
  FROM J1_TBL t1 (id, i, j, t) CROSS JOIN J2_TBL t2 (id, i, k);

-- resolve previous ambiguity by specifying the table name

--Testcase 42:
SELECT t1.i, k, t
  FROM J1_TBL t1 CROSS JOIN J2_TBL t2;

--Testcase 43:
SELECT ii, tt, kk
  FROM (J1_TBL CROSS JOIN J2_TBL)
    AS tx (id1, ii, jj, tt, id2, ii2, kk);

--Testcase 44:
SELECT tx.ii, tx.jj, tx.kk
  FROM (J1_TBL t1 (id, a, b, c) CROSS JOIN J2_TBL t2 (id, d, e))
    AS tx (id1, ii, jj, tt, id2, ii2, kk);

--Testcase 45:
SELECT x.i, x.j, x.t, a.i, a.k, b.i, b.k
  FROM J1_TBL x CROSS JOIN J2_TBL a CROSS JOIN J2_TBL b;

--
--
-- Inner joins (equi-joins)
--
--

--
-- Inner joins (equi-joins) with USING clause
-- The USING syntax changes the shape of the resulting table
-- by including a column in the USING clause only once in the result.
--

-- Inner equi-join on specified column

--Testcase 46:
SELECT i, j, t, k
  FROM J1_TBL INNER JOIN J2_TBL USING (i);

-- Same as above, slightly different syntax

--Testcase 47:
SELECT i, j, t, k
  FROM J1_TBL JOIN J2_TBL USING (i);

--Testcase 48:
SELECT a, b, c, d
  FROM J1_TBL t1 (id, a, b, c) JOIN J2_TBL t2 (id, a, d) USING (a)
  ORDER BY a, d;

--Testcase 49:
SELECT b, t1.a, c, t2.a
  FROM J1_TBL t1 (id, a, b, c) JOIN J2_TBL t2 (id, a, b) USING (b)
  ORDER BY b, t1.a;

-- test join using aliases

--Testcase 50:
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) WHERE J1_TBL.t = 'one';  -- ok

--Testcase 51:
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one';  -- ok

--Testcase 52:
SELECT * FROM (J1_TBL JOIN J2_TBL USING (i)) AS x WHERE J1_TBL.t = 'one';  -- error

--Testcase 53:
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE x.i = 1;  -- ok

--Testcase 54:
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE x.t = 'one';  -- error

--Testcase 55:
SELECT * FROM (J1_TBL JOIN J2_TBL USING (i) AS x) AS xx WHERE x.i = 1;  -- error (XXX could use better hint)

--Testcase 56:
SELECT * FROM J1_TBL a1 JOIN J2_TBL a2 USING (i) AS a1;  -- error

--Testcase 57:
SELECT x.* FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one';

--Testcase 58:
SELECT ROW(x.*) FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one';

--Testcase 59:
SELECT row_to_json(x.*) FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one';

--
-- NATURAL JOIN
-- Inner equi-join on all columns with the same name
--

--Testcase 60:
SELECT i, j, t, k
  FROM J1_TBL t1(id1, i, j, t) NATURAL JOIN J2_TBL t2(id2, i, k);

--Testcase 61:
SELECT a, b, c, d
  FROM J1_TBL t1 (id1, a, b, c) NATURAL JOIN J2_TBL t2 (id2, a, d);

--Testcase 62:
SELECT a, b, c, d
  FROM J1_TBL t1 (id1, a, b, c) NATURAL JOIN J2_TBL t2 (id2, d, a);

-- mismatch number of columns
-- currently, Postgres will fill in with underlying names

--Testcase 63:
SELECT a, b, t, k
  FROM J1_TBL t1 (id1, a, b) NATURAL JOIN J2_TBL t2 (id2, a);

--
-- Inner joins (equi-joins)
--

--Testcase 64:
SELECT J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.i);

--Testcase 65:
SELECT J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.k);

--
-- Non-equi-joins
--

--Testcase 66:
SELECT J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i <= J2_TBL.k);

--
-- Outer joins
-- Note that OUTER is a noise word
--

--Testcase 67:
SELECT i, j, t, k
  FROM J1_TBL LEFT OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 68:
SELECT i, j, t, k
  FROM J1_TBL LEFT JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 69:
SELECT i, j, t, k
  FROM J1_TBL RIGHT OUTER JOIN J2_TBL USING (i);

--Testcase 70:
SELECT i, j, t, k
  FROM J1_TBL RIGHT JOIN J2_TBL USING (i);

--Testcase 71:
SELECT i, j, t, k
  FROM J1_TBL FULL OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 72:
SELECT i, j, t, k
  FROM J1_TBL FULL JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 73:
SELECT i, j, t, k
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (k = 1);

--Testcase 74:
SELECT i, j, t, k
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (i = 1);

--
-- semijoin selectivity for <>
--

--Testcase 75:
explain (costs off)
select * from int4_tbl i4, tenk1 a
where exists(select * from tenk1 b
             where a.twothousand = b.twothousand and a.fivethous <> b.fivethous)
      and i4.f1 = a.tenthous;

--
-- More complicated constructs
--

--
-- Multiway full join
--

--Testcase 76:
CREATE FOREIGN TABLE t11 (name TEXT, n INTEGER) SERVER griddb_svr;

--Testcase 77:
CREATE FOREIGN TABLE t21 (name TEXT, n INTEGER) SERVER griddb_svr;

--Testcase 78:
CREATE FOREIGN TABLE t31 (name TEXT, n INTEGER) SERVER griddb_svr;

--Testcase 79:
INSERT INTO t11 VALUES ( 'bb', 11 );

--Testcase 80:
INSERT INTO t21 VALUES ( 'bb', 12 );

--Testcase 81:
INSERT INTO t21 VALUES ( 'cc', 22 );

--Testcase 82:
INSERT INTO t21 VALUES ( 'ee', 42 );

--Testcase 83:
INSERT INTO t31 VALUES ( 'bb', 13 );

--Testcase 84:
INSERT INTO t31 VALUES ( 'cc', 23 );

--Testcase 85:
INSERT INTO t31 VALUES ( 'dd', 33 );

--Testcase 86:
SELECT * FROM t11 FULL JOIN t21 USING (name) FULL JOIN t31 USING (name);

--
-- Test interactions of join syntax and subqueries
--

-- Basic cases (we expect planner to pull up the subquery here)

--Testcase 87:
SELECT * FROM
(SELECT * FROM t21) as s2
INNER JOIN
(SELECT * FROM t31) s3
USING (name);

--Testcase 88:
SELECT * FROM
(SELECT * FROM t21) as s2
LEFT JOIN
(SELECT * FROM t31) s3
USING (name);

--Testcase 89:
SELECT * FROM
(SELECT * FROM t21) as s2
FULL JOIN
(SELECT * FROM t31) s3
USING (name);

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected

--Testcase 90:
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 91:
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL LEFT JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 92:
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 93:
SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t11) as s1
NATURAL INNER JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 94:
SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t11) as s1
NATURAL FULL JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 95:
SELECT * FROM
(SELECT name, n as s1_n FROM t11) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n FROM t21) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t31) as s3
  ) ss2;

--Testcase 96:
SELECT * FROM
(SELECT name, n as s1_n FROM t11) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t31) as s3
  ) ss2;

-- Constants as join keys can also be problematic

--Testcase 97:
SELECT * FROM
  (SELECT name, n as s1_n FROM t11) as s1
FULL JOIN
  (SELECT name, 2 as s2_n FROM t21) as s2
ON (s1_n = s2_n);

-- Test for propagation of nullability constraints into sub-joins

--Testcase 98:
create foreign table x (x1 int, x2 int) server griddb_svr;

--Testcase 99:
insert into x values (1,11);

--Testcase 100:
insert into x values (2,22);

--Testcase 101:
insert into x values (3,null);

--Testcase 102:
insert into x values (4,44);

--Testcase 103:
insert into x values (5,null);

--Testcase 104:
create foreign table y (y1 int, y2 int) server griddb_svr;

--Testcase 105:
insert into y values (1,111);

--Testcase 106:
insert into y values (2,222);

--Testcase 107:
insert into y values (3,333);

--Testcase 108:
insert into y values (4,null);

--Testcase 109:
select * from x;

--Testcase 110:
select * from y;

--Testcase 111:
select * from x left join y on (x1 = y1 and x2 is not null);

--Testcase 112:
select * from x left join y on (x1 = y1 and y2 is not null);

--Testcase 113:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1);

--Testcase 114:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and x2 is not null);

--Testcase 115:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and y2 is not null);

--Testcase 116:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and xx2 is not null);
-- these should NOT give the same answers as above

--Testcase 117:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (x2 is not null);

--Testcase 118:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (y2 is not null);

--Testcase 119:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (xx2 is not null);

--
-- regression test: check for bug with propagation of implied equality
-- to outside an IN
--

--Testcase 120:
select count(*) from tenk1 a where unique1 in
  (select unique1 from tenk1 b join tenk1 c using (unique1)
   where b.unique2 = 42);

--
-- regression test: check for failure to generate a plan with multiple
-- degenerate IN clauses
--

--Testcase 121:
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);

-- try that with GEQO too
begin;

--Testcase 122:
set geqo = on;

--Testcase 123:
set geqo_threshold = 2;

--Testcase 124:
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);
rollback;

--
-- regression test: be sure we cope with proven-dummy append rels
--

--Testcase 125:
create foreign table b0_star (aa int, bb int) server griddb_svr;

--Testcase 126:
explain (costs off)
select aa, bb, unique1, unique1
  from tenk1 right join b0_star on aa = unique1
  where bb < bb and bb is null;

--Testcase 127:
select aa, bb, unique1, unique1
  from tenk1 right join b0_star on aa = unique1
  where bb < bb and bb is null;

--
-- regression test: check handling of empty-FROM subquery underneath outer join
--

--Testcase 128:
explain (costs off)
select * from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 2, 3;

--Testcase 129:
select i1.q1, i1.q2, i2.q1, i2.q2, x from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 1, 2;

--
-- regression test: check a case where join_clause_is_movable_into()
-- used to give an imprecise result, causing an assertion failure
--

--Testcase 130:
select count(*)
from
  (select t31.tenthous as x1, coalesce(t11.stringu1, t21.stringu1) as x2
   from tenk1 t11
   left join tenk1 t21 on t11.unique1 = t21.unique1
   join tenk1 t31 on t11.unique2 = t31.unique2) ss,
  tenk1 t4,
  tenk1 t5
where t4.thousand = t5.unique1 and ss.x1 = t4.tenthous and ss.x2 = t5.stringu1;

--
-- regression test: check a case where we formerly missed including an EC
-- enforcement clause because it was expected to be handled at scan level
--

--Testcase 131:
explain (costs off)
select a.f1, b.f1, t.thousand, t.tenthous from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl i4a) a,
  (select sum(f1) as f1 from int4_tbl i4b) b
where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous;

--Testcase 132:
select a.f1, b.f1, t.thousand, t.tenthous from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl i4a) a,
  (select sum(f1) as f1 from int4_tbl i4b) b
where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous;

--
-- checks for correct handling of quals in multiway outer joins
--
--Testcase 710:
explain (costs off)
select t1.f1
from int4_tbl t1, int4_tbl t2
  left join int4_tbl t3 on t3.f1 > 0
  left join int4_tbl t4 on t3.f1 > 1
where t4.f1 is null;

--Testcase 711:
select t1.f1
from int4_tbl t1, int4_tbl t2
  left join int4_tbl t3 on t3.f1 > 0
  left join int4_tbl t4 on t3.f1 > 1
where t4.f1 is null;

--Testcase 712:
explain (costs off)
select *
from int4_tbl t1 left join int4_tbl t2 on true
  left join int4_tbl t3 on t2.f1 > 0
  left join int4_tbl t4 on t3.f1 > 0;

--Testcase 713:
explain (costs off)
select * from onek t1
  left join onek t2 on t1.unique1 = t2.unique1
  left join onek t3 on t2.unique1 != t3.unique1
  left join onek t4 on t3.unique1 = t4.unique1;

--Testcase 714:
explain (costs off)
select * from int4_tbl t1
  left join (select now() from int4_tbl t2
             left join int4_tbl t3 on t2.f1 = t3.f1
             left join int4_tbl t4 on t3.f1 = t4.f1) s on true
  inner join int4_tbl t5 on true;

--Testcase 715:
explain (costs off)
select * from int4_tbl t1
  left join int4_tbl t2 on true
  left join int4_tbl t3 on true
  left join int4_tbl t4 on t2.f1 = t3.f1;

--Testcase 716:
explain (costs off)
select * from int4_tbl t1
  left join int4_tbl t2 on true
  left join int4_tbl t3 on t2.f1 = t3.f1
  left join int4_tbl t4 on t3.f1 != t4.f1;

--Testcase 717:
explain (costs off)
select * from int4_tbl t1
  left join (int4_tbl t2 left join int4_tbl t3 on t2.f1 > 0) on t2.f1 > 1
  left join int4_tbl t4 on t2.f1 > 2 and t3.f1 > 3
where t1.f1 = coalesce(t2.f1, 1);

--Testcase 718:
explain (costs off)
select * from int4_tbl t1
  left join ((select t2.f1 from int4_tbl t2
                left join int4_tbl t3 on t2.f1 > 0
                where t3.f1 is null) s
             left join tenk1 t4 on s.f1 > 1)
    on s.f1 = t1.f1;

--Testcase 773:
explain (costs off)
select * from int4_tbl t1
  left join ((select t2.f1 from int4_tbl t2
                left join int4_tbl t3 on t2.f1 > 0
                where t2.f1 <> coalesce(t3.f1, -1)) s
             left join tenk1 t4 on s.f1 > 1)
    on s.f1 = t1.f1;

--Testcase 719:
explain (costs off)
select * from onek t1
    left join onek t2 on t1.unique1 = t2.unique1
    left join onek t3 on t2.unique1 = t3.unique1
    left join onek t4 on t3.unique1 = t4.unique1 and t2.unique2 = t4.unique2;

--Testcase 774:
explain (costs off)
select * from int8_tbl t1 left join
    (int8_tbl t2 left join int8_tbl t3 full join int8_tbl t4 on false on false)
    left join int8_tbl t5 on t2.q1 = t5.q1
on t2.q2 = 123;

--Testcase 775:
explain (costs off)
select * from int8_tbl t1
    left join int8_tbl t2 on true
    left join lateral
      (select * from int8_tbl t3 where t3.q1 = t2.q1 offset 0) s
      on t2.q1 = 1;

--Testcase 776:
explain (costs off)
select * from int8_tbl t1
    left join int8_tbl t2 on true
    left join lateral
      (select * from generate_series(t2.q1, 100)) s
      on t2.q1 = 1;

--Testcase 777:
explain (costs off)
select * from int8_tbl t1
    left join int8_tbl t2 on true
    left join lateral
      (select t2.q1 from int8_tbl t3) s
      on t2.q1 = 1;

--Testcase 778:
explain (costs off)
select * from onek t1
    left join onek t2 on true
    left join lateral
      (select * from onek t3 where t3.two = t2.two offset 0) s
      on t2.unique1 = 1;

--
-- check a case where we formerly got confused by conflicting sort orders
-- in redundant merge join path keys
--

--Testcase 133:
explain (costs off)
select * from
  j1_tbl full join
  (select * from j2_tbl order by j2_tbl.i desc, j2_tbl.k asc) j2_tbl
  on j1_tbl.i = j2_tbl.i and j1_tbl.i = j2_tbl.k;

--Testcase 134:
select j1_tbl.i, j1_tbl.j, j1_tbl.t, j2_tbl.i, j2_tbl.k from
  j1_tbl full join
  (select * from j2_tbl order by j2_tbl.i desc, j2_tbl.k asc) j2_tbl
  on j1_tbl.i = j2_tbl.i and j1_tbl.i = j2_tbl.k;

--
-- a different check for handling of redundant sort keys in merge joins
--

--Testcase 135:
explain (costs off)
select count(*) from
  (select * from tenk1 x order by x.thousand, x.twothousand, x.fivethous) x
  left join
  (select * from tenk1 y order by y.unique2) y
  on x.thousand = y.unique2 and x.twothousand = y.hundred and x.fivethous = y.unique2;

--Testcase 136:
select count(*) from
  (select * from tenk1 x order by x.thousand, x.twothousand, x.fivethous) x
  left join
  (select * from tenk1 y order by y.unique2) y
  on x.thousand = y.unique2 and x.twothousand = y.hundred and x.fivethous = y.unique2;

--Testcase 720:
set enable_hashjoin = 0;
--Testcase 721:
set enable_nestloop = 0;
--Testcase 722:
set enable_hashagg = 0;

--
-- Check that we use the pathkeys from a prefix of the group by / order by
-- clause for the join pathkeys when that prefix covers all join quals.  We
-- expect this to lead to an incremental sort for the group by / order by.
--
--Testcase 723:
explain (costs off)
select x.thousand, x.twothousand, count(*)
from tenk1 x inner join tenk1 y on x.thousand = y.thousand
group by x.thousand, x.twothousand
order by x.thousand desc, x.twothousand;

--Testcase 724:
reset enable_hashagg;
--Testcase 725:
reset enable_nestloop;
--Testcase 726:
reset enable_hashjoin;

--
-- Clean up
--

--Testcase 137:
DROP FOREIGN TABLE t11;

--Testcase 138:
DROP FOREIGN TABLE t21;

--Testcase 139:
DROP FOREIGN TABLE t31;

--Testcase 140:
DROP FOREIGN TABLE J1_TBL;

--Testcase 141:
DROP FOREIGN TABLE J2_TBL;

-- Both DELETE and UPDATE allow the specification of additional tables
-- to "join" against to determine which rows should be modified.

--Testcase 142:
CREATE FOREIGN TABLE t12 (a int OPTIONS (rowkey 'true'), b int) SERVER griddb_svr;

--Testcase 143:
CREATE FOREIGN TABLE t22 (a int OPTIONS (rowkey 'true'), b int) SERVER griddb_svr;

--Testcase 144:
CREATE FOREIGN TABLE t32 (x int OPTIONS (rowkey 'true'), y int) SERVER griddb_svr;

--Testcase 145:
INSERT INTO t12 VALUES (5, 10);

--Testcase 146:
INSERT INTO t12 VALUES (15, 20);

--Testcase 147:
INSERT INTO t12 VALUES (100, 100);

--Testcase 148:
INSERT INTO t12 VALUES (200, 1000);

--Testcase 149:
INSERT INTO t22 VALUES (200, 2000);

--Testcase 150:
INSERT INTO t32 VALUES (5, 20);

--Testcase 151:
INSERT INTO t32 VALUES (6, 7);

--Testcase 152:
INSERT INTO t32 VALUES (7, 8);

--Testcase 153:
INSERT INTO t32 VALUES (500, 100);

--Testcase 154:
DELETE FROM t32 USING t12 table1 WHERE t32.x = table1.a;

--Testcase 155:
SELECT * FROM t32;

--Testcase 156:
DELETE FROM t32 USING t12 JOIN t22 USING (a) WHERE t32.x > t12.a;

--Testcase 157:
SELECT * FROM t32;

--Testcase 158:
DELETE FROM t32 USING t32 t3_other WHERE t32.x = t3_other.x AND t32.y = t3_other.y;

--Testcase 159:
SELECT * FROM t32;

-- Test join against inheritance tree

--Testcase 160:
create temp table t2a () inherits (t22);

--Testcase 161:
insert into t2a values (200, 2001);

--Testcase 162:
select * from t12 left join t22 on (t12.a = t22.a);

-- Test matching of column name with wrong alias

--Testcase 163:
select t12.x from t12 join t32 on (t12.a = t32.x);

-- Test matching of locking clause with wrong alias

--Testcase 705:
select t12.*, t22.*, unnamed_join.* from
  t12 join t22 on (t12.a = t22.a), t32 as unnamed_join
  for update of unnamed_join;

--Testcase 706:
select foo.*, unnamed_join.* from
  t12 join t22 using (a) as foo, t32 as unnamed_join
  for update of unnamed_join;

--Testcase 707:
select foo.*, unnamed_join.* from
  t12 join t22 using (a) as foo, t32 as unnamed_join
  for update of foo;

--Testcase 708:
select bar.*, unnamed_join.* from
  (t12 join t22 using (a) as foo) as bar, t32 as unnamed_join
  for update of foo;

--Testcase 709:
select bar.*, unnamed_join.* from
  (t12 join t22 using (a) as foo) as bar, t32 as unnamed_join
  for update of bar;

--
-- regression test for 8.1 merge right join bug
--

--Testcase 164:
CREATE FOREIGN TABLE tt1 ( tt1_id int4, joincol int4 ) SERVER griddb_svr;

--Testcase 165:
INSERT INTO tt1 VALUES (1, 11);

--Testcase 166:
INSERT INTO tt1 VALUES (2, NULL);

--Testcase 167:
CREATE FOREIGN TABLE tt2 ( tt2_id int4, joincol int4 ) SERVER griddb_svr;

--Testcase 168:
INSERT INTO tt2 VALUES (21, 11);

--Testcase 169:
INSERT INTO tt2 VALUES (22, 11);

--Testcase 170:
set enable_hashjoin to off;

--Testcase 171:
set enable_nestloop to off;

-- these should give the same results

--Testcase 172:
select tt1.*, tt2.* from tt1 left join tt2 on tt1.joincol = tt2.joincol;

--Testcase 173:
select tt1.*, tt2.* from tt2 right join tt1 on tt1.joincol = tt2.joincol;

--Testcase 174:
reset enable_hashjoin;

--Testcase 175:
reset enable_nestloop;

--
-- regression test for bug #13908 (hash join with skew tuples & nbatch increase)
--

--Testcase 176:
set work_mem to '64kB';

--Testcase 177:
set enable_mergejoin to off;

--Testcase 178:
set enable_memoize to off;

--Testcase 179:
explain (costs off)
select count(*) from tenk1 a, tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;

--Testcase 180:
select count(*) from tenk1 a, tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;

--Testcase 181:
reset work_mem;

--Testcase 182:
reset enable_mergejoin;

--Testcase 183:
reset enable_memoize;

--
-- regression test for 8.2 bug with improper re-ordering of left joins
--

--Testcase 184:
create foreign table tt3(f1 int, f2 text) server griddb_svr;

--Testcase 185:
insert into tt3 select x, repeat('xyzzy', 100) from generate_series(1,10000) x;

--Testcase 186:
create foreign table tt4(f1 int) server griddb_svr;

--Testcase 187:
insert into tt4 values (0),(1),(9999);

--Testcase 727:
set enable_nestloop to off;

--Testcase 728:
EXPLAIN (COSTS OFF)
SELECT a.f1
FROM tt4 a
LEFT JOIN (
        SELECT b.f1
        FROM tt3 b LEFT JOIN tt3 c ON (b.f1 = c.f1)
        WHERE COALESCE(c.f1, 0) = 0
) AS d ON (a.f1 = d.f1)
WHERE COALESCE(d.f1, 0) = 0
ORDER BY 1;

--Testcase 729:
SELECT a.f1
FROM tt4 a
LEFT JOIN (
        SELECT b.f1
        FROM tt3 b LEFT JOIN tt3 c ON (b.f1 = c.f1)
        WHERE COALESCE(c.f1, 0) = 0
) AS d ON (a.f1 = d.f1)
WHERE COALESCE(d.f1, 0) = 0
ORDER BY 1;

--Testcase 730:
reset enable_nestloop;

--
-- basic semijoin and antijoin recognition tests
--
--Testcase 731:
explain (costs off)
select a.* from tenk1 a
where unique1 in (select unique2 from tenk1 b);

-- sadly, this is not an antijoin
--Testcase 732:
explain (costs off)
select a.* from tenk1 a
where unique1 not in (select unique2 from tenk1 b);

--Testcase 733:
explain (costs off)
select a.* from tenk1 a
where exists (select 1 from tenk1 b where a.unique1 = b.unique2);

--Testcase 734:
explain (costs off)
select a.* from tenk1 a
where not exists (select 1 from tenk1 b where a.unique1 = b.unique2);

--Testcase 735:
explain (costs off)
select a.* from tenk1 a left join tenk1 b on a.unique1 = b.unique2
where b.unique2 is null;

--
-- regression test for proper handling of outer joins within antijoins
--

--Testcase 189:
create foreign table tt4x(c1 int, c2 int, c3 int) server griddb_svr;

--Testcase 190:
explain (costs off)
select * from tt4x t1
where not exists (
  select 1 from tt4x t2
    left join tt4x t3 on t2.c3 = t3.c1
    left join ( select t5.c1 as c1
                from tt4x t4 left join tt4x t5 on t4.c2 = t5.c1
              ) a1 on t3.c2 = a1.c1
  where t1.c1 = t2.c2
);

--
-- regression test for problems of the sort depicted in bug #3494
--

--Testcase 191:
create foreign table tt5(id serial, f1 int, f2 int) server griddb_svr;

--Testcase 192:
create foreign table tt6(id serial, f1 int, f2 int) server griddb_svr;

--Testcase 193:
insert into tt5(f1, f2) values(1, 10);

--Testcase 194:
insert into tt5(f1, f2) values(1, 11);

--Testcase 195:
insert into tt6(f1, f2) values(1, 9);

--Testcase 196:
insert into tt6(f1, f2) values(1, 2);

--Testcase 197:
insert into tt6(f1, f2) values(2, 9);

--Testcase 198:
select tt5.f1, tt5.f2, tt6.f1, tt6.f2 from tt5,tt6 where tt5.f1 = tt6.f1 and tt5.f1 = tt5.f2 - tt6.f2;

--
-- regression test for problems of the sort depicted in bug #3588
--

--Testcase 199:
create foreign table xx (pkxx int) server griddb_svr;

--Testcase 200:
create foreign table yy (pkyy int, pkxx int) server griddb_svr;

--Testcase 201:
insert into xx values (1);

--Testcase 202:
insert into xx values (2);

--Testcase 203:
insert into xx values (3);

--Testcase 204:
insert into yy values (101, 1);

--Testcase 205:
insert into yy values (201, 2);

--Testcase 206:
insert into yy values (301, NULL);

--Testcase 207:
select yy.pkyy as yy_pkyy, yy.pkxx as yy_pkxx, yya.pkyy as yya_pkyy,
       xxa.pkxx as xxa_pkxx, xxb.pkxx as xxb_pkxx
from yy
     left join (SELECT * FROM yy where pkyy = 101) as yya ON yy.pkyy = yya.pkyy
     left join xx xxa on yya.pkxx = xxa.pkxx
     left join xx xxb on coalesce (xxa.pkxx, 1) = xxb.pkxx;

--
-- regression test for improper pushing of constants across outer-join clauses
-- (as seen in early 8.2.x releases)
--

--Testcase 208:
create foreign table zt1 (f1 int OPTIONS(rowkey 'true')) server griddb_svr;

--Testcase 209:
create foreign table zt2 (f2 int OPTIONS(rowkey 'true')) server griddb_svr;

--Testcase 210:
create foreign table zt3 (f3 int OPTIONS(rowkey 'true')) server griddb_svr;

--Testcase 211:
insert into zt1 values(53);

--Testcase 212:
insert into zt2 values(53);

--Testcase 213:
select * from
  zt2 left join zt3 on (f2 = f3)
      left join zt1 on (f3 = f1)
where f2 = 53;

--Testcase 214:
create temp view zv1 as select *,'dummy'::text AS junk from zt1;

--Testcase 215:
select * from
  zt2 left join zt3 on (f2 = f3)
      left join zv1 on (f3 = f1)
where f2 = 53;

--
-- regression test for improper extraction of OR indexqual conditions
-- (as seen in early 8.3.x releases)
--

--Testcase 216:
select a.unique2, a.ten, b.tenthous, b.unique2, b.hundred
from tenk1 a left join tenk1 b on a.unique2 = b.tenthous
where a.unique1 = 42 and
      ((b.unique2 is null and a.ten = 2) or b.hundred = 3);

--
-- test proper positioning of one-time quals in EXISTS (8.4devel bug)
--

--Testcase 217:
prepare foo(bool) as
  select count(*) from tenk1 a left join tenk1 b
    on (a.unique2 = b.unique1 and exists
        (select 1 from tenk1 c where c.thousand = b.unique2 and $1));

--Testcase 218:
execute foo(true);

--Testcase 219:
execute foo(false);

--
-- test for sane behavior with noncanonical merge clauses, per bug #4926
--

begin;

--Testcase 220:
set enable_mergejoin = 1;

--Testcase 221:
set enable_hashjoin = 0;

--Testcase 222:
set enable_nestloop = 0;

--Testcase 223:
create foreign table a1 (i integer) server griddb_svr;

--Testcase 224:
create foreign table b1 (x integer, y integer) server griddb_svr;

--Testcase 225:
select * from a1 left join b1 on i = x and i = y and x = i;

rollback;

-- skip this test, can not create type on GridDB
-- test handling of merge clauses using record_ops
--
--begin;

--create type mycomptype as (id int, v bigint);

--create temp table tidv (idv mycomptype);
--create index on tidv (idv);

--explain (costs off)
--select a.idv, b.idv from tidv a, tidv b where a.idv = b.idv;

--set enable_mergejoin = 0;

--explain (costs off)
--select a.idv, b.idv from tidv a, tidv b where a.idv = b.idv;

--rollback;

--
-- test NULL behavior of whole-row Vars, per bug #5025
--

--Testcase 226:
select t1.q2, count(t2.*)
from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--Testcase 227:
select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--Testcase 228:
select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl offset 0) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--Testcase 229:
select t1.q2, count(t2.*)
from int8_tbl t1 left join
  (select q1, case when q2=1 then 1 else q2 end as q2 from int8_tbl) t2
  on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--
-- test incorrect failure to NULL pulled-up subexpressions
--
begin;

--Testcase 230:
create foreign table a2 (
     code text OPTIONS (rowkey 'true')
) server griddb_svr;

--Testcase 231:
create foreign table b2 (
     id serial OPTIONS (rowkey 'true'),
     a text,
     num integer
) server griddb_svr;

--Testcase 232:
create foreign table c2 (
     name text OPTIONS (rowkey 'true'),
     a text
) server griddb_svr;

--Testcase 233:
insert into a2 (code) values ('p');

--Testcase 234:
insert into a2 (code) values ('q');

--Testcase 235:
insert into b2 (a, num) values ('p', 1);

--Testcase 236:
insert into b2 (a, num) values ('p', 2);

--Testcase 237:
insert into c2 (name, a) values ('A', 'p');

--Testcase 238:
insert into c2 (name, a) values ('B', 'q');

--Testcase 239:
insert into c2 (name, a) values ('C', null);

--Testcase 240:
select c2.name, ss.code, ss.b_cnt, ss.const
from c2 left join
  (select a2.code, coalesce(b_grp.cnt, 0) as b_cnt, -1 as const
   from a2 left join
     (select count(1) as cnt, b2.a from b2 group by b2.a) as b_grp
     on a2.code = b_grp.a
  ) as ss
  on (c2.a = ss.code)
order by c2.name;

rollback;

--
-- test incorrect handling of placeholders that only appear in targetlists,
-- per bug #6154
--
begin;

--Testcase 241:
create foreign table a1 (i integer) server griddb_svr;

--Testcase 242:
create foreign table b1 (x integer, y integer) server griddb_svr;

--Testcase 243:
INSERT INTO a1 values (1);

--Testcase 244:
INSERT INTO b1 values (2, 42);

--Testcase 245:
SELECT * FROM
( SELECT i as key1 FROM a1) sub1
LEFT JOIN
( SELECT sub3.key3, sub4.value2, COALESCE(sub4.value2, 66) as value3 FROM
    ( SELECT i as key3 FROM a1) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT i as key5 FROM a1 ) sub5
        LEFT JOIN
        ( SELECT x as key6, y as value1 FROM b1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

-- test the path using join aliases, too

--Testcase 246:
SELECT * FROM
( SELECT i as key1 FROM a1 ) sub1
LEFT JOIN
( SELECT sub3.key3, value2, COALESCE(value2, 66) as value3 FROM
    ( SELECT i as key3 FROM a1) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT i as key5 FROM a1 ) sub5
        LEFT JOIN
        ( SELECT x as key6, y as value1 FROM b1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;
rollback;
--
-- test case where a PlaceHolderVar is used as a nestloop parameter
--

--Testcase 247:
EXPLAIN (COSTS OFF)
SELECT qq, unique1
  FROM
  ( SELECT COALESCE(q1, 0) AS qq FROM int8_tbl a ) AS ss1
  FULL OUTER JOIN
  ( SELECT COALESCE(q2, -1) AS qq FROM int8_tbl b ) AS ss2
  USING (qq)
  INNER JOIN tenk1 c ON qq = unique2;

--Testcase 248:
SELECT qq, unique1
  FROM
  ( SELECT COALESCE(q1, 0) AS qq FROM int8_tbl a ) AS ss1
  FULL OUTER JOIN
  ( SELECT COALESCE(q2, -1) AS qq FROM int8_tbl b ) AS ss2
  USING (qq)
  INNER JOIN tenk1 c ON qq = unique2;

--
-- nested nestloops can require nested PlaceHolderVars
--

--Testcase 249:
create foreign table nt1 (
  id int OPTIONS (rowkey 'true'),
  a1 boolean,
  a2 boolean
) server griddb_svr;

--Testcase 250:
create foreign table nt2 (
  id int OPTIONS (rowkey 'true'),
  nt1_id int,
  b1 boolean,
  b2 boolean
) server griddb_svr;

--Testcase 251:
create foreign table nt3 (
  id int OPTIONS (rowkey 'true'),
  nt2_id int,
  c1 boolean
) server griddb_svr;

--Testcase 252:
insert into nt1 values (1,true,true);

--Testcase 253:
insert into nt1 values (2,true,false);

--Testcase 254:
insert into nt1 values (3,false,false);

--Testcase 255:
insert into nt2 values (1,1,true,true);

--Testcase 256:
insert into nt2 values (2,2,true,false);

--Testcase 257:
insert into nt2 values (3,3,false,false);

--Testcase 258:
insert into nt3 values (1,1,true);

--Testcase 259:
insert into nt3 values (2,2,false);

--Testcase 260:
insert into nt3 values (3,3,true);

--Testcase 261:
explain (costs off)
select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 and ss1.a3) AS b3
     from nt2 as nt2
       left join
         (select nt1.*, (nt1.id is not null) as a3 from nt1) as ss1
         on ss1.id = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

--Testcase 262:
select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 and ss1.a3) AS b3
     from nt2 as nt2
       left join
         (select nt1.*, (nt1.id is not null) as a3 from nt1) as ss1
         on ss1.id = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

--
-- test case where a PlaceHolderVar is propagated into a subquery
--

--Testcase 263:
explain (costs off)
select * from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 2,3;

--Testcase 264:
select q1, q2, x, y from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 1,2;

--
-- variant where a PlaceHolderVar is needed at a join, but not above the join
--

--Testcase 265:
explain (costs off)
select * from
  int4_tbl as i41,
  lateral
    (select 1 as x from
      (select i41.f1 as lat,
              i42.f1 as loc from
         int8_tbl as i81, int4_tbl as i42) as ss1
      right join int4_tbl as i43 on (i43.f1 > 1)
      where ss1.loc = ss1.lat) as ss2
where i41.f1 > 0;

--Testcase 266:
select * from
  int4_tbl as i41,
  lateral
    (select 1 as x from
      (select i41.f1 as lat,
              i42.f1 as loc from
         int8_tbl as i81, int4_tbl as i42) as ss1
      right join int4_tbl as i43 on (i43.f1 > 1)
      where ss1.loc = ss1.lat) as ss2
where i41.f1 > 0;

--
-- test the corner cases FULL JOIN ON TRUE and FULL JOIN ON FALSE
--

--Testcase 267:
select a.f1, b.f1 from int4_tbl a full join int4_tbl b on true;

--Testcase 268:
select a.f1, b.f1 from int4_tbl a full join int4_tbl b on false;

--
-- test for ability to use a cartesian join when necessary
--

--Testcase 269:
create foreign table q1(q1 int OPTIONS (rowkey 'true')) server griddb_svr;

--Testcase 270:
create foreign table q2(q2 int OPTIONS (rowkey 'true')) server griddb_svr;

--Testcase 271:
insert into q1 select 1;

--Testcase 272:
insert into q1 select 0;

--Testcase 273:
explain (costs off)
select * from
  tenk1 join int4_tbl on f1 = twothousand,
  q1, q2
where q1 = thousand or q2 = thousand;

--Testcase 274:
explain (costs off)
select * from
  tenk1 join int4_tbl on f1 = twothousand,
  q1, q2
where thousand = (q1 + q2);

--Testcase 275:
drop foreign table q1, q2;

--
-- test ability to generate a suitable plan for a star-schema query
--

--Testcase 276:
explain (costs off)
select * from
  tenk1, int8_tbl a, int8_tbl b
where thousand = a.q1 and tenthous = b.q1 and a.q2 = 1 and b.q2 = 2;

--
-- test a corner case in which we shouldn't apply the star-schema optimization
--

--Testcase 277:
explain (costs off)
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (select 1,0 from onerow) v1(x1,x2)
               left join (select 3,1 from onerow) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

--Testcase 278:
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (select 1,0 from onerow) v1(x1,x2)
               left join (select 3,1 from onerow) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

-- variant that isn't quite a star-schema case

--Testcase 279:
select ss1.d1 from
  tenk1 as t1
  inner join tenk1 as t2
  on t1.tenthous = t2.ten
  inner join
    int8_tbl as i8
    left join int4_tbl as i4
      inner join (select 64::information_schema.cardinal_number as d1
                  from tenk1 t3,
                       lateral (select abs(t3.unique1) + random()) ss0(x)
                  where t3.fivethous < 0) as ss1
      on i4.f1 = ss1.d1
    on i8.q1 = i4.f1
  on t1.tenthous = ss1.d1
where t1.unique1 < i4.f1;

-- this variant is foldable by the remove-useless-RESULT-RTEs code

--Testcase 280:
explain (costs off)
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (values(1,0)) v1(x1,x2)
               left join (values(3,1)) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

--Testcase 281:
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (values(1,0)) v1(x1,x2)
               left join (values(3,1)) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

-- Here's a variant that we can't fold too aggressively, though,
-- or we end up with noplace to evaluate the lateral PHV

--Testcase 282:
explain (verbose, costs off)
select * from
  (select 1 as x) ss1 left join (select 2 as y) ss2 on (true),
  lateral (select ss2.y as z limit 1) ss3;

--Testcase 283:
select * from
  (select 1 as x) ss1 left join (select 2 as y) ss2 on (true),
  lateral (select ss2.y as z limit 1) ss3;

-- Test proper handling of appendrel PHVs during useless-RTE removal

--Testcase 284:
explain (costs off)
select * from
  (select 0 as z) as t1
  left join
  (select true as a) as t2
  on true,
  lateral (select true as b
           union all
           select a as b) as t3
where b;

--Testcase 285:
select * from
  (select 0 as z) as t1
  left join
  (select true as a) as t2
  on true,
  lateral (select true as b
           union all
           select a as b) as t3
where b;

-- Test PHV in a semijoin qual, which confused useless-RTE removal (bug #17700)
--Testcase 779:
explain (verbose, costs off)
with ctetable as not materialized ( select 1 as f1 )
select * from ctetable c1
where f1 in ( select c3.f1 from ctetable c2 full join ctetable c3 on true );

--Testcase 780:
with ctetable as not materialized ( select 1 as f1 )
select * from ctetable c1
where f1 in ( select c3.f1 from ctetable c2 full join ctetable c3 on true );

-- Test PHV that winds up in a Result node, despite having nonempty nullingrels
--Testcase 781:
explain (verbose, costs off)
select table_catalog, table_name
from int4_tbl t1
  inner join (int8_tbl t2
              left join information_schema.column_udt_usage on null)
  on null;

-- Test handling of qual pushdown to appendrel members with non-Var outputs
--Testcase 782:
explain (verbose, costs off)
select * from int4_tbl left join (
  select text 'foo' union all select text 'bar'
) ss(x) on true
where ss.x is null;

--
-- test inlining of immutable functions
--

--Testcase 286:
create function f_immutable_int4(i integer) returns integer as
$$ begin return i; end; $$ language plpgsql immutable;

-- check optimization of function scan with join

--Testcase 287:
explain (costs off)
select unique1 from tenk1, (select * from f_immutable_int4(1) x) x
where x = unique1;

--Testcase 288:
explain (verbose, costs off)
select unique1, x.*
from tenk1, (select *, random() from f_immutable_int4(1) x) x
where x = unique1;

--Testcase 289:
explain (costs off)
select unique1 from tenk1, f_immutable_int4(1) x where x = unique1;

--Testcase 290:
explain (costs off)
select unique1 from tenk1, lateral f_immutable_int4(1) x where x = unique1;

--Testcase 704:
explain (costs off)
select unique1 from tenk1, lateral f_immutable_int4(1) x where x in (select 17);

--Testcase 291:
explain (costs off)
select unique1, x from tenk1 join f_immutable_int4(1) x on unique1 = x;

--Testcase 292:
explain (costs off)
select unique1, x from tenk1 left join f_immutable_int4(1) x on unique1 = x;

--Testcase 293:
explain (costs off)
select unique1, x from tenk1 right join f_immutable_int4(1) x on unique1 = x;

--Testcase 294:
explain (costs off)
select unique1, x from tenk1 full join f_immutable_int4(1) x on unique1 = x;

-- check that pullup of a const function allows further const-folding

--Testcase 295:
explain (costs off)
select unique1 from tenk1, f_immutable_int4(1) x where x = 42;

-- test inlining of immutable functions with PlaceHolderVars

--Testcase 296:
explain (costs off)
select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 or i4 = 42) AS b3
     from nt2 as nt2
       left join
         f_immutable_int4(0) i4
         on i4 = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

--Testcase 297:
drop function f_immutable_int4(int);

-- skip test because cannot cast type record to int8_tbl
-- test inlining when function returns composite

--create function mki8(bigint, bigint) returns int8_tbl as
--$$select row($1,$2)::int8_tbl$$ language sql;

--create function mki4(int) returns int4_tbl as
--$$select row($1)::int4_tbl$$ language sql;

--explain (verbose, costs off)
--select * from mki8(1,2);
--select * from mki8(1,2);

--explain (verbose, costs off)
--select * from mki4(42);
--select * from mki4(42);

--drop function mki8(bigint, bigint);
--drop function mki4(int);

--
-- test extraction of restriction OR clauses from join OR clause
-- (we used to only do this for indexable clauses)
--

--Testcase 298:
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or (a.unique2 = 3 and b.hundred = 4);

--Testcase 299:
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or (a.unique2 = 3 and b.ten = 4);

--Testcase 300:
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or
  ((a.unique2 = 3 or a.unique2 = 7) and b.hundred = 4);

--
-- test placement of movable quals in a parameterized join tree
--

--Testcase 301:
explain (costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten = t3.ten
where t1.unique1 = 1;

--Testcase 302:
explain (costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten + t2.ten = t3.ten
where t1.unique1 = 1;

--Testcase 303:
explain (costs off)
select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

--Testcase 304:
select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

--Testcase 305:
explain (costs off)
select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

--Testcase 306:
select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

--Testcase 307:
explain (costs off)
select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

--Testcase 308:
select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

--Testcase 309:
explain (costs off)
select * from
(values (1, array[10,20]), (2, array[20,30])) as v1(v1x,v1ys)
left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
left join unnest(v1ys) as u1(u1y) on u1y = v2y;

--Testcase 310:
select * from
(values (1, array[10,20]), (2, array[20,30])) as v1(v1x,v1ys)
left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
left join unnest(v1ys) as u1(u1y) on u1y = v2y;

--
-- test handling of potential equivalence clauses above outer joins
--

--Testcase 311:
explain (costs off)
select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

--Testcase 312:
select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

--Testcase 313:
explain (costs off)
select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

--Testcase 314:
select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

--
-- another case with equivalence clauses above outer joins (bug #8591)
--

--Testcase 315:
explain (costs off)
select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

--Testcase 316:
select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

-- related case
--Testcase 736:
explain (costs off)
select * from int8_tbl t1 left join int8_tbl t2 on t1.q2 = t2.q1,
  lateral (select * from int8_tbl t3 where t2.q1 = t2.q2) ss;
--Testcase 737:
select * from int8_tbl t1 left join int8_tbl t2 on t1.q2 = t2.q1,
  lateral (select * from int8_tbl t3 where t2.q1 = t2.q2) ss;

--
-- check handling of join aliases when flattening multiple levels of subquery
--

--Testcase 317:
explain (verbose, costs off)
select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from int4_tbl i1) ss1
    ) foo2
   left join
    (select unique2 as join_key from tenk1 i2) ss2
   using (join_key)
  ) foo3
using (join_key);

--Testcase 318:
select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from int4_tbl i1) ss1
    ) foo2
   left join
    (select unique2 as join_key from tenk1 i2) ss2
   using (join_key)
  ) foo3
using (join_key);

--
-- check handling of a variable-free join alias
--
--Testcase 738:
explain (verbose, costs off)
select * from
int4_tbl i0 left join
( (select *, 123 as x from int4_tbl i1) ss1
  left join
  (select *, q2 as x from int8_tbl i2) ss2
  using (x)
) ss0
on (i0.f1 = ss0.f1)
order by i0.f1, x;
--Testcase 739:
select * from
int4_tbl i0 left join
( (select *, 123 as x from int4_tbl i1) ss1
  left join
  (select *, q2 as x from int8_tbl i2) ss2
  using (x)
) ss0
on (i0.f1 = ss0.f1)
order by i0.f1, x;

--
-- test successful handling of nested outer joins with degenerate join quals
--

--Testcase 319:
create foreign table text_tbl(f1 text) server griddb_svr;

--Testcase 320:
explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 321:
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 322:
explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 323:
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 324:
explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2
                 where q1 = f1) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 325:
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2
                 where q1 = f1) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 326:
explain (verbose, costs off)
select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

--Testcase 327:
select t1.f1, i8.q1, i8.q2, t2.f1, i4.f1 from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

-- check handling of a variable-free qual for a non-commutable outer join
--Testcase 740:
explain (costs off)
select nspname
from (select 1 as x) ss1
left join
( select n.nspname, c.relname
  from pg_class c left join pg_namespace n on n.oid = c.relnamespace
  where c.relkind = 'r'
) ss2 on false;

-- check handling of apparently-commutable outer joins with non-commutable
-- joins between them
--Testcase 741:
explain (costs off)
select 1 from
  int4_tbl i4
  left join int8_tbl i8 on i4.f1 is not null
  left join (select 1 as a) ss1 on null
  join int4_tbl i42 on ss1.a is null or i8.q1 <> i8.q2
  right join (select 2 as b) ss2
  on ss2.b < i4.f1;

--
-- test for appropriate join order in the presence of lateral references
--

--Testcase 328:
explain (verbose, costs off)
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss
where t1.f1 = ss.f1;

--Testcase 329:
select t1.f1, i8.q1, i8.q2, ss.q1, ss.f1 from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss
where t1.f1 = ss.f1;

--Testcase 330:
explain (verbose, costs off)
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

--Testcase 331:
select t1.f1, i8.q1, i8.q2, ss1.q1, ss1.f1, ss2.q1, ss2.f1 from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

--Testcase 332:
explain (verbose, costs off)
select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

--Testcase 333:
select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

--
-- check a case where we formerly generated invalid parameterized paths
--

begin;

--Testcase 783:
create foreign table t(a int OPTIONS (rowkey 'true')) server griddb_svr;

--Testcase 784:
explain (costs off)
select 1 from t t1
  join lateral (select t1.a from (select 1) foo offset 0) as s1 on true
  join
    (select 1 from t t2
       inner join (t t3
                   left join (t t4 left join t t5 on t4.a = 1)
                   on t3.a = t4.a)
       on false
     where t3.a = coalesce(t5.a,1)) as s2
  on true;

rollback;

--
-- check a case in which a PlaceHolderVar forces join order
--

--Testcase 334:
explain (verbose, costs off)
select ss2.* from
  int4_tbl i41
  left join int8_tbl i8
    join (select i42.f1 as c1, i43.f1 as c2, 42 as c3
          from int4_tbl i42, int4_tbl i43) ss1
    on i8.q1 = ss1.c2
  on i41.f1 = ss1.c1,
  lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2
where ss1.c2 = 0;

--Testcase 335:
select ss2.f1, ss2.q1, ss2.q2, ss2.c1, ss2.c2, ss2.c3 from
  int4_tbl i41
  left join int8_tbl i8
    join (select i42.f1 as c1, i43.f1 as c2, 42 as c3
          from int4_tbl i42, int4_tbl i43) ss1
    on i8.q1 = ss1.c2
  on i41.f1 = ss1.c1,
  lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2
where ss1.c2 = 0;

--
-- test successful handling of full join underneath left join (bug #14105)
--

--Testcase 336:
explain (costs off)
select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

--Testcase 337:
select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

--
-- test ability to push constants through outer join clauses
--

--Testcase 338:
explain (costs off)
  select * from int4_tbl a left join tenk1 b on f1 = unique2 where f1 = 0;

--Testcase 339:
explain (costs off)
  select * from tenk1 a full join tenk1 b using(unique2) where unique2 = 42;

--
-- test that quals attached to an outer join have correct semantics,
-- specifically that they don't re-use expressions computed below the join;
-- we force a mergejoin so that coalesce(b.q1, 1) appears as a join input
--

--Testcase 340:
set enable_hashjoin to off;

--Testcase 341:
set enable_nestloop to off;

--Testcase 342:
explain (verbose, costs off)
  select a.q2, b.q1
    from int8_tbl a left join int8_tbl b on a.q2 = coalesce(b.q1, 1)
    where coalesce(b.q1, 1) > 0;

--Testcase 343:
select a.q2, b.q1
  from int8_tbl a left join int8_tbl b on a.q2 = coalesce(b.q1, 1)
  where coalesce(b.q1, 1) > 0;

--Testcase 344:
reset enable_hashjoin;

--Testcase 345:
reset enable_nestloop;

--
-- test join strength reduction with a SubPlan providing the proof
--
--Testcase 742:
explain (costs off)
select a.unique1, b.unique2
  from onek a left join onek b on a.unique1 = b.unique2
  where b.unique2 = any (select q1 from int8_tbl c where c.q1 < b.unique1);
--Testcase 743:
select a.unique1, b.unique2
  from onek a left join onek b on a.unique1 = b.unique2
  where b.unique2 = any (select q1 from int8_tbl c where c.q1 < b.unique1);

--
-- test full-join strength reduction
--
--Testcase 744:
explain (costs off)
select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where a.unique1 = 42;
--Testcase 745:
select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where a.unique1 = 42;
--Testcase 746:
explain (costs off)
select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where b.unique2 = 43;
--Testcase 747:
select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where b.unique2 = 43;
--Testcase 748:
explain (costs off)
select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where a.unique1 = 42 and b.unique2 = 42;
--Testcase 749:
select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where a.unique1 = 42 and b.unique2 = 42;

--
-- test result-RTE removal underneath a full join
--
--Testcase 750:
explain (costs off)
select * from
  (select * from int8_tbl i81 join (values(123,2)) v(v1,v2) on q2=v1) ss1
full join
  (select * from (values(456,2)) w(v1,v2) join int8_tbl i82 on q2=v1) ss2
on true;
--Testcase 751:
select * from
  (select * from int8_tbl i81 join (values(123,2)) v(v1,v2) on q2=v1) ss1
full join
  (select * from (values(456,2)) w(v1,v2) join int8_tbl i82 on q2=v1) ss2
on true;

--
-- test join removal
--

begin;

--Testcase 346:
CREATE FOREIGN TABLE a3 (id int OPTIONS (rowkey 'true'), b_id int) SERVER griddb_svr;

--Testcase 347:
CREATE FOREIGN TABLE b3 (id int OPTIONS (rowkey 'true'), c_id int) SERVER griddb_svr;

--Testcase 348:
CREATE FOREIGN TABLE c3 (id int OPTIONS (rowkey 'true')) SERVER griddb_svr;

--Testcase 349:
CREATE FOREIGN TABLE d3 (a int, b int) SERVER griddb_svr;

--Testcase 350:
INSERT INTO a3 VALUES (0, 0), (1, NULL);

--Testcase 351:
INSERT INTO b3 VALUES (0, 0), (1, NULL);

--Testcase 352:
INSERT INTO c3 VALUES (0), (1);

--Testcase 353:
INSERT INTO d3 VALUES (1,3), (2,2), (3,1);

-- all three cases should be optimizable into a3 simple seqscan

--Testcase 354:
explain (costs off) SELECT a3.* FROM a3 LEFT JOIN b3 ON a3.b_id = b3.id;

--Testcase 355:
explain (costs off) SELECT b3.* FROM b3 LEFT JOIN c3 ON b3.c_id = c3.id;

--Testcase 356:
explain (costs off)
  SELECT a3.* FROM a3 LEFT JOIN (b3 left join c3 on b3.c_id = c3.id)
  ON (a3.b_id = b3.id);

-- check optimization of outer join within another special join

--Testcase 357:
explain (costs off)
select id from a3 where id in (
	select b3.id from b3 left join c3 on b3.id = c3.id
);

-- check optimization with oddly-nested outer joins
--Testcase 752:
explain (costs off)
select a1.id from
  (a3 a1 left join a3 a2 on true)
  left join
  (a3 left join a3 a4 on a3.id = a4.id)
  on a2.id = a3.id;
--Testcase 753:
explain (costs off)
select a1.id from
  (a3 a1 left join a3 a2 on a1.id = a2.id)
  left join
  (a3 left join a3 a4 on a3.id = a4.id)
  on a2.id = a3.id;

--Testcase 785:
explain (costs off)
select 1 from a3 t1
    left join a3 t2 on true
   inner join a3 t3 on true
    left join a3 t4 on t2.id = t4.id and t2.id = t3.id;

-- another example (bug #17781)
--Testcase 754:
explain (costs off)
select ss1.f1
from int4_tbl as t1
  left join (int4_tbl as t2
             right join int4_tbl as t3 on null
             left join (int4_tbl as t4
                        right join int8_tbl as t5 on null)
               on t2.f1 = t4.f1
             left join ((select null as f1 from int4_tbl as t6) as ss1
                        inner join int8_tbl as t7 on null)
               on t5.q1 = t7.q2)
    on false;

-- variant with Var rather than PHV coming from t6
--Testcase 755:
explain (costs off)
select ss1.f1
from int4_tbl as t1
  left join (int4_tbl as t2
             right join int4_tbl as t3 on null
             left join (int4_tbl as t4
                        right join int8_tbl as t5 on null)
               on t2.f1 = t4.f1
             left join ((select f1 from int4_tbl as t6) as ss1
                        inner join int8_tbl as t7 on null)
               on t5.q1 = t7.q2)
    on false;

-- per further discussion of bug #17781
--Testcase 786:
create foreign table a (id integer) server griddb_svr;
--Testcase 756:
explain (costs off)
select ss1.x
from (select f1/2 as x from int4_tbl i4 left join a3 on a3.id = i4.f1) ss1
     right join int8_tbl i8 on true
where current_user is not null;  -- this is to add a Result node

-- and further discussion of bug #17781
--Testcase 757:
explain (costs off)
select *
from int8_tbl t1
  left join (int8_tbl t2 left join onek t3 on t2.q1 > t3.unique1)
    on t1.q2 = t2.q2
  left join onek t4
    on t2.q2 < t3.unique2;

-- More tests of correct placement of pseudoconstant quals

-- simple constant-false condition
--Testcase 758:
explain (costs off)
select * from int8_tbl t1 left join
  (int8_tbl t2 inner join int8_tbl t3 on false
   left join int8_tbl t4 on t2.q2 = t4.q2)
on t1.q1 = t2.q1;

-- deduce constant-false from an EquivalenceClass
--Testcase 759:
explain (costs off)
select * from int8_tbl t1 left join
  (int8_tbl t2 inner join int8_tbl t3 on (t2.q1-t3.q2) = 0 and (t2.q1-t3.q2) = 1
   left join int8_tbl t4 on t2.q2 = t4.q2)
on t1.q1 = t2.q1;

-- pseudoconstant based on an outer-level Param
--Testcase 760:
explain (costs off)
select exists(
  select * from int8_tbl t1 left join
    (int8_tbl t2 inner join int8_tbl t3 on x0.f1 = 1
     left join int8_tbl t4 on t2.q2 = t4.q2)
  on t1.q1 = t2.q1
) from int4_tbl x0;

-- check that join removal works for a left join when joining a subquery
-- that is guaranteed to be unique by its GROUP BY clause

--Testcase 358:
explain (costs off)
select d3.* from d3 left join (select * from b3 group by b3.id, b3.c_id) s
  on d3.a = s.id and d3.b = s.c_id;

-- similarly, but keying off a DISTINCT clause

--Testcase 359:
explain (costs off)
select d3.* from d3 left join (select distinct * from b3) s
  on d3.a = s.id and d3.b = s.c_id;

-- join removal is not possible when the GROUP BY contains a column that is
-- not in the join condition.  (Note: as of 9.6, we notice that b3.id is a
-- primary key and so drop b3.c_id from the GROUP BY of the resulting plan;
-- but this happens too late for join removal in the outer plan level.)

--Testcase 360:
explain (costs off)
select d3.* from d3 left join (select * from b3 group by b3.id, b3.c_id) s
  on d3.a = s.id;

-- similarly, but keying off a DISTINCT clause

--Testcase 361:
explain (costs off)
select d3.* from d3 left join (select distinct * from b3) s
  on d3.a = s.id;

-- join removal is not possible here
--Testcase 761:
explain (costs off)
select 1 from a t1
  left join (a t2 left join a t3 on t2.id = 1) on t2.id = 1;

-- check join removal works when uniqueness of the join condition is enforced
-- by a UNION

--Testcase 362:
explain (costs off)
select d3.* from d3 left join (select id from a3 union select id from b3) s
  on d3.a = s.id;

-- check join removal with a cross-type comparison operator

--Testcase 363:
explain (costs off)
select i8.* from int8_tbl i8 left join (select f1 from int4_tbl group by f1) i4
  on i8.q1 = i4.f1;

-- check join removal with lateral references

--Testcase 364:
explain (costs off)
select 1 from (select a3.id FROM a3 left join b3 on a3.b_id = b3.id) q,
			  lateral generate_series(1, q.id) gs(i) where q.id = gs.i;

-- check join removal within RHS of an outer join
--Testcase 762:
explain (costs off)
select c3.id, ss.a from c3
  left join (select d3.a from onerow, d3 left join b3 on d3.a = b3.id) ss
  on c3.id = ss.a;

--Testcase 763:
CREATE TABLE parted_b (id int) partition by range(id);
--Testcase 764:
CREATE FOREIGN TABLE parted_b1 partition of parted_b for values from (0) to (10) server griddb_svr;

-- test join removals on a partitioned table
--Testcase 765:
explain (costs off)
select a3.* from a3 left join parted_b pb on a3.b_id = pb.id;

rollback;

--Testcase 365:
create foreign table parent (k int options (rowkey 'true'), pd int) server griddb_svr;

--Testcase 366:
create foreign table child (k int options (rowkey 'true'), cd int) server griddb_svr;

--Testcase 367:
insert into parent values (1, 10), (2, 20), (3, 30);

--Testcase 368:
insert into child values (1, 100), (4, 400);

-- this case is optimizable

--Testcase 369:
select p.* from parent p left join child c on (p.k = c.k);

--Testcase 370:
explain (costs off)
  select p.* from parent p left join child c on (p.k = c.k);

-- this case is not

--Testcase 371:
select p.*, linked from parent p
  left join (select c.*, true as linked from child c) as ss
  on (p.k = ss.k);

--Testcase 372:
explain (costs off)
  select p.*, linked from parent p
    left join (select c.*, true as linked from child c) as ss
    on (p.k = ss.k);

-- check for a 9.0rc1 bug: join removal breaks pseudoconstant qual handling

--Testcase 373:
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;

--Testcase 374:
explain (costs off)
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;

--Testcase 375:
select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;

--Testcase 376:
explain (costs off)
select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;

-- bug 5255: this is not optimizable by join removal
begin;

--Testcase 377:
CREATE FOREIGN TABLE a4 (id int OPTIONS (rowkey 'true')) SERVER griddb_svr;

--Testcase 378:
CREATE FOREIGN TABLE b4 (id int OPTIONS (rowkey 'true'), a_id int) SERVER griddb_svr;

--Testcase 379:
INSERT INTO a4 VALUES (0), (1);

--Testcase 380:
INSERT INTO b4 VALUES (0, 0), (1, NULL);

--Testcase 381:
SELECT * FROM b4 LEFT JOIN a4 ON (b4.a_id = a4.id) WHERE (a4.id IS NULL OR a4.id > 0);

--Testcase 382:
SELECT b4.* FROM b4 LEFT JOIN a4 ON (b4.a_id = a4.id) WHERE (a4.id IS NULL OR a4.id > 0);

rollback;

-- another join removal bug: this is not optimizable, either
begin;

--Testcase 383:
create foreign table innertab (id int8 options (rowkey 'true'), dat1 int8) server griddb_svr;

--Testcase 384:
insert into innertab values(123, 42);

--Testcase 385:
SELECT * FROM
    (SELECT 1 AS x) ss1
  LEFT JOIN
    (SELECT q1, q2, COALESCE(dat1, q1) AS y
     FROM int8_tbl LEFT JOIN innertab in1 ON q2 = in1.id) ss2
  ON true;

-- join removal bug #17769: can't remove if there's a pushed-down reference
--Testcase 766:
EXPLAIN (COSTS OFF)
SELECT q2 FROM
  (SELECT *
   FROM int8_tbl LEFT JOIN innertab ON q2 = int8_tbl.id) ss
 WHERE COALESCE(dat1, 0) = q1;

-- join removal bug #17773: otherwise-removable PHV appears in a qual condition
--Testcase 767:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q2 FROM
  (SELECT q2, 'constant'::text AS x
   FROM int8_tbl LEFT JOIN innertab ON q2 = int8_tbl.id) ss
  RIGHT JOIN int4_tbl ON NULL
 WHERE x >= x;

-- join removal bug #17786: check that OR conditions are cleaned up
--Testcase 768:
EXPLAIN (COSTS OFF)
SELECT f1, x
FROM int4_tbl
     JOIN ((SELECT 42 AS x FROM int8_tbl LEFT JOIN innertab ON q1 = innertab.id) AS ss1
           RIGHT JOIN tenk1 ON NULL)
        ON tenk1.unique1 = ss1.x OR tenk1.unique2 = ss1.x;

rollback;

-- another join removal bug: we must clean up correctly when removing a PHV
begin;

--Testcase 386:
create foreign table uniquetbl (f1 text) server griddb_svr;

--Testcase 387:
explain (costs off)
select t1.* from
  uniquetbl as t1
  left join (select *, '***'::text as d1 from uniquetbl) t2
  on t1.f1 = t2.f1
  left join uniquetbl t3
  on t2.d1 = t3.f1;

--Testcase 388:
explain (costs off)
select t0.*
from
 text_tbl t0
 left join
   (select case t1.ten when 0 then 'doh!'::text else null::text end as case1,
           t1.stringu2
     from tenk1 t1
     join int4_tbl i4 ON i4.f1 = t1.unique2
     left join uniquetbl u1 ON u1.f1 = t1.string4) ss
  on t0.f1 = ss.case1
where ss.stringu2 !~* ss.case1;

--Testcase 389:
select t0.*
from
 text_tbl t0
 left join
   (select case t1.ten when 0 then 'doh!'::text else null::text end as case1,
           t1.stringu2
     from tenk1 t1
     join int4_tbl i4 ON i4.f1 = t1.unique2
     left join uniquetbl u1 ON u1.f1 = t1.string4) ss
  on t0.f1 = ss.case1
where ss.stringu2 !~* ss.case1;

rollback;

-- another join removal bug: we must clean up EquivalenceClasses too
begin;

--Testcase 787:
CREATE FOREIGN TABLE t (a int OPTIONS (rowkey 'true')) SERVER griddb_svr OPTIONS (table_name 't1_join');
--Testcase 788:
insert into t values (1);

--Testcase 789:
explain (costs off)
select 1
from t t1
  left join (select 2 as c
             from t t2 left join t t3 on t2.a = t3.a) s
    on true
where t1.a = s.c;

--Testcase 790:
select 1
from t t1
  left join (select 2 as c
             from t t2 left join t t3 on t2.a = t3.a) s
    on true
where t1.a = s.c;

rollback;

-- test cases where we can remove a join, but not a PHV computed at it
begin;

--Testcase 791:
CREATE FOREIGN TABLE t (a int OPTIONS (rowkey 'true'), b int) SERVER griddb_svr OPTIONS (table_name 't2_join');
--Testcase 792:
insert into t values (1,1), (2,2);

--Testcase 793:
explain (costs off)
select 1
from t t1
  left join (select t2.a, 1 as c
             from t t2 left join t t3 on t2.a = t3.a) s
  on true
  left join t t4 on true
where s.a < s.c;

--Testcase 794:
explain (costs off)
select t1.a, s.*
from t t1
  left join lateral (select t2.a, coalesce(t1.a, 1) as c
                     from t t2 left join t t3 on t2.a = t3.a) s
  on true
  left join t t4 on true
where s.a < s.c;

--Testcase 795:
select t1.a, s.*
from t t1
  left join lateral (select t2.a, coalesce(t1.a, 1) as c
                     from t t2 left join t t3 on t2.a = t3.a) s
  on true
  left join t t4 on true
where s.a < s.c;

rollback;

-- test case to expose miscomputation of required relid set for a PHV

--Testcase 390:
explain (verbose, costs off)
select i8.*, ss.v, t.unique2
  from int8_tbl i8
    left join int4_tbl i4 on i4.f1 = 1
    left join lateral (select i4.f1 + 1 as v) as ss on true
    left join tenk1 t on t.unique2 = ss.v
where q2 = 456;

--Testcase 391:
select i8.*, ss.v, t.unique2
  from int8_tbl i8
    left join int4_tbl i4 on i4.f1 = 1
    left join lateral (select i4.f1 + 1 as v) as ss on true
    left join tenk1 t on t.unique2 = ss.v
where q2 = 456;

-- bug #8444: we've historically allowed duplicate aliases within aliased JOINs

--Testcase 392:
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y) j on q1 = f1; -- error

--Testcase 393:
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y) j on q1 = y.f1; -- error

--Testcase 394:
select * from
  int8_tbl x join ((SELECT f1 FROM int4_tbl) x cross join (SELECT f1 FROM int4_tbl) y(ff)) j on q1 = f1; -- ok

--
-- Test hints given on incorrect column references are useful
--

--Testcase 395:
select t1.uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, prefer "t1" suggestion

--Testcase 396:
select t2.uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, prefer "t2" suggestion

--Testcase 397:
select uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, suggest both at once

--Testcase 769:
select ctid from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, need qualification

--
-- Take care to reference the correct RTE
--

--Testcase 398:
select atts.relid::regclass, s.* from pg_stats s join
    pg_attribute a on s.attname = a.attname and s.tablename =
    a.attrelid::regclass::text join (select unnest(indkey) attnum,
    indexrelid from pg_index i) atts on atts.attnum = a.attnum where
    schemaname != 'pg_catalog';

-- Test bug in rangetable flattening
--Testcase 770:
explain (verbose, costs off)
select 1 from
  (select * from int8_tbl where q1 <> (select 42) offset 0) ss
where false;

--
-- Test LATERAL
--

--Testcase 399:
select unique2, x.f1
from tenk1 a, lateral (select * from int4_tbl b where f1 = a.unique1) x;

--Testcase 400:
explain (costs off)
  select unique2, x.*
  from tenk1 a, lateral (select * from int4_tbl b where f1 = a.unique1) x;

--Testcase 401:
select unique2, x.f1
from int4_tbl x, lateral (select unique2 from tenk1 where f1 = unique1) ss;

--Testcase 402:
explain (costs off)
  select unique2, x.*
  from int4_tbl x, lateral (select unique2 from tenk1 where f1 = unique1) ss;

--Testcase 403:
explain (costs off)
  select unique2, x.*
  from int4_tbl x cross join lateral (select unique2 from tenk1 where f1 = unique1) ss;

--Testcase 404:
select unique2, x.f1
from int4_tbl x left join lateral (select unique1, unique2 from tenk1 where f1 = unique1) ss on true;

--Testcase 405:
explain (costs off)
  select unique2, x.*
  from int4_tbl x left join lateral (select unique1, unique2 from tenk1 where f1 = unique1) ss on true;

-- check scoping of lateral versus parent references
-- the first of these should return int8_tbl.q2, the second int8_tbl.q1

--Testcase 406:
select q1, q2, (select r from (select q1 as q2) x, (select q2 as r) y) from int8_tbl;

--Testcase 407:
select q1, q2, (select r from (select q1 as q2) x, lateral (select q2 as r) y) from int8_tbl;

-- lateral with function in FROM

--Testcase 408:
select count(*) from tenk1 a, lateral generate_series(1,two) g;

--Testcase 409:
explain (costs off)
  select count(*) from tenk1 a, lateral generate_series(1,two) g;

--Testcase 410:
explain (costs off)
  select count(*) from tenk1 a cross join lateral generate_series(1,two) g;
-- don't need the explicit LATERAL keyword for functions

--Testcase 411:
explain (costs off)
  select count(*) from tenk1 a, generate_series(1,two) g;

-- lateral with UNION ALL subselect

--Testcase 412:
explain (costs off)
  select * from generate_series(100,200) g,
    lateral (select * from int8_tbl a where g = q1 union all
             select * from int8_tbl b where g = q2) ss;

--Testcase 413:
select g, q1, q2 from generate_series(100,200) g,
  lateral (select * from int8_tbl a where g = q1 union all
           select * from int8_tbl b where g = q2) ss;

-- lateral with VALUES

--Testcase 414:
explain (costs off)
  select count(*) from tenk1 a,
    tenk1 b join lateral (values(a.unique1)) ss(x) on b.unique2 = ss.x;

--Testcase 415:
select count(*) from tenk1 a,
  tenk1 b join lateral (values(a.unique1)) ss(x) on b.unique2 = ss.x;

-- lateral with VALUES, no flattening possible

--Testcase 416:
explain (costs off)
  select count(*) from tenk1 a,
    tenk1 b join lateral (values(a.unique1),(-1)) ss(x) on b.unique2 = ss.x;

--Testcase 417:
select count(*) from tenk1 a,
  tenk1 b join lateral (values(a.unique1),(-1)) ss(x) on b.unique2 = ss.x;

-- lateral injecting a strange outer join condition

--Testcase 418:
explain (costs off)
  select a.q1, a.q2, x.q1, x.q2, ss.z from int8_tbl a,
    int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
      on x.q2 = ss.z
  order by a.q1, a.q2, x.q1, x.q2, ss.z;

--Testcase 419:
select a.q1, a.q2, x.q1, x.q2, ss.z from int8_tbl a,
  int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
    on x.q2 = ss.z
  order by a.q1, a.q2, x.q1, x.q2, ss.z;

-- lateral reference to a join alias variable

--Testcase 420:
select x, f1, y from (select f1/2 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1,
  lateral (select x) ss2(y);

--Testcase 421:
select x, f1, y from (select f1 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1,
  lateral (values(x)) ss2(y);

--Testcase 422:
select x, f1, y from ((select f1/2 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1) j,
  lateral (select x) ss2(y);

-- lateral references requiring pullup

--Testcase 423:
select * from (values(1)) x(lb),
  lateral generate_series(lb,4) x4;

--Testcase 424:
select * from (select f1/1000000000 from int4_tbl) x(lb),
  lateral generate_series(lb,4) x4;

--Testcase 425:
select * from (values(1)) x(lb),
  lateral (values(lb)) y(lbcopy);

--Testcase 426:
select * from (values(1)) x(lb),
  lateral (select lb from int4_tbl) y(lbcopy);

--Testcase 427:
select x.q1, x.q2, y.q1, y.q2, xq1, yq1, yq2 from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (values(x.q1,y.q1,y.q2)) v(xq1,yq1,yq2);

--Testcase 428:
select x.q1, x.q2, y.q1, y.q2, xq1, yq1, yq2 from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);

--Testcase 429:
select x.q1, x.q2 from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);

--Testcase 430:
select v.* from
  (int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy);

--Testcase 431:
select v.* from
  (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy);

--Testcase 432:
select v.* from
  (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 from onerow union all select x.q2,y.q2 from onerow) v(vx,vy);

--Testcase 433:
explain (verbose, costs off)
select a.q1, a.q2, ss.q1, ss.q2, x from
  int8_tbl a left join
  lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;

--Testcase 434:
select a.q1, a.q2, ss.q1, ss.q2, x from
  int8_tbl a left join
  lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;

--Testcase 435:
explain (verbose, costs off)
select a.q1, a.q2, ss.q1, ss.q2, x from
  int8_tbl a left join
  lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;

--Testcase 436:
select a.q1, a.q2, ss.q1, ss.q2, x from
  int8_tbl a left join
  lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;

-- lateral can result in join conditions appearing below their
-- real semantic level

--Testcase 437:
explain (verbose, costs off)
select i.f1, k.f1 from int4_tbl i left join
  lateral (select * from int2_tbl j where i.f1 = j.f1) k on true;

--Testcase 438:
select i.f1, k.f1 from int4_tbl i left join
  lateral (select * from int2_tbl j where i.f1 = j.f1) k on true;

--Testcase 439:
explain (verbose, costs off)
select f1, coalesce from (SELECT f1 FROM int4_tbl) i left join
  lateral (select coalesce(i) from (SELECT f1 FROM int2_tbl) j where i.f1 = j.f1) k on true;

--Testcase 440:
select f1, coalesce from (SELECT f1 FROM int4_tbl) i left join
  lateral (select coalesce(i) from (SELECT f1 FROM int2_tbl) j where i.f1 = j.f1) k on true;

--Testcase 441:
explain (verbose, costs off)
select a.f1, ss.f1, q1, q2 from int4_tbl a,
  lateral (
    select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
  ) ss;

--Testcase 442:
select a.f1, ss.f1, q1, q2 from int4_tbl a,
  lateral (
    select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
  ) ss;

-- lateral reference in a PlaceHolderVar evaluated at join level

--Testcase 443:
explain (verbose, costs off)
select q1, q2, bq1, cq1, least from
  int8_tbl a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   int8_tbl b cross join int8_tbl c) ss
  on a.q2 = ss.bq1;

--Testcase 444:
select q1, q2, bq1, cq1, least from
  int8_tbl a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   int8_tbl b cross join int8_tbl c) ss
  on a.q2 = ss.bq1;

-- case requiring nested PlaceHolderVars

--Testcase 445:
explain (verbose, costs off)
select * from
  int8_tbl c left join (
    int8_tbl a left join (select q1, coalesce(q2,42) as x from int8_tbl b) ss1
      on a.q2 = ss1.q1
    cross join
    lateral (select q1, coalesce(ss1.x,q2) as y from int8_tbl d) ss2
  ) on c.q2 = ss2.q1,
  lateral (select ss2.y offset 0) ss3;

-- case that breaks the old ph_may_need optimization

--Testcase 446:
explain (verbose, costs off)
select c.*,a.*,ss1.q1,ss2.q1,ss3.* from
  int8_tbl c left join (
    int8_tbl a left join
      (select q1, coalesce(q2,f1) as x from int8_tbl b, int4_tbl b2
       where q1 < f1) ss1
      on a.q2 = ss1.q1
    cross join
    lateral (select q1, coalesce(ss1.x,q2) as y from int8_tbl d) ss2
  ) on c.q2 = ss2.q1,
  lateral (select * from int4_tbl i where ss2.y > f1) ss3;

-- check processing of postponed quals (bug #9041)

--Testcase 447:
explain (verbose, costs off)
select * from
  (select 1 as x offset 0) x cross join (select 2 as y offset 0) y
  left join lateral (
    select * from (select 3 as z offset 0) z where z.z = x.x
  ) zz on zz.z = y.y;

-- a new postponed-quals issue (bug #17768)
--Testcase 771:
explain (costs off)
select * from int4_tbl t1,
  lateral (select * from int4_tbl t2 inner join int4_tbl t3 on t1.f1 = 1
           inner join (int4_tbl t4 left join int4_tbl t5 on true) on true) ss;

-- check dummy rels with lateral references (bug #15694)

--Testcase 448:
explain (verbose, costs off)
select * from int8_tbl i8 left join lateral
  (select *, i8.q2 from int4_tbl where false) ss on true;

--Testcase 449:
explain (verbose, costs off)
select * from int8_tbl i8 left join lateral
  (select *, i8.q2 from int4_tbl i1, int4_tbl i2 where false) ss on true;

-- check handling of nested appendrels inside LATERAL

--Testcase 450:
select * from
  ((select 2 as v) union all (select 3 as v)) as q1
  cross join lateral
  ((select * from
      ((select 4 as v) union all (select 5 as v)) as q3)
   union all
   (select q1.v)
  ) as q2;

-- check the number of columns specified
--Testcase 796:
SELECT * FROM (int8_tbl i cross join int4_tbl j) ss(id,a,b,id,c,d);

-- check we don't try to do a unique-ified semijoin with LATERAL

--Testcase 451:
explain (verbose, costs off)
select * from
  (values (0,9998), (1,1000)) v(id,x),
  lateral (select f1 from int4_tbl
           where f1 = any (select unique1 from tenk1
                           where unique2 = v.x offset 0)) ss;

--Testcase 452:
select * from
  (values (0,9998), (1,1000)) v(id,x),
  lateral (select f1 from int4_tbl
           where f1 = any (select unique1 from tenk1
                           where unique2 = v.x offset 0)) ss;

-- check proper extParam/allParam handling (this isn't exactly a LATERAL issue,
-- but we can make the test case much more compact with LATERAL)

--Testcase 453:
explain (verbose, costs off)
select * from (values (0), (1)) v(id),
lateral (select * from int8_tbl t1,
         lateral (select * from
                    (select * from int8_tbl t2
                     where q1 = any (select q2 from int8_tbl t3
                                     where q2 = (select greatest(t1.q1,t2.q2))
                                       and (select v.id=0)) offset 0) ss2) ss
         where t1.q1 = ss.q2) ss0;

--Testcase 454:
select * from (values (0), (1)) v(id),
lateral (select * from int8_tbl t1,
         lateral (select * from
                    (select * from int8_tbl t2
                     where q1 = any (select q2 from int8_tbl t3
                                     where q2 = (select greatest(t1.q1,t2.q2))
                                       and (select v.id=0)) offset 0) ss2) ss
         where t1.q1 = ss.q2) ss0;

-- test some error cases where LATERAL should have been used but wasn't

--Testcase 455:
select f1,g from int4_tbl a, (select f1 as g) ss;

--Testcase 456:
select f1,g from int4_tbl a, (select a.f1 as g) ss;

--Testcase 457:
select f1,g from int4_tbl a cross join (select f1 as g) ss;

--Testcase 458:
select f1,g from int4_tbl a cross join (select a.f1 as g) ss;
-- SQL:2008 says the left table is in scope but illegal to access here

--Testcase 459:
select f1,g from int4_tbl a right join lateral generate_series(0, a.f1) g on true;

--Testcase 460:
select f1,g from int4_tbl a full join lateral generate_series(0, a.f1) g on true;
-- check we complain about ambiguous table references

--Testcase 461:
select * from
  int8_tbl x cross join (int4_tbl x cross join lateral (select x.f1) ss);
-- LATERAL can be used to put an aggregate into the FROM clause of its query

--Testcase 462:
select 1 from tenk1 a, lateral (select max(a.unique1) from int4_tbl b) ss;

-- check behavior of LATERAL in UPDATE/DELETE

--Testcase 463:
create temp table xx1 as select f1 as x1, -f1 as x2 from int4_tbl;

-- error, can't do this:

--Testcase 464:
update xx1 set x2 = f1 from (select * from int4_tbl where f1 = x1) ss;

--Testcase 465:
update xx1 set x2 = f1 from (select * from int4_tbl where f1 = xx1.x1) ss;
-- can't do it even with LATERAL:

--Testcase 466:
update xx1 set x2 = f1 from lateral (select * from int4_tbl where f1 = x1) ss;
-- we might in future allow something like this, but for now it's an error:

--Testcase 467:
update xx1 set x2 = f1 from xx1, lateral (select * from int4_tbl where f1 = x1) ss;

-- also errors:

--Testcase 468:
delete from xx1 using (select * from int4_tbl where f1 = x1) ss;

--Testcase 469:
delete from xx1 using (select * from int4_tbl where f1 = xx1.x1) ss;

--Testcase 470:
delete from xx1 using lateral (select * from int4_tbl where f1 = x1) ss;

--
-- test LATERAL reference propagation down a multi-level inheritance hierarchy
-- produced for a multi-level partitioned table hierarchy.
--

--Testcase 471:
create table join_pt1 (a int, b int, c text) partition by range(a);

--Testcase 472:
create table join_pt1p1 partition of join_pt1 for values from (0) to (100) partition by range(b);

--Testcase 473:
create foreign table join_pt1p2 partition of join_pt1 for values from (100) to (200) server griddb_svr;

--Testcase 474:
create foreign table join_pt1p1p1 partition of join_pt1p1 for values from (0) to (100) server griddb_svr;

--Testcase 475:
insert into join_pt1 values (1, 1, 'x'), (101, 101, 'y');

--Testcase 476:
create foreign table join_ut1 (a int, b int, c text) server griddb_svr;

--Testcase 477:
insert into join_ut1 values (101, 101, 'y'), (2, 2, 'z');

--Testcase 478:
explain (verbose, costs off)
select t1.b, ss.phv from join_ut1 t1 left join lateral
              (select t2.a as t2a, t3.a t3a, least(t1.a, t2.a, t3.a) phv
					  from join_pt1 t2 join join_ut1 t3 on t2.a = t3.b) ss
              on t1.a = ss.t2a order by t1.a;

--Testcase 479:
select t1.b, ss.phv from join_ut1 t1 left join lateral
              (select t2.a as t2a, t3.a t3a, least(t1.a, t2.a, t3.a) phv
					  from join_pt1 t2 join join_ut1 t3 on t2.a = t3.b) ss
              on t1.a = ss.t2a order by t1.a;

--Testcase 480:
drop table t2a;

--Testcase 481:
drop table join_pt1;

--Testcase 482:
drop foreign table join_ut1;

--
-- test estimation behavior with multi-column foreign key and constant qual
--

begin;

--Testcase 483:
create foreign table fkest (x integer options (rowkey 'true'), x10 integer, x10b integer, x100 integer) server griddb_svr;

--Testcase 484:
insert into fkest select x, x/10, x/10, x/100 from generate_series(1,1000) x;

-- gridDB tuple has already unique by rowkey
-- --Testcase 485:
-- create unique index on fkest(x, x10, x100);
-- analyze fkest;

--Testcase 486:
explain (costs off)
select * from fkest f1
  join fkest f2 on (f1.x = f2.x and f1.x10 = f2.x10b and f1.x100 = f2.x100)
  join fkest f3 on f1.x = f3.x
  where f1.x100 = 2;

-- griddb not support foreign key, so ignore the test below
-- --Testcase 487:
-- alter table fkest add constraint fk
--   foreign key (x, x10b, x100) references fkest (x, x10, x100);

-- --Testcase 488:
-- explain (costs off)
-- select * from fkest f1
--   join fkest f2 on (f1.x = f2.x and f1.x10 = f2.x10b and f1.x100 = f2.x100)
--   join fkest f3 on f1.x = f3.x
--   where f1.x100 = 2;

rollback;

--
-- test that foreign key join estimation performs sanely for outer joins
--

begin;

--Testcase 489:
create foreign table fkest (id serial options (rowkey 'true'), a int, b int, c int) server griddb_svr;

--Testcase 490:
create foreign table fkest1 (id serial options (rowkey 'true'), a int, b int) server griddb_svr;

--Testcase 491:
insert into fkest(a,b,c) select x/10, x%10, x from generate_series(1,1000) x;

--Testcase 492:
insert into fkest1(a,b) select x/10, x%10 from generate_series(1,1000) x;

--Testcase 493:
explain (costs off)
select *
from fkest f
  left join fkest1 f1 on f.a = f1.a and f.b = f1.b
  left join fkest1 f2 on f.a = f2.a and f.b = f2.b
  left join fkest1 f3 on f.a = f3.a and f.b = f3.b
where f.c = 1;

rollback;

--
-- test planner's ability to mark joins as unique
--

--Testcase 494:
create foreign table j11 (idx serial options (rowkey 'true'), id int) server griddb_svr;

--Testcase 495:
create foreign table j21 (idx serial options (rowkey 'true'), id int) server griddb_svr;

--Testcase 496:
create foreign table j31 (idx serial options (rowkey 'true'), id int) server griddb_svr;

--Testcase 497:
insert into j11(id) values(1),(2),(3);

--Testcase 498:
insert into j21(id) values(1),(2),(3);

--Testcase 499:
insert into j31(id) values(1),(1);

-- ensure join is properly marked as unique

--Testcase 500:
explain (verbose, costs off)
select * from j11 inner join j21 on j11.id = j21.id;

-- ensure join is not unique when not an equi-join

--Testcase 501:
explain (verbose, costs off)
select * from j11 inner join j21 on j11.id > j21.id;

-- ensure non-unique rel is not chosen as inner

--Testcase 502:
explain (verbose, costs off)
select * from j11 inner join j31 on j11.id = j31.id;

-- ensure left join is marked as unique

--Testcase 503:
explain (verbose, costs off)
select * from j11 left join j21 on j11.id = j21.id;

-- ensure right join is marked as unique

--Testcase 504:
explain (verbose, costs off)
select * from j11 right join j21 on j11.id = j21.id;

-- ensure full join is marked as unique

--Testcase 505:
explain (verbose, costs off)
select * from j11 full join j21 on j11.id = j21.id;

-- a clauseless (cross) join can't be unique

--Testcase 506:
explain (verbose, costs off)
select * from j11 cross join j21;

-- ensure a natural join is marked as unique

--Testcase 507:
explain (verbose, costs off)
select * from j11 natural join j21;

-- ensure a distinct clause allows the inner to become unique

--Testcase 508:
explain (verbose, costs off)
select * from j11
inner join (select distinct id from j31) j31 on j11.id = j31.id;

-- ensure group by clause allows the inner to become unique

--Testcase 509:
explain (verbose, costs off)
select * from j11
inner join (select id from j31 group by id) j31 on j11.id = j31.id;

-- test more complex permutations of unique joins

--Testcase 510:
create foreign table j12 (idx serial options (rowkey 'true'), id1 int, id2 int) server griddb_svr;

--Testcase 511:
create foreign table j22 (idx serial options (rowkey 'true'), id1 int, id2 int) server griddb_svr;

--Testcase 512:
create foreign table j32 (idx serial options (rowkey 'true'), id1 int, id2 int) server griddb_svr;

--Testcase 513:
insert into j12(id1,id2) values(1,1),(1,2);

--Testcase 514:
insert into j22(id1,id2) values(1,1);

--Testcase 515:
insert into j32(id1,id2) values(1,1);

-- ensure there's no unique join when not all columns which are part of the
-- unique index are seen in the join clause

--Testcase 516:
explain (verbose, costs off)
select * from j12
inner join j22 on j12.id1 = j22.id1;

-- ensure proper unique detection with multiple join quals

--Testcase 517:
explain (verbose, costs off)
select * from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2;

-- ensure we don't detect the join to be unique when quals are not part of the
-- join condition

--Testcase 518:
explain (verbose, costs off)
select * from j12
inner join j22 on j12.id1 = j22.id1 where j12.id2 = 1;

-- as above, but for left joins.

--Testcase 519:
explain (verbose, costs off)
select * from j12
left join j22 on j12.id1 = j22.id1 where j12.id2 = 1;

-- gridDB tuple has already unique by rowkey
-- create unique index j1_id2_idx on j12(id2) where j12.id2 is not null;

-- ensure we don't use a partial unique index as unique proofs
--Testcase 797:
explain (verbose, costs off)
select * from j12
inner join j22 on j12.id2 = j22.id2;

-- drop index j1_id2_idx;

-- validate logic in merge joins which skips mark and restore.
-- it should only do this if all quals which were used to detect the unique
-- are present as join quals, and not plain quals.

--Testcase 520:
set enable_nestloop to 0;

--Testcase 521:
set enable_hashjoin to 0;

--Testcase 522:
set enable_sort to 0;

-- create indexes that will be preferred over the PKs to perform the join
--create index j1_id1_idx on j1 (id1) where id1 % 1000 = 1;
--create index j2_id1_idx on j2 (id1) where id1 % 1000 = 1;

-- need an additional row in j2, if we want j2_id1_idx to be preferred

--Testcase 523:
insert into j22(id1,id2) values(1,2);
--analyze j2;

--Testcase 524:
explain (costs off) select j12.id1, j12.id2, j22.id1, j22.id2 from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1;

--Testcase 525:
select j12.id1, j12.id2, j22.id1, j22.id2 from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1;

-- Exercise array keys mark/restore B-Tree code

--Testcase 526:
explain (costs off) select j12.id1, j12.id2, j22.id1, j22.id2 from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1 and j22.id1 = any (array[1]);

--Testcase 527:
select j12.id1, j12.id2, j22.id1, j22.id2 from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1 and j22.id1 = any (array[1]);

-- Exercise array keys "find extreme element" B-Tree code

--Testcase 528:
explain (costs off) select j12.id1, j12.id2, j22.id1, j22.id2 from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1 and j22.id1 >= any (array[1,5]);

--Testcase 529:
select j12.id1, j12.id2, j22.id1, j22.id2 from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1 and j22.id1 >= any (array[1,5]);

--Testcase 530:
reset enable_nestloop;

--Testcase 531:
reset enable_hashjoin;

--Testcase 532:
reset enable_sort;

--Testcase 533:
drop foreign table j12;

--Testcase 534:
drop foreign table j22;

--Testcase 535:
drop foreign table j32;

-- check that semijoin inner is not seen as unique for a portion of the outerrel

--Testcase 537:
explain (verbose, costs off)
select t1.unique1, t2.hundred
from onek t1, tenk1 t2
where exists (select 1 from tenk1 t3
              where t3.thousand = t1.unique1 and t3.tenthous = t2.hundred)
      and t1.unique1 < 1;

-- ... unless it actually is unique

--Testcase 538:
create table j3 as select unique1, tenthous from onek;
vacuum analyze j3;

--Testcase 539:
create unique index on j3(unique1, tenthous);

--Testcase 540:
explain (verbose, costs off)
select t1.unique1, t2.hundred
from onek t1, tenk1 t2
where exists (select 1 from j3
              where j3.unique1 = t1.unique1 and j3.tenthous = t2.hundred)
      and t1.unique1 < 1;

--Testcase 541:
drop table j3;

--
-- exercises for the hash join code
--

begin;

--Testcase 542:
set local min_parallel_table_scan_size = 0;

--Testcase 543:
set local parallel_setup_cost = 0;

--Testcase 544:
set enable_mergejoin to 0;
-- Extract bucket and batch counts from an explain analyze plan.  In
-- general we can't make assertions about how many batches (or
-- buckets) will be required because it can vary, but we can in some
-- special cases and we can check for growth.

--Testcase 545:
create or replace function find_hash(node json)
returns json language plpgsql
as
$$
declare
  x json;
  child json;
begin
  if node->>'Node Type' = 'Hash' then
    return node;
  else
    for child in select json_array_elements(node->'Plans')
    loop
      x := find_hash(child);
      if x is not null then
        return x;
      end if;
    end loop;
    return null;
  end if;
end;
$$;

--Testcase 546:
create or replace function hash_join_batches(query text)
returns table (original int, final int) language plpgsql
as
$$
declare
  whole_plan json;
  hash_node json;
begin
  for whole_plan in

--Testcase 547:
    execute 'explain (analyze, format ''json'') ' || query
  loop
    hash_node := find_hash(json_extract_path(whole_plan, '0', 'Plan'));
    original := hash_node->>'Original Hash Batches';
    final := hash_node->>'Hash Batches';
    return next;
  end loop;
end;
$$;

-- Make a simple relation with well distributed keys and correctly
-- estimated size.

--Testcase 548:
create foreign table simple (id int options (rowkey 'true'), t text) server griddb_svr;

--Testcase 549:
insert into simple select generate_series(1, 20000) AS id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

-- Make a relation whose size we will under-estimate.  We want stats
-- to say 1000 rows, but actually there are 20,000 rows.

--Testcase 550:
create foreign table bigger_than_it_looks (id int options (rowkey 'true'), t text) server griddb_svr;

--Testcase 551:
insert into bigger_than_it_looks select generate_series(1, 20000) as id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

--Testcase 552:
update pg_class set reltuples = 1000 where relname = 'bigger_than_it_looks';

-- Make a relation whose size we underestimate and that also has a
-- kind of skew that breaks our batching scheme.  We want stats to say
-- 2 rows, but actually there are 20,000 rows with the same key.

--Testcase 553:
create foreign table extremely_skewed (idx int options (rowkey 'true'), id int, t text) server griddb_svr;

--Testcase 554:
insert into extremely_skewed
  select idx, 42 as id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
  from generate_series(1, 20000) idx;

--Testcase 555:
update pg_class
  set reltuples = 2, relpages = pg_relation_size('extremely_skewed') / 8192
  where relname = 'extremely_skewed';

-- Make a relation with a couple of enormous tuples.
-- System limiting values: with GridDB, when Block size = 64KB, String data size = 31KB

--Testcase 556:
create foreign table wide (id int options (rowkey 'true'), t text) server griddb_svr;

--Testcase 557:
insert into wide select generate_series(1, 2) as id, rpad('', 31744, 'x') as t;

-- The "optimal" case: the hash table fits in memory; we plan for 1
-- batch, we stick to that number, and peak memory usage stays within
-- our work_mem budget

-- non-parallel

--Testcase 558:
set local max_parallel_workers_per_gather = 0;

--Testcase 559:
set local work_mem = '4MB';
set local hash_mem_multiplier = 1.0;

--Testcase 560:
explain (costs off)
  select count(*) from simple r join simple s using (id);

--Testcase 561:
select count(*) from simple r join simple s using (id);

--Testcase 562:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join simple s using (id);
$$);

-- parallel with parallel-oblivious hash join

--Testcase 563:
set local max_parallel_workers_per_gather = 2;

--Testcase 564:
set local work_mem = '4MB';
set local hash_mem_multiplier = 1.0;

--Testcase 565:
set local enable_parallel_hash = off;

--Testcase 566:
explain (costs off)
  select count(*) from simple r join simple s using (id);

--Testcase 567:
select count(*) from simple r join simple s using (id);

--Testcase 568:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join simple s using (id);
$$);

-- parallel with parallel-aware hash join

--Testcase 569:
set local max_parallel_workers_per_gather = 2;

--Testcase 570:
set local work_mem = '4MB';
set local hash_mem_multiplier = 1.0;

--Testcase 571:
set local enable_parallel_hash = on;

--Testcase 572:
explain (costs off)
  select count(*) from simple r join simple s using (id);

--Testcase 573:
select count(*) from simple r join simple s using (id);

--Testcase 574:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join simple s using (id);
$$);

-- The "good" case: batches required, but we plan the right number; we
-- plan for some number of batches, and we stick to that number, and
-- peak memory usage says within our work_mem budget

-- non-parallel

--Testcase 575:
set local max_parallel_workers_per_gather = 0;

--Testcase 576:
set local work_mem = '128kB';
set local hash_mem_multiplier = 1.0;

--Testcase 577:
explain (costs off)
  select count(*) from simple r join simple s using (id);

--Testcase 578:
select count(*) from simple r join simple s using (id);

--Testcase 579:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join simple s using (id);
$$);

-- parallel with parallel-oblivious hash join

--Testcase 580:
set local max_parallel_workers_per_gather = 2;

--Testcase 581:
set local work_mem = '128kB';
set local hash_mem_multiplier = 1.0;

--Testcase 582:
set local enable_parallel_hash = off;

--Testcase 583:
explain (costs off)
  select count(*) from simple r join simple s using (id);

--Testcase 584:
select count(*) from simple r join simple s using (id);

--Testcase 585:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join simple s using (id);
$$);

-- parallel with parallel-aware hash join

--Testcase 586:
set local max_parallel_workers_per_gather = 2;

--Testcase 587:
set local work_mem = '192kB';
set local hash_mem_multiplier = 1.0;

--Testcase 588:
set local enable_parallel_hash = on;

--Testcase 589:
explain (costs off)
  select count(*) from simple r join simple s using (id);

--Testcase 590:
select count(*) from simple r join simple s using (id);

--Testcase 591:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join simple s using (id);
$$);

-- The "bad" case: during execution we need to increase number of
-- batches; in this case we plan for 1 batch, and increase at least a
-- couple of times, and peak memory usage stays within our work_mem
-- budget

-- non-parallel

--Testcase 592:
set local max_parallel_workers_per_gather = 0;

--Testcase 593:
set local work_mem = '128kB';
set local hash_mem_multiplier = 1.0;

--Testcase 594:
explain (costs off)
  select count(*) FROM simple r JOIN bigger_than_it_looks s USING (id);

--Testcase 595:
select count(*) FROM simple r JOIN bigger_than_it_looks s USING (id);

--Testcase 596:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) FROM simple r JOIN bigger_than_it_looks s USING (id);
$$);

-- parallel with parallel-oblivious hash join

--Testcase 597:
set local max_parallel_workers_per_gather = 2;

--Testcase 598:
set local work_mem = '128kB';
set local hash_mem_multiplier = 1.0;

--Testcase 599:
set local enable_parallel_hash = off;

--Testcase 600:
explain (costs off)
  select count(*) from simple r join bigger_than_it_looks s using (id);

--Testcase 601:
select count(*) from simple r join bigger_than_it_looks s using (id);

--Testcase 602:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join bigger_than_it_looks s using (id);
$$);

-- parallel with parallel-aware hash join

--Testcase 603:
set local max_parallel_workers_per_gather = 1;

--Testcase 604:
set local work_mem = '192kB';
set local hash_mem_multiplier = 1.0;

--Testcase 605:
set local enable_parallel_hash = on;

--Testcase 606:
explain (costs off)
  select count(*) from simple r join bigger_than_it_looks s using (id);

--Testcase 607:
select count(*) from simple r join bigger_than_it_looks s using (id);

--Testcase 608:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join bigger_than_it_looks s using (id);
$$);

-- The "ugly" case: increasing the number of batches during execution
-- doesn't help, so stop trying to fit in work_mem and hope for the
-- best; in this case we plan for 1 batch, increases just once and
-- then stop increasing because that didn't help at all, so we blow
-- right through the work_mem budget and hope for the best...

-- non-parallel

--Testcase 609:
set local max_parallel_workers_per_gather = 0;

--Testcase 610:
set local work_mem = '128kB';
set local hash_mem_multiplier = 1.0;

--Testcase 611:
explain (costs off)
  select count(*) from simple r join extremely_skewed s using (id);

--Testcase 612:
select count(*) from simple r join extremely_skewed s using (id);

--Testcase 613:
select * from hash_join_batches(
$$
  select count(*) from simple r join extremely_skewed s using (id);
$$);

-- parallel with parallel-oblivious hash join

--Testcase 614:
set local max_parallel_workers_per_gather = 2;

--Testcase 615:
set local work_mem = '128kB';
set local hash_mem_multiplier = 1.0;

--Testcase 616:
set local enable_parallel_hash = off;

--Testcase 617:
explain (costs off)
  select count(*) from simple r join extremely_skewed s using (id);

--Testcase 618:
select count(*) from simple r join extremely_skewed s using (id);

--Testcase 619:
select * from hash_join_batches(
$$
  select count(*) from simple r join extremely_skewed s using (id);
$$);

-- parallel with parallel-aware hash join

--Testcase 620:
set local max_parallel_workers_per_gather = 1;

--Testcase 621:
set local work_mem = '128kB';
set local hash_mem_multiplier = 1.0;

--Testcase 622:
set local enable_parallel_hash = on;

--Testcase 623:
explain (costs off)
  select count(*) from simple r join extremely_skewed s using (id);

--Testcase 624:
select count(*) from simple r join extremely_skewed s using (id);

--Testcase 625:
select * from hash_join_batches(
$$
  select count(*) from simple r join extremely_skewed s using (id);
$$);

-- A couple of other hash join tests unrelated to work_mem management.

-- Check that EXPLAIN ANALYZE has data even if the leader doesn't participate

--Testcase 626:
set local max_parallel_workers_per_gather = 2;

--Testcase 627:
set local work_mem = '4MB';
set local hash_mem_multiplier = 1.0;

--Testcase 628:
set local parallel_leader_participation = off;

--Testcase 629:
select * from hash_join_batches(
$$
  select count(*) from simple r join simple s using (id);
$$);

-- Exercise rescans.  We'll turn off parallel_leader_participation so
-- that we can check that instrumentation comes back correctly.

--Testcase 630:
create foreign table join_foo (id int options (rowkey 'true'), t text) server griddb_svr;

--Testcase 631:
insert into join_foo select generate_series(1, 3) as id, 'xxxxx'::text as t;

--Testcase 632:
create foreign table join_bar (id int options (rowkey 'true'), t text) server griddb_svr;

--Testcase 633:
insert into join_bar select generate_series(1, 10000) as id, 'xxxxx'::text as t;

-- multi-batch with rescan, parallel-oblivious

--Testcase 634:
set enable_parallel_hash = off;

--Testcase 635:
set parallel_leader_participation = off;

--Testcase 636:
set min_parallel_table_scan_size = 0;

--Testcase 637:
set parallel_setup_cost = 0;

--Testcase 638:
set parallel_tuple_cost = 0;

--Testcase 639:
set max_parallel_workers_per_gather = 2;

--Testcase 640:
set enable_material = off;

--Testcase 641:
set enable_mergejoin = off;

--Testcase 642:
set work_mem = '64kB';
set local hash_mem_multiplier = 1.0;

--Testcase 643:
explain (costs off)
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;

--Testcase 644:
select count(*) from join_foo
  left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
  on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;

--Testcase 645:
select final > 1 as multibatch
  from hash_join_batches(
$$
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
$$);

-- single-batch with rescan, parallel-oblivious

--Testcase 646:
set enable_parallel_hash = off;

--Testcase 647:
set parallel_leader_participation = off;

--Testcase 648:
set min_parallel_table_scan_size = 0;

--Testcase 649:
set parallel_setup_cost = 0;

--Testcase 650:
set parallel_tuple_cost = 0;

--Testcase 651:
set max_parallel_workers_per_gather = 2;

--Testcase 652:
set enable_material = off;

--Testcase 653:
set enable_mergejoin = off;

--Testcase 654:
set work_mem = '4MB';
set local hash_mem_multiplier = 1.0;

--Testcase 655:
explain (costs off)
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;

--Testcase 656:
select count(*) from join_foo
  left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
  on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;

--Testcase 657:
select final > 1 as multibatch
  from hash_join_batches(
$$
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
$$);

-- multi-batch with rescan, parallel-aware

--Testcase 658:
set enable_parallel_hash = on;

--Testcase 659:
set parallel_leader_participation = off;

--Testcase 660:
set min_parallel_table_scan_size = 0;

--Testcase 661:
set parallel_setup_cost = 0;

--Testcase 662:
set parallel_tuple_cost = 0;

--Testcase 663:
set max_parallel_workers_per_gather = 2;

--Testcase 664:
set enable_material = off;

--Testcase 665:
set enable_mergejoin = off;

--Testcase 666:
set work_mem = '64kB';
set local hash_mem_multiplier = 1.0;

--Testcase 667:
explain (costs off)
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;

--Testcase 668:
select count(*) from join_foo
  left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
  on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;

--Testcase 669:
select final > 1 as multibatch
  from hash_join_batches(
$$
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
$$);

-- single-batch with rescan, parallel-aware

--Testcase 670:
set enable_parallel_hash = on;

--Testcase 671:
set parallel_leader_participation = off;

--Testcase 672:
set min_parallel_table_scan_size = 0;

--Testcase 673:
set parallel_setup_cost = 0;

--Testcase 674:
set parallel_tuple_cost = 0;

--Testcase 675:
set max_parallel_workers_per_gather = 2;

--Testcase 676:
set enable_material = off;

--Testcase 677:
set enable_mergejoin = off;

--Testcase 678:
set work_mem = '4MB';
set local hash_mem_multiplier = 1.0;

--Testcase 679:
explain (costs off)
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;

--Testcase 680:
select count(*) from join_foo
  left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
  on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;

--Testcase 681:
select final > 1 as multibatch
  from hash_join_batches(
$$
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
$$);

-- A full outer join where every record is matched.

-- non-parallel

--Testcase 682:
set local max_parallel_workers_per_gather = 0;

--Testcase 683:
explain (costs off)
     select  count(*) from simple r full outer join simple s using (id);

--Testcase 684:
select  count(*) from simple r full outer join simple s using (id);

-- parallelism not possible with parallel-oblivious outer hash join

--Testcase 685:
set local max_parallel_workers_per_gather = 2;

--Testcase 686:
explain (costs off)
     select  count(*) from simple r full outer join simple s using (id);

--Testcase 687:
select  count(*) from simple r full outer join simple s using (id);

-- An full outer join where every record is not matched.

-- non-parallel

--Testcase 688:
set local max_parallel_workers_per_gather = 0;

--Testcase 689:
explain (costs off)
     select  count(*) from simple r full outer join simple s on (r.id = 0 - s.id);

--Testcase 690:
select  count(*) from simple r full outer join simple s on (r.id = 0 - s.id);

-- parallelism not possible with parallel-oblivious outer hash join

--Testcase 691:
set local max_parallel_workers_per_gather = 2;

--Testcase 692:
explain (costs off)
     select  count(*) from simple r full outer join simple s on (r.id = 0 - s.id);

--Testcase 693:
select  count(*) from simple r full outer join simple s on (r.id = 0 - s.id);

-- exercise special code paths for huge tuples (note use of non-strict
-- expression and left join required to get the detoasted tuple into
-- the hash table)

-- parallel with parallel-aware hash join (hits ExecParallelHashLoadTuple and
-- sts_puttuple oversized tuple cases because it's multi-batch)

--Testcase 694:
set max_parallel_workers_per_gather = 2;

--Testcase 695:
set enable_parallel_hash = on;

--Testcase 696:
set work_mem = '128kB';
set local hash_mem_multiplier = 1.0;

--Testcase 697:
explain (costs off)
  select length(max(s.t))
  from wide left join (select id, coalesce(t, '') || '' as t from wide) s using (id);

--Testcase 698:
select length(max(s.t))
from wide left join (select id, coalesce(t, '') || '' as t from wide) s using (id);

--Testcase 699:
select final > 1 as multibatch
  from hash_join_batches(
$$
  select length(max(s.t))
  from wide left join (select id, coalesce(t, '') || '' as t from wide) s using (id);
$$);

--Testcase 700:
reset enable_mergejoin;

rollback;

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

--Testcase 701:
DROP USER MAPPING FOR public SERVER griddb_svr;

--Testcase 702:
DROP SERVER griddb_svr;

--Testcase 703:
DROP EXTENSION griddb_fdw CASCADE;
