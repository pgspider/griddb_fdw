--
-- JOIN
-- Test JOIN clauses
--
\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 454:
CREATE EXTENSION griddb_fdw;
--Testcase 455:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername :CLUSTER_NAME);
--Testcase 456:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

--Testcase 457:
CREATE FOREIGN TABLE J1_TBL (
  id serial OPTIONS (rowkey 'true'),
  i integer,
  j integer,
  t text
) SERVER griddb_svr; 

--Testcase 458:
CREATE FOREIGN TABLE J2_TBL (
  id serial OPTIONS (rowkey 'true'),
  i integer,
  k integer
) SERVER griddb_svr;

--Testcase 459:
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

--Testcase 460:
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

--Testcase 461:
CREATE FOREIGN TABLE INT4_TBL(id serial OPTIONS (rowkey 'true'), f1 int4) SERVER griddb_svr;
--Testcase 680:
INSERT INTO INT4_TBL(f1) VALUES 
  ('   0  '),
  ('123456     '),
  ('    -123456'),
  ('2147483647'),
  ('-2147483647');

--Testcase 462:
CREATE FOREIGN TABLE FLOAT8_TBL(id serial OPTIONS (rowkey 'true'), f1 float8) SERVER griddb_svr;
--Testcase 681:
INSERT INTO FLOAT8_TBL(f1) VALUES 
  ('    0.0   '),
  ('1004.30  '),
  ('   -34.84'),
  ('1.2345678901234e+200'),
  ('1.2345678901234e-200');

--Testcase 463:
CREATE FOREIGN TABLE INT8_TBL(id serial OPTIONS (rowkey 'true'), q1 int8, q2 int8) SERVER griddb_svr;
--Testcase 682:
INSERT INTO INT8_TBL(q1, q2) VALUES
  ('  123   ','  456'),
  ('123   ','4567890123456789'),
  ('4567890123456789','123'),
  (+4567890123456789,'4567890123456789'),
  ('+4567890123456789','-4567890123456789');

--Testcase 464:
CREATE FOREIGN TABLE INT2_TBL(id int4 OPTIONS (rowkey 'true'), f1 int2) SERVER griddb_svr;

--Testcase 1:
INSERT INTO J1_TBL(i, j, t) VALUES (1, 4, 'one');
--Testcase 2:
INSERT INTO J1_TBL(i, j, t) VALUES (2, 3, 'two');
--Testcase 3:
INSERT INTO J1_TBL(i, j, t) VALUES (3, 2, 'three');
--Testcase 4:
INSERT INTO J1_TBL(i, j, t) VALUES (4, 1, 'four');
--Testcase 5:
INSERT INTO J1_TBL(i, j, t) VALUES (5, 0, 'five');
--Testcase 6:
INSERT INTO J1_TBL(i, j, t) VALUES (6, 6, 'six');
--Testcase 7:
INSERT INTO J1_TBL(i, j, t) VALUES (7, 7, 'seven');
--Testcase 8:
INSERT INTO J1_TBL(i, j, t) VALUES (8, 8, 'eight');
--Testcase 9:
INSERT INTO J1_TBL(i, j, t) VALUES (0, NULL, 'zero');
--Testcase 10:
INSERT INTO J1_TBL(i, j, t) VALUES (NULL, NULL, 'null');
--Testcase 11:
INSERT INTO J1_TBL(i, j, t) VALUES (NULL, 0, 'zero');

--Testcase 12:
INSERT INTO J2_TBL(i, k) VALUES (1, -1);
--Testcase 13:
INSERT INTO J2_TBL(i, k) VALUES (2, 2);
--Testcase 14:
INSERT INTO J2_TBL(i, k) VALUES (3, -3);
--Testcase 15:
INSERT INTO J2_TBL(i, k) VALUES (2, 4);
--Testcase 16:
INSERT INTO J2_TBL(i, k) VALUES (5, -5);
--Testcase 17:
INSERT INTO J2_TBL(i, k) VALUES (5, -5);
--Testcase 18:
INSERT INTO J2_TBL(i, k) VALUES (0, NULL);
--Testcase 19:
INSERT INTO J2_TBL(i, k) VALUES (NULL, NULL);
--Testcase 20:
INSERT INTO J2_TBL(i, k) VALUES (NULL, 0);

-- useful in some tests below
--Testcase 465:
create temp table onerow();
--Testcase 21:
insert into onerow default values;
--
-- CORRELATION NAMES
-- Make sure that table/column aliases are supported
-- before diving into more complex join syntax.
--

--Testcase 22:
SELECT '' AS "xxx", i, j, t
  FROM J1_TBL AS tx;

--Testcase 23:
SELECT '' AS "xxx", i, j, t
  FROM J1_TBL tx;

--Testcase 24:
SELECT '' AS "xxx", a, b, c
  FROM J1_TBL AS t1 (id, a, b, c);

--Testcase 25:
SELECT '' AS "xxx", a, b, c
  FROM J1_TBL t1 (id, a, b, c);

--Testcase 26:
SELECT '' AS "xxx", a, b, c, d, e
  FROM J1_TBL t1 (id, a, b, c), J2_TBL t2 (id, d, e);

--Testcase 27:
SELECT '' AS "xxx", t1.a, t2.e
  FROM J1_TBL t1 (id, a, b, c), J2_TBL t2 (id, d, e)
  WHERE t1.a = t2.d;


--
-- CROSS JOIN
-- Qualifications are not allowed on cross joins,
-- which degenerate into a standard unqualified inner join.
--

--Testcase 28:
SELECT '' AS "xxx", i, j, t, i1 as i, k
  FROM J1_TBL t1 (id, i, j, t) CROSS JOIN J2_TBL t2 (id, i1, k);

-- ambiguous column
--Testcase 29:
SELECT '' AS "xxx", i, k, t
  FROM J1_TBL t1 (id, i, j, t) CROSS JOIN J2_TBL t2 (id, i, k);

-- resolve previous ambiguity by specifying the table name
--Testcase 30:
SELECT '' AS "xxx", t1.i, k, t
  FROM J1_TBL t1 CROSS JOIN J2_TBL t2;

--Testcase 31:
SELECT '' AS "xxx", ii, tt, kk
  FROM (J1_TBL CROSS JOIN J2_TBL)
    AS tx (id1, ii, jj, tt, id2, ii2, kk);

--Testcase 32:
SELECT '' AS "xxx", tx.ii, tx.jj, tx.kk
  FROM (J1_TBL t1 (id, a, b, c) CROSS JOIN J2_TBL t2 (id, d, e))
    AS tx (id1, ii, jj, tt, id2, ii2, kk);

--Testcase 33:
SELECT '' AS "xxx", x.i, x.j, x.t, a.i, a.k, b.i, b.k
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
--Testcase 34:
SELECT '' AS "xxx", i, j, t, k
  FROM J1_TBL INNER JOIN J2_TBL USING (i);

-- Same as above, slightly different syntax
--Testcase 35:
SELECT '' AS "xxx", i, j, t, k
  FROM J1_TBL JOIN J2_TBL USING (i);

--Testcase 36:
SELECT '' AS "xxx", a, b, c, d
  FROM J1_TBL t1 (id, a, b, c) JOIN J2_TBL t2 (id, a, d) USING (a)
  ORDER BY a, d;

--Testcase 37:
SELECT '' AS "xxx", b, t1.a, c, t2.a
  FROM J1_TBL t1 (id, a, b, c) JOIN J2_TBL t2 (id, a, b) USING (b)
  ORDER BY b, t1.a;


--
-- NATURAL JOIN
-- Inner equi-join on all columns with the same name
--

--Testcase 38:
SELECT '' AS "xxx", i, j, t, k
  FROM J1_TBL t1(id1, i, j, t) NATURAL JOIN J2_TBL t2(id2, i, k);

--Testcase 39:
SELECT '' AS "xxx", a, b, c, d
  FROM J1_TBL t1 (id1, a, b, c) NATURAL JOIN J2_TBL t2 (id2, a, d);

--Testcase 40:
SELECT '' AS "xxx", a, b, c, d
  FROM J1_TBL t1 (id1, a, b, c) NATURAL JOIN J2_TBL t2 (id2, d, a);

-- mismatch number of columns
-- currently, Postgres will fill in with underlying names
--Testcase 41:
SELECT '' AS "xxx", a, b, t, k
  FROM J1_TBL t1 (id1, a, b) NATURAL JOIN J2_TBL t2 (id2, a);


--
-- Inner joins (equi-joins)
--

--Testcase 42:
SELECT '' AS "xxx", J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.i);

--Testcase 43:
SELECT '' AS "xxx", J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.k);


--
-- Non-equi-joins
--

--Testcase 44:
SELECT '' AS "xxx", J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i <= J2_TBL.k);


--
-- Outer joins
-- Note that OUTER is a noise word
--

--Testcase 45:
SELECT '' AS "xxx", i, j, t, k
  FROM J1_TBL LEFT OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 46:
SELECT '' AS "xxx", i, j, t, k
  FROM J1_TBL LEFT JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 47:
SELECT '' AS "xxx", i, j, t, k
  FROM J1_TBL RIGHT OUTER JOIN J2_TBL USING (i);

--Testcase 48:
SELECT '' AS "xxx", i, j, t, k
  FROM J1_TBL RIGHT JOIN J2_TBL USING (i);

--Testcase 49:
SELECT '' AS "xxx", i, j, t, k
  FROM J1_TBL FULL OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 50:
SELECT '' AS "xxx", i, j, t, k
  FROM J1_TBL FULL JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 51:
SELECT '' AS "xxx", i, j, t, k
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (k = 1);

--Testcase 52:
SELECT '' AS "xxx", i, j, t, k
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (i = 1);

--
-- semijoin selectivity for <>
--
--Testcase 53:
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

--Testcase 466:
CREATE FOREIGN TABLE t11 (name TEXT, n INTEGER) SERVER griddb_svr;
--Testcase 467:
CREATE FOREIGN TABLE t21 (name TEXT, n INTEGER) SERVER griddb_svr;
--Testcase 468:
CREATE FOREIGN TABLE t31 (name TEXT, n INTEGER) SERVER griddb_svr;

--Testcase 54:
INSERT INTO t11 VALUES ( 'bb', 11 );
--Testcase 55:
INSERT INTO t21 VALUES ( 'bb', 12 );
--Testcase 56:
INSERT INTO t21 VALUES ( 'cc', 22 );
--Testcase 57:
INSERT INTO t21 VALUES ( 'ee', 42 );
--Testcase 58:
INSERT INTO t31 VALUES ( 'bb', 13 );
--Testcase 59:
INSERT INTO t31 VALUES ( 'cc', 23 );
--Testcase 60:
INSERT INTO t31 VALUES ( 'dd', 33 );

--Testcase 61:
SELECT * FROM t11 FULL JOIN t21 USING (name) FULL JOIN t31 USING (name);

--
-- Test interactions of join syntax and subqueries
--

-- Basic cases (we expect planner to pull up the subquery here)
--Testcase 62:
SELECT * FROM
(SELECT * FROM t21) as s2
INNER JOIN
(SELECT * FROM t31) s3
USING (name);

--Testcase 63:
SELECT * FROM
(SELECT * FROM t21) as s2
LEFT JOIN
(SELECT * FROM t31) s3
USING (name);

--Testcase 64:
SELECT * FROM
(SELECT * FROM t21) as s2
FULL JOIN
(SELECT * FROM t31) s3
USING (name);

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
--Testcase 65:
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 66:
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL LEFT JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 67:
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 68:
SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t11) as s1
NATURAL INNER JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 69:
SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t11) as s1
NATURAL FULL JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 70:
SELECT * FROM
(SELECT name, n as s1_n FROM t11) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n FROM t21) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t31) as s3
  ) ss2;

--Testcase 71:
SELECT * FROM
(SELECT name, n as s1_n FROM t11) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t31) as s3
  ) ss2;

-- Constants as join keys can also be problematic
--Testcase 72:
SELECT * FROM
  (SELECT name, n as s1_n FROM t11) as s1
FULL JOIN
  (SELECT name, 2 as s2_n FROM t21) as s2
ON (s1_n = s2_n);


-- Test for propagation of nullability constraints into sub-joins

--Testcase 469:
create foreign table x (x1 int, x2 int) server griddb_svr;
--Testcase 73:
insert into x values (1,11);
--Testcase 74:
insert into x values (2,22);
--Testcase 75:
insert into x values (3,null);
--Testcase 76:
insert into x values (4,44);
--Testcase 77:
insert into x values (5,null);

--Testcase 470:
create foreign table y (y1 int, y2 int) server griddb_svr;
--Testcase 78:
insert into y values (1,111);
--Testcase 79:
insert into y values (2,222);
--Testcase 80:
insert into y values (3,333);
--Testcase 81:
insert into y values (4,null);

--Testcase 82:
select * from x;
--Testcase 83:
select * from y;

--Testcase 84:
select * from x left join y on (x1 = y1 and x2 is not null);
--Testcase 85:
select * from x left join y on (x1 = y1 and y2 is not null);

--Testcase 86:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1);
--Testcase 87:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and x2 is not null);
--Testcase 88:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and y2 is not null);
--Testcase 89:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and xx2 is not null);
-- these should NOT give the same answers as above
--Testcase 90:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (x2 is not null);
--Testcase 91:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (y2 is not null);
--Testcase 92:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (xx2 is not null);

--
-- regression test: check for bug with propagation of implied equality
-- to outside an IN
--
--Testcase 93:
select count(*) from tenk1 a where unique1 in
  (select unique1 from tenk1 b join tenk1 c using (unique1)
   where b.unique2 = 42);

--
-- regression test: check for failure to generate a plan with multiple
-- degenerate IN clauses
--
--Testcase 94:
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);

-- try that with GEQO too
begin;
--Testcase 575:
set geqo = on;
--Testcase 576:
set geqo_threshold = 2;
--Testcase 95:
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);
rollback;

--
-- regression test: be sure we cope with proven-dummy append rels
--
--Testcase 471:
create foreign table b0 (aa int, bb int) server griddb_svr;
--Testcase 96:
explain (costs off)
select aa, bb, unique1, unique1
  from tenk1 right join b0 on aa = unique1
  where bb < bb and bb is null;

--Testcase 97:
select aa, bb, unique1, unique1
  from tenk1 right join b0 on aa = unique1
  where bb < bb and bb is null;

--
-- regression test: check handling of empty-FROM subquery underneath outer join
--
--Testcase 98:
explain (costs off)
select * from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 2, 3;

--Testcase 99:
select i1.q1, i1.q2, i2.q1, i2.q2, x from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 1, 2;

--
-- regression test: check a case where join_clause_is_movable_into() gives
-- an imprecise result, causing an assertion failure
--
--Testcase 100:
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
--Testcase 101:
explain (costs off)
select a.f1, b.f1, t.thousand, t.tenthous from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl i4a) a,
  (select sum(f1) as f1 from int4_tbl i4b) b
where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous;

--Testcase 102:
select a.f1, b.f1, t.thousand, t.tenthous from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl i4a) a,
  (select sum(f1) as f1 from int4_tbl i4b) b
where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous;

--
-- check a case where we formerly got confused by conflicting sort orders
-- in redundant merge join path keys
--
--Testcase 103:
explain (costs off)
select * from
  j1_tbl full join
  (select * from j2_tbl order by j2_tbl.i desc, j2_tbl.k asc) j2_tbl
  on j1_tbl.i = j2_tbl.i and j1_tbl.i = j2_tbl.k;

--Testcase 104:
select j1_tbl.i, j1_tbl.j, j1_tbl.t, j2_tbl.i, j2_tbl.k from
  j1_tbl full join
  (select * from j2_tbl order by j2_tbl.i desc, j2_tbl.k asc) j2_tbl
  on j1_tbl.i = j2_tbl.i and j1_tbl.i = j2_tbl.k;

--
-- a different check for handling of redundant sort keys in merge joins
--
--Testcase 105:
explain (costs off)
select count(*) from
  (select * from tenk1 x order by x.thousand, x.twothousand, x.fivethous) x
  left join
  (select * from tenk1 y order by y.unique2) y
  on x.thousand = y.unique2 and x.twothousand = y.hundred and x.fivethous = y.unique2;

--Testcase 106:
select count(*) from
  (select * from tenk1 x order by x.thousand, x.twothousand, x.fivethous) x
  left join
  (select * from tenk1 y order by y.unique2) y
  on x.thousand = y.unique2 and x.twothousand = y.hundred and x.fivethous = y.unique2;


--
-- Clean up
--

--Testcase 472:
DROP FOREIGN TABLE t11;
--Testcase 473:
DROP FOREIGN TABLE t21;
--Testcase 474:
DROP FOREIGN TABLE t31;

--Testcase 475:
DROP FOREIGN TABLE J1_TBL;
--Testcase 476:
DROP FOREIGN TABLE J2_TBL;

-- Both DELETE and UPDATE allow the specification of additional tables
-- to "join" against to determine which rows should be modified.

--Testcase 477:
CREATE FOREIGN TABLE t12 (a int OPTIONS (rowkey 'true'), b int) SERVER griddb_svr;
--Testcase 478:
CREATE FOREIGN TABLE t22 (a int OPTIONS (rowkey 'true'), b int) SERVER griddb_svr;
--Testcase 479:
CREATE FOREIGN TABLE t32 (x int OPTIONS (rowkey 'true'), y int) SERVER griddb_svr;

--Testcase 107:
INSERT INTO t12 VALUES (5, 10);
--Testcase 108:
INSERT INTO t12 VALUES (15, 20);
--Testcase 109:
INSERT INTO t12 VALUES (100, 100);
--Testcase 110:
INSERT INTO t12 VALUES (200, 1000);
--Testcase 111:
INSERT INTO t22 VALUES (200, 2000);
--Testcase 112:
INSERT INTO t32 VALUES (5, 20);
--Testcase 113:
INSERT INTO t32 VALUES (6, 7);
--Testcase 114:
INSERT INTO t32 VALUES (7, 8);
--Testcase 115:
INSERT INTO t32 VALUES (500, 100);

--Testcase 116:
DELETE FROM t32 USING t12 table1 WHERE t32.x = table1.a;
--Testcase 117:
SELECT * FROM t32;
--Testcase 118:
DELETE FROM t32 USING t12 JOIN t22 USING (a) WHERE t32.x > t12.a;
--Testcase 119:
SELECT * FROM t32;
--Testcase 120:
DELETE FROM t32 USING t32 t3_other WHERE t32.x = t3_other.x AND t32.y = t3_other.y;
--Testcase 121:
SELECT * FROM t32;

-- Test join against inheritance tree

--Testcase 480:
create temp table t2a () inherits (t22);

--Testcase 122:
insert into t2a values (200, 2001);

--Testcase 123:
select * from t12 left join t22 on (t12.a = t22.a);

-- Test matching of column name with wrong alias

--Testcase 124:
select t12.x from t12 join t32 on (t12.a = t32.x);

--
-- regression test for 8.1 merge right join bug
--

--Testcase 481:
CREATE FOREIGN TABLE tt1 ( tt1_id int4, joincol int4 ) SERVER griddb_svr;
--Testcase 125:
INSERT INTO tt1 VALUES (1, 11);
--Testcase 126:
INSERT INTO tt1 VALUES (2, NULL);

--Testcase 482:
CREATE FOREIGN TABLE tt2 ( tt2_id int4, joincol int4 ) SERVER griddb_svr;
--Testcase 127:
INSERT INTO tt2 VALUES (21, 11);
--Testcase 128:
INSERT INTO tt2 VALUES (22, 11);

--Testcase 577:
set enable_hashjoin to off;
--Testcase 578:
set enable_nestloop to off;

-- these should give the same results

--Testcase 129:
select tt1.*, tt2.* from tt1 left join tt2 on tt1.joincol = tt2.joincol;

--Testcase 130:
select tt1.*, tt2.* from tt2 right join tt1 on tt1.joincol = tt2.joincol;

--Testcase 579:
reset enable_hashjoin;
--Testcase 580:
reset enable_nestloop;

--
-- regression test for bug #13908 (hash join with skew tuples & nbatch increase)
--

--Testcase 581:
set work_mem to '64kB';
--Testcase 582:
set enable_mergejoin to off;

--Testcase 131:
explain (costs off)
select count(*) from tenk1 a, tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;
--Testcase 132:
select count(*) from tenk1 a, tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;

--Testcase 583:
reset work_mem;
--Testcase 584:
reset enable_mergejoin;

--
-- regression test for 8.2 bug with improper re-ordering of left joins
--

--Testcase 483:
create foreign table tt3(f1 int, f2 text) server griddb_svr;
--Testcase 133:
insert into tt3 select x, repeat('xyzzy', 100) from generate_series(1,10000) x;

--Testcase 484:
create foreign table tt4(f1 int) server griddb_svr;
--Testcase 134:
insert into tt4 values (0),(1),(9999);

--Testcase 135:
SELECT a.f1
FROM tt4 a
LEFT JOIN (
        SELECT b.f1
        FROM tt3 b LEFT JOIN tt3 c ON (b.f1 = c.f1)
        WHERE c.f1 IS NULL
) AS d ON (a.f1 = d.f1)
WHERE d.f1 IS NULL;

--
-- regression test for proper handling of outer joins within antijoins
--

--Testcase 485:
create foreign table tt4x(c1 int, c2 int, c3 int) server griddb_svr;

--Testcase 136:
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

--Testcase 486:
create foreign table tt5(id serial, f1 int, f2 int) server griddb_svr;
--Testcase 487:
create foreign table tt6(id serial, f1 int, f2 int) server griddb_svr;

--Testcase 137:
insert into tt5(f1, f2) values(1, 10);
--Testcase 138:
insert into tt5(f1, f2) values(1, 11);

--Testcase 139:
insert into tt6(f1, f2) values(1, 9);
--Testcase 140:
insert into tt6(f1, f2) values(1, 2);
--Testcase 141:
insert into tt6(f1, f2) values(2, 9);

--Testcase 142:
select tt5.f1, tt5.f2, tt6.f1, tt6.f2 from tt5,tt6 where tt5.f1 = tt6.f1 and tt5.f1 = tt5.f2 - tt6.f2;

--
-- regression test for problems of the sort depicted in bug #3588
--

--Testcase 488:
create foreign table xx (pkxx int) server griddb_svr;
--Testcase 489:
create foreign table yy (pkyy int, pkxx int) server griddb_svr;

--Testcase 143:
insert into xx values (1);
--Testcase 144:
insert into xx values (2);
--Testcase 145:
insert into xx values (3);

--Testcase 146:
insert into yy values (101, 1);
--Testcase 147:
insert into yy values (201, 2);
--Testcase 148:
insert into yy values (301, NULL);

--Testcase 149:
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

--Testcase 490:
create foreign table zt1 (f1 int OPTIONS(rowkey 'true')) server griddb_svr;
--Testcase 491:
create foreign table zt2 (f2 int OPTIONS(rowkey 'true')) server griddb_svr;
--Testcase 492:
create foreign table zt3 (f3 int OPTIONS(rowkey 'true')) server griddb_svr;
--Testcase 150:
insert into zt1 values(53);
--Testcase 151:
insert into zt2 values(53);

--Testcase 152:
select * from
  zt2 left join zt3 on (f2 = f3)
      left join zt1 on (f3 = f1)
where f2 = 53;

--Testcase 493:
create temp view zv1 as select *,'dummy'::text AS junk from zt1;

--Testcase 153:
select * from
  zt2 left join zt3 on (f2 = f3)
      left join zv1 on (f3 = f1)
where f2 = 53;

--
-- regression test for improper extraction of OR indexqual conditions
-- (as seen in early 8.3.x releases)
--

--Testcase 154:
select a.unique2, a.ten, b.tenthous, b.unique2, b.hundred
from tenk1 a left join tenk1 b on a.unique2 = b.tenthous
where a.unique1 = 42 and
      ((b.unique2 is null and a.ten = 2) or b.hundred = 3);

--
-- test proper positioning of one-time quals in EXISTS (8.4devel bug)
--
--Testcase 155:
prepare foo(bool) as
  select count(*) from tenk1 a left join tenk1 b
    on (a.unique2 = b.unique1 and exists
        (select 1 from tenk1 c where c.thousand = b.unique2 and $1));
--Testcase 156:
execute foo(true);
--Testcase 157:
execute foo(false);

--
-- test for sane behavior with noncanonical merge clauses, per bug #4926
--

begin;

--Testcase 585:
set enable_mergejoin = 1;
--Testcase 586:
set enable_hashjoin = 0;
--Testcase 587:
set enable_nestloop = 0;

--Testcase 494:
create foreign table a1 (i integer) server griddb_svr;
--Testcase 495:
create foreign table b1 (x integer, y integer) server griddb_svr;

--Testcase 158:
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
--Testcase 159:
select t1.q2, count(t2.*)
from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--Testcase 160:
select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--Testcase 161:
select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl offset 0) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--Testcase 162:
select t1.q2, count(t2.*)
from int8_tbl t1 left join
  (select q1, case when q2=1 then 1 else q2 end as q2 from int8_tbl) t2
  on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--
-- test incorrect failure to NULL pulled-up subexpressions
--
begin;

--Testcase 496:
create foreign table a2 (
     code text OPTIONS (rowkey 'true')
) server griddb_svr;
--Testcase 497:
create foreign table b2 (
     id serial OPTIONS (rowkey 'true'),
     a text,
     num integer
) server griddb_svr;
--Testcase 498:
create foreign table c2 (
     name text OPTIONS (rowkey 'true'),
     a text
) server griddb_svr;

--Testcase 163:
insert into a2 (code) values ('p');
--Testcase 164:
insert into a2 (code) values ('q');
--Testcase 165:
insert into b2 (a, num) values ('p', 1);
--Testcase 166:
insert into b2 (a, num) values ('p', 2);
--Testcase 167:
insert into c2 (name, a) values ('A', 'p');
--Testcase 168:
insert into c2 (name, a) values ('B', 'q');
--Testcase 169:
insert into c2 (name, a) values ('C', null);

--Testcase 170:
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
--Testcase 499:
create foreign table a1 (i integer) server griddb_svr;
--Testcase 500:
create foreign table b1 (x integer, y integer) server griddb_svr;

--Testcase 171:
INSERT INTO a1 values (1);
--Testcase 172:
INSERT INTO b1 values (2, 42);

--Testcase 173:
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
--Testcase 174:
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

--Testcase 175:
EXPLAIN (COSTS OFF)
SELECT qq, unique1
  FROM
  ( SELECT COALESCE(q1, 0) AS qq FROM int8_tbl a ) AS ss1
  FULL OUTER JOIN
  ( SELECT COALESCE(q2, -1) AS qq FROM int8_tbl b ) AS ss2
  USING (qq)
  INNER JOIN tenk1 c ON qq = unique2;

--Testcase 176:
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

--Testcase 501:
create foreign table nt1 (
  id int OPTIONS (rowkey 'true'),
  a1 boolean,
  a2 boolean
) server griddb_svr;
--Testcase 502:
create foreign table nt2 (
  id int OPTIONS (rowkey 'true'),
  nt1_id int,
  b1 boolean,
  b2 boolean
) server griddb_svr;
--Testcase 503:
create foreign table nt3 (
  id int OPTIONS (rowkey 'true'),
  nt2_id int,
  c1 boolean
) server griddb_svr;

--Testcase 177:
insert into nt1 values (1,true,true);
--Testcase 178:
insert into nt1 values (2,true,false);
--Testcase 179:
insert into nt1 values (3,false,false);
--Testcase 180:
insert into nt2 values (1,1,true,true);
--Testcase 181:
insert into nt2 values (2,2,true,false);
--Testcase 182:
insert into nt2 values (3,3,false,false);
--Testcase 183:
insert into nt3 values (1,1,true);
--Testcase 184:
insert into nt3 values (2,2,false);
--Testcase 185:
insert into nt3 values (3,3,true);

--Testcase 186:
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

--Testcase 187:
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

--Testcase 188:
explain (costs off)
select * from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 2,3;

--Testcase 189:
select q1, q2, x, y from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 1,2;

--
-- test the corner cases FULL JOIN ON TRUE and FULL JOIN ON FALSE
--
--Testcase 190:
select a.f1, b.f1 from int4_tbl a full join int4_tbl b on true;
--Testcase 191:
select a.f1, b.f1 from int4_tbl a full join int4_tbl b on false;

--
-- test for ability to use a cartesian join when necessary
--

--Testcase 504:
create foreign table q1(q1 int OPTIONS (rowkey 'true')) server griddb_svr;
--Testcase 505:
create foreign table q2(q2 int OPTIONS (rowkey 'true')) server griddb_svr;

--Testcase 506:
insert into q1 select 1;
--Testcase 507:
insert into q1 select 0;

--Testcase 192:
explain (costs off)
select * from
  tenk1 join int4_tbl on f1 = twothousand,
  q1, q2
where q1 = thousand or q2 = thousand;

--Testcase 193:
explain (costs off)
select * from
  tenk1 join int4_tbl on f1 = twothousand,
  q1, q2
where thousand = (q1 + q2);

--Testcase 508:
drop foreign table q1, q2;

--
-- test ability to generate a suitable plan for a star-schema query
--

--Testcase 194:
explain (costs off)
select * from
  tenk1, int8_tbl a, int8_tbl b
where thousand = a.q1 and tenthous = b.q1 and a.q2 = 1 and b.q2 = 2;

--
-- test a corner case in which we shouldn't apply the star-schema optimization
--

--Testcase 195:
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

--Testcase 196:
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

--Testcase 197:
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

--Testcase 198:
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

--Testcase 199:
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
--Testcase 509:
explain (verbose, costs off)
select * from
  (select 1 as x) ss1 left join (select 2 as y) ss2 on (true),
  lateral (select ss2.y as z limit 1) ss3;
--Testcase 510:
select * from
  (select 1 as x) ss1 left join (select 2 as y) ss2 on (true),
  lateral (select ss2.y as z limit 1) ss3;

--
-- test inlining of immutable functions
--
--Testcase 511:
create function f_immutable_int4(i integer) returns integer as
$$ begin return i; end; $$ language plpgsql immutable;

-- check optimization of function scan with join
--Testcase 512:
explain (costs off)
select unique1 from tenk1, (select * from f_immutable_int4(1) x) x
where x = unique1;

--Testcase 513:
explain (verbose, costs off)
select unique1, x.*
from tenk1, (select *, random() from f_immutable_int4(1) x) x
where x = unique1;

--Testcase 514:
explain (costs off)
select unique1 from tenk1, f_immutable_int4(1) x where x = unique1;

--Testcase 515:
explain (costs off)
select unique1 from tenk1, lateral f_immutable_int4(1) x where x = unique1;

--Testcase 516:
explain (costs off)
select unique1, x from tenk1 join f_immutable_int4(1) x on unique1 = x;

--Testcase 517:
explain (costs off)
select unique1, x from tenk1 left join f_immutable_int4(1) x on unique1 = x;

--Testcase 518:
explain (costs off)
select unique1, x from tenk1 right join f_immutable_int4(1) x on unique1 = x;

--Testcase 519:
explain (costs off)
select unique1, x from tenk1 full join f_immutable_int4(1) x on unique1 = x;

-- check that pullup of a const function allows further const-folding
--Testcase 520:
explain (costs off)
select unique1 from tenk1, f_immutable_int4(1) x where x = 42;

-- test inlining of immutable functions with PlaceHolderVars
--Testcase 521:
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

--Testcase 522:
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

--Testcase 200:
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or (a.unique2 = 3 and b.hundred = 4);
--Testcase 201:
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or (a.unique2 = 3 and b.ten = 4);
--Testcase 202:
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or
  ((a.unique2 = 3 or a.unique2 = 7) and b.hundred = 4);

--
-- test placement of movable quals in a parameterized join tree
--

--Testcase 203:
explain (costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten = t3.ten
where t1.unique1 = 1;

--Testcase 204:
explain (costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten + t2.ten = t3.ten
where t1.unique1 = 1;

--Testcase 205:
explain (costs off)
select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

--Testcase 206:
select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

--Testcase 207:
explain (costs off)
select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

--Testcase 208:
select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

--Testcase 209:
explain (costs off)
select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

--Testcase 210:
select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

--Testcase 211:
explain (costs off)
select * from
(values (1, array[10,20]), (2, array[20,30])) as v1(v1x,v1ys)
left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
left join unnest(v1ys) as u1(u1y) on u1y = v2y;

--Testcase 212:
select * from
(values (1, array[10,20]), (2, array[20,30])) as v1(v1x,v1ys)
left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
left join unnest(v1ys) as u1(u1y) on u1y = v2y;

--
-- test handling of potential equivalence clauses above outer joins
--

--Testcase 213:
explain (costs off)
select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

--Testcase 214:
select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

--Testcase 215:
explain (costs off)
select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

--Testcase 216:
select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

--
-- another case with equivalence clauses above outer joins (bug #8591)
--

--Testcase 217:
explain (costs off)
select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

--Testcase 218:
select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

--
-- check handling of join aliases when flattening multiple levels of subquery
--

--Testcase 219:
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

--Testcase 220:
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
-- test successful handling of nested outer joins with degenerate join quals
--
--Testcase 523:
create foreign table text_tbl(f1 text) server griddb_svr;

--Testcase 221:
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

--Testcase 222:
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

--Testcase 223:
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

--Testcase 224:
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

--Testcase 225:
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

--Testcase 226:
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

--Testcase 227:
explain (verbose, costs off)
select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

--Testcase 228:
select t1.f1, i8.q1, i8.q2, t2.f1, i4.f1 from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

--
-- test for appropriate join order in the presence of lateral references
--

--Testcase 229:
explain (verbose, costs off)
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss
where t1.f1 = ss.f1;

--Testcase 230:
select t1.f1, i8.q1, i8.q2, ss.q1, ss.f1 from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss
where t1.f1 = ss.f1;

--Testcase 231:
explain (verbose, costs off)
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

--Testcase 232:
select t1.f1, i8.q1, i8.q2, ss1.q1, ss1.f1, ss2.q1, ss2.f1 from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

--Testcase 233:
explain (verbose, costs off)
select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

--Testcase 234:
select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

--
-- check a case in which a PlaceHolderVar forces join order
--

--Testcase 235:
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

--Testcase 236:
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

--Testcase 237:
explain (costs off)
select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

--Testcase 238:
select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

--
-- test ability to push constants through outer join clauses
--

--Testcase 239:
explain (costs off)
  select * from int4_tbl a left join tenk1 b on f1 = unique2 where f1 = 0;

--Testcase 240:
explain (costs off)
  select * from tenk1 a full join tenk1 b using(unique2) where unique2 = 42;

--
-- test that quals attached to an outer join have correct semantics,
-- specifically that they don't re-use expressions computed below the join;
-- we force a mergejoin so that coalesce(b.q1, 1) appears as a join input
--

--Testcase 588:
set enable_hashjoin to off;
--Testcase 589:
set enable_nestloop to off;

--Testcase 241:
explain (verbose, costs off)
  select a.q2, b.q1
    from int8_tbl a left join int8_tbl b on a.q2 = coalesce(b.q1, 1)
    where coalesce(b.q1, 1) > 0;
--Testcase 242:
select a.q2, b.q1
  from int8_tbl a left join int8_tbl b on a.q2 = coalesce(b.q1, 1)
  where coalesce(b.q1, 1) > 0;

--Testcase 590:
reset enable_hashjoin;
--Testcase 591:
reset enable_nestloop;

--
-- test join removal
--

begin;

--Testcase 524:
CREATE FOREIGN TABLE a3 (id int OPTIONS (rowkey 'true'), b_id int) SERVER griddb_svr;
--Testcase 525:
CREATE FOREIGN TABLE b3 (id int OPTIONS (rowkey 'true'), c_id int) SERVER griddb_svr;
--Testcase 526:
CREATE FOREIGN TABLE c3 (id int OPTIONS (rowkey 'true')) SERVER griddb_svr;
--Testcase 527:
CREATE FOREIGN TABLE d3 (a int, b int) SERVER griddb_svr;
--Testcase 243:
INSERT INTO a3 VALUES (0, 0), (1, NULL);
--Testcase 244:
INSERT INTO b3 VALUES (0, 0), (1, NULL);
--Testcase 245:
INSERT INTO c3 VALUES (0), (1);
--Testcase 246:
INSERT INTO d3 VALUES (1,3), (2,2), (3,1);

-- all three cases should be optimizable into a3 simple seqscan
--Testcase 247:
explain (costs off) SELECT a3.* FROM a3 LEFT JOIN b3 ON a3.b_id = b3.id;
--Testcase 248:
explain (costs off) SELECT b3.* FROM b3 LEFT JOIN c3 ON b3.c_id = c3.id;
--Testcase 249:
explain (costs off)
  SELECT a3.* FROM a3 LEFT JOIN (b3 left join c3 on b3.c_id = c3.id)
  ON (a3.b_id = b3.id);

-- check optimization of outer join within another special join
--Testcase 250:
explain (costs off)
select id from a3 where id in (
	select b3.id from b3 left join c3 on b3.id = c3.id
);

-- check that join removal works for a left join when joining a subquery
-- that is guaranteed to be unique by its GROUP BY clause
--Testcase 251:
explain (costs off)
select d3.* from d3 left join (select * from b3 group by b3.id, b3.c_id) s
  on d3.a = s.id and d3.b = s.c_id;

-- similarly, but keying off a DISTINCT clause
--Testcase 252:
explain (costs off)
select d3.* from d3 left join (select distinct * from b3) s
  on d3.a = s.id and d3.b = s.c_id;

-- join removal is not possible when the GROUP BY contains a column that is
-- not in the join condition.  (Note: as of 9.6, we notice that b3.id is a
-- primary key and so drop b3.c_id from the GROUP BY of the resulting plan;
-- but this happens too late for join removal in the outer plan level.)
--Testcase 253:
explain (costs off)
select d3.* from d3 left join (select * from b3 group by b3.id, b3.c_id) s
  on d3.a = s.id;

-- similarly, but keying off a DISTINCT clause
--Testcase 254:
explain (costs off)
select d3.* from d3 left join (select distinct * from b3) s
  on d3.a = s.id;

-- check join removal works when uniqueness of the join condition is enforced
-- by a UNION
--Testcase 255:
explain (costs off)
select d3.* from d3 left join (select id from a3 union select id from b3) s
  on d3.a = s.id;

-- check join removal with a cross-type comparison operator
--Testcase 256:
explain (costs off)
select i8.* from int8_tbl i8 left join (select f1 from int4_tbl group by f1) i4
  on i8.q1 = i4.f1;

-- check join removal with lateral references
--Testcase 257:
explain (costs off)
select 1 from (select a3.id FROM a3 left join b3 on a3.b_id = b3.id) q,
			  lateral generate_series(1, q.id) gs(i) where q.id = gs.i;

rollback;

--Testcase 528:
create foreign table parent (k int options (rowkey 'true'), pd int) server griddb_svr;
--Testcase 529:
create foreign table child (k int options (rowkey 'true'), cd int) server griddb_svr;
--Testcase 258:
insert into parent values (1, 10), (2, 20), (3, 30);
--Testcase 259:
insert into child values (1, 100), (4, 400);

-- this case is optimizable
--Testcase 260:
select p.* from parent p left join child c on (p.k = c.k);
--Testcase 261:
explain (costs off)
  select p.* from parent p left join child c on (p.k = c.k);

-- this case is not
--Testcase 262:
select p.*, linked from parent p
  left join (select c.*, true as linked from child c) as ss
  on (p.k = ss.k);
--Testcase 263:
explain (costs off)
  select p.*, linked from parent p
    left join (select c.*, true as linked from child c) as ss
    on (p.k = ss.k);

-- check for a 9.0rc1 bug: join removal breaks pseudoconstant qual handling
--Testcase 264:
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;
--Testcase 265:
explain (costs off)
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;

--Testcase 266:
select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;
--Testcase 267:
explain (costs off)
select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;

-- bug 5255: this is not optimizable by join removal
begin;

--Testcase 530:
CREATE FOREIGN TABLE a4 (id int OPTIONS (rowkey 'true')) SERVER griddb_svr;
--Testcase 531:
CREATE FOREIGN TABLE b4 (id int OPTIONS (rowkey 'true'), a_id int) SERVER griddb_svr;
--Testcase 268:
INSERT INTO a4 VALUES (0), (1);
--Testcase 269:
INSERT INTO b4 VALUES (0, 0), (1, NULL);

--Testcase 270:
SELECT * FROM b4 LEFT JOIN a4 ON (b4.a_id = a4.id) WHERE (a4.id IS NULL OR a4.id > 0);
--Testcase 271:
SELECT b4.* FROM b4 LEFT JOIN a4 ON (b4.a_id = a4.id) WHERE (a4.id IS NULL OR a4.id > 0);

rollback;

-- another join removal bug: this is not optimizable, either
begin;

--Testcase 532:
create foreign table innertab (id int8 options (rowkey 'true'), dat1 int8) server griddb_svr;
--Testcase 272:
insert into innertab values(123, 42);

--Testcase 273:
SELECT * FROM
    (SELECT 1 AS x) ss1
  LEFT JOIN
    (SELECT q1, q2, COALESCE(dat1, q1) AS y
     FROM int8_tbl LEFT JOIN innertab in1 ON q2 = in1.id) ss2
  ON true;

rollback;

-- another join removal bug: we must clean up correctly when removing a PHV
begin;

--Testcase 533:
create foreign table uniquetbl (f1 text) server griddb_svr;

--Testcase 274:
explain (costs off)
select t1.* from
  uniquetbl as t1
  left join (select *, '***'::text as d1 from uniquetbl) t2
  on t1.f1 = t2.f1
  left join uniquetbl t3
  on t2.d1 = t3.f1;

--Testcase 275:
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

--Testcase 276:
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

-- bug #8444: we've historically allowed duplicate aliases within aliased JOINs

--Testcase 277:
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y) j on q1 = f1; -- error
--Testcase 278:
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y) j on q1 = y.f1; -- error
--Testcase 279:
select * from
  int8_tbl x join ((SELECT f1 FROM int4_tbl) x cross join (SELECT f1 FROM int4_tbl) y(ff)) j on q1 = f1; -- ok

--
-- Test hints given on incorrect column references are useful
--

--Testcase 280:
select t1.uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, prefer "t1" suggestion
--Testcase 281:
select t2.uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, prefer "t2" suggestion
--Testcase 282:
select uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, suggest both at once

--
-- Take care to reference the correct RTE
--

--Testcase 283:
select atts.relid::regclass, s.* from pg_stats s join
    pg_attribute a on s.attname = a.attname and s.tablename =
    a.attrelid::regclass::text join (select unnest(indkey) attnum,
    indexrelid from pg_index i) atts on atts.attnum = a.attnum where
    schemaname != 'pg_catalog';

--
-- Test LATERAL
--

--Testcase 284:
select unique2, x.f1
from tenk1 a, lateral (select * from int4_tbl b where f1 = a.unique1) x;
--Testcase 285:
explain (costs off)
  select unique2, x.*
  from tenk1 a, lateral (select * from int4_tbl b where f1 = a.unique1) x;
--Testcase 286:
select unique2, x.f1
from int4_tbl x, lateral (select unique2 from tenk1 where f1 = unique1) ss;
--Testcase 287:
explain (costs off)
  select unique2, x.*
  from int4_tbl x, lateral (select unique2 from tenk1 where f1 = unique1) ss;
--Testcase 288:
explain (costs off)
  select unique2, x.*
  from int4_tbl x cross join lateral (select unique2 from tenk1 where f1 = unique1) ss;
--Testcase 289:
select unique2, x.f1
from int4_tbl x left join lateral (select unique1, unique2 from tenk1 where f1 = unique1) ss on true;
--Testcase 290:
explain (costs off)
  select unique2, x.*
  from int4_tbl x left join lateral (select unique1, unique2 from tenk1 where f1 = unique1) ss on true;

-- check scoping of lateral versus parent references
-- the first of these should return int8_tbl.q2, the second int8_tbl.q1
--Testcase 291:
select q1, q2, (select r from (select q1 as q2) x, (select q2 as r) y) from int8_tbl;
--Testcase 292:
select q1, q2, (select r from (select q1 as q2) x, lateral (select q2 as r) y) from int8_tbl;

-- lateral with function in FROM
--Testcase 293:
select count(*) from tenk1 a, lateral generate_series(1,two) g;
--Testcase 294:
explain (costs off)
  select count(*) from tenk1 a, lateral generate_series(1,two) g;
--Testcase 295:
explain (costs off)
  select count(*) from tenk1 a cross join lateral generate_series(1,two) g;
-- don't need the explicit LATERAL keyword for functions
--Testcase 296:
explain (costs off)
  select count(*) from tenk1 a, generate_series(1,two) g;

-- lateral with UNION ALL subselect
--Testcase 297:
explain (costs off)
  select * from generate_series(100,200) g,
    lateral (select * from int8_tbl a where g = q1 union all
             select * from int8_tbl b where g = q2) ss;
--Testcase 298:
select g, q1, q2 from generate_series(100,200) g,
  lateral (select * from int8_tbl a where g = q1 union all
           select * from int8_tbl b where g = q2) ss;

-- lateral with VALUES
--Testcase 299:
explain (costs off)
  select count(*) from tenk1 a,
    tenk1 b join lateral (values(a.unique1)) ss(x) on b.unique2 = ss.x;
--Testcase 300:
select count(*) from tenk1 a,
  tenk1 b join lateral (values(a.unique1)) ss(x) on b.unique2 = ss.x;

-- lateral with VALUES, no flattening possible
--Testcase 301:
explain (costs off)
  select count(*) from tenk1 a,
    tenk1 b join lateral (values(a.unique1),(-1)) ss(x) on b.unique2 = ss.x;
--Testcase 302:
select count(*) from tenk1 a,
  tenk1 b join lateral (values(a.unique1),(-1)) ss(x) on b.unique2 = ss.x;

-- lateral injecting a strange outer join condition
--Testcase 303:
explain (costs off)
  select a.q1, a.q2, x.q1, x.q2, ss.z from int8_tbl a,
    int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
      on x.q2 = ss.z
  order by a.q1, a.q2, x.q1, x.q2, ss.z;
--Testcase 304:
select a.q1, a.q2, x.q1, x.q2, ss.z from int8_tbl a,
  int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
    on x.q2 = ss.z
  order by a.q1, a.q2, x.q1, x.q2, ss.z;

-- lateral reference to a join alias variable
--Testcase 305:
select x, f1, y from (select f1/2 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1,
  lateral (select x) ss2(y);
--Testcase 306:
select x, f1, y from (select f1 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1,
  lateral (values(x)) ss2(y);
--Testcase 307:
select x, f1, y from ((select f1/2 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1) j,
  lateral (select x) ss2(y);

-- lateral references requiring pullup
--Testcase 308:
select * from (values(1)) x(lb),
  lateral generate_series(lb,4) x4;
--Testcase 309:
select * from (select f1/1000000000 from int4_tbl) x(lb),
  lateral generate_series(lb,4) x4;
--Testcase 310:
select * from (values(1)) x(lb),
  lateral (values(lb)) y(lbcopy);
--Testcase 311:
select * from (values(1)) x(lb),
  lateral (select lb from int4_tbl) y(lbcopy);
--Testcase 312:
select x.q1, x.q2, y.q1, y.q2, xq1, yq1, yq2 from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (values(x.q1,y.q1,y.q2)) v(xq1,yq1,yq2);
--Testcase 313:
select x.q1, x.q2, y.q1, y.q2, xq1, yq1, yq2 from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);
--Testcase 314:
select x.q1, x.q2 from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);
--Testcase 315:
select v.* from
  (int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy);
--Testcase 316:
select v.* from
  (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy);
--Testcase 317:
select v.* from
  (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 from onerow union all select x.q2,y.q2 from onerow) v(vx,vy);

--Testcase 318:
explain (verbose, costs off)
select a.q1, a.q2, ss.q1, ss.q2, x from
  int8_tbl a left join
  lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;
--Testcase 319:
select a.q1, a.q2, ss.q1, ss.q2, x from
  int8_tbl a left join
  lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;
--Testcase 320:
explain (verbose, costs off)
select a.q1, a.q2, ss.q1, ss.q2, x from
  int8_tbl a left join
  lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;
--Testcase 321:
select a.q1, a.q2, ss.q1, ss.q2, x from
  int8_tbl a left join
  lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;

-- lateral can result in join conditions appearing below their
-- real semantic level
--Testcase 322:
explain (verbose, costs off)
select i.f1, k.f1 from int4_tbl i left join
  lateral (select * from int2_tbl j where i.f1 = j.f1) k on true;
--Testcase 323:
select i.f1, k.f1 from int4_tbl i left join
  lateral (select * from int2_tbl j where i.f1 = j.f1) k on true;
--Testcase 324:
explain (verbose, costs off)
select f1, coalesce from (SELECT f1 FROM int4_tbl) i left join
  lateral (select coalesce(i) from (SELECT f1 FROM int2_tbl) j where i.f1 = j.f1) k on true;
--Testcase 325:
select f1, coalesce from (SELECT f1 FROM int4_tbl) i left join
  lateral (select coalesce(i) from (SELECT f1 FROM int2_tbl) j where i.f1 = j.f1) k on true;
--Testcase 326:
explain (verbose, costs off)
select a.f1, ss.f1, q1, q2 from int4_tbl a,
  lateral (
    select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
  ) ss;
--Testcase 327:
select a.f1, ss.f1, q1, q2 from int4_tbl a,
  lateral (
    select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
  ) ss;

-- lateral reference in a PlaceHolderVar evaluated at join level
--Testcase 328:
explain (verbose, costs off)
select q1, q2, bq1, cq1, least from
  int8_tbl a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   int8_tbl b cross join int8_tbl c) ss
  on a.q2 = ss.bq1;
--Testcase 329:
select q1, q2, bq1, cq1, least from
  int8_tbl a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   int8_tbl b cross join int8_tbl c) ss
  on a.q2 = ss.bq1;

-- case requiring nested PlaceHolderVars
--Testcase 330:
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
--Testcase 331:
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
--Testcase 332:
explain (verbose, costs off)
select * from
  (select 1 as x offset 0) x cross join (select 2 as y offset 0) y
  left join lateral (
    select * from (select 3 as z offset 0) z where z.z = x.x
  ) zz on zz.z = y.y;

-- check dummy rels with lateral references (bug #15694)
--Testcase 333:
explain (verbose, costs off)
select * from int8_tbl i8 left join lateral
  (select *, i8.q2 from int4_tbl where false) ss on true;
--Testcase 334:
explain (verbose, costs off)
select * from int8_tbl i8 left join lateral
  (select *, i8.q2 from int4_tbl i1, int4_tbl i2 where false) ss on true;

-- check handling of nested appendrels inside LATERAL
--Testcase 335:
select * from
  ((select 2 as v) union all (select 3 as v)) as q1
  cross join lateral
  ((select * from
      ((select 4 as v) union all (select 5 as v)) as q3)
   union all
   (select q1.v)
  ) as q2;

-- check we don't try to do a unique-ified semijoin with LATERAL
--Testcase 336:
explain (verbose, costs off)
select * from
  (values (0,9998), (1,1000)) v(id,x),
  lateral (select f1 from int4_tbl
           where f1 = any (select unique1 from tenk1
                           where unique2 = v.x offset 0)) ss;
--Testcase 337:
select * from
  (values (0,9998), (1,1000)) v(id,x),
  lateral (select f1 from int4_tbl
           where f1 = any (select unique1 from tenk1
                           where unique2 = v.x offset 0)) ss;

-- check proper extParam/allParam handling (this isn't exactly a LATERAL issue,
-- but we can make the test case much more compact with LATERAL)
--Testcase 338:
explain (verbose, costs off)
select * from (values (0), (1)) v(id),
lateral (select * from int8_tbl t1,
         lateral (select * from
                    (select * from int8_tbl t2
                     where q1 = any (select q2 from int8_tbl t3
                                     where q2 = (select greatest(t1.q1,t2.q2))
                                       and (select v.id=0)) offset 0) ss2) ss
         where t1.q1 = ss.q2) ss0;

--Testcase 339:
select * from (values (0), (1)) v(id),
lateral (select * from int8_tbl t1,
         lateral (select * from
                    (select * from int8_tbl t2
                     where q1 = any (select q2 from int8_tbl t3
                                     where q2 = (select greatest(t1.q1,t2.q2))
                                       and (select v.id=0)) offset 0) ss2) ss
         where t1.q1 = ss.q2) ss0;

-- test some error cases where LATERAL should have been used but wasn't
--Testcase 340:
select f1,g from int4_tbl a, (select f1 as g) ss;
--Testcase 341:
select f1,g from int4_tbl a, (select a.f1 as g) ss;
--Testcase 342:
select f1,g from int4_tbl a cross join (select f1 as g) ss;
--Testcase 343:
select f1,g from int4_tbl a cross join (select a.f1 as g) ss;
-- SQL:2008 says the left table is in scope but illegal to access here
--Testcase 344:
select f1,g from int4_tbl a right join lateral generate_series(0, a.f1) g on true;
--Testcase 345:
select f1,g from int4_tbl a full join lateral generate_series(0, a.f1) g on true;
-- check we complain about ambiguous table references
--Testcase 346:
select * from
  int8_tbl x cross join (int4_tbl x cross join lateral (select x.f1) ss);
-- LATERAL can be used to put an aggregate into the FROM clause of its query
--Testcase 347:
select 1 from tenk1 a, lateral (select max(a.unique1) from int4_tbl b) ss;

-- check behavior of LATERAL in UPDATE/DELETE

--Testcase 534:
create temp table xx1 as select f1 as x1, -f1 as x2 from int4_tbl;

-- error, can't do this:
--Testcase 348:
update xx1 set x2 = f1 from (select * from int4_tbl where f1 = x1) ss;
--Testcase 349:
update xx1 set x2 = f1 from (select * from int4_tbl where f1 = xx1.x1) ss;
-- can't do it even with LATERAL:
--Testcase 350:
update xx1 set x2 = f1 from lateral (select * from int4_tbl where f1 = x1) ss;
-- we might in future allow something like this, but for now it's an error:
--Testcase 351:
update xx1 set x2 = f1 from xx1, lateral (select * from int4_tbl where f1 = x1) ss;

-- also errors:
--Testcase 352:
delete from xx1 using (select * from int4_tbl where f1 = x1) ss;
--Testcase 353:
delete from xx1 using (select * from int4_tbl where f1 = xx1.x1) ss;
--Testcase 354:
delete from xx1 using lateral (select * from int4_tbl where f1 = x1) ss;

--
-- test LATERAL reference propagation down a multi-level inheritance hierarchy
-- produced for a multi-level partitioned table hierarchy.
--
--Testcase 535:
create table join_pt1 (a int, b int, c text) partition by range(a);
--Testcase 536:
create table join_pt1p1 partition of join_pt1 for values from (0) to (100) partition by range(b);
--Testcase 537:
create foreign table join_pt1p2 partition of join_pt1 for values from (100) to (200) server griddb_svr;
--Testcase 538:
create foreign table join_pt1p1p1 partition of join_pt1p1 for values from (0) to (100) server griddb_svr;
--Testcase 355:
insert into join_pt1 values (1, 1, 'x'), (101, 101, 'y');
--Testcase 539:
create foreign table join_ut1 (a int, b int, c text) server griddb_svr;
--Testcase 356:
insert into join_ut1 values (101, 101, 'y'), (2, 2, 'z');
--Testcase 357:
explain (verbose, costs off)
select t1.b, ss.phv from join_ut1 t1 left join lateral
              (select t2.a as t2a, t3.a t3a, least(t1.a, t2.a, t3.a) phv
					  from join_pt1 t2 join join_ut1 t3 on t2.a = t3.b) ss
              on t1.a = ss.t2a order by t1.a;
--Testcase 358:
select t1.b, ss.phv from join_ut1 t1 left join lateral
              (select t2.a as t2a, t3.a t3a, least(t1.a, t2.a, t3.a) phv
					  from join_pt1 t2 join join_ut1 t3 on t2.a = t3.b) ss
              on t1.a = ss.t2a order by t1.a;

--Testcase 540:
drop table t2a;
--Testcase 541:
drop table join_pt1;
--Testcase 542:
drop foreign table join_ut1;
--
-- test that foreign key join estimation performs sanely for outer joins
--

begin;

--Testcase 543:
create foreign table fkest (id serial options (rowkey 'true'), a int, b int, c int) server griddb_svr;
--Testcase 544:
create foreign table fkest1 (id serial options (rowkey 'true'), a int, b int) server griddb_svr;

--Testcase 359:
insert into fkest(a,b,c) select x/10, x%10, x from generate_series(1,1000) x;
--Testcase 360:
insert into fkest1(a,b) select x/10, x%10 from generate_series(1,1000) x;

--Testcase 361:
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

--Testcase 545:
create foreign table j11 (idx serial options (rowkey 'true'), id int) server griddb_svr;
--Testcase 546:
create foreign table j21 (idx serial options (rowkey 'true'), id int) server griddb_svr;
--Testcase 547:
create foreign table j31 (idx serial options (rowkey 'true'), id int) server griddb_svr;

--Testcase 362:
insert into j11(id) values(1),(2),(3);
--Testcase 363:
insert into j21(id) values(1),(2),(3);
--Testcase 364:
insert into j31(id) values(1),(1);

-- ensure join is properly marked as unique
--Testcase 365:
explain (verbose, costs off)
select * from j11 inner join j21 on j11.id = j21.id;

-- ensure join is not unique when not an equi-join
--Testcase 366:
explain (verbose, costs off)
select * from j11 inner join j21 on j11.id > j21.id;

-- ensure non-unique rel is not chosen as inner
--Testcase 367:
explain (verbose, costs off)
select * from j11 inner join j31 on j11.id = j31.id;

-- ensure left join is marked as unique
--Testcase 368:
explain (verbose, costs off)
select * from j11 left join j21 on j11.id = j21.id;

-- ensure right join is marked as unique
--Testcase 369:
explain (verbose, costs off)
select * from j11 right join j21 on j11.id = j21.id;

-- ensure full join is marked as unique
--Testcase 370:
explain (verbose, costs off)
select * from j11 full join j21 on j11.id = j21.id;

-- a clauseless (cross) join can't be unique
--Testcase 371:
explain (verbose, costs off)
select * from j11 cross join j21;

-- ensure a natural join is marked as unique
--Testcase 372:
explain (verbose, costs off)
select * from j11 natural join j21;

-- ensure a distinct clause allows the inner to become unique
--Testcase 373:
explain (verbose, costs off)
select * from j11
inner join (select distinct id from j31) j31 on j11.id = j31.id;

-- ensure group by clause allows the inner to become unique
--Testcase 374:
explain (verbose, costs off)
select * from j11
inner join (select id from j31 group by id) j31 on j11.id = j31.id;

-- test more complex permutations of unique joins

--Testcase 548:
create foreign table j12 (idx serial options (rowkey 'true'), id1 int, id2 int) server griddb_svr;
--Testcase 549:
create foreign table j22 (idx serial options (rowkey 'true'), id1 int, id2 int) server griddb_svr;
--Testcase 550:
create foreign table j32 (idx serial options (rowkey 'true'), id1 int, id2 int) server griddb_svr;

--Testcase 375:
insert into j12(id1,id2) values(1,1),(1,2);
--Testcase 376:
insert into j22(id1,id2) values(1,1);
--Testcase 377:
insert into j32(id1,id2) values(1,1);

-- ensure there's no unique join when not all columns which are part of the
-- unique index are seen in the join clause
--Testcase 378:
explain (verbose, costs off)
select * from j12
inner join j22 on j12.id1 = j22.id1;

-- ensure proper unique detection with multiple join quals
--Testcase 379:
explain (verbose, costs off)
select * from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2;

-- ensure we don't detect the join to be unique when quals are not part of the
-- join condition
--Testcase 380:
explain (verbose, costs off)
select * from j12
inner join j22 on j12.id1 = j22.id1 where j12.id2 = 1;

-- as above, but for left joins.
--Testcase 381:
explain (verbose, costs off)
select * from j12
left join j22 on j12.id1 = j22.id1 where j12.id2 = 1;

-- validate logic in merge joins which skips mark and restore.
-- it should only do this if all quals which were used to detect the unique
-- are present as join quals, and not plain quals.
--Testcase 592:
set enable_nestloop to 0;
--Testcase 593:
set enable_hashjoin to 0;
--Testcase 594:
set enable_sort to 0;

-- create indexes that will be preferred over the PKs to perform the join
--create index j1_id1_idx on j1 (id1) where id1 % 1000 = 1;
--create index j2_id1_idx on j2 (id1) where id1 % 1000 = 1;

-- need an additional row in j2, if we want j2_id1_idx to be preferred
--Testcase 551:
insert into j22(id1,id2) values(1,2);
--analyze j2;

--Testcase 382:
explain (costs off) select j12.id1, j12.id2, j22.id1, j22.id2 from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1;

--Testcase 383:
select j12.id1, j12.id2, j22.id1, j22.id2 from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1;

-- Exercise array keys mark/restore B-Tree code
--Testcase 552:
explain (costs off) select j12.id1, j12.id2, j22.id1, j22.id2 from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1 and j22.id1 = any (array[1]);

--Testcase 553:
select j12.id1, j12.id2, j22.id1, j22.id2 from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1 and j22.id1 = any (array[1]);

-- Exercise array keys "find extreme element" B-Tree code
--Testcase 554:
explain (costs off) select j12.id1, j12.id2, j22.id1, j22.id2 from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1 and j22.id1 >= any (array[1,5]);

--Testcase 555:
select j12.id1, j12.id2, j22.id1, j22.id2 from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1 and j22.id1 >= any (array[1,5]);

--Testcase 595:
reset enable_nestloop;
--Testcase 596:
reset enable_hashjoin;
--Testcase 597:
reset enable_sort;

--Testcase 556:
drop foreign table j12;
--Testcase 557:
drop foreign table j22;
--Testcase 558:
drop foreign table j32;

-- check that semijoin inner is not seen as unique for a portion of the outerrel
--Testcase 559:
CREATE FOREIGN TABLE onek (
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
  stringu1  name,
  stringu2  name,
  string4   name
) SERVER griddb_svr;

-- check that semijoin inner is not seen as unique for a portion of the outerrel
--Testcase 384:
explain (verbose, costs off)
select t1.unique1, t2.hundred
from onek t1, tenk1 t2
where exists (select 1 from tenk1 t3
              where t3.thousand = t1.unique1 and t3.tenthous = t2.hundred)
      and t1.unique1 < 1;

-- ... unless it actually is unique
--Testcase 560:
create table j3 as select unique1, tenthous from onek;
vacuum analyze j3;
--Testcase 561:
create unique index on j3(unique1, tenthous);

--Testcase 385:
explain (verbose, costs off)
select t1.unique1, t2.hundred
from onek t1, tenk1 t2
where exists (select 1 from j3
              where j3.unique1 = t1.unique1 and j3.tenthous = t2.hundred)
      and t1.unique1 < 1;

--Testcase 562:
drop table j3;

--
-- exercises for the hash join code
--

begin;

--Testcase 598:
set local min_parallel_table_scan_size = 0;
--Testcase 599:
set local parallel_setup_cost = 0;

--Testcase 600:
set enable_mergejoin to 0;
-- Extract bucket and batch counts from an explain analyze plan.  In
-- general we can't make assertions about how many batches (or
-- buckets) will be required because it can vary, but we can in some
-- special cases and we can check for growth.
--Testcase 563:
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
--Testcase 564:
create or replace function hash_join_batches(query text)
returns table (original int, final int) language plpgsql
as
$$
declare
  whole_plan json;
  hash_node json;
begin
  for whole_plan in
--Testcase 565:
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
--Testcase 566:
create foreign table simple (id int options (rowkey 'true'), t text) server griddb_svr;
--Testcase 386:
insert into simple select generate_series(1, 20000) AS id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

-- Make a relation whose size we will under-estimate.  We want stats
-- to say 1000 rows, but actually there are 20,000 rows.
--Testcase 567:
create foreign table bigger_than_it_looks (id int options (rowkey 'true'), t text) server griddb_svr;
--Testcase 387:
insert into bigger_than_it_looks select generate_series(1, 20000) as id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
--Testcase 388:
update pg_class set reltuples = 1000 where relname = 'bigger_than_it_looks';

-- Make a relation whose size we underestimate and that also has a
-- kind of skew that breaks our batching scheme.  We want stats to say
-- 2 rows, but actually there are 20,000 rows with the same key.
--Testcase 568:
create foreign table extremely_skewed (idx int options (rowkey 'true'), id int, t text) server griddb_svr;
--Testcase 389:
insert into extremely_skewed
  select idx, 42 as id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
  from generate_series(1, 20000) idx;
--Testcase 390:
update pg_class
  set reltuples = 2, relpages = pg_relation_size('extremely_skewed') / 8192
  where relname = 'extremely_skewed';

-- Make a relation with a couple of enormous tuples.
-- System limiting values: with GridDB, when Block size = 64KB, String data size = 31KB
--Testcase 569:
create foreign table wide (id int options (rowkey 'true'), t text) server griddb_svr;
--Testcase 391:
insert into wide select generate_series(1, 2) as id, rpad('', 31744, 'x') as t;

-- The "optimal" case: the hash table fits in memory; we plan for 1
-- batch, we stick to that number, and peak memory usage stays within
-- our work_mem budget

-- non-parallel
--Testcase 601:
set local max_parallel_workers_per_gather = 0;
--Testcase 602:
set local work_mem = '4MB';
--Testcase 392:
explain (costs off)
  select count(*) from simple r join simple s using (id);
--Testcase 393:
select count(*) from simple r join simple s using (id);
--Testcase 394:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join simple s using (id);
$$);

-- parallel with parallel-oblivious hash join
--Testcase 603:
set local max_parallel_workers_per_gather = 2;
--Testcase 604:
set local work_mem = '4MB';
--Testcase 605:
set local enable_parallel_hash = off;
--Testcase 395:
explain (costs off)
  select count(*) from simple r join simple s using (id);
--Testcase 396:
select count(*) from simple r join simple s using (id);
--Testcase 397:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join simple s using (id);
$$);

-- parallel with parallel-aware hash join
--Testcase 606:
set local max_parallel_workers_per_gather = 2;
--Testcase 607:
set local work_mem = '4MB';
--Testcase 608:
set local enable_parallel_hash = on;
--Testcase 398:
explain (costs off)
  select count(*) from simple r join simple s using (id);
--Testcase 399:
select count(*) from simple r join simple s using (id);
--Testcase 400:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join simple s using (id);
$$);

-- The "good" case: batches required, but we plan the right number; we
-- plan for some number of batches, and we stick to that number, and
-- peak memory usage says within our work_mem budget

-- non-parallel
--Testcase 609:
set local max_parallel_workers_per_gather = 0;
--Testcase 610:
set local work_mem = '128kB';
--Testcase 401:
explain (costs off)
  select count(*) from simple r join simple s using (id);
--Testcase 402:
select count(*) from simple r join simple s using (id);
--Testcase 403:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join simple s using (id);
$$);

-- parallel with parallel-oblivious hash join
--Testcase 611:
set local max_parallel_workers_per_gather = 2;
--Testcase 612:
set local work_mem = '128kB';
--Testcase 613:
set local enable_parallel_hash = off;
--Testcase 404:
explain (costs off)
  select count(*) from simple r join simple s using (id);
--Testcase 405:
select count(*) from simple r join simple s using (id);
--Testcase 406:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join simple s using (id);
$$);

-- parallel with parallel-aware hash join
--Testcase 614:
set local max_parallel_workers_per_gather = 2;
--Testcase 615:
set local work_mem = '192kB';
--Testcase 616:
set local enable_parallel_hash = on;
--Testcase 407:
explain (costs off)
  select count(*) from simple r join simple s using (id);
--Testcase 408:
select count(*) from simple r join simple s using (id);
--Testcase 409:
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
--Testcase 617:
set local max_parallel_workers_per_gather = 0;
--Testcase 618:
set local work_mem = '128kB';
--Testcase 410:
explain (costs off)
  select count(*) FROM simple r JOIN bigger_than_it_looks s USING (id);
--Testcase 411:
select count(*) FROM simple r JOIN bigger_than_it_looks s USING (id);
--Testcase 412:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) FROM simple r JOIN bigger_than_it_looks s USING (id);
$$);

-- parallel with parallel-oblivious hash join
--Testcase 619:
set local max_parallel_workers_per_gather = 2;
--Testcase 620:
set local work_mem = '128kB';
--Testcase 621:
set local enable_parallel_hash = off;
--Testcase 413:
explain (costs off)
  select count(*) from simple r join bigger_than_it_looks s using (id);
--Testcase 414:
select count(*) from simple r join bigger_than_it_looks s using (id);
--Testcase 415:
select original > 1 as initially_multibatch, final > original as increased_batches
  from hash_join_batches(
$$
  select count(*) from simple r join bigger_than_it_looks s using (id);
$$);

-- parallel with parallel-aware hash join
--Testcase 622:
set local max_parallel_workers_per_gather = 1;
--Testcase 623:
set local work_mem = '192kB';
--Testcase 624:
set local enable_parallel_hash = on;
--Testcase 416:
explain (costs off)
  select count(*) from simple r join bigger_than_it_looks s using (id);
--Testcase 417:
select count(*) from simple r join bigger_than_it_looks s using (id);
--Testcase 418:
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
--Testcase 625:
set local max_parallel_workers_per_gather = 0;
--Testcase 626:
set local work_mem = '128kB';
--Testcase 419:
explain (costs off)
  select count(*) from simple r join extremely_skewed s using (id);
--Testcase 420:
select count(*) from simple r join extremely_skewed s using (id);
--Testcase 421:
select * from hash_join_batches(
$$
  select count(*) from simple r join extremely_skewed s using (id);
$$);

-- parallel with parallel-oblivious hash join
--Testcase 627:
set local max_parallel_workers_per_gather = 2;
--Testcase 628:
set local work_mem = '128kB';
--Testcase 629:
set local enable_parallel_hash = off;
--Testcase 422:
explain (costs off)
  select count(*) from simple r join extremely_skewed s using (id);
--Testcase 423:
select count(*) from simple r join extremely_skewed s using (id);
--Testcase 424:
select * from hash_join_batches(
$$
  select count(*) from simple r join extremely_skewed s using (id);
$$);

-- parallel with parallel-aware hash join
--Testcase 630:
set local max_parallel_workers_per_gather = 1;
--Testcase 631:
set local work_mem = '128kB';
--Testcase 632:
set local enable_parallel_hash = on;
--Testcase 425:
explain (costs off)
  select count(*) from simple r join extremely_skewed s using (id);
--Testcase 426:
select count(*) from simple r join extremely_skewed s using (id);
--Testcase 427:
select * from hash_join_batches(
$$
  select count(*) from simple r join extremely_skewed s using (id);
$$);

-- A couple of other hash join tests unrelated to work_mem management.

-- Check that EXPLAIN ANALYZE has data even if the leader doesn't participate
--Testcase 633:
set local max_parallel_workers_per_gather = 2;
--Testcase 634:
set local work_mem = '4MB';
--Testcase 635:
set local parallel_leader_participation = off;
--Testcase 428:
select * from hash_join_batches(
$$
  select count(*) from simple r join simple s using (id);
$$);

-- Exercise rescans.  We'll turn off parallel_leader_participation so
-- that we can check that instrumentation comes back correctly.

--Testcase 570:
create foreign table join_foo (id int options (rowkey 'true'), t text) server griddb_svr;
--Testcase 429:
insert into join_foo select generate_series(1, 3) as id, 'xxxxx'::text as t;

--Testcase 571:
create foreign table join_bar (id int options (rowkey 'true'), t text) server griddb_svr;
--Testcase 430:
insert into join_bar select generate_series(1, 10000) as id, 'xxxxx'::text as t;

-- multi-batch with rescan, parallel-oblivious
--Testcase 636:
set enable_parallel_hash = off;
--Testcase 637:
set parallel_leader_participation = off;
--Testcase 638:
set min_parallel_table_scan_size = 0;
--Testcase 639:
set parallel_setup_cost = 0;
--Testcase 640:
set parallel_tuple_cost = 0;
--Testcase 641:
set max_parallel_workers_per_gather = 2;
--Testcase 642:
set enable_material = off;
--Testcase 643:
set enable_mergejoin = off;
--Testcase 644:
set work_mem = '64kB';
--Testcase 431:
explain (costs off)
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
--Testcase 432:
select count(*) from join_foo
  left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
  on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
--Testcase 433:
select final > 1 as multibatch
  from hash_join_batches(
$$
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
$$);

-- single-batch with rescan, parallel-oblivious
--Testcase 645:
set enable_parallel_hash = off;
--Testcase 646:
set parallel_leader_participation = off;
--Testcase 647:
set min_parallel_table_scan_size = 0;
--Testcase 648:
set parallel_setup_cost = 0;
--Testcase 649:
set parallel_tuple_cost = 0;
--Testcase 650:
set max_parallel_workers_per_gather = 2;
--Testcase 651:
set enable_material = off;
--Testcase 652:
set enable_mergejoin = off;
--Testcase 653:
set work_mem = '4MB';
--Testcase 434:
explain (costs off)
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
--Testcase 435:
select count(*) from join_foo
  left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
  on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
--Testcase 436:
select final > 1 as multibatch
  from hash_join_batches(
$$
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
$$);

-- multi-batch with rescan, parallel-aware
--Testcase 654:
set enable_parallel_hash = on;
--Testcase 655:
set parallel_leader_participation = off;
--Testcase 656:
set min_parallel_table_scan_size = 0;
--Testcase 657:
set parallel_setup_cost = 0;
--Testcase 658:
set parallel_tuple_cost = 0;
--Testcase 659:
set max_parallel_workers_per_gather = 2;
--Testcase 660:
set enable_material = off;
--Testcase 661:
set enable_mergejoin = off;
--Testcase 662:
set work_mem = '64kB';
--Testcase 437:
explain (costs off)
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
--Testcase 438:
select count(*) from join_foo
  left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
  on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
--Testcase 439:
select final > 1 as multibatch
  from hash_join_batches(
$$
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
$$);

-- single-batch with rescan, parallel-aware
--Testcase 663:
set enable_parallel_hash = on;
--Testcase 664:
set parallel_leader_participation = off;
--Testcase 665:
set min_parallel_table_scan_size = 0;
--Testcase 666:
set parallel_setup_cost = 0;
--Testcase 667:
set parallel_tuple_cost = 0;
--Testcase 668:
set max_parallel_workers_per_gather = 2;
--Testcase 669:
set enable_material = off;
--Testcase 670:
set enable_mergejoin = off;
--Testcase 671:
set work_mem = '4MB';
--Testcase 440:
explain (costs off)
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
--Testcase 441:
select count(*) from join_foo
  left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
  on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
--Testcase 442:
select final > 1 as multibatch
  from hash_join_batches(
$$
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
$$);

-- A full outer join where every record is matched.

-- non-parallel
--Testcase 672:
set local max_parallel_workers_per_gather = 0;
--Testcase 443:
explain (costs off)
     select  count(*) from simple r full outer join simple s using (id);
--Testcase 444:
select  count(*) from simple r full outer join simple s using (id);

-- parallelism not possible with parallel-oblivious outer hash join
--Testcase 673:
set local max_parallel_workers_per_gather = 2;
--Testcase 445:
explain (costs off)
     select  count(*) from simple r full outer join simple s using (id);
--Testcase 446:
select  count(*) from simple r full outer join simple s using (id);

-- An full outer join where every record is not matched.

-- non-parallel
--Testcase 674:
set local max_parallel_workers_per_gather = 0;
--Testcase 447:
explain (costs off)
     select  count(*) from simple r full outer join simple s on (r.id = 0 - s.id);
--Testcase 448:
select  count(*) from simple r full outer join simple s on (r.id = 0 - s.id);

-- parallelism not possible with parallel-oblivious outer hash join
--Testcase 675:
set local max_parallel_workers_per_gather = 2;
--Testcase 449:
explain (costs off)
     select  count(*) from simple r full outer join simple s on (r.id = 0 - s.id);
--Testcase 450:
select  count(*) from simple r full outer join simple s on (r.id = 0 - s.id);

-- exercise special code paths for huge tuples (note use of non-strict
-- expression and left join required to get the detoasted tuple into
-- the hash table)

-- parallel with parallel-aware hash join (hits ExecParallelHashLoadTuple and
-- sts_puttuple oversized tuple cases because it's multi-batch)
--Testcase 676:
set max_parallel_workers_per_gather = 2;
--Testcase 677:
set enable_parallel_hash = on;
--Testcase 678:
set work_mem = '128kB';
--Testcase 451:
explain (costs off)
  select length(max(s.t))
  from wide left join (select id, coalesce(t, '') || '' as t from wide) s using (id);
--Testcase 452:
select length(max(s.t))
from wide left join (select id, coalesce(t, '') || '' as t from wide) s using (id);
--Testcase 453:
select final > 1 as multibatch
  from hash_join_batches(
$$
  select length(max(s.t))
  from wide left join (select id, coalesce(t, '') || '' as t from wide) s using (id);
$$);

--Testcase 679:
reset enable_mergejoin;

rollback;

--Testcase 683:
DELETE FROM INT4_TBL;
--Testcase 684:
DELETE FROM FLOAT8_TBL;
--Testcase 685:
DELETE FROM INT8_TBL;

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
--Testcase 572:
DROP USER MAPPING FOR public SERVER griddb_svr;
--Testcase 573:
DROP SERVER griddb_svr;
--Testcase 574:
DROP EXTENSION griddb_fdw CASCADE;
