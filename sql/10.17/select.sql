--
-- SELECT
--
\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 68:
CREATE EXTENSION griddb_fdw;
--Testcase 69:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');
--Testcase 70:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

--Testcase 71:
CREATE FOREIGN TABLE onek (
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

--Testcase 72:
CREATE FOREIGN TABLE onek2 (
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

--Testcase 73:
CREATE FOREIGN TABLE INT8_TBL(id int4, q1 int8, q2 int8) SERVER griddb_svr;

--Testcase 74:
CREATE FOREIGN TABLE person (
  name    text,
  age     int4,
  location  text
) SERVER griddb_svr;

-- btree index
-- awk '{if($1<10){print;}else{next;}}' onek.data | sort +0n -1
--
--Testcase 1:
SELECT * FROM onek
   WHERE onek.unique1 < 10
   ORDER BY onek.unique1;

--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
--Testcase 2:
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >;

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
--Testcase 3:
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY stringu1 using <;

--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1d -2 +0nr -1
--
--Testcase 4:
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using <, unique1 using >;

--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1dr -2 +0n -1
--
--Testcase 5:
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using >, unique1 using <;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0nr -1 +1d -2
--
--Testcase 6:
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >, string4 using <;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0n -1 +1dr -2
--
--Testcase 7:
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using <, string4 using >;

--
-- test partial btree indexes
--
-- As of 7.2, planner probably won't pick an indexscan without stats,
-- so ANALYZE first.  Also, we want to prevent it from picking a bitmapscan
-- followed by sort, because that could hide index ordering problems.
--
--ANALYZE onek2;

--Testcase 90:
SET enable_seqscan TO off;
--Testcase 91:
SET enable_bitmapscan TO off;
--Testcase 92:
SET enable_sort TO off;

--
-- awk '{if($1<10){print $0;}else{next;}}' onek.data | sort +0n -1
--
--Testcase 8:
SELECT onek2.* FROM onek2 WHERE onek2.unique1 < 10;

--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
--Testcase 9:
SELECT onek2.unique1, onek2.stringu1 FROM onek2
    WHERE onek2.unique1 < 20
    ORDER BY unique1 using >;

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
--Testcase 10:
SELECT onek2.unique1, onek2.stringu1 FROM onek2
   WHERE onek2.unique1 > 980;

--Testcase 93:
RESET enable_seqscan;
--Testcase 94:
RESET enable_bitmapscan;
--Testcase 95:
RESET enable_sort;


--Testcase 11:
SELECT two, stringu1, ten, string4
   INTO TABLE tmp
   FROM onek;

--
-- awk '{print $1,$2;}' person.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - emp.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=2){print $4,$5;}else{print;}}' - stud_emp.data
--
-- SELECT name, age FROM person*; ??? check if different
--Testcase 12:
SELECT p.name, p.age FROM person* p;

--
-- awk '{print $1,$2;}' person.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - emp.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=1){print $4,$5;}else{print;}}' - stud_emp.data |
-- sort +1nr -2
--
--Testcase 13:
SELECT p.name, p.age FROM person* p ORDER BY age using >, name;

--
-- Test some cases involving whole-row Var referencing a subquery
--
--Testcase 75:
create foreign table bar (id serial OPTIONS (rowkey 'true'), a text, b int, c int) server griddb_svr;
--Testcase 14:
insert into bar(a, b, c) values ('xyzzy',1,null);
--Testcase 15:
select foo from (select b from bar offset 0) as foo;
--Testcase 16:
select foo from (select c from bar offset 0) as foo;
--Testcase 17:
select foo from (select a, b, c from bar offset 0) as foo;

--
-- Test VALUES lists
--
--Testcase 18:
select * from onek, (values(147, 'RFAAAA'), (931, 'VJAAAA')) as v (i, j)
    WHERE onek.unique1 = v.i and onek.stringu1 = v.j;

-- a more complex case
-- looks like we're coding lisp :-)
--Testcase 19:
select * from onek,
  (values ((select i from
    (values(10000), (2), (389), (1000), (2000), ((select 10029))) as foo(i)
    order by i asc limit 1))) bar (i)
  where onek.unique1 = bar.i;

-- try VALUES in a subquery
--Testcase 20:
select * from onek
    where (unique1,ten) in (values (1,1), (20,0), (99,9), (17,99))
    order by unique1;

-- VALUES is also legal as a standalone query or a set-operation member
--Testcase 21:
VALUES (1,2), (3,4+4), (7,77.7);

--Testcase 22:
VALUES (1,2), (3,4+4), (7,77.7)
UNION ALL
SELECT 2+2, 57
UNION ALL
SELECT q1, q2 FROM int8_tbl;

--
-- Test ORDER BY options
--

--Testcase 76:
CREATE FOREIGN TABLE foo (id serial OPTIONS(rowkey 'true'), f1 int) SERVER griddb_svr;

--Testcase 23:
INSERT INTO foo(f1) VALUES (42),(3),(10),(7),(null),(null),(1);

--Testcase 24:
SELECT f1 FROM foo ORDER BY f1;
--Testcase 25:
SELECT f1 FROM foo ORDER BY f1 ASC;	-- same thing
--Testcase 26:
SELECT f1 FROM foo ORDER BY f1 NULLS FIRST;
--Testcase 27:
SELECT f1 FROM foo ORDER BY f1 DESC;
--Testcase 28:
SELECT f1 FROM foo ORDER BY f1 DESC NULLS LAST;

-- skip, cannot create index on foreign table
-- check if indexscans do the right things
/*
CREATE INDEX fooi ON foo (f1);
SET enable_sort = false;

--Testcase 29:
SELECT * FROM foo ORDER BY f1;
--Testcase 30:
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
--Testcase 31:
SELECT * FROM foo ORDER BY f1 DESC;
--Testcase 32:
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

DROP INDEX fooi;
CREATE INDEX fooi ON foo (f1 DESC);

--Testcase 33:
SELECT * FROM foo ORDER BY f1;
--Testcase 34:
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
--Testcase 35:
SELECT * FROM foo ORDER BY f1 DESC;
--Testcase 36:
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

DROP INDEX fooi;
CREATE INDEX fooi ON foo (f1 DESC NULLS LAST);

--Testcase 37:
SELECT * FROM foo ORDER BY f1;
--Testcase 38:
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
--Testcase 39:
SELECT * FROM foo ORDER BY f1 DESC;
--Testcase 40:
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;
*/

--
-- Test planning of some cases with partial indexes
--

-- partial index is usable
--Testcase 41:
explain (costs off)
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
--Testcase 42:
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
-- actually run the query with an analyze to use the partial index
--Testcase 77:
explain (costs off, analyze on, timing off, summary off)
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
--Testcase 44:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
--Testcase 45:
select unique2 from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
-- partial index predicate implies clause, so no need for retest
--Testcase 46:
explain (costs off)
select * from onek2 where unique2 = 11 and stringu1 < 'B';
--Testcase 47:
select * from onek2 where unique2 = 11 and stringu1 < 'B';
--Testcase 48:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
--Testcase 49:
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
-- but if it's an update target, must retest anyway
--Testcase 50:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B' for update;
--Testcase 51:
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B' for update;
-- partial index is not applicable
--Testcase 52:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'C';
--Testcase 53:
select unique2 from onek2 where unique2 = 11 and stringu1 < 'C';
-- partial index implies clause, but bitmap scan must recheck predicate anyway
--Testcase 96:
SET enable_indexscan TO off;
--Testcase 54:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
--Testcase 55:
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
--Testcase 97:
RESET enable_indexscan;
-- check multi-index cases too
--Testcase 56:
explain (costs off)
select unique1, unique2 from onek2
  where (unique2 = 11 or unique1 = 0) and stringu1 < 'B';
--Testcase 57:
select unique1, unique2 from onek2
  where (unique2 = 11 or unique1 = 0) and stringu1 < 'B';
--Testcase 58:
explain (costs off)
select unique1, unique2 from onek2
  where (unique2 = 11 and stringu1 < 'B') or unique1 = 0;
--Testcase 59:
select unique1, unique2 from onek2
  where (unique2 = 11 and stringu1 < 'B') or unique1 = 0;

--
-- Test some corner cases that have been known to confuse the planner
--

-- ORDER BY on a constant doesn't really need any sorting
--Testcase 60:
SELECT 1 AS x ORDER BY x;

-- But ORDER BY on a set-valued expression does
--Testcase 61:
delete from foo;
--Testcase 78:
create function sillysrf(int) returns setof int as
$$
declare
    returnrec int;
begin
--Testcase 79:
	insert into foo(f1) values (1),(10),(2), ($1);
  	for returnrec in select f1 from foo loop
            return next returnrec;
        end loop;
end
$$ language plpgsql;

begin;
--Testcase 62:
select sillysrf(42) ;
rollback;

begin;
--Testcase 63:
select sillysrf(-1) order by 1;
rollback;

--Testcase 80:
drop function sillysrf(int);

-- X = X isn't a no-op, it's effectively X IS NOT NULL assuming = is strict
-- (see bug #5084)
begin;
--Testcase 64:
delete from foo;
--Testcase 65:
insert into foo(f1) values (2),(null),(1);
--Testcase 66:
select f1 as k from foo where f1 = f1 order by f1;
--Testcase 67:
select f1 as k from foo where f1 = f1;
rollback;

-- skip, gridb does not support partition tabl
-- Test partitioned tables with no partitions, which should be handled the
-- same as the non-inheritance case when expanding its RTE.
--create table list_parted_tbl (a int,b int) partition by list (a);
--create table list_parted_tbl1 partition of list_parted_tbl
--  for values in (1) partition by list(b);
--explain (costs off) select * from list_parted_tbl;
--drop table list_parted_tbl;

--Testcase 81:
DROP FOREIGN TABLE onek;
--Testcase 82:
DROP FOREIGN TABLE onek2;
--Testcase 83:
DROP FOREIGN TABLE int8_tbl;
--Testcase 84:
DROP FOREIGN TABLE person;
--Testcase 85:
DROP FOREIGN TABLE foo;
--Testcase 86:
DROP FOREIGN TABLE bar;
--Testcase 87:
DROP USER MAPPING FOR public SERVER griddb_svr;
--Testcase 88:
DROP SERVER griddb_svr;
--Testcase 89:
DROP EXTENSION griddb_fdw CASCADE;
