--SET client_min_messages TO WARNING;
CREATE EXTENSION IF NOT EXISTS griddb_fdw;
DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER IF NOT EXISTS griddb_svr FOREIGN DATA WRAPPER griddb_fdw
            OPTIONS (host '239.0.0.1', port '31999', clustername 'griddbfdwTestCluster')$$;
        EXECUTE $$CREATE SERVER griddb_svr2 FOREIGN DATA WRAPPER griddb_fdw
            OPTIONS (host '239.0.0.1', port '31999', clustername 'griddbfdwTestCluster')$$;
        EXECUTE $$CREATE SERVER testserver1 FOREIGN DATA WRAPPER griddb_fdw$$;
    END;
$d$;
CREATE USER MAPPING IF NOT EXISTS FOR public SERVER griddb_svr OPTIONS (username 'admin', password 'testadmin');
CREATE USER MAPPING FOR public SERVER griddb_svr2 OPTIONS (username 'admin', password 'testadmin');
CREATE USER MAPPING FOR public SERVER testserver1 OPTIONS (username 'value', password 'value');

CREATE SCHEMA "S 1";
IMPORT FOREIGN SCHEMA griddb_schema LIMIT TO
	("T0", "T1", "T2", "T3", "T4", ft1, ft2, ft4, ft5, base_tbl,
	loc1, loc2, loct, loct1, loct2, loct3, loct4, locp1, locp2,
	fprt1_p1, fprt1_p2, fprt2_p1, fprt2_p2, pagg_tab_p1, pagg_tab_p2, pagg_tab_p3)
	FROM SERVER griddb_svr INTO "S 1";
--SET client_min_messages to NOTICE;

-- GridDB containers must be created for this test on GridDB server
INSERT INTO "S 1"."T1"
	SELECT id,
	       id % 10,
	       to_char(id, 'FM00000'),
	       '1970-01-01'::timestamptz + ((id % 100) || ' days')::interval,
	       '1970-01-01'::timestamp + ((id % 100) || ' days')::interval,
	       id % 10,
	       id % 10,
	       'foo'
	FROM generate_series(1, 1000) id;
INSERT INTO "S 1"."T2"
	SELECT id,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
INSERT INTO "S 1"."T3"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
DELETE FROM "S 1"."T3" WHERE c1 % 2 != 0;	-- delete for outer join tests
INSERT INTO "S 1"."T4"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
DELETE FROM "S 1"."T4" WHERE c1 % 3 != 0;	-- delete for outer join tests

-- ===================================================================
-- create foreign tables
-- ===================================================================
CREATE FOREIGN TABLE ft1 (
	-- c0 int,
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	c3 text,
	c4 timestamp,
	c5 timestamp,
	c6 text,
	c7 text default 'ft1',
	c8 text
) SERVER griddb_svr;
-- ALTER FOREIGN TABLE ft1 DROP COLUMN c0;

CREATE FOREIGN TABLE ft2 (
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	-- cx int,
	c3 text,
	c4 timestamp,
	c5 timestamp,
	c6 text,
	c7 text default 'ft2',
	c8 text
) SERVER griddb_svr;
-- ALTER FOREIGN TABLE ft2 DROP COLUMN cx;

CREATE FOREIGN TABLE ft4 (
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	c3 text
) SERVER griddb_svr OPTIONS (table_name 'T3');

CREATE FOREIGN TABLE ft5 (
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	c3 text
) SERVER griddb_svr OPTIONS (table_name 'T4');

CREATE FOREIGN TABLE ft6 (
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	c3 text
) SERVER griddb_svr2 OPTIONS (table_name 'T4');

-- ===================================================================
-- tests for validator
-- ===================================================================
-- requiressl, krbsrvname and gsslib are omitted because they depend on
-- configure options
ALTER SERVER testserver1 OPTIONS (
	updatable 'true',
	fdw_startup_cost '123.456',
	fdw_tuple_cost '0.123',
	host 'value',
	port 'value',
	clustername 'value'
);

ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (DROP username, DROP password);

ALTER FOREIGN TABLE ft1 OPTIONS (table_name 'T1');
ALTER FOREIGN TABLE ft2 OPTIONS (table_name 'T1');
\det+

-- ===================================================================
-- simple queries
-- ===================================================================
-- single table without alias
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
-- single table with alias - also test that tableoid sort is not pushed to remote side
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
-- whole-row reference
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- empty result
SELECT * FROM ft1 WHERE false;
-- with WHERE clause
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6::char = '1' AND t1.c7::char >= '1';
SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6::char = '1' AND t1.c7::char >= '1';
-- with FOR UPDATE/SHARE
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
-- aggregate
SELECT COUNT(*) FROM ft1 t1;
-- subquery
SELECT * FROM ft1 t1 WHERE t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 <= 10) ORDER BY c1;
-- subquery+MAX
SELECT * FROM ft1 t1 WHERE t1.c3 = (SELECT MAX(c3) FROM ft2 t2) ORDER BY c1;
-- used in CTE
WITH t1 AS (SELECT * FROM ft1 WHERE c1 <= 10) SELECT t2.c1, t2.c2, t2.c3, t2.c4 FROM t1, ft2 t2 WHERE t1.c1 = t2.c1 ORDER BY t1.c1;
-- fixed values
SELECT 'fixed', NULL FROM ft1 t1 WHERE c1 = 1;
-- Test forcing the remote server to produce sorted data for a merge join.
SET enable_hashjoin TO false;
SET enable_nestloop TO false;
-- inner join; expressions in the clauses appear in the equivalence class list
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."c1" FROM ft2 t1 JOIN "S 1"."T1" t2 ON (t1.c1 = t2."c1") OFFSET 100 LIMIT 10;
SELECT t1.c1, t2."c1" FROM ft2 t1 JOIN "S 1"."T1" t2 ON (t1.c1 = t2."c1") OFFSET 100 LIMIT 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."c1" FROM ft2 t1 LEFT JOIN "S 1"."T1" t2 ON (t1.c1 = t2."c1") OFFSET 100 LIMIT 10;
SELECT t1.c1, t2."c1" FROM ft2 t1 LEFT JOIN "S 1"."T1" t2 ON (t1.c1 = t2."c1") OFFSET 100 LIMIT 10;
-- A join between local table and foreign join. ORDER BY clause is added to the
-- foreign join so that the local table can be joined using merge join strategy.
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."c1" FROM "S 1"."T1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."c1") OFFSET 100 LIMIT 10;
SELECT t1."c1" FROM "S 1"."T1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."c1") OFFSET 100 LIMIT 10;
-- Test similar to above, except that the full join prevents any equivalence
-- classes from being merged. This produces single relation equivalence classes
-- included in join restrictions.
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."c1", t2.c1, t3.c1 FROM "S 1"."T1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."c1") OFFSET 100 LIMIT 10;
SELECT t1."c1", t2.c1, t3.c1 FROM "S 1"."T1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."c1") OFFSET 100 LIMIT 10;
-- Test similar to above with all full outer joins
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."c1", t2.c1, t3.c1 FROM "S 1"."T1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."c1") OFFSET 100 LIMIT 10;
SELECT t1."c1", t2.c1, t3.c1 FROM "S 1"."T1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."c1") OFFSET 100 LIMIT 10;
RESET enable_hashjoin;
RESET enable_nestloop;

-- ===================================================================
-- WHERE with remotely-executable conditions
-- ===================================================================
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 1;         -- Var, OpExpr(b), Const
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 100 AND t1.c2 = 0; -- BoolExpr
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NULL;        -- NullTest
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NOT NULL;    -- NullTest
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE round(abs(c1), 0) = 1; -- FuncExpr
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = -c1;          -- OpExpr(l)
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE 1 = c1!;           -- OpExpr(r)
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (c1 IS NOT NULL) IS DISTINCT FROM (c1 IS NOT NULL); -- DistinctExpr
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = ANY(ARRAY[c2, 1, c1 + 0]); -- ScalarArrayOpExpr
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = (ARRAY[c1,c2,3])[1]; -- ArrayRef
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c6 = E'foo''s\\bar';  -- check special chars
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c8 = 'foo';  -- can't be sent to remote
-- parameterized remote path for foreign table
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM "S 1"."T1" a, ft2 b WHERE a."c1" = 47 AND b.c1 = a.c2;
SELECT * FROM ft2 a, ft2 b WHERE a.c1 = 47 AND b.c1 = a.c2;

-- check both safe and unsafe join conditions
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft2 a, ft2 b
  WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
SELECT * FROM ft2 a, ft2 b
WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
-- bug before 9.3.5 due to sloppy handling of remote-estimate parameters
SELECT * FROM ft1 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft2 WHERE c1 < 5));
SELECT * FROM ft2 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft1 WHERE c1 < 5));
-- we should not push order by clause with volatile expressions or unsafe
-- collations
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, random();
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, ft2.c3 collate "C";

-- user-defined operator/function
CREATE FUNCTION griddb_fdw_abs(int) RETURNS int AS $$
BEGIN
RETURN abs($1);
END
$$ LANGUAGE plpgsql IMMUTABLE;
CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===
);

-- built-in operators and functions can be shipped for remote execution
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;

-- by default, user-defined ones cannot
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = griddb_fdw_abs(t1.c2);
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = griddb_fdw_abs(t1.c2);
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- but let's put them in an extension ...
ALTER EXTENSION griddb_fdw ADD FUNCTION griddb_fdw_abs(int);
ALTER EXTENSION griddb_fdw ADD OPERATOR === (int, int);

-- ... now they can be shipped
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = griddb_fdw_abs(t1.c2);
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = griddb_fdw_abs(t1.c2);
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- ===================================================================
-- JOIN queries
-- ===================================================================

-- join two tables
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join three tables
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
-- left outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- left outer join three tables
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + placement of clauses.
-- clauses within the nullable side are not pulled up, but top level clause on
-- non-nullable side is pushed into non-nullable side
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
-- clauses within the nullable side are not pulled up, but the top level clause
-- on nullable side is not pushed down into nullable side
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
-- right outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
-- right outer join three tables
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
-- full outer join with restrictions on the joining relations
-- a. the joining relations are both base relations
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
-- b. one of the joining relations is a base relation and the other is a join
-- relation
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- c. test deparsing the remote query as nested subqueries
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- d. test deparsing rowmarked relations as subqueries
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
-- full outer join + inner join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
-- full outer join three tables
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + right outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- right outer join + full outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + left outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + full outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- right outer join + left outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + right outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause, only matched rows
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause with shippable extensions set
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE griddb_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
-- join two tables with FOR UPDATE clause
-- tests whole-row reference for row marks
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
-- TODO
--SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
-- join two tables with FOR SHARE clause
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
-- TODO
--SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
-- join in CTE
EXPLAIN (VERBOSE, COSTS OFF)
WITH t (c1_1, c1_3, c2_1) AS (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
WITH t (c1_1, c1_3, c2_1) AS (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
-- ctid with whole-row reference
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.ctid, t1, t2, t1.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- SEMI JOIN, not pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- ANTI JOIN, not pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- CROSS JOIN, not pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- different server, not pushed down. No result expected.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe join conditions (c8 has a UDT), not pushed down. Practically a CROSS
-- JOIN since c8 in both tables has same value.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe conditions on one side (c8 has a UDT), not pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join where unsafe to pushdown condition in WHERE clause has a column not
-- in the SELECT clause. In this test unsafe clause needs to have column
-- references from both joining sides so that the clause is not pushed down
-- into one of the joining sides.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- Aggregate after UNION, for testing setrefs
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
-- join with lateral reference
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1."c1" FROM "S 1"."T1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."c1" OFFSET 10 LIMIT 10;
SELECT t1."c1" FROM "S 1"."T1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."c1" OFFSET 10 LIMIT 10;

-- non-Var items in targetlist of the nullable rel of a join preventing
-- push-down in some cases
-- unable to push {ft1, ft2}
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;

-- ok to push {ft1, ft2} but not {ft1, ft2, ft4}
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;

-- join with nullable side with some columns with null values
UPDATE ft5 SET c3 = null where c1 % 9 = 0;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;

-- multi-way join involving multiple merge joins
-- (this case used to have EPQ-related planning problems)
SET enable_nestloop TO false;
SET enable_hashjoin TO false;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1, ft2, ft4, ft5 WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 FOR UPDATE;
-- TODO
--SELECT * FROM ft1, ft2, ft4, ft5 WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
--    AND ft1.c2 = ft5.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 FOR UPDATE;
RESET enable_nestloop;
RESET enable_hashjoin;

-- check join pushdown in situations where multiple userids are involved
CREATE ROLE regress_view_owner SUPERUSER;
CREATE USER MAPPING FOR regress_view_owner SERVER griddb_svr OPTIONS (username 'admin', password 'testadmin');
GRANT SELECT ON ft4 TO regress_view_owner;
GRANT SELECT ON ft5 TO regress_view_owner;

CREATE VIEW v4 AS SELECT * FROM ft4;
CREATE VIEW v5 AS SELECT * FROM ft5;
ALTER VIEW v5 OWNER TO regress_view_owner;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, different view owners
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
ALTER VIEW v4 OWNER TO regress_view_owner;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, view owner not current user
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
ALTER VIEW v4 OWNER TO CURRENT_USER;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
ALTER VIEW v4 OWNER TO regress_view_owner;

-- cleanup
DROP OWNED BY regress_view_owner;
DROP ROLE regress_view_owner;

-- ===================================================================
-- Aggregate and grouping queries
-- ===================================================================

-- Simple aggregates
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;

-- Aggregate is not pushed down as aggregation contains random()
explain (verbose, costs off)
select sum(c1 * (random() <= 1)::int) as sum, avg(c1) from ft1;

-- Aggregate over join query
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;

-- Not pushed down due to local conditions present in underneath input rel
explain (verbose, costs off)
select sum(t1.c1), count(t2.c1) from ft1 t1 inner join ft2 t2 on (t1.c1 = t2.c1) where ((t1.c1 * t2.c1)/(t1.c1 * t2.c1)) * random() <= 1;

-- GROUP BY clause having expressions
explain (verbose, costs off)
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;

-- Aggregates in subquery are pushed down.
explain (verbose, costs off)
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;

-- Aggregate is still pushed down by taking unshippable expression out
explain (verbose, costs off)
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed
explain (verbose, costs off)
select c2 * (random() <= 1)::int as c2 from ft2 group by c2 * (random() <= 1)::int order by 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression
explain (verbose, costs off)
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;

-- GROUP BY clause referring to same column multiple times
-- Also, ORDER BY contains an aggregate function
explain (verbose, costs off)
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);

-- Testing HAVING clause shippability
explain (verbose, costs off)
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;

-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down
explain (verbose, costs off)
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;

-- Aggregate in HAVING clause is not pushable, and thus aggregation is not pushed down
explain (verbose, costs off)
select sum(c1) from ft1 group by c2 having avg(c1 * (random() <= 1)::int) > 100 order by 1;

-- Testing ORDER BY, DISTINCT, FILTER, Ordered-sets and VARIADIC within aggregates

-- ORDER BY within aggregate, same column used to order
explain (verbose, costs off)
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;

-- ORDER BY within aggregate, different column used to order also using DESC
explain (verbose, costs off)
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;

-- DISTINCT within aggregate
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- DISTINCT combined with ORDER BY within aggregate
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- FILTER within aggregate
explain (verbose, costs off)
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;

-- DISTINCT, ORDER BY and FILTER within aggregate
explain (verbose, costs off)
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;

-- Outer query is aggregation query
explain (verbose, costs off)
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
-- Inner query is aggregation query
explain (verbose, costs off)
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;

-- Aggregate not pushed down as FILTER condition is not pushable
explain (verbose, costs off)
select sum(c1) filter (where (c1 / c1) * random() <= 1) from ft1 group by c2 order by 1;
explain (verbose, costs off)
select sum(c2) filter (where c2 in (select c2 from ft1 where c2 < 5)) from ft1;

-- Ordered-sets within aggregate
explain (verbose, costs off)
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;

-- Using multiple arguments within aggregates
explain (verbose, costs off)
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;

-- User defined function for user defined aggregate, VARIADIC
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);

-- Disable hash aggregation for plan stability.
set enable_hashagg to false;

-- Not pushed down due to user defined aggregate
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Add function and aggregate into extension
alter extension griddb_fdw add function least_accum(anyelement, variadic anyarray);
alter extension griddb_fdw add aggregate least_agg(variadic items anyarray);
--alter server griddb_svr options (set extensions 'griddb_fdw');

-- Now aggregate will be pushed.  Aggregate will display VARIADIC argument.
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;

-- Remove function and aggregate from extension
alter extension griddb_fdw drop function least_accum(anyelement, variadic anyarray);
alter extension griddb_fdw drop aggregate least_agg(variadic items anyarray);
--alter server griddb_svr options (set extensions 'griddb_fdw');

-- Not pushed down as we have dropped objects from extension.
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Cleanup
reset enable_hashagg;
drop aggregate least_agg(variadic items anyarray);
drop function least_accum(anyelement, variadic anyarray);


-- Testing USING OPERATOR() in ORDER BY within aggregate.
-- For this, we need user defined operators along with operator family and
-- operator class.  Create those and then add them in extension.  Note that
-- user defined objects are considered unshippable unless they are part of
-- the extension.
create operator public.<^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4eq
);

create operator public.=^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4lt
);

create operator public.>^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4gt
);

create operator family my_op_family using btree;

create function my_op_cmp(a int, b int) returns int as
  $$begin return btint4cmp(a, b); end $$ language plpgsql;

create operator class my_op_class for type int using btree family my_op_family as
 operator 1 public.<^,
 operator 3 public.=^,
 operator 5 public.>^,
 function 1 my_op_cmp(int, int);

-- This will not be pushed as user defined sort operator is not part of the
-- extension yet.
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Add into extension
alter extension griddb_fdw add operator class my_op_class using btree;
alter extension griddb_fdw add function my_op_cmp(a int, b int);
alter extension griddb_fdw add operator family my_op_family using btree;
alter extension griddb_fdw add operator public.<^(int, int);
alter extension griddb_fdw add operator public.=^(int, int);
alter extension griddb_fdw add operator public.>^(int, int);
--alter server griddb_svr options (set extensions 'griddb_fdw');

-- Now this will be pushed as sort operator is part of the extension.
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Remove from extension
alter extension griddb_fdw drop operator class my_op_class using btree;
alter extension griddb_fdw drop function my_op_cmp(a int, b int);
alter extension griddb_fdw drop operator family my_op_family using btree;
alter extension griddb_fdw drop operator public.<^(int, int);
alter extension griddb_fdw drop operator public.=^(int, int);
alter extension griddb_fdw drop operator public.>^(int, int);

-- This will not be pushed as sort operator is now removed from the extension.
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Cleanup
drop operator class my_op_class using btree;
drop function my_op_cmp(a int, b int);
drop operator family my_op_family using btree;
drop operator public.>^(int, int);
drop operator public.=^(int, int);
drop operator public.<^(int, int);

-- Input relation to aggregate push down hook is not safe to pushdown and thus
-- the aggregate cannot be pushed down to foreign server.
explain (verbose, costs off)
select count(t1.c3) from ft2 t1 left join ft2 t2 on (t1.c1 = random() * t2.c2);

-- Subquery in FROM clause having aggregate
explain (verbose, costs off)
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;

-- FULL join with IS NULL check in HAVING
explain (verbose, costs off)
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;

-- Aggregate over FULL join needing to deparse the joining relations as
-- subqueries.
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.
explain (verbose, costs off)
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;

-- LATERAL join, with parameterization
set enable_hashagg to false;
explain (verbose, costs off)
select c2, sum from "S 1"."T1" t1, lateral (select sum(t2.c1 + t1."c1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."c1" < 100 order by 1;
select c2, sum from "S 1"."T1" t1, lateral (select sum(t2.c1 + t1."c1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."c1" < 100 order by 1;
reset enable_hashagg;

-- Check with placeHolderVars
explain (verbose, costs off)
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);


-- Not supported cases
-- Grouping sets
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
explain (verbose, costs off)
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
explain (verbose, costs off)
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;

-- DISTINCT itself is not pushed down, whereas underneath aggregate is pushed
explain (verbose, costs off)
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;

-- WindowAgg
explain (verbose, costs off)
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;


-- ===================================================================
-- parameterized queries
-- ===================================================================
-- simple join
PREPARE st1(int, int) AS SELECT t1.c3, t2.c3 FROM ft1 t1, ft2 t2 WHERE t1.c1 = $1 AND t2.c1 = $2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st1(1, 2);
EXECUTE st1(1, 1);
EXECUTE st1(101, 101);
-- subquery using stable function (can't be sent to remote)
PREPARE st2(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c4) = '1970-01-17'::date) ORDER BY c1;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st2(10, 20);
EXECUTE st2(10, 20);
EXECUTE st2(101, 121);
-- subquery using immutable function (can be sent to remote)
PREPARE st3(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c5) = '1970-01-17'::date) ORDER BY c1;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st3(10, 20);
EXECUTE st3(10, 20);
EXECUTE st3(20, 30);
-- custom plan should be chosen initially
PREPARE st4(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 = $1;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- once we try it enough times, should switch to generic plan
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- value of $1 should not be sent to remote
PREPARE st5(text,int) AS SELECT * FROM ft1 t1 WHERE c8 = $1 and c1 = $2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
EXECUTE st5('foo', 1);

-- altering FDW options requires replanning
PREPARE st6 AS SELECT * FROM ft1 t1 WHERE t1.c1 = t1.c2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
PREPARE st7 AS INSERT INTO ft1 (c1,c2,c3) VALUES (1001,101,'foo');
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;

ALTER FOREIGN TABLE ft1 OPTIONS (SET table_name 'T0');  -- Modified the link to 'T0'
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
EXECUTE st6;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;

ALTER FOREIGN TABLE ft1 OPTIONS (SET table_name 'T1');  -- Re-modified the link to 'T1'
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
EXECUTE st6;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;

ALTER FOREIGN TABLE ft1 RENAME TO ftt;  -- Modified the foreign table name, the link is kept
PREPARE stt AS SELECT * FROM ftt t1 WHERE t1.c1 = t1.c2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE stt;
EXECUTE stt;
ALTER FOREIGN TABLE ftt RENAME TO ft1;  -- Re-modified the foreign table name

PREPARE st8 AS SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
EXECUTE st8;

-- cleanup
DEALLOCATE st1;
DEALLOCATE st2;
DEALLOCATE st3;
DEALLOCATE st4;
DEALLOCATE st5;
DEALLOCATE st6;
DEALLOCATE st7;
DEALLOCATE st8;

-- System columns, except ctid and oid, should not be sent to remote
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'pg_class'::regclass LIMIT 1;
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'ft1'::regclass LIMIT 1;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, * FROM ft1 t1 LIMIT 1;
SELECT ctid, * FROM ft1 t1 LIMIT 1;

-- ===================================================================
-- used in PL/pgSQL function
-- ===================================================================
CREATE OR REPLACE FUNCTION f_test(p_c1 int) RETURNS int AS $$
DECLARE
	v_c1 int;
BEGIN
    SELECT c1 INTO v_c1 FROM ft1 WHERE c1 = p_c1 LIMIT 1;
    PERFORM c1 FROM ft1 WHERE c1 = p_c1 AND p_c1 = v_c1 LIMIT 1;
    RETURN v_c1;
END;
$$ LANGUAGE plpgsql;
SELECT f_test(100);
DROP FUNCTION f_test(int);

-- ===================================================================
-- conversion error
-- ===================================================================
ALTER FOREIGN TABLE ft1 ALTER COLUMN c8 TYPE int;
SELECT * FROM ft1 WHERE c1 = 1;  -- ERROR
SELECT  ft1.c1, ft2.c2, ft1.c8 FROM ft1, ft2 WHERE ft1.c1 = ft2.c1 AND ft1.c1 = 1; -- ERROR
SELECT  ft1.c1, ft2.c2, ft1 FROM ft1, ft2 WHERE ft1.c1 = ft2.c1 AND ft1.c1 = 1; -- ERROR
SELECT sum(c2), array_agg(c8) FROM ft1 GROUP BY c8; -- ERROR
ALTER FOREIGN TABLE ft1 ALTER COLUMN c8 TYPE text;

-- ===================================================================
-- subtransaction
--  + local/remote error doesn't break cursor
-- ===================================================================
BEGIN;
DECLARE c CURSOR FOR SELECT * FROM ft1 ORDER BY c1;
FETCH c;
-- SAVEPOINT s;        -- Not support
FETCH c;
SELECT * FROM ft1 ORDER BY c1 LIMIT 1;
FETCH c;
SELECT * FROM ft1 WHERE 1 / (c1 - 1) > 0;  -- ERROR
ROLLBACK;

-- ===================================================================
-- test handling of collations
-- ===================================================================
create table loct3 (f1 text collate "C" unique, f2 text, f3 text unique);
create foreign table ft3 (f1 text collate "C", f2 text, f3 text)
  server griddb_svr options (table_name 'loct3');

-- can be sent to remote
explain (verbose, costs off) select * from ft3 where f1 = 'foo';
explain (verbose, costs off) select * from ft3 where f1 COLLATE "C" = 'foo';
explain (verbose, costs off) select * from ft3 where f2 = 'foo';
explain (verbose, costs off) select * from ft3 where f3 = 'foo';
explain (verbose, costs off) select * from ft3 f, loct3 l
  where f.f3 = l.f3 and l.f1 = 'foo';
-- can't be sent to remote
explain (verbose, costs off) select * from ft3 where f1 COLLATE "POSIX" = 'foo';
explain (verbose, costs off) select * from ft3 where f1 = 'foo' COLLATE "C";
explain (verbose, costs off) select * from ft3 where f2 COLLATE "C" = 'foo';
explain (verbose, costs off) select * from ft3 where f2 = 'foo' COLLATE "C";
explain (verbose, costs off) select * from ft3 f, loct3 l
  where f.f3 = l.f3 COLLATE "POSIX" and l.f1 = 'foo';

-- ===================================================================
-- test writable foreign table stuff
-- ===================================================================
EXPLAIN (verbose, costs off)
INSERT INTO ft2 (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;
INSERT INTO ft2 (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;
INSERT INTO ft2 (c1,c2,c3)
  VALUES (1101,201,'aaa'), (1102,202,'bbb'), (1103,203,'ccc');
SELECT * FROM ft2 WHERE c1 > 1100 AND c1 < 1104;
INSERT INTO ft2 (c1,c2,c3) VALUES (1104,204,'ddd'), (1105,205,'eee');
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;              -- can be pushed down
UPDATE ft2 SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7;              -- can be pushed down
UPDATE ft2 SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7;
SELECT * FROM ft2 WHERE c1 % 10 = 7;
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c2 = ft2.c2 + 500, c3 = ft2.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 9;                               -- can be pushed down
UPDATE ft2 SET c2 = ft2.c2 + 500, c3 = ft2.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 9;
EXPLAIN (verbose, costs off)
  DELETE FROM ft2 WHERE c1 % 10 = 5;                                                -- can be pushed down
DELETE FROM ft2 WHERE c1 % 10 = 5;
SELECT c1,c4 FROM ft2 WHERE c1 % 10 = 5;
EXPLAIN (verbose, costs off)
DELETE FROM ft2 USING ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 2;                -- can be pushed down
DELETE FROM ft2 USING ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 2;
SELECT c1,c2,c3,c4 FROM ft2 ORDER BY c1;
EXPLAIN (verbose, costs off)
INSERT INTO ft2 (c1,c2,c3) VALUES (1200,999,'foo');
INSERT INTO ft2 (c1,c2,c3) VALUES (1200,999,'foo');
SELECT tableoid::regclass FROM ft2 WHERE c1 = 1200;
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c3 = 'bar' WHERE c1 = 1200;                                          -- can be pushed down
UPDATE ft2 SET c3 = 'bar' WHERE c1 = 1200;
SELECT tableoid::regclass FROM ft2 WHERE c1 = 1200;
EXPLAIN (verbose, costs off)
DELETE FROM ft2 WHERE c1 = 1200;                                                    -- can be pushed down
DELETE FROM ft2 WHERE c1 = 1200;
SELECT tableoid::regclass FROM ft2 WHERE c1 = 1200;

-- Test UPDATE/DELETE with WHERE or JOIN/ON conditions containing
-- user-defined operators/functions
INSERT INTO ft2 (c1,c2,c3)
  SELECT id, id % 10, to_char(id, 'FM00000') FROM generate_series(2001, 2010) id;
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c3 = 'bar' WHERE griddb_fdw_abs(c1) > 2000;                          -- can't be pushed down
UPDATE ft2 SET c3 = 'bar' WHERE griddb_fdw_abs(c1) > 2000;
SELECT * FROM ft2 WHERE griddb_fdw_abs(c1) > 2000;
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 === ft4.c1;                                        -- can't be pushed down
UPDATE ft2 SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 === ft4.c1;
SELECT ft2.*, ft4.*, ft5.* FROM ft2
  INNER JOIN ft4 ON (ft2.c1 > 2000 AND ft2.c2 === ft4.c1)
  INNER JOIN ft5 ON (ft4.c1 = ft5.c1);
EXPLAIN (verbose, costs off)
DELETE FROM ft2
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 = ft4.c1;                                          -- can't be pushed down
DELETE FROM ft2
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 = ft4.c1;
SELECT ft2.c1, ft2.c2, ft2.c3 FROM ft2
  INNER JOIN ft4 ON (ft2.c1 > 2000 AND ft2.c2 = ft4.c1)
  INNER JOIN ft5 ON (ft4.c1 === ft5.c1);
DELETE FROM ft2 WHERE ft2.c1 > 2000;

-- Test that trigger on remote table works as expected
CREATE OR REPLACE FUNCTION "S 1".F_BRTRIG() RETURNS trigger AS $$
BEGIN
    NEW.c3 = NEW.c3 || '_trig_update';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER t1_br_insert BEFORE INSERT OR UPDATE
    ON "S 1"."T1" FOR EACH ROW EXECUTE PROCEDURE "S 1".F_BRTRIG();

INSERT INTO "S 1"."T1" (c1,c2,c3,c7) VALUES (1208, 818, 'fff', 'ft2');
SELECT * FROM ft2 WHERE c1 = 1208;
INSERT INTO "S 1"."T1" (c1,c2,c3,c6,c7) VALUES (1218, 818, 'ggg', '(--;', 'ft2');
SELECT * FROM ft2 WHERE c1 = 1218;
-- TODO
-- UPDATE "S 1"."T1" SET c2 = c2 + 600 WHERE c1 % 10 = 8 AND c1 < 1200;             -- It should work
UPDATE "S 1"."T1" SET c2 = c2 + 600, c3 = c3 WHERE c1 % 10 = 8 AND c1 < 1200;       -- Temporary add "c3 = c3" to sync the data
SELECT * FROM "S 1"."T1" WHERE c1 % 10 = 8 AND c1 < 1200;

-- Test insert with duplicate key
INSERT INTO ft1(c1, c2) VALUES(1228, 11);
INSERT INTO ft1(c1, c2) VALUES(1228, 12);  -- duplicate key
SELECT * FROM ft1 WHERE c1 = 1228;
DELETE FROM ft1 WHERE c1 = 1228;

update "S 1"."T1" set c2 = 42, c3 = c3 where c2 = 0;
update "S 1"."T1" set c2 = 44, c3 = c3 where c2 = 4;

-- Above DMLs add data with c6 as NULL in ft1, so test ORDER BY NULLS LAST and NULLs
-- FIRST behavior here.
-- ORDER BY DESC NULLS LAST options
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795 LIMIT 10;
SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795 LIMIT 10;
-- ORDER BY DESC NULLS FIRST options
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
-- ORDER BY ASC NULLS FIRST options
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;

-- ===================================================================
-- test WITH CHECK OPTION constraints
-- ===================================================================

CREATE FOREIGN TABLE foreign_tbl (a int OPTIONS (rowkey 'true'), b int)
  SERVER griddb_svr OPTIONS(table_name 'base_tbl');
CREATE VIEW rw_view AS SELECT * FROM foreign_tbl
  WHERE a < b WITH CHECK OPTION;
\d+ rw_view

INSERT INTO rw_view VALUES (0, 10); -- ok
INSERT INTO rw_view VALUES (10, 0); -- should fail
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = 20 WHERE a = 0; -- not pushed down
UPDATE rw_view SET b = 20 WHERE a = 0; -- ok
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = -20 WHERE a = 0; -- not pushed down
UPDATE rw_view SET b = -20 WHERE a = 0; -- should fail
SELECT * FROM foreign_tbl;

DROP FOREIGN TABLE foreign_tbl CASCADE;

-- ===================================================================
-- test serial columns (ie, sequence-based defaults)
-- ===================================================================

create foreign table rem1 (f1 serial OPTIONS (rowkey 'true'), f2 text)
  server griddb_svr options(table_name 'loc1');
insert into rem1(f2) values('hi');
insert into rem1(f2) values('bye');
select * from rem1;
select pg_catalog.setval('rem1_f1_seq', 10, false);
insert into rem1(f2) values('hi after setval');
insert into rem1(f2) values('bye after setval');
select * from rem1;

-- ===================================================================
-- test local triggers
-- ===================================================================

-- Trigger functions "borrowed" from triggers regress test.
CREATE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE NOTICE 'trigger_func(%) called: action = %, when = %, level = %',
		TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;$$;

CREATE TRIGGER trig_stmt_before BEFORE DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
CREATE TRIGGER trig_stmt_after AFTER DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

CREATE OR REPLACE FUNCTION trigger_data()  RETURNS trigger
LANGUAGE plpgsql AS $$

declare
	oldnew text[];
	relid text;
    argstr text;
begin

	relid := TG_relid::regclass;
	argstr := '';
	for i in 0 .. TG_nargs - 1 loop
		if i > 0 then
			argstr := argstr || ', ';
		end if;
		argstr := argstr || TG_argv[i];
	end loop;

    RAISE NOTICE '%(%) % % % ON %',
		tg_name, argstr, TG_when, TG_level, TG_OP, relid;
    oldnew := '{}'::text[];
	if TG_OP != 'INSERT' then
		oldnew := array_append(oldnew, format('OLD: %s', OLD));
	end if;

	if TG_OP != 'DELETE' then
		oldnew := array_append(oldnew, format('NEW: %s', NEW));
	end if;

    RAISE NOTICE '%', array_to_string(oldnew, ',');

	if TG_OP = 'DELETE' then
		return OLD;
	else
		return NEW;
	end if;
end;
$$;

-- Test basic functionality
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

delete from rem1;
insert into rem1 values(1,'insert');
update rem1 set f2  = 'update' where f1 = 1;
update rem1 set f2 = f2 || f2;


-- cleanup
DROP TRIGGER trig_row_before ON rem1;
DROP TRIGGER trig_row_after ON rem1;
DROP TRIGGER trig_stmt_before ON rem1;
DROP TRIGGER trig_stmt_after ON rem1;

DELETE from rem1;


-- Test WHEN conditions

CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

CREATE TRIGGER trig_row_after_insupd
AFTER INSERT OR UPDATE ON rem1
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Insert or update not matching: nothing happens
INSERT INTO rem1 values(1, 'insert');
UPDATE rem1 set f2 = 'test';

-- Insert or update matching: triggers are fired
INSERT INTO rem1 values(2, 'update');
UPDATE rem1 set f2 = 'update update' where f1 = '2';

CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Trigger is fired for f1=2, not for f1=1
DELETE FROM rem1;

-- cleanup
DROP TRIGGER trig_row_before_insupd ON rem1;
DROP TRIGGER trig_row_after_insupd ON rem1;
DROP TRIGGER trig_row_before_delete ON rem1;
DROP TRIGGER trig_row_after_delete ON rem1;


-- Test various RETURN statements in BEFORE triggers.

CREATE FUNCTION trig_row_before_insupdate() RETURNS TRIGGER AS $$
  BEGIN
    NEW.f2 := NEW.f2 || ' triggered !';
    RETURN NEW;
  END
$$ language plpgsql;

CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

-- The new values should have 'triggered' appended
INSERT INTO rem1 values(1, 'insert');
SELECT * from rem1;
INSERT INTO rem1 values(2, 'insert');
SELECT * from rem1;
UPDATE rem1 set f2 = '';
SELECT * from rem1;
UPDATE rem1 set f2 = 'skidoo';
SELECT * from rem1;

DELETE FROM rem1;

-- Add a second trigger, to check that the changes are propagated correctly
-- from trigger to trigger
CREATE TRIGGER trig_row_before_insupd2
BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

INSERT INTO rem1 values(1, 'insert');
SELECT * from rem1;
INSERT INTO rem1 values(2, 'insert');
SELECT * from rem1;
UPDATE rem1 set f2 = '';
SELECT * from rem1;
UPDATE rem1 set f2 = 'skidoo';
SELECT * from rem1;

DROP TRIGGER trig_row_before_insupd ON rem1;
DROP TRIGGER trig_row_before_insupd2 ON rem1;

DELETE from rem1;

INSERT INTO rem1 VALUES (1, 'test');

-- Test with a trigger returning NULL
CREATE FUNCTION trig_null() RETURNS TRIGGER AS $$
  BEGIN
    RETURN NULL;
  END
$$ language plpgsql;

CREATE TRIGGER trig_null
BEFORE INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_null();

-- Nothing should have changed.
INSERT INTO rem1 VALUES (2, 'test2');

SELECT * from rem1;

UPDATE rem1 SET f2 = 'test2';

SELECT * from rem1;

DELETE from rem1;

SELECT * from rem1;

DROP TRIGGER trig_null ON rem1;
DELETE from rem1;


-- Test direct foreign table modification functionality

-- Test with statement-level triggers
CREATE TRIGGER trig_stmt_before
	BEFORE DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
DROP TRIGGER trig_stmt_before ON rem1;

CREATE TRIGGER trig_stmt_after
	AFTER DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
DROP TRIGGER trig_stmt_after ON rem1;

-- Test with row-level ON INSERT triggers
CREATE TRIGGER trig_row_before_insert
BEFORE INSERT ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
DROP TRIGGER trig_row_before_insert ON rem1;

CREATE TRIGGER trig_row_after_insert
AFTER INSERT ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
DROP TRIGGER trig_row_after_insert ON rem1;

-- Test with row-level ON UPDATE triggers
CREATE TRIGGER trig_row_before_update
BEFORE UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
DROP TRIGGER trig_row_before_update ON rem1;

CREATE TRIGGER trig_row_after_update
AFTER UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
DROP TRIGGER trig_row_after_update ON rem1;

-- Test with row-level ON DELETE triggers
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can't be pushed down
DROP TRIGGER trig_row_before_delete ON rem1;

CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can't be pushed down
DROP TRIGGER trig_row_after_delete ON rem1;

-- ===================================================================
-- test inheritance features
-- ===================================================================

CREATE TABLE a (aa TEXT);
ALTER TABLE a SET (autovacuum_enabled = 'false');
CREATE FOREIGN TABLE b (bb TEXT) INHERITS (a)
  SERVER griddb_svr OPTIONS (table_name 'loct');

ALTER FOREIGN TABLE b ALTER COLUMN aa OPTIONS (rowkey 'true');

INSERT INTO a(aa) VALUES('aaa');
INSERT INTO a(aa) VALUES('aaaa');
INSERT INTO a(aa) VALUES('aaaaa');

INSERT INTO b(aa) VALUES('bbb');
INSERT INTO b(aa) VALUES('bbbb');
INSERT INTO b(aa) VALUES('bbbbb');

SELECT tableoid::regclass, * FROM a;
SELECT tableoid::regclass, * FROM b;
SELECT tableoid::regclass, * FROM ONLY a;

UPDATE a SET aa = 'zzzzzz' WHERE aa LIKE 'aaaa%'; -- limitation

UPDATE b SET bb = 'new' WHERE aa LIKE 'bbbb%';

SELECT tableoid::regclass, * FROM a;
SELECT tableoid::regclass, * FROM b;
SELECT tableoid::regclass, * FROM ONLY a;

DELETE FROM a;

SELECT tableoid::regclass, * FROM a;
SELECT tableoid::regclass, * FROM b;
SELECT tableoid::regclass, * FROM ONLY a;

DROP TABLE a CASCADE;

-- Check SELECT FOR UPDATE/SHARE with an inherited source table

create table foo (f1 int, f2 int);
create foreign table foo2 (f3 int) inherits (foo)
  server griddb_svr options (table_name 'loct1');
create table bar (f1 int, f2 int);
create foreign table bar2 (f3 int) inherits (bar)
  server griddb_svr options (table_name 'loct2');

alter table foo set (autovacuum_enabled = 'false');
alter table bar set (autovacuum_enabled = 'false');

alter foreign table foo2 alter column f1 options (rowkey 'true');
alter foreign table bar2 alter column f1 options (rowkey 'true');

insert into foo values(1,1);
insert into foo values(3,3);
insert into foo2 values(2,2,2);
insert into foo2 values(4,4,4);
insert into bar values(1,11);
insert into bar values(2,22);
insert into bar values(6,66);
insert into bar2 values(3,33,33);
insert into bar2 values(4,44,44);
insert into bar2 values(7,77,77);

explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo) for update;
select * from bar where f1 in (select f1 from foo) for update;

explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo) for share;
select * from bar where f1 in (select f1 from foo) for share;

-- Check UPDATE with inherited target and an inherited source table
explain (verbose, costs off)
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);

select tableoid::regclass, * from bar order by 1,2;

-- Check UPDATE with inherited target and an appendrel subquery
explain (verbose, costs off)
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;

select tableoid::regclass, * from bar order by 1,2;

-- Test forcing the remote server to produce sorted data for a merge join,
-- but the foreign table is an inheritance child.
delete from "S 1".loct1;
truncate table only foo;
\set num_rows_foo 2000
insert into "S 1".loct1 select generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2);
insert into foo select generate_series(1, :num_rows_foo, 2), generate_series(1, :num_rows_foo, 2);
SET enable_hashjoin to false;
SET enable_nestloop to false;
-- inner join; expressions in the clauses appear in the equivalence class list
explain (verbose, costs off)
	select foo.f1, "S 1".loct1.f1 from foo join "S 1".loct1 on (foo.f1 = "S 1".loct1.f1) order by foo.f2 offset 10 limit 10;
select foo.f1, "S 1".loct1.f1 from foo join "S 1".loct1 on (foo.f1 = "S 1".loct1.f1) order by foo.f2 offset 10 limit 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
explain (verbose, costs off)
	select foo.f1, "S 1".loct1.f1 from foo left join "S 1".loct1 on (foo.f1 = "S 1".loct1.f1) order by foo.f2 offset 10 limit 10;
select foo.f1, "S 1".loct1.f1 from foo left join "S 1".loct1 on (foo.f1 = "S 1".loct1.f1) order by foo.f2 offset 10 limit 10;
RESET enable_hashjoin;
RESET enable_nestloop;

-- Test that WHERE CURRENT OF is not supported
begin;
declare c cursor for select * from bar where f1 = 7;
fetch from c;
update bar set f2 = null where current of c;
rollback;

explain (verbose, costs off)
delete from foo where f1 < 5;
delete from foo where f1 < 5;
select * from foo where f1 < 5;
explain (verbose, costs off)
update bar set f2 = f2 + 100;
update bar set f2 = f2 + 100;
select * from bar;

-- Test that UPDATE/DELETE with inherited target works with row-level triggers
CREATE TRIGGER trig_row_before
BEFORE UPDATE OR DELETE ON bar2
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

CREATE TRIGGER trig_row_after
AFTER UPDATE OR DELETE ON bar2
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

explain (verbose, costs off)
update bar set f2 = f2 + 100;
update bar set f2 = f2 + 100;

explain (verbose, costs off)
delete from bar where f2 < 400;
delete from bar where f2 < 400;

-- cleanup
drop table foo cascade;
drop table bar cascade;

-- Test pushing down UPDATE/DELETE joins to the remote server
create table parent (a int, b text);
create foreign table remt1 (a int, b text)
  server griddb_svr options (table_name 'loct3');
create foreign table remt2 (a int, b text)
  server griddb_svr options (table_name 'loct4');
alter foreign table remt1 inherit parent;

alter foreign table remt1 alter column a options (rowkey 'true');
alter foreign table remt2 alter column a options (rowkey 'true');

insert into remt1 values (1, 'foo');
insert into remt1 values (2, 'bar');
insert into remt2 values (1, 'foo');
insert into remt2 values (2, 'bar');

explain (verbose, costs off)
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a;
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a;
explain (verbose, costs off)
delete from parent using remt2 where parent.a = remt2.a;
delete from parent using remt2 where parent.a = remt2.a;

-- cleanup
drop foreign table remt1;
drop foreign table remt2;
drop table parent;

-- ===================================================================
-- test tuple routing for foreign-table partitions
-- ===================================================================

-- Test insert tuple routing
create table itrtest (a int, b int, c text) partition by list (b);
create foreign table remp1 (a int, b int, c text) server griddb_svr options (table_name 'locp1');
create foreign table remp2 (a int, b int, c text) server griddb_svr options (table_name 'locp2');

alter foreign table remp1 alter column a options (rowkey 'true');
alter foreign table remp2 alter column a options (rowkey 'true');

alter table itrtest attach partition remp1 for values in (1);
alter table itrtest attach partition remp2 for values in (2);

insert into itrtest values (1, 1, 'foo');
insert into itrtest values (2, 1, 'bar');
insert into itrtest values (3, 2, 'baz');
insert into itrtest values (4, 2, 'qux');
insert into itrtest values (5, 1, 'test1'), (6, 2, 'test2');

select tableoid::regclass, * FROM itrtest;
select tableoid::regclass, * FROM remp1;
select tableoid::regclass, * FROM remp2;

delete from itrtest;
select * from itrtest;

-- Test copy tuple routing
delete from "S 1".loct3;
delete from "S 1".loct4;
create table ctrtest (a int, b text) partition by list (a);
create foreign table remp3 (a int, b text) server griddb_svr options (table_name 'loct3');
create foreign table remp4 (a int, b text) server griddb_svr options (table_name 'loct4');
alter table ctrtest attach partition remp3 for values in (1);
alter table ctrtest attach partition remp4 for values in (2);

copy ctrtest from stdin;
1	foo
2	qux
\.

select tableoid::regclass, * FROM ctrtest;
select tableoid::regclass, * FROM remp3;
select tableoid::regclass, * FROM remp4;

-- Copying into foreign partitions directly should work as well
copy remp3 from stdin;
1	bar
\.

select tableoid::regclass, * FROM remp3;

drop table ctrtest;

-- ===================================================================
-- test COPY FROM
-- ===================================================================

create foreign table rem2 (f1 int, f2 text) server griddb_svr options(table_name 'loc2');
alter foreign table rem2 alter column f1 options (rowkey 'true');

-- Test basic functionality
copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

delete from rem2;

-- Test local triggers
create trigger trig_stmt_before before insert on rem2
	for each statement execute procedure trigger_func();
create trigger trig_stmt_after after insert on rem2
	for each statement execute procedure trigger_func();
create trigger trig_row_before before insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');
create trigger trig_row_after after insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');

copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

drop trigger trig_row_before on rem2;
drop trigger trig_row_after on rem2;
drop trigger trig_stmt_before on rem2;
drop trigger trig_stmt_after on rem2;

delete from rem2;

create trigger trig_row_before_insert before insert on rem2
	for each row execute procedure trig_row_before_insupdate();

-- The new values are concatenated with ' triggered !'
copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

drop trigger trig_row_before_insert on rem2;

delete from rem2;

create trigger trig_null before insert on rem2
	for each row execute procedure trig_null();

-- Nothing happens
copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

drop trigger trig_null on rem2;

delete from rem2;

-- Test remote triggers
create trigger trig_row_before_insert before insert on "S 1"."loc2"
	for each row execute procedure trig_row_before_insupdate();

-- The new values are concatenated with ' triggered !'
copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

drop trigger trig_row_before_insert on "S 1"."loc2";

delete from rem2;

create trigger trig_null before insert on "S 1"."loc2"
	for each row execute procedure trig_null();

-- Nothing happens
copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

drop trigger trig_null on "S 1"."loc2";

delete from rem2;

-- Test a combination of local and remote triggers
create trigger rem2_trig_row_before before insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');
create trigger rem2_trig_row_after after insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');
create trigger loc2_trig_row_before_insert before insert on "S 1"."loc2"
	for each row execute procedure trig_row_before_insupdate();

copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

drop trigger rem2_trig_row_before on rem2;
drop trigger rem2_trig_row_after on rem2;
drop trigger loc2_trig_row_before_insert on "S 1"."loc2";

delete from rem2;

-- ===================================================================
-- test IMPORT FOREIGN SCHEMA
-- ===================================================================

CREATE SCHEMA import_grid1;
IMPORT FOREIGN SCHEMA "S 1" LIMIT TO
	("T0", "T1", "T2", "T3", "T4", ft1, ft2, ft4, ft5, base_tbl,
	loc1, loc2, loct, loct1, loct2, loct3, loct4, locp1, locp2,
	fprt1_p1, fprt1_p2, fprt2_p1, fprt2_p2, pagg_tab_p1, pagg_tab_p2, pagg_tab_p3)
	FROM SERVER griddb_svr INTO import_grid1;
\det+ import_grid1.*
\d import_grid1.*

-- Check LIMIT TO and EXCEPT
CREATE SCHEMA import_grid2;
IMPORT FOREIGN SCHEMA griddb_schema LIMIT TO ("T1", loct, nonesuch)
  FROM SERVER griddb_svr INTO import_grid2;
\det+ import_grid2.*
-- IMPORT FOREIGN SCHEMA griddb_schema EXCEPT ("T1", loct, nonesuch)
--  FROM SERVER griddb_svr INTO import_grid2;
-- \det+ import_grid2.*

-- Assorted error cases
-- IMPORT FOREIGN SCHEMA griddb_schema FROM SERVER griddb_svr INTO import_grid2;
-- IMPORT FOREIGN SCHEMA nonesuch FROM SERVER griddb_svr INTO import_grid2; -- same as 'public'
IMPORT FOREIGN SCHEMA nonesuch FROM SERVER griddb_svr INTO notthere;
IMPORT FOREIGN SCHEMA nonesuch FROM SERVER nowhere INTO notthere;

-- Drop schemas
SET client_min_messages to WARNING;
DROP SCHEMA import_grid1 CASCADE;
DROP SCHEMA import_grid2 CASCADE;
SET client_min_messages to NOTICE;

-- ===================================================================
-- test partitionwise joins
-- ===================================================================
SET enable_partitionwise_join=on;

CREATE TABLE fprt1 (a int, b int, c text) PARTITION BY RANGE(a);
INSERT INTO "S 1".fprt1_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 2) i;
INSERT INTO "S 1".fprt1_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 2) i;
CREATE FOREIGN TABLE ftprt1_p1 PARTITION OF fprt1 FOR VALUES FROM (0) TO (250)
	SERVER griddb_svr OPTIONS (table_name 'fprt1_p1');
CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (250) TO (500)
	SERVER griddb_svr OPTIONS (TABLE_NAME 'fprt1_p2');

CREATE TABLE fprt2 (a int, b int, c text) PARTITION BY RANGE(b);
INSERT INTO "S 1".fprt2_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 3) i;
INSERT INTO "S 1".fprt2_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 3) i;
CREATE FOREIGN TABLE ftprt2_p1 (a int, b int, c text)
	SERVER griddb_svr OPTIONS (table_name 'fprt2_p1');
ALTER TABLE fprt2 ATTACH PARTITION ftprt2_p1 FOR VALUES FROM (0) TO (250);
CREATE FOREIGN TABLE ftprt2_p2 PARTITION OF fprt2 FOR VALUES FROM (250) TO (500)
	SERVER griddb_svr OPTIONS (table_name 'fprt2_p2');

-- inner join three tables
EXPLAIN (COSTS OFF)
SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;
SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;

-- left outer join + nullable clasue
EXPLAIN (COSTS OFF)
SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;
SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;

-- with whole-row reference; partitionwise join does not apply
EXPLAIN (COSTS OFF)
SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;
SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;

-- join with lateral reference
EXPLAIN (COSTS OFF)
SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;
SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;

-- with PHVs, partitionwise join selected but no join pushdown
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;

-- test FOR UPDATE; partitionwise join does not apply
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;
SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;

RESET enable_partitionwise_join;


-- ===================================================================
-- test partitionwise aggregates
-- ===================================================================

CREATE TABLE pagg_tab (t int, a int, b int, c text) PARTITION BY RANGE(a);

INSERT INTO "S 1".pagg_tab_p1 SELECT i, i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 10;
INSERT INTO "S 1".pagg_tab_p2 SELECT i, i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 20 and (i % 30) >= 10;
INSERT INTO "S 1".pagg_tab_p3 SELECT i, i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 30 and (i % 30) >= 20;

-- Create foreign partitions
CREATE FOREIGN TABLE fpagg_tab_p1 PARTITION OF pagg_tab FOR VALUES FROM (0) TO (10) SERVER griddb_svr OPTIONS (table_name 'pagg_tab_p1');
CREATE FOREIGN TABLE fpagg_tab_p2 PARTITION OF pagg_tab FOR VALUES FROM (10) TO (20) SERVER griddb_svr OPTIONS (table_name 'pagg_tab_p2');;
CREATE FOREIGN TABLE fpagg_tab_p3 PARTITION OF pagg_tab FOR VALUES FROM (20) TO (30) SERVER griddb_svr OPTIONS (table_name 'pagg_tab_p3');;

-- When GROUP BY clause matches with PARTITION KEY.
-- Plan with partitionwise aggregates is disabled
SET enable_partitionwise_aggregate TO false;
EXPLAIN (COSTS OFF)
SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- Plan with partitionwise aggregates is enabled
SET enable_partitionwise_aggregate TO true;
EXPLAIN (COSTS OFF)
SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;
SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- Check with whole-row reference
-- Should have all the columns in the target list for the given relation
EXPLAIN (VERBOSE, COSTS OFF)
SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;
SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- When GROUP BY clause does not match with PARTITION KEY.
EXPLAIN (COSTS OFF)
SELECT b, avg(a), max(a), count(*) FROM pagg_tab GROUP BY b HAVING sum(a) < 700 ORDER BY 1;


-- Clean-up
RESET enable_partitionwise_aggregate;
SET client_min_messages to WARNING;

DO $$ DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename);
    END LOOP;
END $$;

-- Drop all foreign tables
DROP USER MAPPING FOR public SERVER griddb_svr;
DROP USER MAPPING FOR public SERVER griddb_svr2;
DROP USER MAPPING FOR public SERVER testserver1;
DROP SERVER griddb_svr CASCADE;
DROP SERVER griddb_svr2 CASCADE;
DROP SERVER testserver1 CASCADE;
DROP EXTENSION griddb_fdw CASCADE;
SET client_min_messages to NOTICE;
