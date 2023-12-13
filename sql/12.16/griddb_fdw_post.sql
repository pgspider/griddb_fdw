\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--SET client_min_messages TO WARNING;
--Testcase 703:
CREATE EXTENSION IF NOT EXISTS griddb_fdw;

--Testcase 925:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw
    OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');
--Testcase 926:
CREATE SERVER griddb_svr2 FOREIGN DATA WRAPPER griddb_fdw
    OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');
--Testcase 927:
CREATE SERVER testserver1 FOREIGN DATA WRAPPER griddb_fdw;

--Testcase 704:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);
--Testcase 705:
CREATE USER MAPPING FOR public SERVER griddb_svr2 OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);
--Testcase 706:
CREATE USER MAPPING FOR public SERVER testserver1 OPTIONS (username 'value', password 'value');

--Testcase 928:
CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');
--Testcase 707:
CREATE SCHEMA "S 1";
IMPORT FOREIGN SCHEMA griddb_schema LIMIT TO
	("T0", "T1", "T2", "T3", "T4", ft1, ft2, ft4, ft5, base_tbl,
	loc1, loc2, loct, loct1, loct2, loct3, loct4, locp1, locp2,
	fprt1_p1, fprt1_p2, fprt2_p1, fprt2_p2, pagg_tab_p1, pagg_tab_p2, pagg_tab_p3)
	FROM SERVER griddb_svr INTO "S 1";
--SET client_min_messages to NOTICE;

-- GridDB containers must be created for this test on GridDB server
--Testcase 1:
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
--Testcase 2:
INSERT INTO "S 1"."T2"
	SELECT id,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 3:
INSERT INTO "S 1"."T3"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 708:
DELETE FROM "S 1"."T3" WHERE c1 % 2 != 0;	-- delete for outer join tests
--Testcase 4:
INSERT INTO "S 1"."T4"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 709:
DELETE FROM "S 1"."T4" WHERE c1 % 3 != 0;	-- delete for outer join tests

-- ===================================================================
-- create foreign tables
-- ===================================================================
--Testcase 710:
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

--Testcase 711:
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

--Testcase 712:
CREATE FOREIGN TABLE ft4 (
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	c3 text
) SERVER griddb_svr OPTIONS (table_name 'T3');

--Testcase 713:
CREATE FOREIGN TABLE ft5 (
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	c3 text
) SERVER griddb_svr OPTIONS (table_name 'T4');

--Testcase 714:
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
-- HINT: valid options in this context are: host, port, clustername, database,
-- notification_member, updatable, fdw_startup_cost, fdw_tuple_cost
--Testcase 929:
ALTER SERVER testserver1 OPTIONS (
	--use_remote_estimate 'false',
	updatable 'true',
	fdw_startup_cost '123.456',
	fdw_tuple_cost '0.123',
	--service 'value',
	--connect_timeout 'value',
	--dbname 'value',
	host 'value',
	--hostaddr 'value',
	port 'value',
	clustername 'value'
	--client_encoding 'value',
	--application_name 'value',
	--fallback_application_name 'value',
	--keepalives 'value',
	--keepalives_idle 'value',
	--keepalives_interval 'value',
	--tcp_user_timeout 'value',
	-- requiressl 'value',
	--sslcompression 'value',
	--sslmode 'value',
	--sslcert 'value',
	--sslkey 'value',
	--sslrootcert 'value',
	--sslcrl 'value',
	--requirepeer 'value',
	--krbsrvname 'value',
	--gsslib 'value'
	--replication 'value'
);
-- GridDB does not support 'extensions' option
-- Error, invalid list syntax
-- ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo; bar');

-- OK but gets a warning
-- ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo, bar');
-- ALTER SERVER testserver1 OPTIONS (DROP extensions);

--Testcase 930:
ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (DROP username, DROP password);

--Testcase 931:
ALTER FOREIGN TABLE ft1 OPTIONS (table_name 'T1');
--Testcase 932:
ALTER FOREIGN TABLE ft2 OPTIONS (table_name 'T1');
--Testcase 933:
ALTER FOREIGN TABLE ft1 ALTER COLUMN c1 OPTIONS (column_name 'C_1');
--Testcase 934:
ALTER FOREIGN TABLE ft2 ALTER COLUMN c1 OPTIONS (column_name 'C_1');

--Testcase 5:
\det+

-- skip does not support dbname
-- Test that alteration of server options causes reconnection
-- Remote's errors might be non-English, so hide them to ensure stable results
/*
\set VERBOSITY terse
--Testcase 6:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work
ALTER SERVER griddb_svr OPTIONS (SET dbname 'no such database');
--Testcase 7:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
DO $d$
    BEGIN
        EXECUTE $$ALTER SERVER griddb_svr
            OPTIONS (SET dbname '$$||current_database()||$$')$$;
    END;
$d$;
--Testcase 8:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again
*/

-- skip, does not support option 'user'
/*
-- Test that alteration of user mapping options causes reconnection
ALTER USER MAPPING FOR CURRENT_USER SERVER griddb_svr
  OPTIONS (ADD user 'no such user');
--Testcase 9:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback
  OPTIONS (DROP user);
--Testcase 10:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again
\set VERBOSITY default

-- Now we should be able to run ANALYZE.
-- To exercise multiple code paths, we use local stats on ft1
-- and remote-estimate mode on ft2.
ANALYZE ft1;
ALTER FOREIGN TABLE ft2 OPTIONS (use_remote_estimate 'true');
*/

-- ===================================================================
-- simple queries
-- ===================================================================
-- single table without alias
--Testcase 11:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
--Testcase 12:
SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
-- single table with alias - also test that tableoid sort is not pushed to remote side
--Testcase 13:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
--Testcase 14:
SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
-- whole-row reference
--Testcase 15:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 16:
SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- empty result
--Testcase 17:
SELECT * FROM ft1 WHERE false;
-- with WHERE clause
--Testcase 18:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6::char = '1' AND t1.c7::char >= '1';
--Testcase 19:
SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6::char = '1' AND t1.c7::char >= '1';
-- with FOR UPDATE/SHARE
--Testcase 20:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
--Testcase 21:
SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
--Testcase 22:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
--Testcase 23:
SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
-- aggregate
--Testcase 24:
SELECT COUNT(*) FROM ft1 t1;
-- subquery
--Testcase 25:
SELECT * FROM ft1 t1 WHERE t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 <= 10) ORDER BY c1;
-- subquery+MAX
--Testcase 26:
SELECT * FROM ft1 t1 WHERE t1.c3 = (SELECT MAX(c3) FROM ft2 t2) ORDER BY c1;
-- used in CTE
--Testcase 27:
WITH t1 AS (SELECT * FROM ft1 WHERE c1 <= 10) SELECT t2.c1, t2.c2, t2.c3, t2.c4 FROM t1, ft2 t2 WHERE t1.c1 = t2.c1 ORDER BY t1.c1;
-- fixed values
--Testcase 28:
SELECT 'fixed', NULL FROM ft1 t1 WHERE c1 = 1;
-- Test forcing the remote server to produce sorted data for a merge join.
--Testcase 935:
SET enable_hashjoin TO false;
--Testcase 936:
SET enable_nestloop TO false;
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 29:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."C_1" FROM ft2 t1 JOIN "S 1"."T1" t2 ON (t1.c1 = t2."C_1") OFFSET 100 LIMIT 10;
--Testcase 30:
SELECT t1.c1, t2."C_1" FROM ft2 t1 JOIN "S 1"."T1" t2 ON (t1.c1 = t2."C_1") OFFSET 100 LIMIT 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 31:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."C_1" FROM ft2 t1 LEFT JOIN "S 1"."T1" t2 ON (t1.c1 = t2."C_1") OFFSET 100 LIMIT 10;
--Testcase 32:
SELECT t1.c1, t2."C_1" FROM ft2 t1 LEFT JOIN "S 1"."T1" t2 ON (t1.c1 = t2."C_1") OFFSET 100 LIMIT 10;
-- A join between local table and foreign join. ORDER BY clause is added to the
-- foreign join so that the local table can be joined using merge join strategy.
--Testcase 33:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C_1" FROM "S 1"."T1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C_1") OFFSET 100 LIMIT 10;
--Testcase 34:
SELECT t1."C_1" FROM "S 1"."T1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C_1") OFFSET 100 LIMIT 10;
-- Test similar to above, except that the full join prevents any equivalence
-- classes from being merged. This produces single relation equivalence classes
-- included in join restrictions.
--Testcase 35:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C_1", t2.c1, t3.c1 FROM "S 1"."T1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C_1") OFFSET 100 LIMIT 10;
--Testcase 36:
SELECT t1."C_1", t2.c1, t3.c1 FROM "S 1"."T1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C_1") OFFSET 100 LIMIT 10;
-- Test similar to above with all full outer joins
--Testcase 37:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C_1", t2.c1, t3.c1 FROM "S 1"."T1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C_1") OFFSET 100 LIMIT 10;
--Testcase 38:
SELECT t1."C_1", t2.c1, t3.c1 FROM "S 1"."T1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C_1") OFFSET 100 LIMIT 10;
--Testcase 937:
RESET enable_hashjoin;
--Testcase 938:
RESET enable_nestloop;

-- ===================================================================
-- WHERE with remotely-executable conditions
-- ===================================================================
--Testcase 39:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 1;         -- Var, OpExpr(b), Const
--Testcase 40:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 100 AND t1.c2 = 0; -- BoolExpr
--Testcase 41:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NULL;        -- NullTest
--Testcase 42:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NOT NULL;    -- NullTest
--Testcase 43:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE round(abs(c1), 0) = 1; -- FuncExpr
--Testcase 44:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = -c1;          -- OpExpr(l)
--Testcase 45:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE 1 = c1!;           -- OpExpr(r)
--Testcase 46:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (c1 IS NOT NULL) IS DISTINCT FROM (c1 IS NOT NULL); -- DistinctExpr
--Testcase 47:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = ANY(ARRAY[c2, 1, c1 + 0]); -- ScalarArrayOpExpr
--Testcase 48:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = (ARRAY[c1,c2,3])[1]; -- SubscriptingRef
--Testcase 49:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c6 = E'foo''s\\bar';  -- check special chars
--Testcase 50:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c8 = 'foo';  -- can't be sent to remote
-- parameterized remote path for foreign table
--Testcase 51:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM "S 1"."T1" a, ft2 b WHERE a."C_1" = 47 AND b.c1 = a.c2;
--Testcase 52:
SELECT * FROM ft2 a, ft2 b WHERE a.c1 = 47 AND b.c1 = a.c2;

-- check both safe and unsafe join conditions
--Testcase 53:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft2 a, ft2 b
  WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
--Testcase 54:
SELECT * FROM ft2 a, ft2 b
WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
-- bug before 9.3.5 due to sloppy handling of remote-estimate parameters
--Testcase 55:
SELECT * FROM ft1 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft2 WHERE c1 < 5));
--Testcase 56:
SELECT * FROM ft2 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft1 WHERE c1 < 5));
-- we should not push order by clause with volatile expressions or unsafe
-- collations
--Testcase 57:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, random();
--Testcase 58:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, ft2.c3 collate "C";

-- user-defined operator/function
--Testcase 715:
CREATE FUNCTION griddb_fdw_abs(int) RETURNS int AS $$
BEGIN
RETURN abs($1);
END
$$ LANGUAGE plpgsql IMMUTABLE;
--Testcase 716:
CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===
);

-- built-in operators and functions can be shipped for remote execution
--Testcase 59:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 60:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 61:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;
--Testcase 62:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;

-- by default, user-defined ones cannot
--Testcase 63:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = griddb_fdw_abs(t1.c2);
--Testcase 64:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = griddb_fdw_abs(t1.c2);
--Testcase 65:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 66:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- ORDER BY can be shipped, though
--Testcase 67:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 68:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- but let's put them in an extension ...
--Testcase 939:
ALTER EXTENSION griddb_fdw ADD FUNCTION griddb_fdw_abs(int);
--Testcase 940:
ALTER EXTENSION griddb_fdw ADD OPERATOR === (int, int);

-- ... now they can be shipped
--Testcase 69:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = griddb_fdw_abs(t1.c2);
--Testcase 70:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = griddb_fdw_abs(t1.c2);
--Testcase 71:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 72:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- and both ORDER BY and LIMIT can be shipped
--Testcase 73:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 74:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- ===================================================================
-- JOIN queries
-- ===================================================================
-- Analyze ft4 and ft5 so that we have better statistics. These tables do not
-- have use_remote_estimate set.
--ANALYZE ft4;
--ANALYZE ft5;

-- join two tables
--Testcase 75:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 76:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join three tables
--Testcase 77:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 78:
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
-- left outer join
--Testcase 79:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 80:
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- left outer join three tables
--Testcase 81:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 82:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + placement of clauses.
-- clauses within the nullable side are not pulled up, but top level clause on
-- non-nullable side is pushed into non-nullable side
--Testcase 83:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
--Testcase 84:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
-- clauses within the nullable side are not pulled up, but the top level clause
-- on nullable side is not pushed down into nullable side
--Testcase 85:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
--Testcase 86:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
-- right outer join
--Testcase 87:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 88:
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
-- right outer join three tables
--Testcase 89:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 90:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join
--Testcase 91:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
--Testcase 92:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
-- full outer join with restrictions on the joining relations
-- a. the joining relations are both base relations
--Testcase 93:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 94:
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 95:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
--Testcase 96:
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
-- b. one of the joining relations is a base relation and the other is a join
-- relation
--Testcase 97:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 98:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- c. test deparsing the remote query as nested subqueries
--Testcase 99:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 100:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- d. test deparsing rowmarked relations as subqueries
--Testcase 101:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
--Testcase 102:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
-- full outer join + inner join
--Testcase 103:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
--Testcase 104:
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
-- full outer join three tables
--Testcase 105:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 106:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + right outer join
--Testcase 107:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 108:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- right outer join + full outer join
--Testcase 109:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 110:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + left outer join
--Testcase 111:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 112:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + full outer join
--Testcase 113:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 114:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- right outer join + left outer join
--Testcase 115:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 116:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + right outer join
--Testcase 117:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 118:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause, only matched rows
--Testcase 119:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 120:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause with shippable extensions set
--Testcase 121:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE griddb_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
--ALTER SERVER griddb_svr OPTIONS (DROP extensions);
-- full outer join + WHERE clause with shippable extensions not set
--Testcase 717:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE griddb_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
--ALTER SERVER griddb_svr OPTIONS (ADD extensions 'griddb_fdw');
-- join two tables with FOR UPDATE clause
-- tests whole-row reference for row marks
--Testcase 122:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 123:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 124:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
-- Skip test case: Relate #112
--SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
-- join two tables with FOR SHARE clause
--Testcase 125:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 126:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 127:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
-- Skip test case: Relate #112
--SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
-- join in CTE
--Testcase 128:
EXPLAIN (VERBOSE, COSTS OFF)
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
--Testcase 129:
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
-- ctid with whole-row reference
--Testcase 130:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.ctid, t1, t2, t1.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- SEMI JOIN, not pushed down
--Testcase 131:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 132:
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- ANTI JOIN, not pushed down
--Testcase 133:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 134:
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- CROSS JOIN, not pushed down
--Testcase 135:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 136:
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- different server, not pushed down. No result expected.
--Testcase 137:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 138:
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe join conditions (c8 has a UDT), not pushed down. Practically a CROSS
-- JOIN since c8 in both tables has same value.
--Testcase 139:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 140:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe conditions on one side (c8 has a UDT), not pushed down.
--Testcase 141:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 142:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join where unsafe to pushdown condition in WHERE clause has a column not
-- in the SELECT clause. In this test unsafe clause needs to have column
-- references from both joining sides so that the clause is not pushed down
-- into one of the joining sides.
--Testcase 143:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 144:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- Aggregate after UNION, for testing setrefs
--Testcase 145:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
--Testcase 146:
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
-- join with lateral reference
--Testcase 147:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1."C_1" FROM "S 1"."T1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C_1" OFFSET 10 LIMIT 10;
--Testcase 148:
SELECT t1."C_1" FROM "S 1"."T1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C_1" OFFSET 10 LIMIT 10;

-- non-Var items in targetlist of the nullable rel of a join preventing
-- push-down in some cases
-- unable to push {ft1, ft2}
--Testcase 149:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;
--Testcase 150:
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;

-- ok to push {ft1, ft2} but not {ft1, ft2, ft4}
--Testcase 151:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;
--Testcase 152:
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;

-- join with nullable side with some columns with null values
--Testcase 153:
UPDATE ft5 SET c3 = null where c1 % 9 = 0;
--Testcase 154:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;
--Testcase 155:
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;

-- multi-way join involving multiple merge joins
-- (this case used to have EPQ-related planning problems)
--Testcase 718:
CREATE FOREIGN TABLE local_tbl (c1 int OPTIONS(rowkey 'true'), c2 int, c3 text) SERVER griddb_svr;
--Testcase 156:
INSERT INTO local_tbl(c1, c2, c3) SELECT id, id % 10, to_char(id, 'FM0000') FROM generate_series(1, 1000) id;
--ANALYZE local_tbl;
--Testcase 941:
SET enable_nestloop TO false;
--Testcase 942:
SET enable_hashjoin TO false;
--Testcase 157:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 FOR UPDATE;
-- Skip test case: Relate #112
--SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
--  AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 FOR UPDATE;
--Testcase 943:
RESET enable_nestloop;
--Testcase 944:
RESET enable_hashjoin;
--Testcase 945:
DROP FOREIGN TABLE local_tbl;
-- check join pushdown in situations where multiple userids are involved
--Testcase 719:
CREATE ROLE regress_view_owner SUPERUSER;
--Testcase 720:
CREATE USER MAPPING FOR regress_view_owner SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);
GRANT SELECT ON ft4 TO regress_view_owner;
GRANT SELECT ON ft5 TO regress_view_owner;

--Testcase 721:
CREATE VIEW v4 AS SELECT * FROM ft4;
--Testcase 722:
CREATE VIEW v5 AS SELECT * FROM ft5;
--Testcase 946:
ALTER VIEW v5 OWNER TO regress_view_owner;
--Testcase 158:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, different view owners
--Testcase 159:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 947:
ALTER VIEW v4 OWNER TO regress_view_owner;
--Testcase 160:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 161:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

--Testcase 162:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, view owner not current user
--Testcase 163:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 948:
ALTER VIEW v4 OWNER TO CURRENT_USER;
--Testcase 164:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 165:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 949:
ALTER VIEW v4 OWNER TO regress_view_owner;

-- cleanup
--Testcase 723:
DROP OWNED BY regress_view_owner;
--Testcase 724:
DROP ROLE regress_view_owner;


-- ===================================================================
-- Aggregate and grouping queries
-- ===================================================================

-- Simple aggregates
--Testcase 166:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;
--Testcase 167:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;

--Testcase 168:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;
--Testcase 169:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;

-- Aggregate is not pushed down as aggregation contains random()
--Testcase 170:
explain (verbose, costs off)
select sum(c1 * (random() <= 1)::int) as sum, avg(c1) from ft1;

-- Aggregate over join query
--Testcase 171:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;
--Testcase 172:
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;

-- Not pushed down due to local conditions present in underneath input rel
--Testcase 173:
explain (verbose, costs off)
select sum(t1.c1), count(t2.c1) from ft1 t1 inner join ft2 t2 on (t1.c1 = t2.c1) where ((t1.c1 * t2.c1)/(t1.c1 * t2.c1)) * random() <= 1;

-- GROUP BY clause having expressions
--Testcase 174:
explain (verbose, costs off)
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;
--Testcase 175:
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;

-- Aggregates in subquery are pushed down.
--Testcase 176:
explain (verbose, costs off)
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;
--Testcase 177:
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;

-- Aggregate is still pushed down by taking unshippable expression out
--Testcase 178:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;
--Testcase 179:
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed
--Testcase 180:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as c2 from ft2 group by c2 * (random() <= 1)::int order by 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression
--Testcase 181:
explain (verbose, costs off)
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;
--Testcase 182:
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;

-- GROUP BY clause referring to same column multiple times
-- Also, ORDER BY contains an aggregate function
--Testcase 183:
explain (verbose, costs off)
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);
--Testcase 184:
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);

-- Testing HAVING clause shippability
--Testcase 185:
explain (verbose, costs off)
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;
--Testcase 186:
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;

-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down
--Testcase 187:
explain (verbose, costs off)
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;
--Testcase 188:
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;

-- Aggregate in HAVING clause is not pushable, and thus aggregation is not pushed down
--Testcase 189:
explain (verbose, costs off)
select sum(c1) from ft1 group by c2 having avg(c1 * (random() <= 1)::int) > 100 order by 1;

-- GridDB does not create type user_enum so pg_enum table has no record.
-- Remote aggregate in combination with a local Param (for the output
-- of an initplan) can be trouble, per bug #15781
--Testcase 190:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1;
--Testcase 191:
select exists(select 1 from pg_enum), sum(c1) from ft1;

--Testcase 192:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;
--Testcase 193:
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;


-- Testing ORDER BY, DISTINCT, FILTER, Ordered-sets and VARIADIC within aggregates

-- ORDER BY within aggregate, same column used to order
--Testcase 194:
explain (verbose, costs off)
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;
--Testcase 195:
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;

-- ORDER BY within aggregate, different column used to order also using DESC
--Testcase 196:
explain (verbose, costs off)
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;
--Testcase 197:
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;

-- DISTINCT within aggregate
--Testcase 198:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 199:
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- DISTINCT combined with ORDER BY within aggregate
--Testcase 200:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 201:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

--Testcase 202:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 203:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- FILTER within aggregate
--Testcase 204:
explain (verbose, costs off)
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;
--Testcase 205:
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;

-- DISTINCT, ORDER BY and FILTER within aggregate
--Testcase 206:
explain (verbose, costs off)
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;
--Testcase 207:
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;

-- Outer query is aggregation query
--Testcase 208:
explain (verbose, costs off)
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 209:
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
-- Inner query is aggregation query
--Testcase 210:
explain (verbose, costs off)
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 211:
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;

-- Aggregate not pushed down as FILTER condition is not pushable
--Testcase 212:
explain (verbose, costs off)
select sum(c1) filter (where (c1 / c1) * random() <= 1) from ft1 group by c2 order by 1;
--Testcase 213:
explain (verbose, costs off)
select sum(c2) filter (where c2 in (select c2 from ft1 where c2 < 5)) from ft1;

-- Ordered-sets within aggregate
--Testcase 214:
explain (verbose, costs off)
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;
--Testcase 215:
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;

-- Using multiple arguments within aggregates
--Testcase 216:
explain (verbose, costs off)
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;
--Testcase 217:
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;

-- User defined function for user defined aggregate, VARIADIC
--Testcase 725:
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';
--Testcase 726:
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);

-- Disable hash aggregation for plan stability.
--Testcase 950:
set enable_hashagg to false;

-- Not pushed down due to user defined aggregate
--Testcase 218:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Add function and aggregate into extension
--Testcase 951:
alter extension griddb_fdw add function least_accum(anyelement, variadic anyarray);
--Testcase 952:
alter extension griddb_fdw add aggregate least_agg(variadic items anyarray);
--alter server griddb_svr options (set extensions 'griddb_fdw');

-- Now aggregate will be pushed.  Aggregate will display VARIADIC argument.
--Testcase 219:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;
--Testcase 220:
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;

-- Remove function and aggregate from extension
--Testcase 953:
alter extension griddb_fdw drop function least_accum(anyelement, variadic anyarray);
--Testcase 954:
alter extension griddb_fdw drop aggregate least_agg(variadic items anyarray);
--alter server griddb_svr options (set extensions 'griddb_fdw');

-- Not pushed down as we have dropped objects from extension.
--Testcase 221:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Cleanup
--Testcase 955:
reset enable_hashagg;
--Testcase 727:
drop aggregate least_agg(variadic items anyarray);
--Testcase 728:
drop function least_accum(anyelement, variadic anyarray);


-- Testing USING OPERATOR() in ORDER BY within aggregate.
-- For this, we need user defined operators along with operator family and
-- operator class.  Create those and then add them in extension.  Note that
-- user defined objects are considered unshippable unless they are part of
-- the extension.
--Testcase 729:
create operator public.<^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4eq
);

--Testcase 730:
create operator public.=^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4lt
);

--Testcase 731:
create operator public.>^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4gt
);

--Testcase 732:
create operator family my_op_family using btree;

--Testcase 733:
create function my_op_cmp(a int, b int) returns int as
  $$begin return btint4cmp(a, b); end $$ language plpgsql;

--Testcase 734:
create operator class my_op_class for type int using btree family my_op_family as
 operator 1 public.<^,
 operator 3 public.=^,
 operator 5 public.>^,
 function 1 my_op_cmp(int, int);

-- This will not be pushed as user defined sort operator is not part of the
-- extension yet.
--Testcase 222:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Update local stats on ft2
--ANALYZE ft2;

-- Add into extension
--Testcase 956:
alter extension griddb_fdw add operator class my_op_class using btree;
--Testcase 957:
alter extension griddb_fdw add function my_op_cmp(a int, b int);
--Testcase 958:
alter extension griddb_fdw add operator family my_op_family using btree;
--Testcase 959:
alter extension griddb_fdw add operator public.<^(int, int);
--Testcase 960:
alter extension griddb_fdw add operator public.=^(int, int);
--Testcase 961:
alter extension griddb_fdw add operator public.>^(int, int);
--alter server griddb_svr options (set extensions 'griddb_fdw');

-- Now this will be pushed as sort operator is part of the extension.
--Testcase 223:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;
--Testcase 224:
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Remove from extension
--Testcase 962:
alter extension griddb_fdw drop operator class my_op_class using btree;
--Testcase 963:
alter extension griddb_fdw drop function my_op_cmp(a int, b int);
--Testcase 964:
alter extension griddb_fdw drop operator family my_op_family using btree;
--Testcase 965:
alter extension griddb_fdw drop operator public.<^(int, int);
--Testcase 966:
alter extension griddb_fdw drop operator public.=^(int, int);
--Testcase 967:
alter extension griddb_fdw drop operator public.>^(int, int);

-- This will not be pushed as sort operator is now removed from the extension.
--Testcase 225:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Cleanup
--Testcase 735:
drop operator class my_op_class using btree;
--Testcase 736:
drop function my_op_cmp(a int, b int);
--Testcase 737:
drop operator family my_op_family using btree;
--Testcase 738:
drop operator public.>^(int, int);
--Testcase 739:
drop operator public.=^(int, int);
--Testcase 740:
drop operator public.<^(int, int);

-- Input relation to aggregate push down hook is not safe to pushdown and thus
-- the aggregate cannot be pushed down to foreign server.
--Testcase 226:
explain (verbose, costs off)
select count(t1.c3) from ft2 t1 left join ft2 t2 on (t1.c1 = random() * t2.c2);

-- Subquery in FROM clause having aggregate
--Testcase 227:
explain (verbose, costs off)
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;
--Testcase 228:
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;

-- FULL join with IS NULL check in HAVING
--Testcase 229:
explain (verbose, costs off)
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;
--Testcase 230:
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;

-- Aggregate over FULL join needing to deparse the joining relations as
-- subqueries.
--Testcase 231:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);
--Testcase 232:
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.
--Testcase 233:
explain (verbose, costs off)
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;
--Testcase 234:
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;

-- LATERAL join, with parameterization
--Testcase 968:
set enable_hashagg to false;
--Testcase 235:
explain (verbose, costs off)
select c2, sum from "S 1"."T1" t1, lateral (select sum(t2.c1 + t1."C_1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."C_1" < 100 order by 1;
--Testcase 236:
select c2, sum from "S 1"."T1" t1, lateral (select sum(t2.c1 + t1."C_1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."C_1" < 100 order by 1;
--Testcase 969:
reset enable_hashagg;

-- bug #15613: bad plan for foreign table scan with lateral reference
--Testcase 237:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ref_0.c2, subq_1.*
FROM
    "S 1"."T1" AS ref_0,
    LATERAL (
        SELECT ref_0."C_1", subq_0.*
        FROM (SELECT ref_0.c2, ref_1.c3
              FROM ft1 AS ref_1) AS subq_0
             RIGHT JOIN ft2 AS ref_3 ON (subq_0.c3 = ref_3.c3)
    ) AS subq_1
WHERE ref_0."C_1" < 10 AND subq_1.c3 = '00001'
ORDER BY ref_0."C_1";

--Testcase 238:
SELECT ref_0.c2, subq_1.*
FROM
    "S 1"."T1" AS ref_0,
    LATERAL (
        SELECT ref_0."C_1", subq_0.*
        FROM (SELECT ref_0.c2, ref_1.c3
              FROM ft1 AS ref_1) AS subq_0
             RIGHT JOIN ft2 AS ref_3 ON (subq_0.c3 = ref_3.c3)
    ) AS subq_1
WHERE ref_0."C_1" < 10 AND subq_1.c3 = '00001'
ORDER BY ref_0."C_1";

-- Check with placeHolderVars
--Testcase 239:
explain (verbose, costs off)
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);
--Testcase 240:
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);


-- Not supported cases
-- Grouping sets
--Testcase 241:
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
--Testcase 242:
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
--Testcase 243:
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
--Testcase 244:
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
--Testcase 245:
explain (verbose, costs off)
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
--Testcase 246:
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
--Testcase 247:
explain (verbose, costs off)
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;
--Testcase 248:
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;

-- DISTINCT itself is not pushed down, whereas underneath aggregate is pushed
--Testcase 249:
explain (verbose, costs off)
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;
--Testcase 250:
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;

-- WindowAgg
--Testcase 251:
explain (verbose, costs off)
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
--Testcase 252:
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
--Testcase 253:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 254:
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 255:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 256:
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;


-- ===================================================================
-- parameterized queries
-- ===================================================================
-- simple join
--Testcase 257:
PREPARE st1(int, int) AS SELECT t1.c3, t2.c3 FROM ft1 t1, ft2 t2 WHERE t1.c1 = $1 AND t2.c1 = $2;
--Testcase 258:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st1(1, 2);
--Testcase 259:
EXECUTE st1(1, 1);
--Testcase 260:
EXECUTE st1(101, 101);
-- subquery using stable function (can't be sent to remote)
--Testcase 261:
PREPARE st2(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c4) = '1970-01-17'::date) ORDER BY c1;
--Testcase 262:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st2(10, 20);
--Testcase 263:
EXECUTE st2(10, 20);
--Testcase 264:
EXECUTE st2(101, 121);
-- subquery using immutable function (can be sent to remote)
--Testcase 265:
PREPARE st3(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c5) = '1970-01-17'::date) ORDER BY c1;
--Testcase 266:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st3(10, 20);
--Testcase 267:
EXECUTE st3(10, 20);
--Testcase 268:
EXECUTE st3(20, 30);
-- custom plan should be chosen initially
--Testcase 269:
PREPARE st4(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 = $1;
--Testcase 270:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 271:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 272:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 273:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 274:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- once we try it enough times, should switch to generic plan
--Testcase 275:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- value of $1 should not be sent to remote
--Testcase 276:
PREPARE st5(text,int) AS SELECT * FROM ft1 t1 WHERE c8 = $1 and c1 = $2;
--Testcase 277:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 278:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 279:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 280:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 281:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 282:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 283:
EXECUTE st5('foo', 1);

-- altering FDW options requires replanning
--Testcase 284:
PREPARE st6 AS SELECT * FROM ft1 t1 WHERE t1.c1 = t1.c2;
--Testcase 285:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
--Testcase 286:
PREPARE st7 AS INSERT INTO ft1 (c1,c2,c3) VALUES (1001,101,'foo');
--Testcase 287:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
--Testcase 741:
INSERT INTO "S 1"."T0" SELECT * FROM "S 1"."T1";
--Testcase 970:
ALTER FOREIGN TABLE ft1 OPTIONS (SET table_name 'T0');
--Testcase 288:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
--Testcase 289:
EXECUTE st6;
--Testcase 290:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
--Testcase 742:
DELETE FROM "S 1"."T0";
--Testcase 971:
ALTER FOREIGN TABLE ft1 OPTIONS (SET table_name 'T1');

--Testcase 294:
PREPARE st8 AS SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--ALTER SERVER loopback OPTIONS (DROP extensions);
--Testcase 295:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
--Testcase 296:
EXECUTE st8;
--ALTER SERVER loopback OPTIONS (ADD extensions 'griddb_fdw');

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
--Testcase 297:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'pg_class'::regclass LIMIT 1;
--Testcase 298:
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'ft1'::regclass LIMIT 1;
--Testcase 299:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
--Testcase 300:
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
--Testcase 301:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
-- ctid cannot be pushed down, so the result is empty
--Testcase 302:
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
--Testcase 303:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, * FROM ft1 t1 LIMIT 1;
--Testcase 304:
SELECT ctid, * FROM ft1 t1 LIMIT 1;

-- ===================================================================
-- used in PL/pgSQL function
-- ===================================================================
--Testcase 743:
CREATE OR REPLACE FUNCTION f_test(p_c1 int) RETURNS int AS $$
DECLARE
	v_c1 int;
BEGIN
--Testcase 744:
    SELECT c1 INTO v_c1 FROM ft1 WHERE c1 = p_c1 LIMIT 1;
    PERFORM c1 FROM ft1 WHERE c1 = p_c1 AND p_c1 = v_c1 LIMIT 1;
    RETURN v_c1;
END;
$$ LANGUAGE plpgsql;
--Testcase 305:
SELECT f_test(100);
--Testcase 745:
DROP FUNCTION f_test(int);

-- ===================================================================
-- conversion error
-- ===================================================================
--Testcase 972:
ALTER FOREIGN TABLE ft1 ALTER COLUMN c8 TYPE int;
--Testcase 306:
SELECT * FROM ft1 WHERE c1 = 1;  -- ERROR
--Testcase 307:
SELECT  ft1.c1, ft2.c2, ft1.c8 FROM ft1, ft2 WHERE ft1.c1 = ft2.c1 AND ft1.c1 = 1; -- ERROR
--Testcase 308:
SELECT  ft1.c1, ft2.c2, ft1 FROM ft1, ft2 WHERE ft1.c1 = ft2.c1 AND ft1.c1 = 1; -- ERROR
--Testcase 309:
SELECT sum(c2), array_agg(c8) FROM ft1 GROUP BY c8; -- ERROR
--Testcase 973:
ALTER FOREIGN TABLE ft1 ALTER COLUMN c8 TYPE text;

-- ===================================================================
-- subtransaction
--  + local/remote error doesn't break cursor
-- ===================================================================
BEGIN;
DECLARE c CURSOR FOR SELECT * FROM ft1 ORDER BY c1;
--Testcase 310:
FETCH c;
SAVEPOINT s;        -- Not support
ERROR OUT;
ROLLBACK TO s;
--Testcase 311:
FETCH c;
SAVEPOINT s;
--Testcase 312:
SELECT * FROM ft1 WHERE 1 / (c1 - 1) > 0;  -- ERROR
ROLLBACK TO s;
--Testcase 313:
FETCH c;
--Testcase 314:
SELECT * FROM ft1 ORDER BY c1 LIMIT 1;
COMMIT;

-- ===================================================================
-- test handling of collations
-- ===================================================================
--Testcase 746:
create foreign table loct3 (
	f1 text OPTIONS (rowkey 'true'), 
	f2 text, 
	f3 text OPTIONS (rowkey 'true'))
  server griddb_svr;
--Testcase 747:
create foreign table ft3 (
	f1 text OPTIONS (rowkey 'true'), 
	f2 text, 
	f3 text OPTIONS (rowkey 'true'))
  server griddb_svr options (table_name 'loct3');

-- can be sent to remote
--Testcase 315:
explain (verbose, costs off) select * from ft3 where f1 = 'foo';
--Testcase 748:
explain (verbose, costs off) select * from ft3 where f1 COLLATE "C" = 'foo';
--Testcase 316:
explain (verbose, costs off) select * from ft3 where f2 = 'foo';
--Testcase 317:
explain (verbose, costs off) select * from ft3 where f3 = 'foo';
--Testcase 749:
explain (verbose, costs off) select * from ft3 f, loct3 l
  where f.f3 = l.f3 and l.f1 = 'foo';
-- can't be sent to remote
--Testcase 319:
explain (verbose, costs off) select * from ft3 where f1 COLLATE "POSIX" = 'foo';
--Testcase 320:
explain (verbose, costs off) select * from ft3 where f1 = 'foo' COLLATE "C";
--Testcase 321:
explain (verbose, costs off) select * from ft3 where f2 COLLATE "C" = 'foo';
--Testcase 322:
explain (verbose, costs off) select * from ft3 where f2 = 'foo' COLLATE "C";
--Testcase 750:
explain (verbose, costs off) select * from ft3 f, loct3 l
  where f.f3 = l.f3 COLLATE "POSIX" and l.f1 = 'foo';

-- ===================================================================
-- test writable foreign table stuff
-- ===================================================================
--Testcase 323:
EXPLAIN (verbose, costs off)
INSERT INTO ft2 (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;
--Testcase 324:
INSERT INTO ft2 (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;
-- RETURNING is not supported by GridDB. Use SELECT instead.
--Testcase 325:
INSERT INTO ft2 (c1,c2,c3)
  VALUES (1101,201,'aaa'), (1102,202,'bbb'), (1103,203,'ccc');
--Testcase 326:
SELECT * FROM ft2 WHERE c1 > 1100 AND c1 < 1104;
--Testcase 327:
INSERT INTO ft2 (c1,c2,c3) VALUES (1104,204,'ddd'), (1105,205,'eee');
--Testcase 328:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;              -- can be pushed down
--Testcase 329:
UPDATE ft2 SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;
--Testcase 330:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7;              -- can be pushed down
--Testcase 331:
UPDATE ft2 SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7;
--Testcase 332:
SELECT * FROM ft2 WHERE c1 % 10 = 7;
--Testcase 333:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c2 = ft2.c2 + 500, c3 = ft2.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 9;                               -- can be pushed down
--Testcase 334:
UPDATE ft2 SET c2 = ft2.c2 + 500, c3 = ft2.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 9;
--Testcase 335:
EXPLAIN (verbose, costs off)
  DELETE FROM ft2 WHERE c1 % 10 = 5;                                                -- can be pushed down
--Testcase 336:
SELECT c1,c4 FROM ft2 WHERE c1 % 10 = 5;
--Testcase 337:
DELETE FROM ft2 WHERE c1 % 10 = 5;

--Testcase 338:
EXPLAIN (verbose, costs off)
DELETE FROM ft2 USING ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 2;                -- can be pushed down
--Testcase 339:
DELETE FROM ft2 USING ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 2;
--Testcase 340:
SELECT c1,c2,c3,c4 FROM ft2 ORDER BY c1;
--Testcase 341:
EXPLAIN (verbose, costs off)
INSERT INTO ft2 (c1,c2,c3) VALUES (1200,999,'foo');
--Testcase 342:
INSERT INTO ft2 (c1,c2,c3) VALUES (1200,999,'foo');
--Testcase 343:
SELECT tableoid::regclass FROM ft2 WHERE c1 = 1200;
--Testcase 344:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c3 = 'bar' WHERE c1 = 1200;                                          -- can be pushed down
--Testcase 345:
UPDATE ft2 SET c3 = 'bar' WHERE c1 = 1200;
--Testcase 346:
SELECT tableoid::regclass FROM ft2 WHERE c1 = 1200;
--Testcase 347:
EXPLAIN (verbose, costs off)
DELETE FROM ft2 WHERE c1 = 1200;                                                    -- can be pushed down
--Testcase 348:
SELECT tableoid::regclass FROM ft2 WHERE c1 = 1200;
--Testcase 349:
DELETE FROM ft2 WHERE c1 = 1200;

-- Test UPDATE/DELETE with RETURNING on a three-table join
--Testcase 350:
INSERT INTO ft2 (c1,c2,c3)
  SELECT id, id - 1200, to_char(id, 'FM00000') FROM generate_series(1201, 1300) id;
--Testcase 351:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 1200 AND ft2.c2 = ft4.c1;
--Testcase 352:
UPDATE ft2 SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 1200 AND ft2.c2 = ft4.c1;
--Testcase 353:
SELECT ft2, ft2.*, ft4, ft4.* FROM ft2, ft4 WHERE ft2.c1 > 1200 AND ft2.c2 = ft4.c1 AND ft2.c3 = 'foo';
--Testcase 354:
EXPLAIN (verbose, costs off)
DELETE FROM ft2
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 1200 AND ft2.c1 % 10 = 0 AND ft2.c2 = ft4.c1;
--Testcase 355:
SELECT 100 FROM ft2, ft4 WHERE ft2.c1 > 1200 AND ft2.c1 % 10 = 0 AND ft2.c2 = ft4.c1;
--Testcase 356:
DELETE FROM ft2 
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 1200 AND ft2.c1 % 10 = 0 AND ft2.c2 = ft4.c1;
--Testcase 357:
DELETE FROM ft2 WHERE ft2.c1 > 1200;

-- Test UPDATE with a MULTIEXPR sub-select
-- (maybe someday this'll be remotely executable, but not today)
--Testcase 751:
EXPLAIN (verbose, costs off)
UPDATE ft2 AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;
--Testcase 752:
UPDATE ft2 AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

--Testcase 753:
UPDATE ft2 AS target SET (c2) = (
    SELECT c2 / 10
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

-- Test UPDATE/DELETE with WHERE or JOIN/ON conditions containing
-- user-defined operators/functions
--ALTER SERVER loopback OPTIONS (DROP extensions);
--Testcase 358:
INSERT INTO ft2 (c1,c2,c3)
  SELECT id, id % 10, to_char(id, 'FM00000') FROM generate_series(2001, 2010) id;
--Testcase 359:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c3 = 'bar' WHERE griddb_fdw_abs(c1) > 2000;                          -- can't be pushed down
--Testcase 360:
UPDATE ft2 SET c3 = 'bar' WHERE griddb_fdw_abs(c1) > 2000;
--Testcase 361:
SELECT * FROM ft2 WHERE griddb_fdw_abs(c1) > 2000;
--Testcase 362:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 === ft4.c1;                                        -- can't be pushed down
--Testcase 363:
UPDATE ft2 SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 === ft4.c1;
--Testcase 364:
SELECT ft2.*, ft4.*, ft5.* FROM ft2
  INNER JOIN ft4 ON (ft2.c1 > 2000 AND ft2.c2 === ft4.c1)
  INNER JOIN ft5 ON (ft4.c1 = ft5.c1);
--Testcase 365:
EXPLAIN (verbose, costs off)
DELETE FROM ft2
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 = ft4.c1;                                          -- can't be pushed down
--Testcase 366:
SELECT ft2.c1, ft2.c2, ft2.c3 FROM ft2
  INNER JOIN ft4 ON (ft2.c1 > 2000 AND ft2.c2 = ft4.c1)
  INNER JOIN ft5 ON (ft4.c1 === ft5.c1);
--Testcase 367:
DELETE FROM ft2
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 = ft4.c1;
--Testcase 368:
DELETE FROM ft2 WHERE ft2.c1 > 2000;
--ALTER SERVER loopback OPTIONS (ADD extensions 'griddb_fdw');

-- Test that trigger on remote table works as expected
--Testcase 754:
CREATE OR REPLACE FUNCTION "S 1".F_BRTRIG() RETURNS trigger AS $$
BEGIN
    NEW.c3 = NEW.c3 || '_trig_update';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--Testcase 755:
CREATE TRIGGER t1_br_insert BEFORE INSERT OR UPDATE
    ON ft2 FOR EACH ROW EXECUTE PROCEDURE "S 1".F_BRTRIG();

--Testcase 369:
INSERT INTO ft2 (c1,c2,c3) VALUES (1208, 818, 'fff');
--Testcase 370:
SELECT * FROM ft2 WHERE c1 = 1208;
--Testcase 371:
INSERT INTO ft2 (c1,c2,c3,c6) VALUES (1218, 818, 'ggg', '(--;');
--Testcase 372:
SELECT * FROM ft2 WHERE c1 = 1218;
--Testcase 373:
UPDATE ft2 SET c2 = c2 + 600 WHERE c1 % 10 = 8 AND c1 < 1200;
--Testcase 374:
SELECT * FROM ft2 WHERE c1 % 10 = 8 AND c1 < 1200;

--Testcase 974:
DROP TRIGGER t1_br_insert ON ft2;

-- Test errors thrown on remote side during update
--Testcase 975:
ALTER FOREIGN TABLE ft1 ADD CONSTRAINT c2positive CHECK (c2 >= 0);

-- row was updated instead of insert because same row key has already existed.
--Testcase 375:
--INSERT INTO ft1(c1, c2) VALUES(11, 12);
-- ON CONFLICT is not suported
--Testcase 376:
INSERT INTO ft1(c1, c2) VALUES(11, 12) ON CONFLICT DO NOTHING; -- not supported
--Testcase 377:
INSERT INTO ft1(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO NOTHING; -- unsupported
--Testcase 378:
INSERT INTO ft1(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO UPDATE SET c3 = 'ffg'; -- unsupported
-- GridDB not support constraints
--Testcase 379:
--INSERT INTO ft1(c1, c2) VALUES(1111, -2);  -- c2positive
--Testcase 380:
--UPDATE ft1 SET c2 = -c2 WHERE c1 = 1;  -- c2positive

-- Test savepoint/rollback behavior
--Testcase 381:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
--Testcase 382:
select c2, count(*) from "S 1"."T1" where c2 < 500 group by 1 order by 1;
begin;
--Testcase 383:
update ft2 set c2 = 42 where c2 = 0;
--Testcase 384:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s1;
--Testcase 385:
update ft2 set c2 = 44 where c2 = 4;
--Testcase 386:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s1;
--Testcase 387:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s2;
--Testcase 388:
update ft2 set c2 = 46 where c2 = 6;
--Testcase 389:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
rollback to savepoint s2;
-- savepoint not supported.
--Testcase 756:
update ft2 set c2 = 6 where c2 = 46;
--Testcase 390:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s2;
--Testcase 391:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s3;
--Testcase 392:
-- GridDB not support constraints
--update ft2 set c2 = -2 where c2 = 42 and c1 = 10; -- fail on remote side
rollback to savepoint s3;
--Testcase 393:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s3;
--Testcase 394:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- none of the above is committed yet remotely
--Testcase 395:
select c2, count(*) from "S 1"."T1" where c2 < 500 group by 1 order by 1;
commit;
--Testcase 396:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
--Testcase 397:
select c2, count(*) from "S 1"."T1" where c2 < 500 group by 1 order by 1;

--VACUUM ANALYZE "S 1"."T 1";

-- Above DMLs add data with c6 as NULL in ft1, so test ORDER BY NULLS LAST and NULLs
-- FIRST behavior here.
-- ORDER BY DESC NULLS LAST options
--Testcase 398:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795 LIMIT 10;
--Testcase 399:
SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795  LIMIT 10;
-- ORDER BY DESC NULLS FIRST options
--Testcase 400:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
--Testcase 401:
SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
-- ORDER BY ASC NULLS FIRST options
--Testcase 402:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
--Testcase 403:
SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;

/*
-- GridDB not support constraints
-- ===================================================================
-- test check constraints
-- ===================================================================
-- Consistent check constraints provide consistent results
ALTER FOREIGN TABLE ft1 ADD CONSTRAINT ft1_c2positive CHECK (c2 >= 0);
--Testcase 404:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 405:
SELECT count(*) FROM ft1 WHERE c2 < 0;
SET constraint_exclusion = 'on';
--Testcase 406:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 407:
SELECT count(*) FROM ft1 WHERE c2 < 0;
RESET constraint_exclusion;
-- check constraint is enforced on the remote side, not locally
--Testcase 408:
INSERT INTO ft1(c1, c2) VALUES(1111, -2);  -- c2positive
--Testcase 409:
UPDATE ft1 SET c2 = -c2 WHERE c1 = 1;  -- c2positive
ALTER FOREIGN TABLE ft1 DROP CONSTRAINT ft1_c2positive;

-- But inconsistent check constraints provide inconsistent results
ALTER FOREIGN TABLE ft1 ADD CONSTRAINT ft1_c2negative CHECK (c2 < 0);
--Testcase 410:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 411:
SELECT count(*) FROM ft1 WHERE c2 >= 0;
SET constraint_exclusion = 'on';
--Testcase 412:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 413:
SELECT count(*) FROM ft1 WHERE c2 >= 0;
RESET constraint_exclusion;
-- local check constraint is not actually enforced
--Testcase 414:
INSERT INTO ft1(c1, c2) VALUES(1111, 2);
--Testcase 415:
UPDATE ft1 SET c2 = c2 + 1 WHERE c1 = 1;
ALTER FOREIGN TABLE ft1 DROP CONSTRAINT ft1_c2negative;
*/

-- ===================================================================
-- test WITH CHECK OPTION constraints
-- ===================================================================

--Testcase 757:
CREATE FUNCTION row_before_insupd_trigfunc() RETURNS trigger AS $$BEGIN NEW.a := NEW.a + 10; RETURN NEW; END$$ LANGUAGE plpgsql;

--Testcase 758:
CREATE FOREIGN TABLE foreign_tbl (id serial OPTIONS (rowkey 'true'), a int, b int)
  SERVER griddb_svr OPTIONS(table_name 'base_tbl');
--Testcase 759:
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON foreign_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();

--Testcase 760:
CREATE VIEW rw_view AS SELECT * FROM foreign_tbl
  WHERE a < b WITH CHECK OPTION;
--Testcase 761:
\d+ rw_view

--Testcase 416:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view(a, b) VALUES (0, 5);
--Testcase 417:
INSERT INTO rw_view(a, b) VALUES (0, 5); -- should fail
--Testcase 418:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view(a, b) VALUES (0, 15);
--Testcase 419:
INSERT INTO rw_view(a, b) VALUES (0, 15); -- ok
--Testcase 420:
SELECT a, b FROM foreign_tbl;

--Testcase 421:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 5;
--Testcase 422:
UPDATE rw_view SET b = b + 5; -- should fail
--Testcase 423:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 15;
--Testcase 424:
UPDATE rw_view SET b = b + 15; -- ok
--Testcase 425:
SELECT a, b FROM foreign_tbl;

--Testcase 762:
DROP TRIGGER row_before_insupd_trigger ON foreign_tbl;
--Testcase 763:
DROP FOREIGN TABLE foreign_tbl CASCADE;

-- test WCO for partitions
--Testcase 764:
CREATE FOREIGN TABLE foreign_tbl (id serial OPTIONS (rowkey 'true'), a int, b int)
  SERVER griddb_svr OPTIONS (table_name 'child_tbl');
--Testcase 765:
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON foreign_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();

--Testcase 766:
CREATE TABLE parent_tbl (id serial, a int, b int) PARTITION BY RANGE(a);
--Testcase 976:
ALTER TABLE parent_tbl ATTACH PARTITION foreign_tbl FOR VALUES FROM (0) TO (100);

--Testcase 767:
CREATE VIEW rw_view AS SELECT * FROM parent_tbl
  WHERE a < b WITH CHECK OPTION;
--Testcase 768:
\d+ rw_view

--Testcase 426:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view(a, b) VALUES (0, 5);
--Testcase 427:
INSERT INTO rw_view(a, b) VALUES (0, 5); -- should fail
--Testcase 428:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view(a, b) VALUES (0, 15);
--Testcase 429:
INSERT INTO rw_view(a, b) VALUES (0, 15); -- ok
--Testcase 430:
SELECT a, b FROM foreign_tbl;

--Testcase 431:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 5;
--Testcase 432:
UPDATE rw_view SET b = b + 5; -- should fail
--Testcase 433:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 15;
--Testcase 434:
UPDATE rw_view SET b = b + 15; -- ok
--Testcase 435:
SELECT a, b FROM foreign_tbl;

--Testcase 769:
DROP TRIGGER row_before_insupd_trigger ON foreign_tbl;
--Testcase 770:
DROP FOREIGN TABLE foreign_tbl CASCADE;
--Testcase 771:
DROP TABLE parent_tbl CASCADE;

--Testcase 772:
DROP FUNCTION row_before_insupd_trigfunc;

-- ===================================================================
-- test serial columns (ie, sequence-based defaults)
-- ===================================================================

--Testcase 773:
create foreign table rem1 (id serial OPTIONS (rowkey 'true'), f1 serial, f2 text)
  server griddb_svr options(table_name 'loct13');
--Testcase 436:
insert into rem1(f2) values('hi');
--Testcase 437:
insert into rem1(f2) values('bye');
--Testcase 438:
select pg_catalog.setval('rem1_f1_seq', 10, false);
--Testcase 439:
insert into rem1(f2) values('hi remote');
--Testcase 440:
insert into rem1(f2) values('bye remote');
--Testcase 441:
select f1, f2 from rem1;

-- ===================================================================
-- test generated columns
-- ===================================================================
--create table gloc1 (a int, b int);
--alter table gloc1 set (autovacuum_enabled = 'false');
--Testcase 774:
create foreign table grem1 (
  id serial OPTIONS (rowkey 'true'),
  a int,
  b int generated always as (a * 2) stored)
  server griddb_svr options(table_name 'gloc1');
--Testcase 442:
insert into grem1 (a) values (1), (2);
--Testcase 443:
update grem1 set a = 22 where a = 2;
--Testcase 444:
select a, b from grem1;

-- ===================================================================
-- test local triggers
-- ===================================================================

-- Trigger functions "borrowed" from triggers regress test.
--Testcase 775:
CREATE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE NOTICE 'trigger_func(%) called: action = %, when = %, level = %',
		TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;$$;

--Testcase 776:
CREATE TRIGGER trig_stmt_before BEFORE DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 777:
CREATE TRIGGER trig_stmt_after AFTER DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

--Testcase 778:
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
--Testcase 779:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 780:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 445:
delete from rem1;
--Testcase 446:
insert into rem1(f1, f2) values(1,'insert');
--Testcase 447:
update rem1 set f2  = 'update' where f1 = 1;
--Testcase 448:
update rem1 set f2 = f2 || f2;


-- cleanup
--Testcase 781:
DROP TRIGGER trig_row_before ON rem1;
--Testcase 782:
DROP TRIGGER trig_row_after ON rem1;
--Testcase 783:
DROP TRIGGER trig_stmt_before ON rem1;
--Testcase 784:
DROP TRIGGER trig_stmt_after ON rem1;

--Testcase 449:
DELETE from rem1;

-- Test multiple AFTER ROW triggers on a foreign table
--Testcase 785:
CREATE TRIGGER trig_row_after1
AFTER INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 786:
CREATE TRIGGER trig_row_after2
AFTER INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 787:
insert into rem1(f1, f2) values(1,'insert');
--Testcase 788:
update rem1 set f2  = 'update' where f1 = 1;
--Testcase 789:
update rem1 set f2 = f2 || f2;
--Testcase 790:
delete from rem1;

-- cleanup
--Testcase 791:
DROP TRIGGER trig_row_after1 ON rem1;
--Testcase 792:
DROP TRIGGER trig_row_after2 ON rem1;

-- Test WHEN conditions

--Testcase 793:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 794:
CREATE TRIGGER trig_row_after_insupd
AFTER INSERT OR UPDATE ON rem1
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Insert or update not matching: nothing happens
--Testcase 452:
INSERT INTO rem1(f1, f2) values(1, 'insert');
--Testcase 453:
UPDATE rem1 set f2 = 'test';

-- Insert or update matching: triggers are fired
--Testcase 454:
INSERT INTO rem1(f1, f2) values(2, 'update');
--Testcase 455:
UPDATE rem1 set f2 = 'update update' where f1 = '2';

--Testcase 795:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 796:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Trigger is fired for f1=2, not for f1=1
--Testcase 458:
DELETE FROM rem1;

-- cleanup
--Testcase 797:
DROP TRIGGER trig_row_before_insupd ON rem1;
--Testcase 798:
DROP TRIGGER trig_row_after_insupd ON rem1;
--Testcase 799:
DROP TRIGGER trig_row_before_delete ON rem1;
--Testcase 800:
DROP TRIGGER trig_row_after_delete ON rem1;


-- Test various RETURN statements in BEFORE triggers.

--Testcase 801:
CREATE FUNCTION trig_row_before_insupdate() RETURNS TRIGGER AS $$
  BEGIN
    NEW.f2 := NEW.f2 || ' triggered !';
    RETURN NEW;
  END
$$ language plpgsql;

--Testcase 802:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

-- The new values should have 'triggered' appended
--Testcase 459:
INSERT INTO rem1(f1, f2) values(1, 'insert');
--Testcase 460:
SELECT f1, f2 from rem1;
--Testcase 461:
INSERT INTO rem1(f1, f2) values(2, 'insert');
--Testcase 462:
SELECT f1, f2 from rem1;
--Testcase 463:
UPDATE rem1 set f2 = '';
--Testcase 464:
SELECT f1, f2 from rem1;
--Testcase 465:
UPDATE rem1 set f2 = 'skidoo';
--Testcase 466:
SELECT f1, f2 from rem1;

--Testcase 467:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f1 = 10;          -- all columns should be transmitted
--Testcase 468:
UPDATE rem1 set f1 = 10;
--Testcase 469:
SELECT f1, f2 from rem1;

--Testcase 470:
DELETE FROM rem1;

-- Add a second trigger, to check that the changes are propagated correctly
-- from trigger to trigger
--Testcase 803:
CREATE TRIGGER trig_row_before_insupd2
BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 471:
INSERT INTO rem1(f1, f2) values(1, 'insert');
--Testcase 472:
SELECT f1, f2 from rem1;
--Testcase 473:
INSERT INTO rem1(f1, f2) values(2, 'insert');
--Testcase 474:
SELECT f1, f2 from rem1;
--Testcase 475:
UPDATE rem1 set f2 = '';
--Testcase 476:
SELECT f1, f2 from rem1;
--Testcase 477:
UPDATE rem1 set f2 = 'skidoo';
--Testcase 478:
SELECT f1, f2 from rem1;

--Testcase 804:
DROP TRIGGER trig_row_before_insupd ON rem1;
--Testcase 805:
DROP TRIGGER trig_row_before_insupd2 ON rem1;

--Testcase 479:
DELETE from rem1;

--Testcase 480:
INSERT INTO rem1(f1, f2) VALUES (1, 'test');

-- Test with a trigger returning NULL
--Testcase 806:
CREATE FUNCTION trig_null() RETURNS TRIGGER AS $$
  BEGIN
    RETURN NULL;
  END
$$ language plpgsql;

--Testcase 807:
CREATE TRIGGER trig_null
BEFORE INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_null();

-- Nothing should have changed.
--Testcase 481:
INSERT INTO rem1(f1, f2) VALUES (2, 'test2');

--Testcase 482:
SELECT f1, f2 from rem1;

--Testcase 483:
UPDATE rem1 SET f2 = 'test2';

--Testcase 484:
SELECT f1, f2 from rem1;

--Testcase 485:
DELETE from rem1;

--Testcase 486:
SELECT f1, f2 from rem1;

--Testcase 808:
DROP TRIGGER trig_null ON rem1;
--Testcase 487:
DELETE from rem1;

-- Test a combination of local and remote triggers
--Testcase 809:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 810:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 811:
CREATE TRIGGER trig_local_before 
BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 488:
INSERT INTO rem1(f2) VALUES ('test');
--Testcase 489:
UPDATE rem1 SET f2 = 'testo';

-- Test returning a system attribute
--Testcase 490:
INSERT INTO rem1(f2) VALUES ('test');
--Testcase 491:
SELECT ctid FROM rem1 WHERE f2 = 'test triggered !';

-- cleanup
--Testcase 812:
DROP TRIGGER trig_row_before ON rem1;
--Testcase 813:
DROP TRIGGER trig_row_after ON rem1;
--Testcase 814:
DROP TRIGGER trig_local_before ON rem1;


-- Test direct foreign table modification functionality

-- Test with statement-level triggers
--Testcase 815:
CREATE TRIGGER trig_stmt_before
	BEFORE DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 492:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
--Testcase 493:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
--Testcase 816:
DROP TRIGGER trig_stmt_before ON rem1;

--Testcase 817:
CREATE TRIGGER trig_stmt_after
	AFTER DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 494:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
--Testcase 495:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
--Testcase 818:
DROP TRIGGER trig_stmt_after ON rem1;

-- Test with row-level ON INSERT triggers
--Testcase 819:
CREATE TRIGGER trig_row_before_insert
BEFORE INSERT ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 496:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
--Testcase 497:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
--Testcase 820:
DROP TRIGGER trig_row_before_insert ON rem1;

--Testcase 821:
CREATE TRIGGER trig_row_after_insert
AFTER INSERT ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 498:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
--Testcase 499:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
--Testcase 822:
DROP TRIGGER trig_row_after_insert ON rem1;

-- Test with row-level ON UPDATE triggers
--Testcase 823:
CREATE TRIGGER trig_row_before_update
BEFORE UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 500:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
--Testcase 501:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
--Testcase 824:
DROP TRIGGER trig_row_before_update ON rem1;

--Testcase 825:
CREATE TRIGGER trig_row_after_update
AFTER UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 502:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
--Testcase 503:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
--Testcase 826:
DROP TRIGGER trig_row_after_update ON rem1;

-- Test with row-level ON DELETE triggers
--Testcase 827:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 504:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
--Testcase 505:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can't be pushed down
--Testcase 828:
DROP TRIGGER trig_row_before_delete ON rem1;

--Testcase 829:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 506:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
--Testcase 507:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can't be pushed down
--Testcase 830:
DROP TRIGGER trig_row_after_delete ON rem1;

-- ===================================================================
-- test inheritance features
-- ===================================================================

--Testcase 831:
CREATE TABLE a (id serial, aa TEXT);
--Testcase 977:
ALTER TABLE a SET (autovacuum_enabled = 'false');
--Testcase 832:
CREATE FOREIGN TABLE b (bb TEXT) INHERITS (a)
  SERVER griddb_svr OPTIONS (table_name 'loct');
--Testcase 978:
ALTER FOREIGN TABLE b ALTER COLUMN id OPTIONS (rowkey 'true');

--Testcase 508:
INSERT INTO a(aa) VALUES('aaa');
--Testcase 509:
INSERT INTO a(aa) VALUES('aaaa');
--Testcase 510:
INSERT INTO a(aa) VALUES('aaaaa');

--Testcase 511:
INSERT INTO b(aa) VALUES('bbb');
--Testcase 512:
INSERT INTO b(aa) VALUES('bbbb');
--Testcase 513:
INSERT INTO b(aa) VALUES('bbbbb');

--Testcase 514:
SELECT tableoid::regclass, aa FROM a;
--Testcase 515:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 516:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 517:
UPDATE a SET aa = 'zzzzzz' WHERE aa LIKE 'aaaa%'; -- limitation

--Testcase 518:
SELECT tableoid::regclass, aa FROM a;
--Testcase 519:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 520:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 521:
UPDATE b SET aa = 'new';

--Testcase 522:
SELECT tableoid::regclass, aa FROM a;
--Testcase 523:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 524:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 525:
UPDATE a SET aa = 'newtoo';

--Testcase 526:
SELECT tableoid::regclass, aa FROM a;
--Testcase 527:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 528:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 529:
DELETE FROM a;

--Testcase 530:
SELECT tableoid::regclass, aa FROM a;
--Testcase 531:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 532:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 833:
DROP TABLE a CASCADE;

-- Check SELECT FOR UPDATE/SHARE with an inherited source table

--Testcase 834:
create table foo (f1 int, f2 int);
--Testcase 835:
create foreign table foo2 (f3 int) inherits (foo)
  server griddb_svr options (table_name 'loct1');
--Testcase 836:
create table bar (f1 int, f2 int);
--Testcase 837:
create foreign table bar2 (f3 int) inherits (bar)
  server griddb_svr options (table_name 'loct2');

--Testcase 979:
alter table foo set (autovacuum_enabled = 'false');
--Testcase 980:
alter table bar set (autovacuum_enabled = 'false');

--Testcase 981:
alter foreign table foo2 alter column f1 options (rowkey 'true');
--Testcase 982:
alter foreign table bar2 alter column f1 options (rowkey 'true');

--Testcase 533:
insert into foo values(1,1);
--Testcase 534:
insert into foo values(3,3);
--Testcase 535:
insert into foo2 values(2,2,2);
--Testcase 536:
insert into foo2 values(4,4,4);
--Testcase 537:
insert into bar values(1,11);
--Testcase 538:
insert into bar values(2,22);
--Testcase 539:
insert into bar values(6,66);
--Testcase 540:
insert into bar2 values(3,33,33);
--Testcase 541:
insert into bar2 values(4,44,44);
--Testcase 542:
insert into bar2 values(7,77,77);

--Testcase 543:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo) for update;
--Testcase 544:
select * from bar where f1 in (select f1 from foo) for update;

--Testcase 547:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo) for share;
--Testcase 548:
select * from bar where f1 in (select f1 from foo) for share;

-- Check UPDATE with inherited target and an inherited source table
--Testcase 549:
explain (verbose, costs off)
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);
--Testcase 550:
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);

--Testcase 551:
select tableoid::regclass, * from bar order by 1,2;

-- Check UPDATE with inherited target and an appendrel subquery
--Testcase 552:
explain (verbose, costs off)
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;
--Testcase 553:
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;

--Testcase 554:
select tableoid::regclass, * from bar order by 1,2;

-- Test forcing the remote server to produce sorted data for a merge join,
-- but the foreign table is an inheritance child.
--Testcase 555:
delete from "S 1".loct1;
truncate table only foo;
\set num_rows_foo 2000
--Testcase 556:
insert into "S 1".loct1 select generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2);
--Testcase 557:
insert into foo select generate_series(1, :num_rows_foo, 2), generate_series(1, :num_rows_foo, 2);
--Testcase 983:
SET enable_hashjoin to false;
--Testcase 984:
SET enable_nestloop to false;
-- skip, does not support 'use_remote_estimate'
/*
alter foreign table foo2 options (use_remote_estimate 'true');
create index i_loct1_f1 on loct1(f1);
create index i_foo_f1 on foo(f1);
analyze foo;
analyze loct1;
*/
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 558:
explain (verbose, costs off)
	select foo.f1, "S 1".loct1.f1 from foo join "S 1".loct1 on (foo.f1 = "S 1".loct1.f1) order by foo.f2 offset 10 limit 10;
--Testcase 559:
select foo.f1, "S 1".loct1.f1 from foo join "S 1".loct1 on (foo.f1 = "S 1".loct1.f1) order by foo.f2 offset 10 limit 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 560:
explain (verbose, costs off)
	select foo.f1, "S 1".loct1.f1 from foo left join "S 1".loct1 on (foo.f1 = "S 1".loct1.f1) order by foo.f2 offset 10 limit 10;
--Testcase 561:
select foo.f1, "S 1".loct1.f1 from foo left join "S 1".loct1 on (foo.f1 = "S 1".loct1.f1) order by foo.f2 offset 10 limit 10;
--Testcase 985:
RESET enable_hashjoin;
--Testcase 986:
RESET enable_nestloop;

-- Test that WHERE CURRENT OF is not supported
begin;
declare c cursor for select * from bar where f1 = 7;
--Testcase 562:
fetch from c;
--Testcase 563:
update bar set f2 = null where current of c;
rollback;

--Testcase 564:
explain (verbose, costs off)
delete from foo where f1 < 5;
--Testcase 565:
select * from foo where f1 < 5;
--Testcase 566:
delete from foo where f1 < 5;
--Testcase 567:
explain (verbose, costs off)
update bar set f2 = f2 + 100;
--Testcase 568:
update bar set f2 = f2 + 100;
--Testcase 569:
select * from bar;

-- Test that UPDATE/DELETE with inherited target works with row-level triggers
--Testcase 838:
CREATE TRIGGER trig_row_before
BEFORE UPDATE OR DELETE ON bar2
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 839:
CREATE TRIGGER trig_row_after
AFTER UPDATE OR DELETE ON bar2
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 570:
explain (verbose, costs off)
update bar set f2 = f2 + 100;
--Testcase 571:
update bar set f2 = f2 + 100;

--Testcase 572:
explain (verbose, costs off)
delete from bar where f2 < 400;
--Testcase 573:
delete from bar where f2 < 400;

-- cleanup
--Testcase 840:
drop table foo cascade;
--Testcase 841:
drop table bar cascade;

-- Test pushing down UPDATE/DELETE joins to the remote server
--Testcase 842:
create table parent (a int, b text);
--Testcase 843:
create foreign table remt1 (a int, b text)
  server griddb_svr options (table_name 'loct11');
--Testcase 844:
create foreign table remt2 (a int, b text)
  server griddb_svr options (table_name 'loct22');
--Testcase 987:
alter foreign table remt1 inherit parent;

--Testcase 988:
alter foreign table remt1 alter column a options (rowkey 'true');
--Testcase 989:
alter foreign table remt2 alter column a options (rowkey 'true');

--Testcase 574:
insert into remt1 values (1, 'foo');
--Testcase 575:
insert into remt1 values (2, 'bar');
--Testcase 576:
insert into remt2 values (1, 'foo');
--Testcase 577:
insert into remt2 values (2, 'bar');

--Testcase 578:
explain (verbose, costs off)
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a;
--Testcase 579:
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a;
--Testcase 845:
select * from parent, remt2 where parent.a = remt2.a;
--Testcase 580:
explain (verbose, costs off)
delete from parent using remt2 where parent.a = remt2.a;
--Testcase 581:
select parent.* from parent, remt2 where parent.a = remt2.a;
--Testcase 582:
delete from parent using remt2 where parent.a = remt2.a;

-- cleanup
--Testcase 846:
drop foreign table remt1;
--Testcase 847:
drop foreign table remt2;
--Testcase 848:
drop table parent;

-- ===================================================================
-- test tuple routing for foreign-table partitions
-- ===================================================================

-- Test insert tuple routing
--Testcase 849:
create table itrtest (id serial, a int, b text) partition by list (a);
--Testcase 850:
create foreign table remp1 (id serial, a int, b text) server griddb_svr options (table_name 'loct12');
--Testcase 851:
create foreign table remp2 (id serial, a int, b text) server griddb_svr options (table_name 'loct21');
--Testcase 990:
alter foreign table remp1 alter column id options (rowkey 'true');
--Testcase 991:
alter foreign table remp2 alter column id options (rowkey 'true');
--Testcase 992:
alter table itrtest attach partition remp1 for values in (1);
--Testcase 993:
alter table itrtest attach partition remp2 for values in (2);

--Testcase 583:
insert into itrtest(a, b) values (1, 'foo');
--Testcase 584:
insert into itrtest(a, b) values (1, 'bar');
--Testcase 585:
insert into itrtest(a, b) values (2, 'baz');
--Testcase 586:
insert into itrtest(a, b) values (2, 'qux');
--Testcase 587:
insert into itrtest(a, b) values (1, 'test1'), (2, 'test2');

--Testcase 588:
select tableoid::regclass, a, b FROM itrtest;
--Testcase 589:
select tableoid::regclass, a, b FROM remp1;
--Testcase 590:
select tableoid::regclass, b, a FROM remp2;

--Testcase 591:
delete from itrtest;

-- skip, griddb does not support on conflict
--create unique index loct1_idx on loct1 (a);

-- DO NOTHING without an inference specification is supported
--insert into itrtest values (1, 'foo') on conflict do nothing returning *;
--insert into itrtest values (1, 'foo') on conflict do nothing returning *;

-- But other cases are not supported
--insert into itrtest values (1, 'bar') on conflict (a) do nothing;
--insert into itrtest values (1, 'bar') on conflict (a) do update set b = excluded.b;

--select tableoid::regclass, * FROM itrtest;

--delete from itrtest;

--drop index loct1_idx;

-- Test that remote triggers work with insert tuple routing
--Testcase 852:
create function br_insert_trigfunc() returns trigger as $$
begin
	new.b := new.b || ' triggered !';
	return new;
end
$$ language plpgsql;
--Testcase 853:
create trigger remp1_br_insert_trigger before insert on remp1
	for each row execute procedure br_insert_trigfunc();
--Testcase 854:
create trigger remp2_br_insert_trigger before insert on remp2
	for each row execute procedure br_insert_trigfunc();

-- The new values are concatenated with ' triggered !'
--Testcase 592:
insert into itrtest(a, b) values (1, 'foo');
--Testcase 593:
insert into itrtest(a, b) values (2, 'qux');
--Testcase 594:
insert into itrtest(a, b) values (1, 'test1'), (2, 'test2');
--Testcase 855:
with result as (insert into itrtest(a ,b) values (1, 'test1'), (2, 'test2') returning *) select a, b from result;

--Testcase 856:
drop trigger remp1_br_insert_trigger on remp1;
--Testcase 857:
drop trigger remp2_br_insert_trigger on remp2;

--Testcase 595:
delete from itrtest;
--Testcase 858:
drop table itrtest;


-- Test update tuple routing
--Testcase 859:
create table utrtest (id serial, a int, b text) partition by list (a);
--Testcase 860:
create foreign table remp (id serial, a int check (a in (1)), b text) server griddb_svr options (table_name 'loct12');
--Testcase 994:
alter foreign table remp alter column id options (rowkey 'true');
--Testcase 861:
create table locp (id serial, a int check (a in (2)), b text);
--Testcase 995:
alter table utrtest attach partition remp for values in (1);
--Testcase 996:
alter table utrtest attach partition locp for values in (2);

--Testcase 596:
insert into utrtest(a, b) values (1, 'foo');
--Testcase 597:
insert into utrtest(a, b) values (2, 'qux');

--Testcase 598:
select tableoid::regclass, a, b FROM utrtest;
--Testcase 599:
select tableoid::regclass, a, b FROM remp;
--Testcase 600:
select tableoid::regclass, a, b FROM locp;

-- GridDB not support
-- It's not allowed to move a row from a partition that is foreign to another
--Testcase 601:
--update utrtest set a = 2 where b = 'foo' returning *;

-- But the reverse is allowed
--Testcase 603:
update utrtest set a = 1 where b = 'qux';
--Testcase 604:
select a, b from utrtest where b = 'qux';

--Testcase 605:
select tableoid::regclass, a, b FROM utrtest;
--Testcase 606:
select tableoid::regclass, a, b FROM remp;
--Testcase 607:
select tableoid::regclass, a, b FROM locp;

-- The executor should not let unexercised FDWs shut down
--Testcase 608:
update utrtest set a = 1 where b = 'foo';

-- Test that remote triggers work with update tuple routing
--Testcase 862:
create trigger remp_br_insert_trigger before insert on remp
	for each row execute procedure br_insert_trigfunc();

--Testcase 609:
delete from utrtest;
--Testcase 610:
insert into utrtest(a, b) values (2, 'qux');

-- Check case where the foreign partition is a subplan target rel
--Testcase 611:
explain (verbose, costs off)
update utrtest set a = 1 where a = 1 or a = 2;
-- The new values are concatenated with ' triggered !'
--Testcase 612:
update utrtest set a = 1 where a = 1 or a = 2;
--Testcase 613:
select a, b from utrtest;

--Testcase 614:
delete from utrtest;
--Testcase 615:
insert into utrtest(a, b) values (2, 'qux');

-- Check case where the foreign partition isn't a subplan target rel
--Testcase 616:
explain (verbose, costs off)
update utrtest set a = 1 where a = 2;
-- The new values are concatenated with ' triggered !'
--Testcase 617:
update utrtest set a = 1 where a = 2;
--Testcase 618:
select a, b from utrtest;

--Testcase 863:
drop trigger remp_br_insert_trigger on remp;

-- We can move rows to a foreign partition that has been updated already,
-- but can't move rows to a foreign partition that hasn't been updated yet

--Testcase 619:
delete from utrtest;
--Testcase 620:
insert into utrtest(a, b) values (1, 'foo');
--Testcase 621:
insert into utrtest(a, b) values (2, 'qux');

-- Test the former case:
-- with a direct modification plan
--Testcase 622:
explain (verbose, costs off)
update utrtest set a = 1;
--Testcase 623:
update utrtest set a = 1;
--Testcase 624:
select a, b from utrtest;

--Testcase 625:
delete from utrtest;
--Testcase 626:
insert into utrtest(a, b) values (1, 'foo');
--Testcase 627:
insert into utrtest(a, b) values (2, 'qux');

-- with a non-direct modification plan
--Testcase 628:
explain (verbose, costs off)
update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x;
--Testcase 629:
update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x;
--Testcase 630:
select * from utrtest;

-- Change the definition of utrtest so that the foreign partition get updated
-- after the local partition
--Testcase 631:
delete from utrtest;
--Testcase 997:
alter table utrtest detach partition remp;
--Testcase 864:
drop foreign table remp;
--Testcase 865:
create foreign table remp (id serial, a int check (a in (3)), b text) server griddb_svr options (table_name 'loct21');
--Testcase 998:
alter foreign table remp alter column id options (rowkey 'true');
--Testcase 999:
alter foreign table remp drop constraint remp_a_check;
--Testcase 1000:
alter foreign table remp add check (a in (3));

--Testcase 1001:
alter table utrtest attach partition remp for values in (3);
--Testcase 632:
insert into utrtest(a, b) values (2, 'qux');
--Testcase 633:
insert into utrtest(a, b) values (3, 'xyzzy');

-- Test the latter case:
-- with a direct modification plan
--Testcase 634:
explain (verbose, costs off)
update utrtest set a = 3;
--Testcase 635:
update utrtest set a = 3; -- ERROR

-- with a non-direct modification plan
--Testcase 636:
explain (verbose, costs off)
update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x;
--update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x; -- ERROR

--Testcase 637:
delete from utrtest;
--Testcase 866:
drop table utrtest;
--drop table loct;

-- Test copy tuple routing
--Testcase 867:
create table ctrtest (id serial, a int, b text) partition by list (a);
--Testcase 868:
create foreign table remp1 (id serial, a int, b text) server griddb_svr options (table_name 'loct12');
--Testcase 869:
create foreign table remp2 (id serial, a int, b text) server griddb_svr options (table_name 'loct21');
--Testcase 1002:
alter foreign table remp1 alter column id options (rowkey 'true');
--Testcase 1003:
alter foreign table remp2 alter column id options (rowkey 'true');
--Testcase 1004:
alter table ctrtest attach partition remp1 for values in (1);
--Testcase 1005:
alter table ctrtest attach partition remp2 for values in (2);

--Testcase 638:
insert into ctrtest(a, b) values (1, 'foo'), (2, 'qux');

--Testcase 639:
select tableoid::regclass, a, b FROM ctrtest;
--Testcase 640:
select tableoid::regclass, a, b FROM remp1;
--Testcase 641:
select tableoid::regclass, b, a FROM remp2;

-- GridDB not support partitions by
-- Copying into foreign partitions directly should work as well
copy remp1(a, b) from stdin;
1	bar
\.

--Testcase 642:
select tableoid::regclass, a, b FROM remp1;

--Testcase 643:
delete from ctrtest;
--Testcase 870:
drop table ctrtest;

-- ===================================================================
-- test COPY FROM
-- ===================================================================

--Testcase 871:
create foreign table rem2 (id serial, f1 int, f2 text) server griddb_svr options(table_name 'loct12');
--Testcase 1006:
alter foreign table rem2 alter column id options (rowkey 'true');

-- Test basic functionality
--Testcase 644:
insert into rem2(f1, f2) values (1, 'foo'), (2, 'bar');
--Testcase 645:
select f1, f2 from rem2;

--Testcase 646:
delete from rem2;

-- Test check constraints
--Testcase 1007:
alter foreign table rem2 add constraint rem2_f1positive check (f1 >= 0);

-- check constraint is enforced on the remote side, not locally
--Testcase 647:
insert into rem2(f1, f2) values (1, 'foo'), (2, 'bar');
-- GridDB not support constraint
--Testcase 648:
--insert into rem2(f1, f2) values (-1, 'xyzzy');

--Testcase 649:
select f1, f2 from rem2;

--Testcase 1008:
alter foreign table rem2 drop constraint rem2_f1positive;
--alter table loc2 drop constraint loc2_f1positive;

--Testcase 650:
delete from rem2;

-- Test local triggers
--Testcase 872:
create trigger trig_stmt_before before insert on rem2
	for each statement execute procedure trigger_func();
--Testcase 873:
create trigger trig_stmt_after after insert on rem2
	for each statement execute procedure trigger_func();
--Testcase 874:
create trigger trig_row_before before insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');
--Testcase 875:
create trigger trig_row_after after insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');

copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
--Testcase 651:
select f1, f2 from rem2;

--Testcase 876:
drop trigger trig_row_before on rem2;
--Testcase 877:
drop trigger trig_row_after on rem2;
--Testcase 878:
drop trigger trig_stmt_before on rem2;
--Testcase 879:
drop trigger trig_stmt_after on rem2;

--Testcase 652:
delete from rem2;

--Testcase 880:
CREATE FUNCTION trig_row_before_insupdate1() RETURNS TRIGGER AS $$
  BEGIN
    NEW.f2 := NEW.f2 || ' triggered !';
    RETURN NEW;
  END
$$ language plpgsql;


--Testcase 881:
create trigger trig_row_before_insert before insert on rem2
	for each row execute procedure trig_row_before_insupdate1();

-- The new values are concatenated with ' triggered !'
copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
--Testcase 653:
select f1, f2 from rem2;

--Testcase 882:
drop trigger trig_row_before_insert on rem2;

--Testcase 654:
delete from rem2;

--Testcase 883:
create trigger trig_null before insert on rem2
	for each row execute procedure trig_null();

-- Nothing happens
copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
--Testcase 655:
select f1, f2 from rem2;

--Testcase 884:
drop trigger trig_null on rem2;

--Testcase 656:
delete from rem2;

-- Test remote triggers
--Testcase 885:
create trigger trig_row_before_insert before insert on rem2
	for each row execute procedure trig_row_before_insupdate1();

-- The new values are concatenated with ' triggered !'
copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
--Testcase 657:
select f1, f2 from rem2;

--Testcase 886:
drop trigger trig_row_before_insert on rem2;

--Testcase 658:
delete from rem2;

--Testcase 887:
create trigger trig_null before insert on rem2
	for each row execute procedure trig_null();

-- Nothing happens
copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
--Testcase 659:
select f1, f2 from rem2;

--Testcase 888:
drop trigger trig_null on rem2;

--Testcase 660:
delete from rem2;

-- Test a combination of local and remote triggers
--Testcase 889:
create trigger rem2_trig_row_before before insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');
--Testcase 890:
create trigger rem2_trig_row_after after insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');
--Testcase 891:
create trigger loc2_trig_row_before_insert before insert on rem2
	for each row execute procedure trig_row_before_insupdate1();

copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
--Testcase 661:
select f1, f2 from rem2;

--Testcase 892:
drop trigger rem2_trig_row_before on rem2;
--Testcase 893:
drop trigger rem2_trig_row_after on rem2;
--Testcase 894:
drop trigger loc2_trig_row_before_insert on rem2;

--Testcase 662:
delete from rem2;

-- test COPY FROM with foreign table created in the same transaction
--create table loc3 (f1 int, f2 text);
begin;
--Testcase 895:
create foreign table rem3 (f1 int, f2 text)
	server griddb_svr options(table_name 'loc3');
copy rem3(f1, f2) from stdin;
1	foo
2	bar
\.
commit;
--Testcase 663:
select * from rem3;
--Testcase 896:
drop foreign table rem3;
--drop table loc3;

-- ===================================================================
-- test IMPORT FOREIGN SCHEMA
-- ===================================================================

--Testcase 897:
CREATE SCHEMA import_grid1;
IMPORT FOREIGN SCHEMA "S 1" LIMIT TO
	("T0", "T1", "T2", "T3", "T4", ft1)
	FROM SERVER griddb_svr INTO import_grid1;
--Testcase 664:
\det+ import_grid1.*
--Testcase 665:
\d import_grid1.*


-- Options
-- GridDB does not support the option "import_default"
/*
CREATE SCHEMA import_grid2;
IMPORT FOREIGN SCHEMA "S 1" LIMIT TO
	("T0", "T1", "T2", "T3", "T4", ft1)
	FROM SERVER griddb_svr INTO import_grid2
  OPTIONS (import_default 'true');
--Testcase 666:
\det+ import_grid2.*
--Testcase 667:
\d import_grid2.*

CREATE SCHEMA import_grid3;
IMPORT FOREIGN SCHEMA "S 1" LIMIT TO
	("T0", "T1", "T2", "T3", "T4", ft1)
	FROM SERVER griddb_svr INTO import_grid3
  OPTIONS (import_collate 'false', import_not_null 'false');
--Testcase 668:
\det+ import_grid3.*
--Testcase 669:
\d import_grid3.*
*/
-- Check LIMIT TO and EXCEPT
--Testcase 898:
CREATE SCHEMA import_grid4;
IMPORT FOREIGN SCHEMA griddb_schema LIMIT TO ("T1", nonesuch)
  FROM SERVER griddb_svr INTO import_grid4;
--Testcase 670:
\det+ import_grid4.*

IMPORT FOREIGN SCHEMA griddb_schema EXCEPT ("T1", "T2", nonesuch)
FROM SERVER griddb_svr INTO import_grid4;
--Testcase 671:
\det+ import_grid4.*

-- Assorted error cases
IMPORT FOREIGN SCHEMA griddb_schema FROM SERVER griddb_svr INTO import_grid4;
IMPORT FOREIGN SCHEMA nonesuch FROM SERVER griddb_svr INTO import_grid4; -- same as 'public'
IMPORT FOREIGN SCHEMA nonesuch FROM SERVER griddb_svr INTO notthere;
IMPORT FOREIGN SCHEMA nonesuch FROM SERVER nowhere INTO notthere;

-- Check case of a type present only on the remote server.
-- We can fake this by dropping the type locally in our transaction.
--Testcase 899:
CREATE SCHEMA import_grid5;
BEGIN;
IMPORT FOREIGN SCHEMA griddb_schema LIMIT TO ("T1")
FROM SERVER griddb_svr INTO import_grid5; --ERROR
ROLLBACK;

-- Skip, does not support option 'fetch_size'
--BEGIN;
--CREATE SERVER fetch101 FOREIGN DATA WRAPPER griddb_fdw OPTIONS( fetch_size '101' );
/*
--Testcase 672:
SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'fetch101'
AND srvoptions @> array['fetch_size=101'];

ALTER SERVER fetch101 OPTIONS( SET fetch_size '202' );

--Testcase 673:
SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'fetch101'
AND srvoptions @> array['fetch_size=101'];

--Testcase 674:
SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'fetch101'
AND srvoptions @> array['fetch_size=202'];

CREATE FOREIGN TABLE table30000 ( x int ) SERVER fetch101 OPTIONS ( fetch_size '30000' );

--Testcase 675:
SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30000'::regclass
AND ftoptions @> array['fetch_size=30000'];

ALTER FOREIGN TABLE table30000 OPTIONS ( SET fetch_size '60000');

--Testcase 676:
SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30000'::regclass
AND ftoptions @> array['fetch_size=30000'];

--Testcase 677:
SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30000'::regclass
AND ftoptions @> array['fetch_size=60000'];

ROLLBACK;
*/
-- Drop schemas
--Testcase 1009:
SET client_min_messages to WARNING;
--Testcase 900:
DROP SCHEMA import_grid1 CASCADE;
--Testcase 901:
DROP SCHEMA import_grid2 CASCADE;
--Testcase 902:
DROP SCHEMA import_grid3 CASCADE;
--Testcase 903:
DROP SCHEMA import_grid4 CASCADE;
--Testcase 904:
DROP SCHEMA import_grid5 CASCADE;
--Testcase 1010:
SET client_min_messages to NOTICE;

-- ===================================================================
-- test partitionwise joins
-- ===================================================================
--Testcase 1011:
SET enable_partitionwise_join=on;

--Testcase 905:
CREATE TABLE fprt1 (a int, b int, c text) PARTITION BY RANGE(a);
--Testcase 678:
INSERT INTO "S 1".fprt1_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 2) i;
--Testcase 679:
INSERT INTO "S 1".fprt1_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 2) i;
--Testcase 906:
CREATE FOREIGN TABLE ftprt1_p1 PARTITION OF fprt1 FOR VALUES FROM (0) TO (250)
	SERVER griddb_svr OPTIONS (table_name 'fprt1_p1');
--Testcase 907:
CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (250) TO (500)
	SERVER griddb_svr OPTIONS (TABLE_NAME 'fprt1_p2');
--ANALYZE fprt1;
--ANALYZE fprt1_p1;
--ANALYZE fprt1_p2;
--Testcase 908:
CREATE TABLE fprt2 (a int, b int, c text) PARTITION BY RANGE(b);
--Testcase 680:
INSERT INTO "S 1".fprt2_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 3) i;
--Testcase 681:
INSERT INTO "S 1".fprt2_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 3) i;
--Testcase 909:
CREATE FOREIGN TABLE ftprt2_p1 (a int, b int, c text)
	SERVER griddb_svr OPTIONS (table_name 'fprt2_p1');
--Testcase 1012:
ALTER TABLE fprt2 ATTACH PARTITION ftprt2_p1 FOR VALUES FROM (0) TO (250);
--Testcase 910:
CREATE FOREIGN TABLE ftprt2_p2 PARTITION OF fprt2 FOR VALUES FROM (250) TO (500)
	SERVER griddb_svr OPTIONS (table_name 'fprt2_p2');
--ANALYZE fprt2;
--ANALYZE fprt2_p1;
--ANALYZE fprt2_p2;
-- inner join three tables
--Testcase 682:
EXPLAIN (COSTS OFF)
SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;
--Testcase 683:
SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;

-- left outer join + nullable clasue
--Testcase 684:
EXPLAIN (COSTS OFF)
SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;
--Testcase 685:
SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;

-- with whole-row reference; partitionwise join does not apply
--Testcase 686:
EXPLAIN (COSTS OFF)
SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;
--Testcase 687:
SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;

-- join with lateral reference
--Testcase 688:
EXPLAIN (COSTS OFF)
SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;
--Testcase 689:
SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;

-- with PHVs, partitionwise join selected but no join pushdown
--Testcase 690:
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;
--Testcase 691:
SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;

-- test FOR UPDATE; partitionwise join does not apply
--Testcase 692:
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;
--Testcase 693:
SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;

--Testcase 1013:
RESET enable_partitionwise_join;


-- ===================================================================
-- test partitionwise aggregates
-- ===================================================================

--Testcase 911:
CREATE TABLE pagg_tab (t int, a int, b int, c text) PARTITION BY RANGE(a);

--Testcase 694:
INSERT INTO "S 1".pagg_tab_p1 SELECT i, i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 10;
--Testcase 695:
INSERT INTO "S 1".pagg_tab_p2 SELECT i, i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 20 and (i % 30) >= 10;
--Testcase 696:
INSERT INTO "S 1".pagg_tab_p3 SELECT i, i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 30 and (i % 30) >= 20;

-- Create foreign partitions
--Testcase 912:
CREATE FOREIGN TABLE fpagg_tab_p1 PARTITION OF pagg_tab FOR VALUES FROM (0) TO (10) SERVER griddb_svr OPTIONS (table_name 'pagg_tab_p1');
--Testcase 913:
CREATE FOREIGN TABLE fpagg_tab_p2 PARTITION OF pagg_tab FOR VALUES FROM (10) TO (20) SERVER griddb_svr OPTIONS (table_name 'pagg_tab_p2');;
--Testcase 914:
CREATE FOREIGN TABLE fpagg_tab_p3 PARTITION OF pagg_tab FOR VALUES FROM (20) TO (30) SERVER griddb_svr OPTIONS (table_name 'pagg_tab_p3');;
--ANALYZE pagg_tab;
--ANALYZE fpagg_tab_p1;
--ANALYZE fpagg_tab_p2;
--ANALYZE fpagg_tab_p3;
-- When GROUP BY clause matches with PARTITION KEY.
-- Plan with partitionwise aggregates is disabled
--Testcase 1014:
SET enable_partitionwise_aggregate TO false;
--Testcase 697:
EXPLAIN (COSTS OFF)
SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- Plan with partitionwise aggregates is enabled
--Testcase 1015:
SET enable_partitionwise_aggregate TO true;
--Testcase 698:
EXPLAIN (COSTS OFF)
SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;
--Testcase 699:
SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- Check with whole-row reference
-- Should have all the columns in the target list for the given relation
--Testcase 700:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;
--Testcase 701:
SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- When GROUP BY clause does not match with PARTITION KEY.
--Testcase 702:
EXPLAIN (COSTS OFF)
SELECT b, avg(a), max(a), count(*) FROM pagg_tab GROUP BY b HAVING sum(a) < 700 ORDER BY 1;

-- Skip test because GridDB not support no super user
-- ===================================================================
-- access rights and superuser
-- ===================================================================
/*
-- Non-superuser cannot create a FDW without a password in the connstr
CREATE ROLE regress_nosuper NOSUPERUSER;

GRANT USAGE ON FOREIGN DATA WRAPPER griddb_fdw TO regress_nosuper;

SET ROLE regress_nosuper;

SHOW is_superuser;

-- This will be OK, we can create the FDW
CREATE SERVER griddb_fdw_nopw FOREIGN DATA WRAPPER griddb_fdw
    OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');

-- But creation of user mappings for non-superusers should fail
CREATE USER MAPPING FOR public SERVER griddb_fdw_nopw OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);
CREATE USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw;

CREATE FOREIGN TABLE ft1_nopw (
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	c3 text,
	c4 timestamp,
	c5 timestamp,
	c6 text,
	c7 text default 'ft1',
	c8 text
) SERVER griddb_fdw_nopw OPTIONS (table_name 'ft1');

SELECT * FROM ft1_nopw LIMIT 1;

-- If we add a password to the connstr it'll fail, because we don't allow passwords
-- in connstrs only in user mappings.

DO $d$
    BEGIN
        EXECUTE $$ALTER SERVER griddb_fdw_nopw OPTIONS (ADD password 'dummypw')$$;
    END;
$d$;

-- If we add a password for our user mapping instead, we should get a different
-- error because the password wasn't actually *used* when we run with trust auth.
--
-- This won't work with installcheck, but neither will most of the FDW checks.

ALTER USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw OPTIONS (ADD password 'dummypw');

SELECT * FROM ft1_nopw LIMIT 1;

-- Unpriv user cannot make the mapping passwordless
ALTER USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw OPTIONS (ADD password_required 'false');


SELECT * FROM ft1_nopw LIMIT 1;

RESET ROLE;

-- But the superuser can
ALTER USER MAPPING FOR regress_nosuper SERVER griddb_fdw_nopw OPTIONS (ADD password_required 'false');

SET ROLE regress_nosuper;

-- Should finally work now
SELECT * FROM ft1_nopw LIMIT 1;

-- unpriv user also cannot set sslcert / sslkey on the user mapping
-- first set password_required so we see the right error messages
ALTER USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw OPTIONS (SET password_required 'true');
ALTER USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw OPTIONS (ADD sslcert 'foo.crt');
ALTER USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw OPTIONS (ADD sslkey 'foo.key');

-- We're done with the role named after a specific user and need to check the
-- changes to the public mapping.
DROP USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw;

-- This will fail again as it'll resolve the user mapping for public, which
-- lacks password_required=false
SELECT * FROM ft1_nopw LIMIT 1;

RESET ROLE;

-- The user mapping for public is passwordless and lacks the password_required=false
-- mapping option, but will work because the current user is a superuser.
SELECT * FROM ft1_nopw LIMIT 1;

-- cleanup
DROP USER MAPPING FOR public SERVER griddb_fdw_nopw;
DROP OWNED BY regress_nosuper;
DROP ROLE regress_nosuper;
*/

-- Clean-up
--Testcase 1016:
RESET enable_partitionwise_aggregate;

-- GridDB has different result because GridDB does not run test check constraints
-- Two-phase transactions are not supported.
BEGIN;
--Testcase 915:
SELECT count(*) FROM ft1;
-- error here
--Testcase 916:
PREPARE TRANSACTION 'fdw_tpc';
ROLLBACK;

--Testcase 1017:
SET client_min_messages to WARNING;

DO $$ DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
--Testcase 917:
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename);
    END LOOP;
END $$;

-- Drop all foreign tables
--Testcase 918:
DROP USER MAPPING FOR public SERVER griddb_svr;
--Testcase 919:
DROP USER MAPPING FOR public SERVER griddb_svr2;
--Testcase 920:
DROP USER MAPPING FOR public SERVER testserver1;
--Testcase 921:
DROP SERVER griddb_svr CASCADE;
--Testcase 922:
DROP SERVER griddb_svr2 CASCADE;
--Testcase 923:
DROP SERVER testserver1 CASCADE;
--Testcase 924:
DROP EXTENSION griddb_fdw CASCADE;
--Testcase 1018:
SET client_min_messages to NOTICE;

