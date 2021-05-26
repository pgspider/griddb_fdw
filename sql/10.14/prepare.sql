-- Regression tests for prepareable statements. We query the content
-- of the pg_prepared_statements view as prepared statements are
-- created and removed.
\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 27:
CREATE EXTENSION griddb_fdw;
--Testcase 28:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');
--Testcase 29:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

--Testcase 30:
CREATE FOREIGN TABLE tenk1 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	text,
	stringu2	text,
	string4		text
) SERVER griddb_svr;

--ALTER TABLE tenk1 SET WITH OIDS;

--Testcase 31:
CREATE FOREIGN TABLE road (
	name		text,
	thepath 	text
) SERVER griddb_svr;

--Testcase 32:
CREATE FOREIGN TABLE road_tmp (
	id serial OPTIONS (rowkey 'true'),
	a int4,
	b int4
) SERVER griddb_svr;

--Testcase 33:
insert into road_tmp(a, b) values (1, 2);

--Testcase 1:
SELECT name, statement, parameter_types FROM pg_prepared_statements;

--Testcase 2:
PREPARE q1 AS SELECT a FROM road_tmp;
--Testcase 3:
EXECUTE q1;

--Testcase 4:
SELECT name, statement, parameter_types FROM pg_prepared_statements;

-- should fail
--Testcase 5:
PREPARE q1 AS SELECT b FROM road_tmp;

-- should succeed
DEALLOCATE q1;
--Testcase 6:
PREPARE q1 AS SELECT b FROM road_tmp;
--Testcase 7:
EXECUTE q1;

--Testcase 8:
PREPARE q2 AS SELECT b FROM road_tmp;
--Testcase 9:
SELECT name, statement, parameter_types FROM pg_prepared_statements;

-- sql92 syntax
DEALLOCATE PREPARE q1;

--Testcase 10:
SELECT name, statement, parameter_types FROM pg_prepared_statements;

DEALLOCATE PREPARE q2;
-- the view should return the empty set again
--Testcase 11:
SELECT name, statement, parameter_types FROM pg_prepared_statements;

-- parameterized queries
--Testcase 12:
PREPARE q2(text) AS
	SELECT datname, datistemplate, datallowconn
	FROM pg_database WHERE datname = $1;

--Testcase 13:
EXECUTE q2('postgres');

--Testcase 14:
PREPARE q3(text, int, float, boolean, smallint) AS
	SELECT * FROM tenk1 WHERE string4 = $1 AND (four = $2 OR
	ten = $3::bigint OR true = $4 OR odd = $5::int)
	ORDER BY unique1;

--Testcase 15:
EXECUTE q3('AAAAxx', 5::smallint, 10.5::float, false, 4::bigint);

-- too few params
--Testcase 16:
EXECUTE q3('bool');

-- too many params
--Testcase 17:
EXECUTE q3('bytea', 5::smallint, 10.5::float, false, 4::bigint, true);

-- wrong param types
--Testcase 18:
EXECUTE q3(5::smallint, 10.5::float, false, 4::bigint, 'bytea');

-- invalid type
--Testcase 19:
PREPARE q4(nonexistenttype) AS SELECT * FROM road WHERE name = $1;

-- create table as execute
--Testcase 20:
PREPARE q5(int, text) AS
	SELECT * FROM tenk1 WHERE unique1 = $1 OR stringu1 = $2
	ORDER BY unique1;
--Testcase 34:
CREATE TEMPORARY TABLE q5_prep_results AS EXECUTE q5(200, 'DTAAAA');
--Testcase 21:
SELECT * FROM q5_prep_results;
--Testcase 35:
CREATE TEMPORARY TABLE q5_prep_nodata AS EXECUTE q5(200, 'DTAAAA')
    WITH NO DATA;
--Testcase 22:
SELECT * FROM q5_prep_nodata;

-- unknown or unspecified parameter types: should succeed
--Testcase 23:
PREPARE q6 AS
    SELECT * FROM tenk1 WHERE unique1 = $1 AND stringu1 = $2;
--Testcase 24:
PREPARE q7(unknown) AS
    SELECT * FROM road WHERE thepath = $1;

--Testcase 25:
SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

-- test DEALLOCATE ALL;
DEALLOCATE ALL;
--Testcase 26:
SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

--Testcase 36:
DROP FOREIGN TABLE tenk1;
--Testcase 37:
DROP FOREIGN TABLE road;
--Testcase 38:
DROP USER MAPPING FOR public SERVER griddb_svr;
--Testcase 39:
DROP SERVER griddb_svr CASCADE;
--Testcase 40:
DROP EXTENSION griddb_fdw CASCADE;
