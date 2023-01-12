\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 1:
CREATE EXTENSION griddb_fdw;

--Testcase 2:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');

--Testcase 3:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

IMPORT FOREIGN SCHEMA griddb_schema FROM SERVER griddb_svr INTO public;
-- GridDB containers must be created for this test on GridDB server
/*
CREATE TABLE department (department_id integer primary key, department_name text)
CREATE TABLE employee (emp_id integer primary key, emp_name text, emp_dept_id integer)
CREATE TABLE empdata (emp_id integer primary key, emp_dat blob)
CREATE TABLE numbers (a integer primary key, b text)
CREATE TABLE shorty (id integer primary key, c text)
CREATE TABLE evennumbers (a integer primary key, b text)
*/

--Testcase 4:
DELETE FROM department;

--Testcase 5:
DELETE FROM employee;

--Testcase 6:
DELETE FROM empdata;

--Testcase 7:
DELETE FROM numbers;

--Testcase 8:
DELETE FROM evennumbers;

--Testcase 9:
DELETE FROM rowkey_tbl;

--Testcase 10:
SELECT * FROM department LIMIT 10;

--Testcase 11:
SELECT * FROM employee LIMIT 10;

--Testcase 12:
SELECT * FROM empdata LIMIT 10;

--Testcase 13:
INSERT INTO department VALUES(generate_series(1,100), 'dept - ' || generate_series(1,100));

--Testcase 14:
INSERT INTO employee VALUES(generate_series(1,100), 'emp - ' || generate_series(1,100), generate_series(1,100));

--Testcase 15:
INSERT INTO empdata  VALUES(1, decode ('01234567', 'hex'));

--Testcase 16:
INSERT INTO numbers VALUES(1, 'One');

--Testcase 17:
INSERT INTO numbers VALUES(2, 'Two');

--Testcase 18:
INSERT INTO numbers VALUES(3, 'Three');

--Testcase 19:
INSERT INTO numbers VALUES(4, 'Four');

--Testcase 20:
INSERT INTO numbers VALUES(5, 'Five');

--Testcase 21:
INSERT INTO numbers VALUES(6, 'Six');

--Testcase 22:
INSERT INTO numbers VALUES(7, 'Seven');

--Testcase 23:
INSERT INTO numbers VALUES(8, 'Eight');

--Testcase 24:
INSERT INTO numbers VALUES(9, 'Nine');

--Testcase 25:
INSERT INTO evennumbers VALUES(2, 'Two');

--Testcase 26:
INSERT INTO evennumbers VALUES(4, 'Four');

--Testcase 27:
INSERT INTO evennumbers VALUES(6, 'Six');

--Testcase 28:
INSERT INTO evennumbers VALUES(8, 'Eight');

--Testcase 29:
SELECT count(*) FROM department;

--Testcase 30:
SELECT count(*) FROM employee;

--Testcase 31:
SELECT count(*) FROM empdata;

-- Join

--Testcase 32:
SELECT * FROM department d, employee e WHERE d.department_id = e.emp_dept_id LIMIT 10;
-- Subquery

--Testcase 33:
SELECT * FROM department d, employee e WHERE d.department_id IN (SELECT department_id FROM department) LIMIT 10;

--Testcase 34:
SELECT * FROM empdata;
-- Delete single row

--Testcase 35:
DELETE FROM employee WHERE emp_id = 10;

--Testcase 36:
SELECT COUNT(*) FROM department LIMIT 10;

--Testcase 37:
SELECT COUNT(*) FROM employee WHERE emp_id = 10;
-- Update single row

--Testcase 38:
UPDATE employee SET emp_name = 'Updated emp' WHERE emp_id = 20;

--Testcase 39:
SELECT emp_id, emp_name FROM employee WHERE emp_name like 'Updated emp';

--Testcase 40:
UPDATE empdata SET emp_dat = decode ('0123', 'hex');

--Testcase 41:
SELECT * FROM empdata;

--Testcase 42:
SELECT * FROM employee LIMIT 10;

--Testcase 43:
SELECT * FROM employee WHERE emp_id IN (1);

--Testcase 44:
SELECT * FROM employee WHERE emp_id IN (1,3,4,5);

--Testcase 45:
SELECT * FROM employee WHERE emp_id IN (10000,1000);

--Testcase 46:
SELECT * FROM employee WHERE emp_id NOT IN (1) LIMIT 5;

--Testcase 47:
SELECT * FROM employee WHERE emp_id NOT IN (1,3,4,5) LIMIT 5;

--Testcase 48:
SELECT * FROM employee WHERE emp_id NOT IN (10000,1000) LIMIT 5;

--Testcase 49:
SELECT * FROM employee WHERE emp_id NOT IN (SELECT emp_id FROM employee WHERE emp_id IN (1,10));

--Testcase 50:
SELECT * FROM employee WHERE emp_name NOT IN ('emp - 1', 'emp - 2') LIMIT 5;

--Testcase 51:
SELECT * FROM employee WHERE emp_name NOT IN ('emp - 10') LIMIT 5;

--Testcase 52:
CREATE OR REPLACE FUNCTION test_param_where() RETURNS void AS $$
DECLARE
  n varchar;
BEGIN
  FOR x IN 1..9 LOOP

--Testcase 53:
    SELECT b INTO n FROM numbers WHERE a=x;
    RAISE NOTICE 'Found number %', n;
  END LOOP;
  RETURN;
END
$$ LANGUAGE plpgsql;

--Testcase 54:
SELECT test_param_where();

--Testcase 55:
ALTER FOREIGN TABLE numbers OPTIONS (table_name 'evennumbers');

--Testcase 56:
INSERT INTO numbers VALUES(10, 'Ten');

--Testcase 57:
SELECT * FROM numbers;

--Testcase 58:
SET griddbfdw.enable_partial_execution TO TRUE;

--Testcase 59:
SELECT * FROM numbers;

--Testcase 60:
SET griddbfdw.enable_partial_execution TO FALSE;

--Testcase 61:
DELETE FROM employee;

--Testcase 62:
DELETE FROM department;

--Testcase 63:
DELETE FROM empdata;

--Testcase 64:
DELETE FROM numbers;

--Testcase 65:
DROP FUNCTION test_param_where();

--Testcase 66:
DROP FOREIGN TABLE numbers;

--Testcase 67:
DROP FOREIGN TABLE department;

--Testcase 68:
DROP FOREIGN TABLE employee;

--Testcase 69:
DROP FOREIGN TABLE empdata;

-- -----------------------------------------------------------------------------

--Testcase 70:
DELETE FROM shorty;

--Testcase 71:
INSERT INTO shorty (id, c) VALUES (1, 'Z');

--Testcase 72:
INSERT INTO shorty (id, c) VALUES (2, 'Y');

--Testcase 73:
INSERT INTO shorty (id, c) VALUES (5, 'A');

--Testcase 74:
INSERT INTO shorty (id, c) VALUES (3, 'X');

--Testcase 75:
INSERT INTO shorty (id, c) VALUES (4, 'B');

-- ORDER BY.

--Testcase 76:
SELECT c FROM shorty ORDER BY id;

-- Transaction INSERT
BEGIN;

--Testcase 77:
INSERT INTO shorty (id, c) VALUES (6, 'T');
ROLLBACK;

--Testcase 78:
SELECT id, c FROM shorty;

-- Transaction UPDATE single row
BEGIN;

--Testcase 79:
UPDATE shorty SET c = 'd' WHERE id = 5;
ROLLBACK;

--Testcase 80:
SELECT id, c FROM shorty;

-- Transaction UPDATE all
BEGIN;

--Testcase 81:
UPDATE shorty SET c = 'd';
ROLLBACK;

--Testcase 82:
SELECT id, c FROM shorty;

-- Transaction DELETE single row
BEGIN;

--Testcase 83:
DELETE FROM shorty WHERE id = 1;
ROLLBACK;

--Testcase 84:
SELECT id, c FROM shorty;

-- Transaction DELETE all
BEGIN;

--Testcase 85:
DELETE FROM shorty;
ROLLBACK;

--Testcase 86:
SELECT id, c FROM shorty;

-- Use of NULL value
BEGIN;

--Testcase 87:
INSERT INTO shorty VALUES(99, NULL);

--Testcase 88:
UPDATE shorty SET c = NULL WHERE id = 3;

--Testcase 89:
SELECT id FROM shorty WHERE c IS NULL;
ROLLBACK;

-- parameters.

--Testcase 90:
PREPARE stmt(integer) AS SELECT * FROM shorty WHERE id = $1;

--Testcase 91:
EXECUTE stmt(1);

--Testcase 92:
EXECUTE stmt(2);
DEALLOCATE stmt;

-- test NULL parameter

--Testcase 93:
SELECT id FROM shorty WHERE c = (SELECT NULL::text);

-- Use of system column

--Testcase 94:
SELECT tableoid::regclass, * from shorty WHERE id = 1;

--Testcase 95:
SELECT * from shorty WHERE id = 1 AND tableoid = 'shorty'::regclass;

-- Clean up

--Testcase 96:
DROP FOREIGN TABLE shorty;

-- Test rowkey columni with trigger and without trigger
-- Prepare data

--Testcase 97:
INSERT INTO rowkey_tbl VALUES (0, 5);

-- Test with trigger

--Testcase 98:
CREATE FUNCTION row_before_insupd_trigfunc() RETURNS trigger AS $$BEGIN NEW.a := NEW.a + 10; RETURN NEW; END$$ LANGUAGE plpgsql;

--Testcase 99:
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON rowkey_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();

--Testcase 100:
INSERT INTO rowkey_tbl VALUES (0, 5);

--Testcase 101:
SELECT * FROM rowkey_tbl;

-- This test failed because new rowkey value will be updated to old rowkey value

--Testcase 102:
UPDATE rowkey_tbl SET b = b + 10; -- failed

-- Test without trigger

--Testcase 103:
DROP TRIGGER row_before_insupd_trigger ON rowkey_tbl;

-- This test OK because rowkey value is not changed

--Testcase 104:
UPDATE rowkey_tbl SET b = b + 10; -- ok

--Testcase 105:
SELECT * FROM rowkey_tbl;

-- This test failed  because rowkey is updated directly even its value not changed

--Testcase 106:
UPDATE rowkey_tbl SET a = 10, b = b + 15 WHERE a = 10; --failed

--Testcase 107:
SELECT * FROM rowkey_tbl;

-- This test failed because new rowkey is updated directly

--Testcase 108:
UPDATE rowkey_tbl SET a = 15, b = b + 15 WHERE a = 10; --failed

--get version

--Testcase 109:
\df griddb*

--Testcase 110:
SELECT * FROM public.griddb_fdw_version();

--Testcase 111:
SELECT griddb_fdw_version();
--Test pushdown LIMIT...OFFSET

--Testcase 112:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, * FROM onek LIMIT 1 OFFSET 0;

--Testcase 113:
SELECT tableoid::regclass, * FROM onek LIMIT 1 OFFSET 0;

--Testcase 114:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, * FROM onek LIMIT 1 OFFSET 10;

--Testcase 115:
SELECT tableoid::regclass, * FROM onek LIMIT 1 OFFSET 10;

--Testcase 116:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, * FROM rowkey_tbl LIMIT 1 OFFSET 0;

--Testcase 117:
SELECT ctid, * FROM rowkey_tbl LIMIT 1 OFFSET 0;

--Testcase 118:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, * FROM rowkey_tbl LIMIT 1 OFFSET 1;

--Testcase 119:
SELECT ctid, * FROM rowkey_tbl LIMIT 1 OFFSET 1;

--Testcase 120:
EXPLAIN (verbose, costs off)
INSERT INTO ft2 (c1,c2) SELECT c1+1000, c2 || c2 FROM ft2 LIMIT 20 OFFSET 0;

--Testcase 121:
EXPLAIN (verbose, costs off)
INSERT INTO ft2 (c1,c2) SELECT c1+1000,c2 || c2 FROM ft2 LIMIT 20 OFFSET 5;

--Testcase 122:
DROP FOREIGN TABLE ft1;

--Testcase 123:
CREATE FOREIGN TABLE ft1 (
	c1 int OPTIONS (rowkey 'true', column_name 'C_1'),
	c2 int NOT NULL,
	c3 text,
	c4 timestamp,
	c5 timestamp,
	c6 text,
	c7 text default 'ft1',
	c8 text
) SERVER griddb_svr OPTIONS (table_name 'T1');

--Testcase 124:
INSERT INTO ft1 (c1, c2) VALUES (100, 100), (200, 200);

--Testcase 125:
EXPLAIN VERBOSE
SELECT c1 FROM ft1 WHERE c1 <= 100;

--Testcase 126:
SELECT c1 FROM ft1 WHERE c1 <= 100;

--Testcase 127:
DELETE FROM ft1;

--Testcase 128:
EXPLAIN (verbose, costs off)
SELECT * FROM
  onek t1
  LEFT JOIN num_data num
  ON num.id = 123,
  LATERAL (SELECT num.id, t2.two FROM onek t2 LIMIT 1 OFFSET 0) AS ss
WHERE t1.ten = ss.id;

--Testcase 129:
EXPLAIN (verbose, costs off)
SELECT * FROM
  tenk1 t1
  LEFT JOIN num_data num
  ON num.id = 123,
  LATERAL (SELECT num.id, t2.two FROM onek t2 LIMIT 1 OFFSET 5) AS ss1,
  LATERAL (SELECT ss1.* from onek t3 LIMIT 1 OFFSET 20) AS ss2
WHERE t1.ten = ss2.id;

--Testcase 130:
EXPLAIN (verbose, costs off)
SELECT 1 FROM
  tenk1 AS tt1
  INNER JOIN tenk1 AS tt2 ON (tt1.stringu1 = 'foo')
  LEFT JOIN tenk1 AS tt3 ON (tt3.stringu1 = 'foo')
  LEFT JOIN tenk1 AS tt4 ON (tt3.stringu1 = tt4.stringu2),
  LATERAL (SELECT tt4.ten AS c0 FROM tenk1 AS tt5 LIMIT 1 OFFSET 30) AS ss1
WHERE tt1.ten = ss1.c0;

--Testcase 131:
EXPLAIN (verbose, costs off)
SELECT ss2.* FROM
  tenk1 i41
  LEFT JOIN num_data num
    JOIN (SELECT i42.two AS c1, i43.four AS c2, 42 AS c3
          FROM tenk1 i42, tenk1 i43) ss1
    ON num.id = ss1.c2
  ON i41.two = ss1.c1,
  LATERAL (SELECT i41.*, num.*, ss1.* FROM tenk1 LIMIT 1 OFFSET 10) ss2
WHERE ss1.c2 = 0;

--Testcase 132:
DROP FUNCTION row_before_insupd_trigfunc;

--Testcase 133:
DROP FOREIGN TABLE rowkey_tbl;

--Testcase 134:
CREATE OR REPLACE FUNCTION drop_all_foreign_tables() RETURNS void AS $$
DECLARE
  tbl_name varchar;
  cmd varchar;
BEGIN
  FOR tbl_name IN SELECT foreign_table_name FROM information_schema._pg_foreign_tables LOOP
    cmd := 'DROP FOREIGN TABLE ' || quote_ident(tbl_name);

--Testcase 135:
    EXECUTE cmd;
  END LOOP;
  RETURN;
END
$$ LANGUAGE plpgsql;

--Testcase 136:
SELECT drop_all_foreign_tables();

--Testcase 137:
DROP USER MAPPING FOR public SERVER griddb_svr;

--Testcase 138:
DROP SERVER griddb_svr CASCADE;

--Testcase 139:
DROP EXTENSION griddb_fdw CASCADE;
