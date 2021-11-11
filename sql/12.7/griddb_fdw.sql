\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 83:
CREATE EXTENSION griddb_fdw;
--Testcase 84:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');
--Testcase 85:
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

--Testcase 1:
DELETE FROM department;
--Testcase 2:
DELETE FROM employee;
--Testcase 3:
DELETE FROM empdata;
--Testcase 4:
DELETE FROM numbers;
--Testcase 5:
DELETE FROM evennumbers;

--Testcase 114:
DELETE FROM rowkey_tbl;

--Testcase 6:
SELECT * FROM department LIMIT 10;
--Testcase 7:
SELECT * FROM employee LIMIT 10;
--Testcase 8:
SELECT * FROM empdata LIMIT 10;

--Testcase 9:
INSERT INTO department VALUES(generate_series(1,100), 'dept - ' || generate_series(1,100));
--Testcase 10:
INSERT INTO employee VALUES(generate_series(1,100), 'emp - ' || generate_series(1,100), generate_series(1,100));
--Testcase 11:
INSERT INTO empdata  VALUES(1, decode ('01234567', 'hex'));

--Testcase 12:
INSERT INTO numbers VALUES(1, 'One');
--Testcase 13:
INSERT INTO numbers VALUES(2, 'Two');
--Testcase 14:
INSERT INTO numbers VALUES(3, 'Three');
--Testcase 15:
INSERT INTO numbers VALUES(4, 'Four');
--Testcase 16:
INSERT INTO numbers VALUES(5, 'Five');
--Testcase 17:
INSERT INTO numbers VALUES(6, 'Six');
--Testcase 18:
INSERT INTO numbers VALUES(7, 'Seven');
--Testcase 19:
INSERT INTO numbers VALUES(8, 'Eight');
--Testcase 20:
INSERT INTO numbers VALUES(9, 'Nine');

--Testcase 21:
INSERT INTO evennumbers VALUES(2, 'Two');
--Testcase 22:
INSERT INTO evennumbers VALUES(4, 'Four');
--Testcase 23:
INSERT INTO evennumbers VALUES(6, 'Six');
--Testcase 24:
INSERT INTO evennumbers VALUES(8, 'Eight');

--Testcase 25:
SELECT count(*) FROM department;
--Testcase 26:
SELECT count(*) FROM employee;
--Testcase 27:
SELECT count(*) FROM empdata;

-- Join
--Testcase 28:
SELECT * FROM department d, employee e WHERE d.department_id = e.emp_dept_id LIMIT 10;
-- Subquery
--Testcase 29:
SELECT * FROM department d, employee e WHERE d.department_id IN (SELECT department_id FROM department) LIMIT 10;
--Testcase 30:
SELECT * FROM empdata;
-- Delete single row
--Testcase 31:
DELETE FROM employee WHERE emp_id = 10;

--Testcase 32:
SELECT COUNT(*) FROM department LIMIT 10;
--Testcase 33:
SELECT COUNT(*) FROM employee WHERE emp_id = 10;
-- Update single row
--Testcase 34:
UPDATE employee SET emp_name = 'Updated emp' WHERE emp_id = 20;
--Testcase 35:
SELECT emp_id, emp_name FROM employee WHERE emp_name like 'Updated emp';

--Testcase 36:
UPDATE empdata SET emp_dat = decode ('0123', 'hex');
--Testcase 37:
SELECT * FROM empdata;

--Testcase 38:
SELECT * FROM employee LIMIT 10;
--Testcase 39:
SELECT * FROM employee WHERE emp_id IN (1);
--Testcase 40:
SELECT * FROM employee WHERE emp_id IN (1,3,4,5);
--Testcase 41:
SELECT * FROM employee WHERE emp_id IN (10000,1000);

--Testcase 42:
SELECT * FROM employee WHERE emp_id NOT IN (1) LIMIT 5;
--Testcase 43:
SELECT * FROM employee WHERE emp_id NOT IN (1,3,4,5) LIMIT 5;
--Testcase 44:
SELECT * FROM employee WHERE emp_id NOT IN (10000,1000) LIMIT 5;

--Testcase 45:
SELECT * FROM employee WHERE emp_id NOT IN (SELECT emp_id FROM employee WHERE emp_id IN (1,10));
--Testcase 46:
SELECT * FROM employee WHERE emp_name NOT IN ('emp - 1', 'emp - 2') LIMIT 5;
--Testcase 47:
SELECT * FROM employee WHERE emp_name NOT IN ('emp - 10') LIMIT 5;

--Testcase 86:
CREATE OR REPLACE FUNCTION test_param_where() RETURNS void AS $$
DECLARE
  n varchar;
BEGIN
  FOR x IN 1..9 LOOP
--Testcase 87:
    SELECT b INTO n FROM numbers WHERE a=x;
    RAISE NOTICE 'Found number %', n;
  END LOOP;
  RETURN;
END
$$ LANGUAGE plpgsql;

--Testcase 48:
SELECT test_param_where();

--Testcase 135:
ALTER FOREIGN TABLE numbers OPTIONS (table_name 'evennumbers');
--Testcase 49:
INSERT INTO numbers VALUES(10, 'Ten');
--Testcase 50:
SELECT * FROM numbers;

--Testcase 136:
SET griddbfdw.enable_partial_execution TO TRUE;
--Testcase 51:
SELECT * FROM numbers;
--Testcase 137:
SET griddbfdw.enable_partial_execution TO FALSE;

--Testcase 52:
DELETE FROM employee;
--Testcase 53:
DELETE FROM department;
--Testcase 54:
DELETE FROM empdata;
--Testcase 55:
DELETE FROM numbers;

--Testcase 88:
DROP FUNCTION test_param_where();
--Testcase 89:
DROP FOREIGN TABLE numbers;
--Testcase 90:
DROP FOREIGN TABLE department;
--Testcase 91:
DROP FOREIGN TABLE employee;
--Testcase 92:
DROP FOREIGN TABLE empdata;

-- -----------------------------------------------------------------------------
--Testcase 56:
DELETE FROM shorty;
--Testcase 57:
INSERT INTO shorty (id, c) VALUES (1, 'Z');
--Testcase 58:
INSERT INTO shorty (id, c) VALUES (2, 'Y');
--Testcase 59:
INSERT INTO shorty (id, c) VALUES (5, 'A');
--Testcase 60:
INSERT INTO shorty (id, c) VALUES (3, 'X');
--Testcase 61:
INSERT INTO shorty (id, c) VALUES (4, 'B');

-- ORDER BY.
--Testcase 62:
SELECT c FROM shorty ORDER BY id;

-- Transaction INSERT
BEGIN;
--Testcase 63:
INSERT INTO shorty (id, c) VALUES (6, 'T');
ROLLBACK;
--Testcase 64:
SELECT id, c FROM shorty;

-- Transaction UPDATE single row
BEGIN;
--Testcase 65:
UPDATE shorty SET c = 'd' WHERE id = 5;
ROLLBACK;
--Testcase 66:
SELECT id, c FROM shorty;

-- Transaction UPDATE all
BEGIN;
--Testcase 67:
UPDATE shorty SET c = 'd';
ROLLBACK;
--Testcase 68:
SELECT id, c FROM shorty;

-- Transaction DELETE single row
BEGIN;
--Testcase 69:
DELETE FROM shorty WHERE id = 1;
ROLLBACK;
--Testcase 70:
SELECT id, c FROM shorty;

-- Transaction DELETE all
BEGIN;
--Testcase 71:
DELETE FROM shorty;
ROLLBACK;
--Testcase 72:
SELECT id, c FROM shorty;

-- Use of NULL value
BEGIN;
--Testcase 73:
INSERT INTO shorty VALUES(99, NULL);
--Testcase 74:
UPDATE shorty SET c = NULL WHERE id = 3;
--Testcase 75:
SELECT id FROM shorty WHERE c IS NULL;
ROLLBACK;

-- parameters.
--Testcase 76:
PREPARE stmt(integer) AS SELECT * FROM shorty WHERE id = $1;
--Testcase 77:
EXECUTE stmt(1);
--Testcase 78:
EXECUTE stmt(2);
DEALLOCATE stmt;

-- test NULL parameter
--Testcase 79:
SELECT id FROM shorty WHERE c = (SELECT NULL::text);

-- Use of system column
--Testcase 80:
SELECT tableoid::regclass, * from shorty WHERE id = 1;
--Testcase 81:
SELECT * from shorty WHERE id = 1 AND tableoid = 'shorty'::regclass;

-- Clean up
--Testcase 93:
DROP FOREIGN TABLE shorty;

-- Test rowkey columni with trigger and without trigger
-- Prepare data
--Testcase 100:
INSERT INTO rowkey_tbl VALUES (0, 5);

-- Test with trigger
--Testcase 101:
CREATE FUNCTION row_before_insupd_trigfunc() RETURNS trigger AS $$BEGIN NEW.a := NEW.a + 10; RETURN NEW; END$$ LANGUAGE plpgsql;
--Testcase 102:
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON rowkey_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();

--Testcase 103:
INSERT INTO rowkey_tbl VALUES (0, 5);
--Testcase 104:
SELECT * FROM rowkey_tbl;

-- This test failed because new rowkey value will be updated to old rowkey value
--Testcase 105:
UPDATE rowkey_tbl SET b = b + 10; -- failed

-- Test without trigger
--Testcase 106:
DROP TRIGGER row_before_insupd_trigger ON rowkey_tbl;

-- This test OK because rowkey value is not changed
--Testcase 107:
UPDATE rowkey_tbl SET b = b + 10; -- ok
--Testcase 108:
SELECT * FROM rowkey_tbl;

-- This test failed  because rowkey is updated directly even its value not changed
--Testcase 109:
UPDATE rowkey_tbl SET a = 10, b = b + 15 WHERE a = 10; --failed
--Testcase 110:
SELECT * FROM rowkey_tbl;

-- This test failed because new rowkey is updated directly
--Testcase 111:
UPDATE rowkey_tbl SET a = 15, b = b + 15 WHERE a = 10; --failed

--get version
--Testcase 114:
\df griddb*
--Testcase 115:
SELECT * FROM public.griddb_fdw_version();
--Testcase 116:
SELECT griddb_fdw_version();
--Test pushdown LIMIT...OFFSET
--Testcase 115:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, * FROM onek LIMIT 1 OFFSET 0;

--Testcase 116:
SELECT tableoid::regclass, * FROM onek LIMIT 1 OFFSET 0;

--Testcase 117:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, * FROM onek LIMIT 1 OFFSET 10;

--Testcase 118:
SELECT tableoid::regclass, * FROM onek LIMIT 1 OFFSET 10;

--Testcase 119:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, * FROM rowkey_tbl LIMIT 1 OFFSET 0;

--Testcase 120:
SELECT ctid, * FROM rowkey_tbl LIMIT 1 OFFSET 0;

--Testcase 121:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, * FROM rowkey_tbl LIMIT 1 OFFSET 1;

--Testcase 122:
SELECT ctid, * FROM rowkey_tbl LIMIT 1 OFFSET 1;

--Testcase 123:
EXPLAIN (verbose, costs off)
INSERT INTO ft2 (c1,c2) SELECT c1+1000, c2 || c2 FROM ft2 LIMIT 20 OFFSET 0;

--Testcase 124:
EXPLAIN (verbose, costs off)
INSERT INTO ft2 (c1,c2) SELECT c1+1000,c2 || c2 FROM ft2 LIMIT 20 OFFSET 5;

--Testcase 129:
DROP FOREIGN TABLE ft1;
--Testcase 130:
CREATE FOREIGN TABLE ft1 (
	c1 int OPTIONS (rowkey 'true', column_name 'C_1'),
	c2 int NOT NULL,
	c3 text,
	c4 timestamp,
	c5 timestamp,
	c6 text default 'position',
	c7 text default 'ft1',
	c8 text default 'company'
) SERVER griddb_svr OPTIONS (table_name 'T1');

--Testcase 133:
INSERT INTO ft1 (c1, c2) VALUES (100, 100), (200, 200);

--Testcase 131:
EXPLAIN VERBOSE
SELECT c1 FROM ft1 WHERE c1 <= 100;
--Testcase 132:
SELECT c1 FROM ft1 WHERE c1 <= 100;

--Testcase 134:
DELETE FROM ft1;

--Testcase 138:
INSERT INTO ft1 (c1, c2, c3, c4, c5) VALUES (1, 11, 'ADMIN', '1970-05-06 00:00:00', '1970-05-06 00:00:00');
--Testcase 139:
INSERT INTO ft1 (c1, c2, c3, c4, c5) VALUES (2, 12, 'SALESMAN', '1970-05-07 00:00:00', '1970-05-07 00:00:00');
--Testcase 140:
INSERT INTO ft1 (c1, c2, c3, c4, c5) VALUES (3, 13, 'SALESMAN', '1970-05-08 00:00:00', '1970-05-08 00:00:00');
--Testcase 141:
INSERT INTO ft1 (c1, c2, c3, c4, c5) VALUES (4, 14, 'MANAGER', '1970-05-09 00:00:00', '1970-05-09 00:00:00');
--Testcase 142:
INSERT INTO ft1 (c1, c2, c3, c4, c5) VALUES (5, 15, 'SALESMAN', '1970-05-10 00:00:00', '1970-05-10 00:00:00');
--Testcase 143:
INSERT INTO ft1 (c1, c2, c3, c4, c5) VALUES (6, 16, 'MANAGER', '1970-05-11 00:00:00', '1970-05-11 00:00:00');
--Testcase 144:
INSERT INTO ft1 (c1, c2, c3, c4, c5) VALUES (7, 17, 'MANAGER', '1970-05-12 00:00:00', '1970-05-12 00:00:00');
--Testcase 145:
INSERT INTO ft1 (c1, c2, c3, c4, c5) VALUES (8, 18, 'FINANCE', '1970-05-13 00:00:00', '1970-05-13 00:00:00');
--Testcase 146:
INSERT INTO ft1 (c1, c2, c3, c4, c5) VALUES (9, 19, 'HEAD', '1970-05-14 00:00:00', '1970-05-14 00:00:00');
--Testcase 147:
INSERT INTO ft1 (c1, c2, c3, c4, c5) VALUES (10, 20, 'SALESMAN', '1970-05-15 00:00:00', '1970-05-15 00:00:00');

-- ANY(ARRAY(parameter)) with parameter is const/expression and operators (=, <>, <, >, <=, >=)
--Testcase 148:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 = ANY (ARRAY[1,2,3]);
--Testcase 149:
SELECT * FROM ft1 WHERE c1 = ANY (ARRAY[1,2,3]);
--Testcase 150:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 <> ANY (ARRAY[1,2,3]);
--Testcase 151:
SELECT * FROM ft1 WHERE c1 <> ANY (ARRAY[1,2,3]);
--Testcase 152:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 > ANY (ARRAY[1,2,3]);
--Testcase 153:
SELECT * FROM ft1 WHERE c1 > ANY (ARRAY[1,2,3]);
--Testcase 154:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 < ANY (ARRAY[1,2,3]);
--Testcase 155:
SELECT * FROM ft1 WHERE c1 < ANY (ARRAY[1,2,3]);
--Testcase 156:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 >= ANY (ARRAY[1,2,3]);
--Testcase 157:
SELECT * FROM ft1 WHERE c1 >= ANY (ARRAY[1,2,3]);
--Testcase 158:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 <= ANY (ARRAY[1,2,3]);
--Testcase 159:
SELECT * FROM ft1 WHERE c1 <= ANY (ARRAY[1,2,3]);
--Testcase 160:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = ANY(ARRAY[c1, 1, c2 + 1]);
--Testcase 161:
SELECT * FROM ft1 t1 WHERE c1 = ANY(ARRAY[c1, 1, c2 + 1]);
--Testcase 162:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 <> ANY(ARRAY[c1, 1, c2 + 1]);
--Testcase 163:
SELECT * FROM ft1 t1 WHERE c1 <> ANY(ARRAY[c1, 1, c2 + 1]);
--Testcase 164:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 > ANY(ARRAY[c1, 1, c2 + 1]);
--Testcase 165:
SELECT * FROM ft1 t1 WHERE c1 > ANY(ARRAY[c1, 1, c2 + 1]);
--Testcase 166:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 < ANY(ARRAY[c1, 1, c2 + 1]);
--Testcase 167:
SELECT * FROM ft1 t1 WHERE c1 < ANY(ARRAY[c1, 1, c2 + 1]);
--Testcase 168:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 <= ANY(ARRAY[c1, 1, c2 + 1]);
--Testcase 169:
SELECT * FROM ft1 t1 WHERE c1 <= ANY(ARRAY[c1, 1, c2 + 1]);
--Testcase 170:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 >= ANY(ARRAY[c1, 1, c2 + 1]);
--Testcase 171:
SELECT * FROM ft1 t1 WHERE c1 >= ANY(ARRAY[c1, 1, c2 + 1]);

-- ALL(ARRAY(parameter)) with parameter is const/expression and operators (=, <>, <, >, <=, >=)
--Testcase 172:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 <> ALL (ARRAY[1,2,3]);
--Testcase 173:
SELECT * FROM ft1 WHERE c1 <> ALL (ARRAY[1,2,3]);
--Testcase 174:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 > ALL (ARRAY[1,2,3]);
--Testcase 175:
SELECT * FROM ft1 WHERE c1 > ALL (ARRAY[1,2,3]);
--Testcase 176:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 < ALL (ARRAY[2,3,4]);
--Testcase 177:
SELECT * FROM ft1 WHERE c1 < ALL (ARRAY[2,3,4]);
--Testcase 178:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 >= ALL (ARRAY[1,2,3]);
--Testcase 179:
SELECT * FROM ft1 WHERE c1 >= ALL (ARRAY[1,2,3]);
--Testcase 180:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 <= ALL (ARRAY[1,2,3]);
--Testcase 181:
SELECT * FROM ft1 WHERE c1 <= ALL (ARRAY[1,2,3]);
--Testcase 182:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 <> ALL(ARRAY[c1 + 1, 1, 3]);
--Testcase 183:
SELECT * FROM ft1 t1 WHERE c1 <> ALL(ARRAY[c1 + 1, 1, 3]);
--Testcase 184:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 > ALL(ARRAY[c1 - 1, 1, 3]);
--Testcase 185:
SELECT * FROM ft1 t1 WHERE c1 > ALL(ARRAY[c1 - 1, 1, 3]);
--Testcase 186:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 < ALL(ARRAY[c1 + 1, 5, 6]);
--Testcase 187:
SELECT * FROM ft1 t1 WHERE c1 < ALL(ARRAY[c1 + 1, 5, 6]);
--Testcase 188:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 <= ALL(ARRAY[c1, 1, c2 + 1]);
--Testcase 189:
SELECT * FROM ft1 t1 WHERE c1 <= ALL(ARRAY[c1, 1, c2 + 1]);
--Testcase 190:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 >= ALL(ARRAY[c1 - 1, 4, 5]);
--Testcase 191:
SELECT * FROM ft1 t1 WHERE c1 >= ALL(ARRAY[c1 - 1, 4, 5]);

--Testcase 192:
DELETE FROM ft1;

--Testcase 125:
EXPLAIN (verbose, costs off)
SELECT * FROM
  onek t1
  LEFT JOIN num_data num
  ON num.id = 123,
  LATERAL (SELECT num.id, t2.two FROM onek t2 LIMIT 1 OFFSET 0) AS ss
WHERE t1.ten = ss.id;

--Testcase 126:
EXPLAIN (verbose, costs off)
SELECT * FROM
  tenk1 t1
  LEFT JOIN num_data num
  ON num.id = 123,
  LATERAL (SELECT num.id, t2.two FROM onek t2 LIMIT 1 OFFSET 5) AS ss1,
  LATERAL (SELECT ss1.* from onek t3 LIMIT 1 OFFSET 20) AS ss2
WHERE t1.ten = ss2.id;

--Testcase 127:
EXPLAIN (verbose, costs off)
SELECT 1 FROM
  tenk1 AS tt1
  INNER JOIN tenk1 AS tt2 ON (tt1.stringu1 = 'foo')
  LEFT JOIN tenk1 AS tt3 ON (tt3.stringu1 = 'foo')
  LEFT JOIN tenk1 AS tt4 ON (tt3.stringu1 = tt4.stringu2),
  LATERAL (SELECT tt4.ten AS c0 FROM tenk1 AS tt5 LIMIT 1 OFFSET 30) AS ss1
WHERE tt1.ten = ss1.c0;

--Testcase 128:
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

--Testcase 112:
DROP FUNCTION row_before_insupd_trigfunc;
--Testcase 113:
DROP FOREIGN TABLE rowkey_tbl;

--Testcase 94:
CREATE OR REPLACE FUNCTION drop_all_foreign_tables() RETURNS void AS $$
DECLARE
  tbl_name varchar;
  cmd varchar;
BEGIN
  FOR tbl_name IN SELECT foreign_table_name FROM information_schema._pg_foreign_tables LOOP
    cmd := 'DROP FOREIGN TABLE ' || quote_ident(tbl_name);
--Testcase 95:
    EXECUTE cmd;
  END LOOP;
  RETURN;
END
$$ LANGUAGE plpgsql;
--Testcase 82:
SELECT drop_all_foreign_tables();

--Testcase 96:
DROP USER MAPPING FOR public SERVER griddb_svr;
--Testcase 97:
DROP SERVER griddb_svr CASCADE;
--Testcase 98:
DROP EXTENSION griddb_fdw CASCADE;
