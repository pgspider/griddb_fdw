CREATE EXTENSION griddb_fdw;
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS(host '239.0.0.1', port '31999', clustername 'ktymCluster');
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS(username 'admin', password 'testadmin');

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

DELETE FROM department;
DELETE FROM employee;
DELETE FROM empdata;
DELETE FROM numbers;
DELETE FROM evennumbers;

SELECT * FROM department LIMIT 10;
SELECT * FROM employee LIMIT 10;
SELECT * FROM empdata LIMIT 10;

INSERT INTO department VALUES(generate_series(1,100), 'dept - ' || generate_series(1,100));
INSERT INTO employee VALUES(generate_series(1,100), 'emp - ' || generate_series(1,100), generate_series(1,100));
INSERT INTO empdata  VALUES(1, decode ('01234567', 'hex'));

INSERT INTO numbers VALUES(1, 'One');
INSERT INTO numbers VALUES(2, 'Two');
INSERT INTO numbers VALUES(3, 'Three');
INSERT INTO numbers VALUES(4, 'Four');
INSERT INTO numbers VALUES(5, 'Five');
INSERT INTO numbers VALUES(6, 'Six');
INSERT INTO numbers VALUES(7, 'Seven');
INSERT INTO numbers VALUES(8, 'Eight');
INSERT INTO numbers VALUES(9, 'Nine');

INSERT INTO evennumbers VALUES(2, 'Two');
INSERT INTO evennumbers VALUES(4, 'Four');
INSERT INTO evennumbers VALUES(6, 'Six');
INSERT INTO evennumbers VALUES(8, 'Eight');

SELECT count(*) FROM department;
SELECT count(*) FROM employee;
SELECT count(*) FROM empdata;

-- Join
SELECT * FROM department d, employee e WHERE d.department_id = e.emp_dept_id LIMIT 10;
-- Subquery
SELECT * FROM department d, employee e WHERE d.department_id IN (SELECT department_id FROM department) LIMIT 10;
SELECT * FROM empdata;
-- Delete single row
DELETE FROM employee WHERE emp_id = 10;

SELECT COUNT(*) FROM department LIMIT 10;
SELECT COUNT(*) FROM employee WHERE emp_id = 10;
-- Update single row
UPDATE employee SET emp_name = 'Updated emp' WHERE emp_id = 20;
SELECT emp_id, emp_name FROM employee WHERE emp_name like 'Updated emp';

UPDATE empdata SET emp_dat = decode ('0123', 'hex');
SELECT * FROM empdata;

SELECT * FROM employee LIMIT 10;
SELECT * FROM employee WHERE emp_id IN (1);
SELECT * FROM employee WHERE emp_id IN (1,3,4,5);
SELECT * FROM employee WHERE emp_id IN (10000,1000);

SELECT * FROM employee WHERE emp_id NOT IN (1) LIMIT 5;
SELECT * FROM employee WHERE emp_id NOT IN (1,3,4,5) LIMIT 5;
SELECT * FROM employee WHERE emp_id NOT IN (10000,1000) LIMIT 5;

SELECT * FROM employee WHERE emp_id NOT IN (SELECT emp_id FROM employee WHERE emp_id IN (1,10));
SELECT * FROM employee WHERE emp_name NOT IN ('emp - 1', 'emp - 2') LIMIT 5;
SELECT * FROM employee WHERE emp_name NOT IN ('emp - 10') LIMIT 5;

CREATE OR REPLACE FUNCTION test_param_where() RETURNS void AS $$
DECLARE
  n varchar;
BEGIN
  FOR x IN 1..9 LOOP
    SELECT b INTO n FROM numbers WHERE a=x;
    RAISE NOTICE 'Found number %', n;
  END LOOP;
  RETURN;
END
$$ LANGUAGE plpgsql;

SELECT test_param_where();

ALTER FOREIGN TABLE numbers OPTIONS (table_name 'evennumbers');
INSERT INTO numbers VALUES(10, 'Ten');
SELECT * FROM numbers;

DELETE FROM employee;
DELETE FROM department;
DELETE FROM empdata;
DELETE FROM numbers;

DROP FUNCTION test_param_where();
DROP FOREIGN TABLE numbers;
DROP FOREIGN TABLE department;
DROP FOREIGN TABLE employee;
DROP FOREIGN TABLE empdata;

-- -----------------------------------------------------------------------------
DELETE FROM shorty;
INSERT INTO shorty (id, c) VALUES (1, 'Z');
INSERT INTO shorty (id, c) VALUES (2, 'Y');
INSERT INTO shorty (id, c) VALUES (5, 'A');
INSERT INTO shorty (id, c) VALUES (3, 'X');
INSERT INTO shorty (id, c) VALUES (4, 'B');

-- ORDER BY.
SELECT c FROM shorty ORDER BY id;

-- Transaction INSERT
BEGIN;
INSERT INTO shorty (id, c) VALUES (6, 'T');
ROLLBACK;
SELECT id, c FROM shorty;

-- Transaction UPDATE single row
BEGIN;
UPDATE shorty SET c = 'd' WHERE id = 5;
ROLLBACK;
SELECT id, c FROM shorty;

-- Transaction UPDATE all
BEGIN;
UPDATE shorty SET c = 'd';
ROLLBACK;
SELECT id, c FROM shorty;

-- Transaction DELETE single row
BEGIN;
DELETE FROM shorty WHERE id = 1;
ROLLBACK;
SELECT id, c FROM shorty;

-- Transaction DELETE all
BEGIN;
DELETE FROM shorty;
ROLLBACK;
SELECT id, c FROM shorty;

-- parameters.
PREPARE stmt(integer) AS SELECT * FROM shorty WHERE id = $1;
EXECUTE stmt(1);
EXECUTE stmt(2);
DEALLOCATE stmt;

-- test NULL parameter
SELECT id FROM shorty WHERE c = (SELECT NULL::text);

-- Clean up
DROP FOREIGN TABLE shorty;

CREATE OR REPLACE FUNCTION drop_all_foreign_tables() RETURNS void AS $$
DECLARE
  tbl_name varchar;
  cmd varchar;
BEGIN
  FOR tbl_name IN SELECT foreign_table_name FROM information_schema._pg_foreign_tables LOOP
    cmd := 'DROP FOREIGN TABLE ' || tbl_name;
    EXECUTE cmd;
  END LOOP;
  RETURN;
END
$$ LANGUAGE plpgsql;
SELECT drop_all_foreign_tables();

DROP USER MAPPING FOR public SERVER griddb_svr;
DROP SERVER griddb_svr CASCADE;
DROP EXTENSION griddb_fdw CASCADE;
