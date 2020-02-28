--
-- INT4
--
CREATE EXTENSION griddb_fdw;
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS(host '239.0.0.1', port '31999', clustername 'griddbfdwTestCluster');
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS(username 'admin', password 'testadmin');
CREATE FOREIGN TABLE INT4_TBL(id serial OPTIONS (rowkey 'true'), f1 int4) SERVER griddb_svr; 

INSERT INTO INT4_TBL(f1) VALUES ('   0  ');

INSERT INTO INT4_TBL(f1) VALUES ('123456     ');

INSERT INTO INT4_TBL(f1) VALUES ('    -123456');

INSERT INTO INT4_TBL(f1) VALUES ('34.5');

-- largest and smallest values
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');

INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');

-- bad input values -- should give errors
INSERT INTO INT4_TBL(f1) VALUES ('1000000000000');
INSERT INTO INT4_TBL(f1) VALUES ('asdf');
INSERT INTO INT4_TBL(f1) VALUES ('     ');
INSERT INTO INT4_TBL(f1) VALUES ('   asdf   ');
INSERT INTO INT4_TBL(f1) VALUES ('- 1234');
INSERT INTO INT4_TBL(f1) VALUES ('123       5');
INSERT INTO INT4_TBL(f1) VALUES ('');


SELECT '' AS five, f1 FROM INT4_TBL;

SELECT '' AS four, i.f1 FROM INT4_TBL i WHERE i.f1 <> int2 '0';

SELECT '' AS four, i.f1 FROM INT4_TBL i WHERE i.f1 <> int4 '0';

SELECT '' AS one, i.f1 FROM INT4_TBL i WHERE i.f1 = int2 '0';

SELECT '' AS one, i.f1 FROM INT4_TBL i WHERE i.f1 = int4 '0';

SELECT '' AS two, i.f1 FROM INT4_TBL i WHERE i.f1 < int2 '0';

SELECT '' AS two, i.f1 FROM INT4_TBL i WHERE i.f1 < int4 '0';

SELECT '' AS three, i.f1 FROM INT4_TBL i WHERE i.f1 <= int2 '0';

SELECT '' AS three, i.f1 FROM INT4_TBL i WHERE i.f1 <= int4 '0';

SELECT '' AS two, i.f1 FROM INT4_TBL i WHERE i.f1 > int2 '0';

SELECT '' AS two, i.f1 FROM INT4_TBL i WHERE i.f1 > int4 '0';

SELECT '' AS three, i.f1 FROM INT4_TBL i WHERE i.f1 >= int2 '0';

SELECT '' AS three, i.f1 FROM INT4_TBL i WHERE i.f1 >= int4 '0';

-- positive odds
SELECT '' AS one, i.f1 FROM INT4_TBL i WHERE (i.f1 % int2 '2') = int2 '1';

-- any evens
SELECT '' AS three, i.f1 FROM INT4_TBL i WHERE (i.f1 % int4 '2') = int2 '0';

SELECT '' AS five, i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

SELECT '' AS five, i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

SELECT '' AS five, i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

SELECT '' AS five, i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

SELECT '' AS five, i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

SELECT '' AS five, i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

SELECT '' AS five, i.f1, i.f1 / int2 '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 / int4 '2' AS x FROM INT4_TBL i;

--
-- more complex expressions
--

-- variations on unary minus parsing
BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES (-2+3);
SELECT f1 as one FROM INT4_TBL;
ROLLBACK;

BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES (4-2);
SELECT f1 as two FROM INT4_TBL;
ROLLBACK;

BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES (2- -1);
SELECT f1 as three FROM INT4_TBL;
ROLLBACK;

BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES (2 - -2);
SELECT f1 as four FROM INT4_TBL;
ROLLBACK;

BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES ((int2 '2' * int2 '2' = int2 '16' / int2 '4')::int4);
SELECT f1::BOOLEAN as true FROM INT4_TBL;
ROLLBACK;

BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES ((int4 '2' * int2 '2' = int2 '16' / int4 '4')::int4);
SELECT f1::BOOLEAN as true FROM INT4_TBL;
ROLLBACK;

BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES ((int2 '2' * int4 '2' = int4 '16' / int2 '4')::int4);
SELECT f1::BOOLEAN as true FROM INT4_TBL;
ROLLBACK;

BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES ((int4 '1000' < int4 '999')::int4);
SELECT f1::BOOLEAN as false FROM INT4_TBL;
ROLLBACK;

BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES (4!);
SELECT f1 as twenty_four FROM INT4_TBL;
ROLLBACK;

BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES (!!3);
SELECT f1 as six FROM INT4_TBL;
ROLLBACK;

BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES (1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1);
SELECT f1 as ten FROM INT4_TBL;
ROLLBACK;

BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES (2 + 2 / 2);
SELECT f1 as three FROM INT4_TBL;
ROLLBACK;

BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES ((2 + 2) / 2);
SELECT f1 as two FROM INT4_TBL;
ROLLBACK;

-- corner case
BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES ((-1::int4<<31));
INSERT INTO INT4_TBL(f1) VALUES (((-1::int4<<31)+1));
SELECT f1 FROM INT4_TBL;
ROLLBACK;

-- check sane handling of INT_MIN overflow cases
BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4 * f1 (-1)::int4);
SELECT f1 FROM INT4_TBL;
ROLLBACK;
BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4 / (-1)::int4);
SELECT f1 FROM INT4_TBL;
ROLLBACK;
BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4 % (-1)::int4);
SELECT f1 FROM INT4_TBL;
ROLLBACK;
BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4 * (-1)::int2);
SELECT f1 FROM INT4_TBL;
ROLLBACK;
BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4 / (-1)::int2);
SELECT f1 FROM INT4_TBL;
ROLLBACK;
BEGIN;
DELETE FROM INT4_TBL;
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4 % (-1)::int2);
SELECT f1 FROM INT4_TBL;
ROLLBACK;

-- check rounding when casting from float
CREATE FOREIGN TABLE FLOAT8_TBL(id serial OPTIONS (rowkey 'true'), f1 float8) SERVER griddb_svr; 
BEGIN;
DELETE FROM FLOAT8_TBL;
INSERT INTO FLOAT8_TBL(f1) VALUES 
	(-2.5::float8),
        (-1.5::float8),
        (-0.5::float8),
        (0.0::float8),
        (0.5::float8),
        (1.5::float8),
        (2.5::float8);
SELECT f1 as x, f1::int4 AS int4_value FROM FLOAT8_TBL;
ROLLBACK;

-- check rounding when casting from numeric
BEGIN;
DELETE FROM FLOAT8_TBL;
INSERT INTO FLOAT8_TBL(f1) VALUES 
	(-2.5::numeric),
        (-1.5::numeric),
        (-0.5::numeric),
        (0.0::numeric),
        (0.5::numeric),
        (1.5::numeric),
        (2.5::numeric);
SELECT f1::numeric as x, f1::numeric::int4 AS int4_value FROM FLOAT8_TBL;
ROLLBACK;

DROP FOREIGN TABLE INT4_TBL;
DROP FOREIGN TABLE FLOAT8_TBL;
DROP USER MAPPING FOR public SERVER griddb_svr;
DROP SERVER griddb_svr;
DROP EXTENSION griddb_fdw CASCADE;
