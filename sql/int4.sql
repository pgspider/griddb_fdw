--
-- INT4
--
DROP EXTENSION griddb_fdw cascade;
CREATE EXTENSION griddb_fdw;
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS(host '239.0.0.1', port '31999', clustername 'griddbfdwTestCluster');
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS(username 'admin', password 'testadmin');
CREATE FOREIGN TABLE INT4_TBL(id serial OPTIONS (rowkey 'true'), f1 int4) SERVER griddb_svr;

--Testcase 1:
INSERT INTO INT4_TBL(f1) VALUES ('   0  ');

--Testcase 2:
INSERT INTO INT4_TBL(f1) VALUES ('123456     ');

--Testcase 3:
INSERT INTO INT4_TBL(f1) VALUES ('    -123456');

--Testcase 4:
INSERT INTO INT4_TBL(f1) VALUES ('34.5');

-- largest and smallest values
--Testcase 5:
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');

--Testcase 6:
INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');

-- bad input values -- should give errors
--Testcase 7:
INSERT INTO INT4_TBL(f1) VALUES ('1000000000000');
--Testcase 8:
INSERT INTO INT4_TBL(f1) VALUES ('asdf');
--Testcase 9:
INSERT INTO INT4_TBL(f1) VALUES ('     ');
--Testcase 10:
INSERT INTO INT4_TBL(f1) VALUES ('   asdf   ');
--Testcase 11:
INSERT INTO INT4_TBL(f1) VALUES ('- 1234');
--Testcase 12:
INSERT INTO INT4_TBL(f1) VALUES ('123       5');
--Testcase 13:
INSERT INTO INT4_TBL(f1) VALUES ('');


--Testcase 14:
SELECT '' AS five, f1 FROM INT4_TBL;

--Testcase 15:
SELECT '' AS four, i.f1 FROM INT4_TBL i WHERE i.f1 <> int2 '0';

--Testcase 16:
SELECT '' AS four, i.f1 FROM INT4_TBL i WHERE i.f1 <> int4 '0';

--Testcase 17:
SELECT '' AS one, i.f1 FROM INT4_TBL i WHERE i.f1 = int2 '0';

--Testcase 18:
SELECT '' AS one, i.f1 FROM INT4_TBL i WHERE i.f1 = int4 '0';

--Testcase 19:
SELECT '' AS two, i.f1 FROM INT4_TBL i WHERE i.f1 < int2 '0';

--Testcase 20:
SELECT '' AS two, i.f1 FROM INT4_TBL i WHERE i.f1 < int4 '0';

--Testcase 21:
SELECT '' AS three, i.f1 FROM INT4_TBL i WHERE i.f1 <= int2 '0';

--Testcase 22:
SELECT '' AS three, i.f1 FROM INT4_TBL i WHERE i.f1 <= int4 '0';

--Testcase 23:
SELECT '' AS two, i.f1 FROM INT4_TBL i WHERE i.f1 > int2 '0';

--Testcase 24:
SELECT '' AS two, i.f1 FROM INT4_TBL i WHERE i.f1 > int4 '0';

--Testcase 25:
SELECT '' AS three, i.f1 FROM INT4_TBL i WHERE i.f1 >= int2 '0';

--Testcase 26:
SELECT '' AS three, i.f1 FROM INT4_TBL i WHERE i.f1 >= int4 '0';

-- positive odds
--Testcase 27:
SELECT '' AS one, i.f1 FROM INT4_TBL i WHERE (i.f1 % int2 '2') = int2 '1';

-- any evens
--Testcase 28:
SELECT '' AS three, i.f1 FROM INT4_TBL i WHERE (i.f1 % int4 '2') = int2 '0';

--Testcase 29:
SELECT '' AS five, i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i;

--Testcase 30:
SELECT '' AS five, i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

--Testcase 31:
SELECT '' AS five, i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i;

--Testcase 32:
SELECT '' AS five, i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

--Testcase 33:
SELECT '' AS five, i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i;

--Testcase 34:
SELECT '' AS five, i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

--Testcase 35:
SELECT '' AS five, i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i;

--Testcase 36:
SELECT '' AS five, i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

--Testcase 37:
SELECT '' AS five, i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i;

--Testcase 38:
SELECT '' AS five, i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

--Testcase 39:
SELECT '' AS five, i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i;

--Testcase 40:
SELECT '' AS five, i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

--Testcase 41:
SELECT '' AS five, i.f1, i.f1 / int2 '2' AS x FROM INT4_TBL i;

--Testcase 42:
SELECT '' AS five, i.f1, i.f1 / int4 '2' AS x FROM INT4_TBL i;

--
-- more complex expressions
--

-- variations on unary minus parsing
BEGIN;
--Testcase 43:
DELETE FROM INT4_TBL;
--Testcase 44:
INSERT INTO INT4_TBL(f1) VALUES (-2+3);
--Testcase 45:
SELECT f1 as one FROM INT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 46:
DELETE FROM INT4_TBL;
--Testcase 47:
INSERT INTO INT4_TBL(f1) VALUES (4-2);
--Testcase 48:
SELECT f1 as two FROM INT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 49:
DELETE FROM INT4_TBL;
--Testcase 50:
INSERT INTO INT4_TBL(f1) VALUES (2- -1);
--Testcase 51:
SELECT f1 as three FROM INT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 52:
DELETE FROM INT4_TBL;
--Testcase 53:
INSERT INTO INT4_TBL(f1) VALUES (2 - -2);
--Testcase 54:
SELECT f1 as four FROM INT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 55:
DELETE FROM INT4_TBL;
--Testcase 56:
INSERT INTO INT4_TBL(f1) VALUES ((int2 '2' * int2 '2' = int2 '16' / int2 '4')::int4);
--Testcase 57:
SELECT f1::BOOLEAN as true FROM INT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 58:
DELETE FROM INT4_TBL;
--Testcase 59:
INSERT INTO INT4_TBL(f1) VALUES ((int4 '2' * int2 '2' = int2 '16' / int4 '4')::int4);
--Testcase 60:
SELECT f1::BOOLEAN as true FROM INT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 61:
DELETE FROM INT4_TBL;
--Testcase 62:
INSERT INTO INT4_TBL(f1) VALUES ((int2 '2' * int4 '2' = int4 '16' / int2 '4')::int4);
--Testcase 63:
SELECT f1::BOOLEAN as true FROM INT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 64:
DELETE FROM INT4_TBL;
--Testcase 65:
INSERT INTO INT4_TBL(f1) VALUES ((int4 '1000' < int4 '999')::int4);
--Testcase 66:
SELECT f1::BOOLEAN as false FROM INT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 67:
DELETE FROM INT4_TBL;
--Testcase 68:
INSERT INTO INT4_TBL(f1) VALUES (4!);
--Testcase 69:
SELECT f1 as twenty_four FROM INT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 70:
DELETE FROM INT4_TBL;
--Testcase 71:
INSERT INTO INT4_TBL(f1) VALUES (!!3);
--Testcase 72:
SELECT f1 as six FROM INT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 73:
DELETE FROM INT4_TBL;
--Testcase 74:
INSERT INTO INT4_TBL(f1) VALUES (1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1);
--Testcase 75:
SELECT f1 as ten FROM INT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 76:
DELETE FROM INT4_TBL;
--Testcase 77:
INSERT INTO INT4_TBL(f1) VALUES (2 + 2 / 2);
--Testcase 78:
SELECT f1 as three FROM INT4_TBL;
ROLLBACK;

BEGIN;
--Testcase 79:
DELETE FROM INT4_TBL;
--Testcase 80:
INSERT INTO INT4_TBL(f1) VALUES ((2 + 2) / 2);
--Testcase 81:
SELECT f1 as two FROM INT4_TBL;
ROLLBACK;

-- corner case
BEGIN;
--Testcase 82:
DELETE FROM INT4_TBL;
--Testcase 83:
INSERT INTO INT4_TBL(f1) VALUES ((-1::int4<<31));
--Testcase 84:
INSERT INTO INT4_TBL(f1) VALUES (((-1::int4<<31)+1));
--Testcase 85:
SELECT f1 FROM INT4_TBL;
ROLLBACK;

-- check sane handling of INT_MIN overflow cases
BEGIN;
--Testcase 86:
DELETE FROM INT4_TBL;
--Testcase 87:
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4 * f1 (-1)::int4);
--Testcase 88:
SELECT f1 FROM INT4_TBL;
ROLLBACK;
BEGIN;
--Testcase 89:
DELETE FROM INT4_TBL;
--Testcase 90:
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4 / (-1)::int4);
--Testcase 91:
SELECT f1 FROM INT4_TBL;
ROLLBACK;
BEGIN;
--Testcase 92:
DELETE FROM INT4_TBL;
--Testcase 93:
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4 % (-1)::int4);
--Testcase 94:
SELECT f1 FROM INT4_TBL;
ROLLBACK;
BEGIN;
--Testcase 95:
DELETE FROM INT4_TBL;
--Testcase 96:
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4 * (-1)::int2);
--Testcase 97:
SELECT f1 FROM INT4_TBL;
ROLLBACK;
BEGIN;
--Testcase 98:
DELETE FROM INT4_TBL;
--Testcase 99:
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4 / (-1)::int2);
--Testcase 100:
SELECT f1 FROM INT4_TBL;
ROLLBACK;
BEGIN;
--Testcase 101:
DELETE FROM INT4_TBL;
--Testcase 102:
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4 % (-1)::int2);
--Testcase 103:
SELECT f1 FROM INT4_TBL;
ROLLBACK;

-- check rounding when casting from float
CREATE FOREIGN TABLE FLOAT8_TBL(id serial OPTIONS (rowkey 'true'), f1 float8) SERVER griddb_svr; 
BEGIN;
--Testcase 104:
DELETE FROM FLOAT8_TBL;
--Testcase 105:
INSERT INTO FLOAT8_TBL(f1) VALUES 
	(-2.5::float8),
        (-1.5::float8),
        (-0.5::float8),
        (0.0::float8),
        (0.5::float8),
        (1.5::float8),
        (2.5::float8);
--Testcase 106:
SELECT f1 as x, f1::int4 AS int4_value FROM FLOAT8_TBL;
ROLLBACK;

-- check rounding when casting from numeric
BEGIN;
--Testcase 107:
DELETE FROM FLOAT8_TBL;
--Testcase 108:
INSERT INTO FLOAT8_TBL(f1) VALUES 
	(-2.5::numeric),
        (-1.5::numeric),
        (-0.5::numeric),
        (0.0::numeric),
        (0.5::numeric),
        (1.5::numeric),
        (2.5::numeric);
--Testcase 109:
SELECT f1::numeric as x, f1::numeric::int4 AS int4_value FROM FLOAT8_TBL;
ROLLBACK;

DROP FOREIGN TABLE INT4_TBL;
DROP FOREIGN TABLE FLOAT8_TBL;
DROP USER MAPPING FOR public SERVER griddb_svr;
DROP SERVER griddb_svr;
DROP EXTENSION griddb_fdw CASCADE;
