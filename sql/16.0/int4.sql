--
-- INT4
--
\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 1:
DROP EXTENSION griddb_fdw cascade;

--Testcase 2:
CREATE EXTENSION griddb_fdw;

--Testcase 3:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');

--Testcase 4:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

--Testcase 5:
CREATE FOREIGN TABLE INT4_TBL(id serial OPTIONS (rowkey 'true'), f1 int4) SERVER griddb_svr;

--Testcase 6:
CREATE FOREIGN TABLE INT4_TMP(id serial OPTIONS (rowkey 'true'), a int4, b int4) SERVER griddb_svr;

--Testcase 7:
INSERT INTO INT4_TBL(f1) VALUES ('   0  ');

--Testcase 8:
INSERT INTO INT4_TBL(f1) VALUES ('123456     ');

--Testcase 9:
INSERT INTO INT4_TBL(f1) VALUES ('    -123456');

--Testcase 10:
INSERT INTO INT4_TBL(f1) VALUES ('34.5');

-- largest and smallest values

--Testcase 11:
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');

--Testcase 12:
INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');

-- bad input values -- should give errors

--Testcase 13:
INSERT INTO INT4_TBL(f1) VALUES ('1000000000000');

--Testcase 14:
INSERT INTO INT4_TBL(f1) VALUES ('asdf');

--Testcase 15:
INSERT INTO INT4_TBL(f1) VALUES ('     ');

--Testcase 16:
INSERT INTO INT4_TBL(f1) VALUES ('   asdf   ');

--Testcase 17:
INSERT INTO INT4_TBL(f1) VALUES ('- 1234');

--Testcase 18:
INSERT INTO INT4_TBL(f1) VALUES ('123       5');

--Testcase 19:
INSERT INTO INT4_TBL(f1) VALUES ('');


--Testcase 20:
SELECT f1 FROM INT4_TBL;

-- Also try it with non-error-throwing API
--Testcase 149:
CREATE FOREIGN TABLE non_error_throwing_api(id serial OPTIONS (rowkey 'true'), f1 text) SERVER griddb_svr;
--Testcase 150:
INSERT INTO non_error_throwing_api VALUES (1, '34'), (2, 'asdf'), (3, '1000000000000');
--Testcase 151:
SELECT pg_input_is_valid(f1, 'int4') FROM non_error_throwing_api WHERE id = 1;
--Testcase 152:
SELECT pg_input_is_valid(f1, 'int4') FROM non_error_throwing_api WHERE id = 2;
--Testcase 153:
SELECT pg_input_is_valid(f1, 'int4') FROM non_error_throwing_api WHERE id = 3;
--Testcase 154:
SELECT * FROM pg_input_error_info((SELECT f1 FROM non_error_throwing_api WHERE id = 3), 'int4');
--Testcase 155:
DELETE FROM non_error_throwing_api;

--Testcase 21:
SELECT i.f1 FROM INT4_TBL i WHERE i.f1 <> int2 '0';

--Testcase 22:
SELECT i.f1 FROM INT4_TBL i WHERE i.f1 <> int4 '0';

--Testcase 23:
SELECT i.f1 FROM INT4_TBL i WHERE i.f1 = int2 '0';

--Testcase 24:
SELECT i.f1 FROM INT4_TBL i WHERE i.f1 = int4 '0';

--Testcase 25:
SELECT i.f1 FROM INT4_TBL i WHERE i.f1 < int2 '0';

--Testcase 26:
SELECT i.f1 FROM INT4_TBL i WHERE i.f1 < int4 '0';

--Testcase 27:
SELECT i.f1 FROM INT4_TBL i WHERE i.f1 <= int2 '0';

--Testcase 28:
SELECT i.f1 FROM INT4_TBL i WHERE i.f1 <= int4 '0';

--Testcase 29:
SELECT i.f1 FROM INT4_TBL i WHERE i.f1 > int2 '0';

--Testcase 30:
SELECT i.f1 FROM INT4_TBL i WHERE i.f1 > int4 '0';

--Testcase 31:
SELECT i.f1 FROM INT4_TBL i WHERE i.f1 >= int2 '0';

--Testcase 32:
SELECT i.f1 FROM INT4_TBL i WHERE i.f1 >= int4 '0';

-- positive odds

--Testcase 33:
SELECT i.f1 FROM INT4_TBL i WHERE (i.f1 % int2 '2') = int2 '1';

-- any evens

--Testcase 34:
SELECT i.f1 FROM INT4_TBL i WHERE (i.f1 % int4 '2') = int2 '0';

--Testcase 35:
SELECT i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i;

--Testcase 36:
SELECT i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

--Testcase 37:
SELECT i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i;

--Testcase 38:
SELECT i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

--Testcase 39:
SELECT i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i;

--Testcase 40:
SELECT i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

--Testcase 41:
SELECT i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i;

--Testcase 42:
SELECT i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

--Testcase 43:
SELECT i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i;

--Testcase 44:
SELECT i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

--Testcase 45:
SELECT i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i;

--Testcase 46:
SELECT i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

--Testcase 47:
SELECT i.f1, i.f1 / int2 '2' AS x FROM INT4_TBL i;

--Testcase 48:
SELECT i.f1, i.f1 / int4 '2' AS x FROM INT4_TBL i;

--
-- more complex expressions
--

-- variations on unary minus parsing
BEGIN;

--Testcase 49:
DELETE FROM INT4_TBL;

--Testcase 50:
INSERT INTO INT4_TBL(f1) VALUES (-2);

--Testcase 51:
SELECT (f1+3) as one FROM INT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 52:
DELETE FROM INT4_TBL;

--Testcase 53:
INSERT INTO INT4_TBL(f1) VALUES (4);

--Testcase 54:
SELECT (f1-2) as two FROM INT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 55:
DELETE FROM INT4_TBL;

--Testcase 56:
INSERT INTO INT4_TBL(f1) VALUES (2);

--Testcase 57:
SELECT (f1- -1) as three FROM INT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 58:
DELETE FROM INT4_TBL;

--Testcase 59:
INSERT INTO INT4_TBL(f1) VALUES (2);

--Testcase 60:
SELECT (f1 - -2) as four FROM INT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 61:
DELETE FROM INT4_TMP;

--Testcase 62:
INSERT INTO INT4_TMP(a, b) VALUES (int2 '2' * int2 '2', int2 '16' / int2 '4');

--Testcase 63:
SELECT (a = b) as true FROM INT4_TMP;
ROLLBACK;

BEGIN;

--Testcase 64:
DELETE FROM INT4_TMP;

--Testcase 65:
INSERT INTO INT4_TMP(a, b) VALUES (int4 '2' * int2 '2', int2 '16' / int4 '4');

--Testcase 66:
SELECT (a = b) as true FROM INT4_TMP;
ROLLBACK;

BEGIN;

--Testcase 67:
DELETE FROM INT4_TMP;

--Testcase 68:
INSERT INTO INT4_TMP(a, b) VALUES (int2 '2' * int4 '2', int4 '16' / int2 '4');

--Testcase 69:
SELECT (a = b) as true FROM INT4_TMP;
ROLLBACK;

BEGIN;

--Testcase 70:
DELETE FROM INT4_TMP;

--Testcase 71:
INSERT INTO INT4_TMP(a, b) VALUES (int4 '1000', int4 '999');

--Testcase 72:
SELECT (a < b) as false FROM INT4_TMP;
ROLLBACK;

BEGIN;

--Testcase 73:
DELETE FROM INT4_TBL;

--Testcase 74:
INSERT INTO INT4_TBL(f1) VALUES (factorial(4));

--Testcase 75:
SELECT f1 as twenty_four FROM INT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 76:
DELETE FROM INT4_TBL;

--Testcase 77:
INSERT INTO INT4_TBL(f1) VALUES (1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1);

--Testcase 78:
SELECT f1 as ten FROM INT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 79:
DELETE FROM INT4_TBL;

--Testcase 80:
INSERT INTO INT4_TBL(f1) VALUES (2 + 2 / 2);

--Testcase 81:
SELECT f1 as three FROM INT4_TBL;
ROLLBACK;

BEGIN;

--Testcase 82:
DELETE FROM INT4_TBL;

--Testcase 83:
INSERT INTO INT4_TBL(f1) VALUES ((2 + 2) / 2);

--Testcase 84:
SELECT f1 as two FROM INT4_TBL;
ROLLBACK;

-- corner case
BEGIN;

--Testcase 85:
DELETE FROM INT4_TBL;

--Testcase 86:
INSERT INTO INT4_TBL(f1) VALUES ((-1::int4<<31));

--Testcase 87:
SELECT f1::text AS text FROM INT4_TBL;

--Testcase 88:
SELECT (f1+1)::text FROM INT4_TBL;
ROLLBACK;

-- check sane handling of INT_MIN overflow cases
BEGIN;

--Testcase 89:
DELETE FROM INT4_TBL;

--Testcase 90:
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4);

--Testcase 91:
SELECT (f1 * (-1)::int4) FROM INT4_TBL;
ROLLBACK;
BEGIN;

--Testcase 92:
DELETE FROM INT4_TBL;

--Testcase 93:
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4);

--Testcase 94:
SELECT (f1 / (-1)::int4) FROM INT4_TBL;
ROLLBACK;
BEGIN;

--Testcase 95:
DELETE FROM INT4_TBL;

--Testcase 96:
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4);

--Testcase 97:
SELECT (f1 % (-1)::int4) FROM INT4_TBL;
ROLLBACK;
BEGIN;

--Testcase 98:
DELETE FROM INT4_TBL;

--Testcase 99:
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4);

--Testcase 100:
SELECT (f1 * (-1)::int2) FROM INT4_TBL;
ROLLBACK;
BEGIN;

--Testcase 101:
DELETE FROM INT4_TBL;

--Testcase 102:
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4);

--Testcase 103:
SELECT (f1 / (-1)::int2) FROM INT4_TBL;
ROLLBACK;
BEGIN;

--Testcase 104:
DELETE FROM INT4_TBL;

--Testcase 105:
INSERT INTO INT4_TBL(f1) VALUES ((-2147483648)::int4);

--Testcase 106:
SELECT (f1 % (-1)::int2) FROM INT4_TBL;
ROLLBACK;

-- check rounding when casting from float

--Testcase 107:
CREATE FOREIGN TABLE FLOAT8_TBL(id serial OPTIONS (rowkey 'true'), f1 float8) SERVER griddb_svr; 
BEGIN;

--Testcase 108:
DELETE FROM FLOAT8_TBL;

--Testcase 109:
INSERT INTO FLOAT8_TBL(f1) VALUES 
	(-2.5::float8),
        (-1.5::float8),
        (-0.5::float8),
        (0.0::float8),
        (0.5::float8),
        (1.5::float8),
        (2.5::float8);

--Testcase 110:
SELECT f1 as x, f1::int4 AS int4_value FROM FLOAT8_TBL;
ROLLBACK;

-- check rounding when casting from numeric
BEGIN;

--Testcase 111:
DELETE FROM FLOAT8_TBL;

--Testcase 112:
INSERT INTO FLOAT8_TBL(f1) VALUES 
	(-2.5::numeric),
        (-1.5::numeric),
        (-0.5::numeric),
        (0.0::numeric),
        (0.5::numeric),
        (1.5::numeric),
        (2.5::numeric);

--Testcase 113:
SELECT f1::numeric as x, f1::numeric::int4 AS int4_value FROM FLOAT8_TBL;
ROLLBACK;

-- test gcd()
BEGIN;

--Testcase 114:
DELETE FROM INT4_TMP;

--Testcase 115:
INSERT INTO INT4_TMP(a, b) VALUES (0::int4, 0::int4);

--Testcase 116:
INSERT INTO INT4_TMP(a, b) VALUES (0::int4, 6410818::int4);

--Testcase 117:
INSERT INTO INT4_TMP(a, b) VALUES (61866666::int4, 6410818::int4);

--Testcase 118:
INSERT INTO INT4_TMP(a, b) VALUES (-61866666::int4, 6410818::int4);

--Testcase 119:
INSERT INTO INT4_TMP(a, b) VALUES ((-2147483648)::int4, 1::int4);

--Testcase 120:
INSERT INTO INT4_TMP(a, b) VALUES ((-2147483648)::int4, 2147483647::int4);

--Testcase 121:
INSERT INTO INT4_TMP(a, b) VALUES ((-2147483648)::int4, 1073741824::int4);

--Testcase 122:
SELECT a, b, gcd(a, b), gcd(a, -b), gcd(b, a), gcd(-b, a) FROM INT4_TMP;
ROLLBACK;

BEGIN;

--Testcase 123:
DELETE FROM INT4_TMP;

--Testcase 124:
INSERT INTO INT4_TMP(a, b) VALUES ((-2147483648)::int4, 0::int4);

--Testcase 125:
SELECT gcd(a, b) FROM INT4_TMP;    -- overflow
ROLLBACK;

BEGIN;

--Testcase 126:
DELETE FROM INT4_TMP;

--Testcase 127:
INSERT INTO INT4_TMP(a, b) VALUES ((-2147483648)::int4, (-2147483648)::int4);

--Testcase 128:
SELECT gcd(a, b) FROM INT4_TMP;    -- overflow
ROLLBACK;

-- test lcm()
BEGIN;

--Testcase 129:
DELETE FROM INT4_TMP;

--Testcase 130:
INSERT INTO INT4_TMP(a, b) VALUES (0::int4, 0::int4);

--Testcase 131:
INSERT INTO INT4_TMP(a, b) VALUES (0::int4, 42::int4);

--Testcase 132:
INSERT INTO INT4_TMP(a, b) VALUES (42::int4, 42::int4);

--Testcase 133:
INSERT INTO INT4_TMP(a, b) VALUES (330::int4, 462::int4);

--Testcase 134:
INSERT INTO INT4_TMP(a, b) VALUES (-330::int4, 462::int4);

--Testcase 135:
INSERT INTO INT4_TMP(a, b) VALUES ((-2147483648)::int4, 0::int4);

--Testcase 136:
SELECT a, b, lcm(a, b), lcm(a, -b), lcm(b, a), lcm(-b, a) FROM INT4_TMP;
ROLLBACK;

BEGIN;

--Testcase 137:
DELETE FROM INT4_TMP;

--Testcase 138:
INSERT INTO INT4_TMP(a, b) VALUES ((-2147483648)::int4, 1::int4);

--Testcase 139:
SELECT lcm(a, b) FROM INT4_TMP;    -- overflow
ROLLBACK;

BEGIN;

--Testcase 140:
DELETE FROM INT4_TMP;

--Testcase 141:
INSERT INTO INT4_TMP(a, b) VALUES (2147483647::int4, 2147483646::int4);

--Testcase 142:
SELECT lcm(a, b) FROM INT4_TMP;    -- overflow
ROLLBACK;

BEGIN;
-- non-decimal literals
DELETE FROM INT4_TBL;
--Testcase 156:
INSERT INTO INT4_TBL(f1) VALUES ('0b100101');
--Testcase 157:
INSERT INTO INT4_TBL(f1) VALUES ('0o273');
--Testcase 158:
INSERT INTO INT4_TBL(f1) VALUES ('0x42F');
--Testcase 159:
SELECT * FROM INT4_TBL;
--Testcase 160:
ROLLBACK;

--Testcase 161:
INSERT INTO INT4_TBL(f1) VALUES ('0b');
--Testcase 162:
INSERT INTO INT4_TBL(f1) VALUES ('0o');
--Testcase 163:
INSERT INTO INT4_TBL(f1) VALUES ('0x');

-- cases near overflow
BEGIN;
--Testcase 164:
INSERT INTO INT4_TBL(f1) VALUES ('0b1111111111111111111111111111111');
--Testcase 165:
SELECT * FROM INT4_TBL;
ROLLBACK;
--Testcase 166:
INSERT INTO INT4_TBL(f1) VALUES ('0b10000000000000000000000000000000');

BEGIN;
--Testcase 167:
INSERT INTO INT4_TBL(f1) VALUES ('0o17777777777');
--Testcase 168:
SELECT * FROM INT4_TBL;
ROLLBACK;

--Testcase 169:
INSERT INTO INT4_TBL(f1) VALUES ('0o20000000000');

BEGIN;
--Testcase 170:
INSERT INTO INT4_TBL(f1) VALUES ('0x7FFFFFFF');
--Testcase 171:
SELECT * FROM INT4_TBL;
ROLLBACK;
--Testcase 172:
INSERT INTO INT4_TBL(f1) VALUES ('0x80000000');

--Testcase 173:
BEGIN;
--Testcase 174:
INSERT INTO INT4_TBL(f1) VALUES ('-0b10000000000000000000000000000000');
--Testcase 175:
SELECT * FROM INT4_TBL;
ROLLBACK;

--Testcase 176:
INSERT INTO INT4_TBL(f1) VALUES ('-0b10000000000000000000000000000001');

BEGIN;
--Testcase 177:
INSERT INTO INT4_TBL(f1) VALUES ('-0o20000000000');
--Testcase 178:
SELECT * FROM INT4_TBL;
ROLLBACK;

--Testcase 179:
INSERT INTO INT4_TBL(f1) VALUES ('-0o20000000001');

BEGIN;
--Testcase 180:
INSERT INTO INT4_TBL(f1) VALUES ('-0x80000000');
--Testcase 181:
SELECT * FROM INT4_TBL;
ROLLBACK;

--Testcase 182:
INSERT INTO INT4_TBL(f1) VALUES ('-0x80000001');

-- underscores
BEGIN;
--Testcase 177:
INSERT INTO INT4_TBL(f1) VALUES ('1_000_000');
--Testcase 178:
INSERT INTO INT4_TBL(f1) VALUES ('1_2_3');
--Testcase 179:
INSERT INTO INT4_TBL(f1) VALUES ('0x1EEE_FFFF');
--Testcase 180:
INSERT INTO INT4_TBL(f1) VALUES ('0o2_73');
--Testcase 181:
INSERT INTO INT4_TBL(f1) VALUES ('0b_10_0101');
--Testcase 182:
SELECT * FROM INT4_TBL;
ROLLBACK;

-- error cases
--Testcase 183:
INSERT INTO INT4_TBL(f1) VALUES ('_100');
--Testcase 184:
INSERT INTO INT4_TBL(f1) VALUES ('100_');
--Testcase 185:
INSERT INTO INT4_TBL(f1) VALUES ('100__000');

--Testcase 143:
DROP FOREIGN TABLE INT4_TMP;

--Testcase 144:
DROP FOREIGN TABLE INT4_TBL;

--Testcase 145:
DROP FOREIGN TABLE FLOAT8_TBL;

--Testcase 146:
DROP USER MAPPING FOR public SERVER griddb_svr;

--Testcase 147:
DROP SERVER griddb_svr CASCADE;

--Testcase 148:
DROP EXTENSION griddb_fdw CASCADE;
