--
-- INT8
-- Test int8 64-bit integers.
--
\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 1:
CREATE EXTENSION griddb_fdw;

--Testcase 2:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');

--Testcase 3:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

--Testcase 4:
CREATE FOREIGN TABLE INT8_TBL(id serial OPTIONS (rowkey 'true'), q1 int8, q2 int8) SERVER griddb_svr; 

--Testcase 5:
INSERT INTO INT8_TBL(q1, q2) VALUES('  123   ','  456');

--Testcase 6:
INSERT INTO INT8_TBL(q1, q2) VALUES('123   ','4567890123456789');

--Testcase 7:
INSERT INTO INT8_TBL(q1, q2) VALUES('4567890123456789','123');

--Testcase 8:
INSERT INTO INT8_TBL(q1, q2) VALUES(+4567890123456789,'4567890123456789');

--Testcase 9:
INSERT INTO INT8_TBL(q1, q2) VALUES('+4567890123456789','-4567890123456789');

-- bad inputs

--Testcase 10:
INSERT INTO INT8_TBL(q1) VALUES ('      ');

--Testcase 11:
INSERT INTO INT8_TBL(q1) VALUES ('xxx');

--Testcase 12:
INSERT INTO INT8_TBL(q1) VALUES ('3908203590239580293850293850329485');

--Testcase 13:
INSERT INTO INT8_TBL(q1) VALUES ('-1204982019841029840928340329840934');

--Testcase 14:
INSERT INTO INT8_TBL(q1) VALUES ('- 123');

--Testcase 15:
INSERT INTO INT8_TBL(q1) VALUES ('  345     5');

--Testcase 16:
INSERT INTO INT8_TBL(q1) VALUES ('');

--Testcase 17:
SELECT q1, q2 FROM INT8_TBL;

-- Also try it with non-error-throwing API
--Testcase 241:
CREATE FOREIGN TABLE non_error_throwing_api(id serial OPTIONS (rowkey 'true'), f1 text) SERVER griddb_svr;
--Testcase 242:
INSERT INTO non_error_throwing_api VALUES (1, '34'), (2, 'asdf'), (3, '10000000000000000000');
--Testcase 243:
SELECT pg_input_is_valid(f1, 'int8') FROM non_error_throwing_api WHERE id = 1;
--Testcase 244:
SELECT pg_input_is_valid(f1, 'int8') FROM non_error_throwing_api WHERE id = 2;
--Testcase 245:
SELECT pg_input_is_valid(f1, 'int8') FROM non_error_throwing_api WHERE id = 3;
--Testcase 246:
SELECT * FROM pg_input_error_info((SELECT f1 FROM non_error_throwing_api WHERE id = 3), 'int8');
--Testcase 247:
DELETE FROM non_error_throwing_api;

-- int8/int8 cmp

--Testcase 18:
SELECT q1, q2 FROM INT8_TBL WHERE q2 = 4567890123456789;

--Testcase 19:
SELECT q1, q2 FROM INT8_TBL WHERE q2 <> 4567890123456789;

--Testcase 20:
SELECT q1, q2 FROM INT8_TBL WHERE q2 < 4567890123456789;

--Testcase 21:
SELECT q1, q2 FROM INT8_TBL WHERE q2 > 4567890123456789;

--Testcase 22:
SELECT q1, q2 FROM INT8_TBL WHERE q2 <= 4567890123456789;

--Testcase 23:
SELECT q1, q2 FROM INT8_TBL WHERE q2 >= 4567890123456789;

-- int8/int4 cmp

--Testcase 24:
SELECT q1, q2 FROM INT8_TBL WHERE q2 = 456;

--Testcase 25:
SELECT q1, q2 FROM INT8_TBL WHERE q2 <> 456;

--Testcase 26:
SELECT q1, q2 FROM INT8_TBL WHERE q2 < 456;

--Testcase 27:
SELECT q1, q2 FROM INT8_TBL WHERE q2 > 456;

--Testcase 28:
SELECT q1, q2 FROM INT8_TBL WHERE q2 <= 456;

--Testcase 29:
SELECT q1, q2 FROM INT8_TBL WHERE q2 >= 456;

-- int4/int8 cmp

--Testcase 30:
SELECT q1, q2 FROM INT8_TBL WHERE 123 = q1;

--Testcase 31:
SELECT q1, q2 FROM INT8_TBL WHERE 123 <> q1;

--Testcase 32:
SELECT q1, q2 FROM INT8_TBL WHERE 123 < q1;

--Testcase 33:
SELECT q1, q2 FROM INT8_TBL WHERE 123 > q1;

--Testcase 34:
SELECT q1, q2 FROM INT8_TBL WHERE 123 <= q1;

--Testcase 35:
SELECT q1, q2 FROM INT8_TBL WHERE 123 >= q1;

-- int8/int2 cmp

--Testcase 36:
SELECT q1, q2 FROM INT8_TBL WHERE q2 = '456'::int2;

--Testcase 37:
SELECT q1, q2 FROM INT8_TBL WHERE q2 <> '456'::int2;

--Testcase 38:
SELECT q1, q2 FROM INT8_TBL WHERE q2 < '456'::int2;

--Testcase 39:
SELECT q1, q2 FROM INT8_TBL WHERE q2 > '456'::int2;

--Testcase 40:
SELECT q1, q2 FROM INT8_TBL WHERE q2 <= '456'::int2;

--Testcase 41:
SELECT q1, q2 FROM INT8_TBL WHERE q2 >= '456'::int2;

-- int2/int8 cmp

--Testcase 42:
SELECT q1, q2 FROM INT8_TBL WHERE '123'::int2 = q1;

--Testcase 43:
SELECT q1, q2 FROM INT8_TBL WHERE '123'::int2 <> q1;

--Testcase 44:
SELECT q1, q2 FROM INT8_TBL WHERE '123'::int2 < q1;

--Testcase 45:
SELECT q1, q2 FROM INT8_TBL WHERE '123'::int2 > q1;

--Testcase 46:
SELECT q1, q2 FROM INT8_TBL WHERE '123'::int2 <= q1;

--Testcase 47:
SELECT q1, q2 FROM INT8_TBL WHERE '123'::int2 >= q1;


--Testcase 48:
SELECT q1 AS plus, -q1 AS minus FROM INT8_TBL;

--Testcase 49:
SELECT q1, q2, q1 + q2 AS plus FROM INT8_TBL;

--Testcase 50:
SELECT q1, q2, q1 - q2 AS minus FROM INT8_TBL;

--Testcase 51:
SELECT q1, q2, q1 * q2 AS multiply FROM INT8_TBL;

--Testcase 52:
SELECT q1, q2, q1 * q2 AS multiply FROM INT8_TBL
 WHERE q1 < 1000 or (q2 > 0 and q2 < 1000);

--Testcase 53:
SELECT q1, q2, q1 / q2 AS divide, q1 % q2 AS mod FROM INT8_TBL;

--Testcase 54:
SELECT q1, float8(q1) FROM INT8_TBL;

--Testcase 55:
SELECT q2, float8(q2) FROM INT8_TBL;

--Testcase 56:
SELECT 37 + q1 AS plus4 FROM INT8_TBL;

--Testcase 57:
SELECT 37 - q1 AS minus4 FROM INT8_TBL;

--Testcase 58:
SELECT 2 * q1 AS "twice int4" FROM INT8_TBL;

--Testcase 59:
SELECT q1 * 2 AS "twice int4" FROM INT8_TBL;

-- int8 op int4

--Testcase 60:
SELECT q1 + 42::int4 AS "8plus4", q1 - 42::int4 AS "8minus4", q1 * 42::int4 AS "8mul4", q1 / 42::int4 AS "8div4" FROM INT8_TBL;
-- int4 op int8

--Testcase 61:
SELECT 246::int4 + q1 AS "4plus8", 246::int4 - q1 AS "4minus8", 246::int4 * q1 AS "4mul8", 246::int4 / q1 AS "4div8" FROM INT8_TBL;

-- int8 op int2

--Testcase 62:
SELECT q1 + 42::int2 AS "8plus2", q1 - 42::int2 AS "8minus2", q1 * 42::int2 AS "8mul2", q1 / 42::int2 AS "8div2" FROM INT8_TBL;
-- int2 op int8

--Testcase 63:
SELECT 246::int2 + q1 AS "2plus8", 246::int2 - q1 AS "2minus8", 246::int2 * q1 AS "2mul8", 246::int2 / q1 AS "2div8" FROM INT8_TBL;

--Testcase 64:
SELECT q2, abs(q2) FROM INT8_TBL;

--Testcase 65:
SELECT min(q1), min(q2) FROM INT8_TBL;

--Testcase 66:
SELECT max(q1), max(q2) FROM INT8_TBL;

-- TO_CHAR()
--

--Testcase 67:
SELECT to_char(q1, '9G999G999G999G999G999'), to_char(q2, '9,999,999,999,999,999')
	FROM INT8_TBL;

--Testcase 68:
SELECT to_char(q1, '9G999G999G999G999G999D999G999'), to_char(q2, '9,999,999,999,999,999.999,999')
	FROM INT8_TBL;

--Testcase 69:
SELECT to_char( (q1 * -1), '9999999999999999PR'), to_char( (q2 * -1), '9999999999999999.999PR')
	FROM INT8_TBL;

--Testcase 70:
SELECT to_char( (q1 * -1), '9999999999999999S'), to_char( (q2 * -1), 'S9999999999999999')
	FROM INT8_TBL;

--Testcase 71:
SELECT to_char(q2, 'MI9999999999999999')     FROM INT8_TBL;

--Testcase 72:
SELECT to_char(q2, 'FMS9999999999999999')    FROM INT8_TBL;

--Testcase 73:
SELECT to_char(q2, 'FM9999999999999999THPR') FROM INT8_TBL;

--Testcase 74:
SELECT to_char(q2, 'SG9999999999999999th')   FROM INT8_TBL;

--Testcase 75:
SELECT to_char(q2, '0999999999999999')       FROM INT8_TBL;

--Testcase 76:
SELECT to_char(q2, 'S0999999999999999')      FROM INT8_TBL;

--Testcase 77:
SELECT to_char(q2, 'FM0999999999999999')     FROM INT8_TBL;

--Testcase 78:
SELECT to_char(q2, 'FM9999999999999999.000') FROM INT8_TBL;

--Testcase 79:
SELECT to_char(q2, 'L9999999999999999.000')  FROM INT8_TBL;

--Testcase 80:
SELECT to_char(q2, 'FM9999999999999999.999') FROM INT8_TBL;

--Testcase 81:
SELECT to_char(q2, 'S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9') FROM INT8_TBL;

--Testcase 82:
SELECT to_char(q2, E'99999 "text" 9999 "9999" 999 "\\"text between quote marks\\"" 9999') FROM INT8_TBL;

--Testcase 83:
SELECT to_char(q2, '999999SG9999999999')     FROM INT8_TBL;

-- check min/max values and overflow behavior
BEGIN;

--Testcase 84:
DELETE FROM INT8_TBL;

--Testcase 85:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775808'::int8);

--Testcase 86:
SELECT q1 FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 87:
DELETE FROM INT8_TBL;

--Testcase 88:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775809'::int8);
ROLLBACK;

BEGIN;

--Testcase 89:
DELETE FROM INT8_TBL;

--Testcase 90:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775807'::int8);

--Testcase 91:
SELECT q1 FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 92:
DELETE FROM INT8_TBL;

--Testcase 93:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775808'::int8);
ROLLBACK;

BEGIN;

--Testcase 94:
DELETE FROM INT8_TBL;

--Testcase 95:
INSERT INTO INT8_TBL(q1) VALUES (-('-9223372036854775807'::int8));

--Testcase 96:
SELECT q1 FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 97:
DELETE FROM INT8_TBL;

--Testcase 98:
INSERT INTO INT8_TBL(q1) VALUES (-('-9223372036854775808'::int8));
ROLLBACK;

BEGIN;

--Testcase 99:
DELETE FROM INT8_TBL;

--Testcase 100:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 + '9223372036854775800'::int8);
ROLLBACK;

BEGIN;

--Testcase 101:
DELETE FROM INT8_TBL;

--Testcase 102:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775800'::int8 + '-9223372036854775800'::int8);
ROLLBACK;

BEGIN;

--Testcase 103:
DELETE FROM INT8_TBL;

--Testcase 104:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 - '-9223372036854775800'::int8);
ROLLBACK;

BEGIN;

--Testcase 105:
DELETE FROM INT8_TBL;

--Testcase 106:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775800'::int8 - '9223372036854775800'::int8);
ROLLBACK;

BEGIN;

--Testcase 107:
DELETE FROM INT8_TBL;

--Testcase 108:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 * '9223372036854775800'::int8);
ROLLBACK;

BEGIN;

--Testcase 109:
DELETE FROM INT8_TBL;

--Testcase 110:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 / '0'::int8);
ROLLBACK;

BEGIN;

--Testcase 111:
DELETE FROM INT8_TBL;

--Testcase 112:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 % '0'::int8);
ROLLBACK;

BEGIN;

--Testcase 113:
DELETE FROM INT8_TBL;

--Testcase 114:
INSERT INTO INT8_TBL(q1) VALUES (abs('-9223372036854775808'::int8));
ROLLBACK;

BEGIN;

--Testcase 115:
DELETE FROM INT8_TBL;

--Testcase 116:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 + '100'::int4);
ROLLBACK;

BEGIN;

--Testcase 117:
DELETE FROM INT8_TBL;

--Testcase 118:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775800'::int8 - '100'::int4);
ROLLBACK;

BEGIN;

--Testcase 119:
DELETE FROM INT8_TBL;

--Testcase 120:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 * '100'::int4);
ROLLBACK;

BEGIN;

--Testcase 121:
DELETE FROM INT8_TBL;

--Testcase 122:
INSERT INTO INT8_TBL(q1) VALUES ('100'::int4 + '9223372036854775800'::int8);
ROLLBACK;

BEGIN;

--Testcase 123:
DELETE FROM INT8_TBL;

--Testcase 124:
INSERT INTO INT8_TBL(q1) VALUES ('-100'::int4 - '9223372036854775800'::int8);
ROLLBACK;

BEGIN;

--Testcase 125:
DELETE FROM INT8_TBL;

--Testcase 126:
INSERT INTO INT8_TBL(q1) VALUES ('100'::int4 * '9223372036854775800'::int8);
ROLLBACK;

BEGIN;

--Testcase 127:
DELETE FROM INT8_TBL;

--Testcase 128:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 + '100'::int2);
ROLLBACK;

BEGIN;

--Testcase 129:
DELETE FROM INT8_TBL;

--Testcase 130:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775800'::int8 - '100'::int2);
ROLLBACK;

BEGIN;

--Testcase 131:
DELETE FROM INT8_TBL;

--Testcase 132:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 * '100'::int2);
ROLLBACK;

BEGIN;

--Testcase 133:
DELETE FROM INT8_TBL;

--Testcase 134:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775808'::int8 / '0'::int2);
ROLLBACK;

BEGIN;

--Testcase 135:
DELETE FROM INT8_TBL;

--Testcase 136:
INSERT INTO INT8_TBL(q1) VALUES ('100'::int2 + '9223372036854775800'::int8);
ROLLBACK;

BEGIN;

--Testcase 137:
DELETE FROM INT8_TBL;

--Testcase 138:
INSERT INTO INT8_TBL(q1) VALUES ('-100'::int2 - '9223372036854775800'::int8);
ROLLBACK;

BEGIN;

--Testcase 139:
DELETE FROM INT8_TBL;

--Testcase 140:
INSERT INTO INT8_TBL(q1) VALUES ('100'::int2 * '9223372036854775800'::int8);
ROLLBACK;

BEGIN;

--Testcase 141:
DELETE FROM INT8_TBL;

--Testcase 142:
INSERT INTO INT8_TBL(q1) VALUES ('100'::int2 / '0'::int8);
ROLLBACK;

--Testcase 143:
SELECT CAST(q1 AS int4) FROM int8_tbl WHERE q2 = 456;

--Testcase 144:
SELECT CAST(q1 AS int4) FROM int8_tbl WHERE q2 <> 456;

--Testcase 145:
SELECT CAST(q1 AS int2) FROM int8_tbl WHERE q2 = 456;

--Testcase 146:
SELECT CAST(q1 AS int2) FROM int8_tbl WHERE q2 <> 456;

BEGIN;

--Testcase 147:
DELETE FROM INT8_TBL;

--Testcase 148:
INSERT INTO INT8_TBL(q1,q2) VALUES ('42'::int2, '-37'::int2);

--Testcase 149:
SELECT CAST(q1 AS int8), CAST(q2 AS int8) FROM INT8_TBL;
ROLLBACK;

--Testcase 150:
SELECT CAST(q1 AS float4), CAST(q2 AS float8) FROM INT8_TBL;

BEGIN;

--Testcase 151:
DELETE FROM INT8_TBL;

--Testcase 152:
INSERT INTO INT8_TBL(q1) VALUES ('36854775807.0'::float4);

--Testcase 153:
SELECT CAST(q1::float4 AS int8) FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 154:
DELETE FROM INT8_TBL;

--Testcase 155:
INSERT INTO INT8_TBL(q1) VALUES ('922337203685477580700.0'::float8);

--Testcase 156:
SELECT CAST(q1::float8 AS int8) FROM INT8_TBL;
ROLLBACK;

--Testcase 157:
SELECT CAST(q1 AS oid) FROM INT8_TBL;

-- bit operations

--Testcase 158:
SELECT q1, q2, q1 & q2 AS "and", q1 | q2 AS "or", q1 # q2 AS "xor", ~q1 AS "not" FROM INT8_TBL;

--Testcase 159:
SELECT q1, q1 << 2 AS "shl", q1 >> 3 AS "shr" FROM INT8_TBL;

-- generate_series

BEGIN;

--Testcase 160:
DELETE FROM INT8_TBL;

--Testcase 161:
INSERT INTO INT8_TBL(q1) SELECT q1 FROM generate_series('+4567890123456789'::int8, '+4567890123456799'::int8) q1;

--Testcase 162:
SELECT q1 AS generate_series FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 163:
DELETE FROM INT8_TBL;

--Testcase 164:
INSERT INTO INT8_TBL(q1) SELECT q1 FROM generate_series('+4567890123456789'::int8, '+4567890123456799'::int8, 0) q1; -- should error

--Testcase 165:
SELECT q1 AS generate_series FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 166:
DELETE FROM INT8_TBL;

--Testcase 167:
INSERT INTO INT8_TBL(q1) SELECT q1 FROM generate_series('+4567890123456789'::int8, '+4567890123456799'::int8, 2) q1;

--Testcase 168:
SELECT q1 AS generate_series FROM INT8_TBL;
ROLLBACK;

-- corner case
BEGIN;

--Testcase 169:
DELETE FROM INT8_TBL;

--Testcase 170:
INSERT INTO INT8_TBL(q1) VALUES(-1::int8<<63);

--Testcase 171:
SELECT q1::text AS text FROM INT8_TBL;

--Testcase 172:
SELECT (q1+1)::text FROM INT8_TBL;
ROLLBACK;

-- check sane handling of INT64_MIN overflow cases
BEGIN;

--Testcase 173:
DELETE FROM INT8_TBL;

--Testcase 174:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);

--Testcase 175:
SELECT (q1 * (-1)::int8) FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 176:
DELETE FROM INT8_TBL;

--Testcase 177:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);

--Testcase 178:
SELECT (q1 / (-1)::int8) FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 179:
DELETE FROM INT8_TBL;

--Testcase 180:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);

--Testcase 181:
SELECT (q1 % (-1)::int8) FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 182:
DELETE FROM INT8_TBL;

--Testcase 183:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);

--Testcase 184:
SELECT (q1 * (-1)::int4) FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 185:
DELETE FROM INT8_TBL;

--Testcase 186:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);

--Testcase 187:
SELECT (q1 / (-1)::int4) FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 188:
DELETE FROM INT8_TBL;

--Testcase 189:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);

--Testcase 190:
SELECT (q1 % (-1)::int4) FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 191:
DELETE FROM INT8_TBL;

--Testcase 192:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);

--Testcase 193:
SELECT (q1 * (-1)::int2) FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 194:
DELETE FROM INT8_TBL;

--Testcase 195:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);

--Testcase 196:
SELECT (q1 / (-1)::int2) FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 197:
DELETE FROM INT8_TBL;

--Testcase 198:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);

--Testcase 199:
SELECT (q1 % (-1)::int2) FROM INT8_TBL;
ROLLBACK;

-- check rounding when casting from float

--Testcase 200:
CREATE FOREIGN TABLE FLOAT8_TBL(id serial OPTIONS (rowkey 'true'), f1 float8) SERVER griddb_svr;
BEGIN;

--Testcase 201:
DELETE FROM FLOAT8_TBL;

--Testcase 202:
INSERT INTO FLOAT8_TBL(f1) VALUES
	     (-2.5::float8),
             (-1.5::float8),
             (-0.5::float8),
             (0.0::float8),
             (0.5::float8),
             (1.5::float8),
             (2.5::float8);

--Testcase 203:
SELECT f1 as x, f1::int8 AS int8_value FROM FLOAT8_TBL;
ROLLBACK;
 
-- check rounding when casting from numeric
BEGIN;

--Testcase 204:
DELETE FROM FLOAT8_TBL;

--Testcase 205:
INSERT INTO FLOAT8_TBL(f1) VALUES
	     (-2.5::numeric),
             (-1.5::numeric),
             (-0.5::numeric),
             (0.0::numeric),
             (0.5::numeric),
             (1.5::numeric),
             (2.5::numeric);

--Testcase 206:
SELECT f1::numeric as x, f1::numeric::int8 AS int8_value FROM FLOAT8_TBL;
ROLLBACK;

-- test gcd()
BEGIN;

--Testcase 207:
DELETE FROM INT8_TBL;

--Testcase 208:
INSERT INTO INT8_TBL(q1, q2) VALUES (0::int8, 0::int8);

--Testcase 209:
INSERT INTO INT8_TBL(q1, q2) VALUES (0::int8, 29893644334::int8);

--Testcase 210:
INSERT INTO INT8_TBL(q1, q2) VALUES (288484263558::int8, 29893644334::int8);

--Testcase 211:
INSERT INTO INT8_TBL(q1, q2) VALUES (-288484263558::int8, 29893644334::int8);

--Testcase 212:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, 1::int8);

--Testcase 213:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, 9223372036854775807::int8);

--Testcase 214:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, 4611686018427387904::int8);

--Testcase 215:
SELECT q1 AS a, q2 AS b, gcd(q1, q2), gcd(q1, -q2), gcd(q2, q1), gcd(-q2, q1) FROM INT8_TBL;
ROlLBACK;

BEGIN;

--Testcase 216:
DELETE FROM INT8_TBL;

--Testcase 217:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, 0::int8);

--Testcase 218:
SELECT gcd(q1, q2) FROM INT8_TBL;    -- overflow
ROLLBACK;

BEGIN;

--Testcase 219:
DELETE FROM INT8_TBL;

--Testcase 220:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, (-9223372036854775808)::int8);

--Testcase 221:
SELECT gcd(q1, q2) FROM INT8_TBL;    -- overflow
ROLLBACK;

-- test lcm()
BEGIN;

--Testcase 222:
DELETE FROM INT8_TBL;

--Testcase 223:
INSERT INTO INT8_TBL(q1, q2) VALUES (0::int8, 0::int8);

--Testcase 224:
INSERT INTO INT8_TBL(q1, q2) VALUES (0::int8, 29893644334::int8);

--Testcase 225:
INSERT INTO INT8_TBL(q1, q2) VALUES (29893644334::int8, 29893644334::int8);

--Testcase 226:
INSERT INTO INT8_TBL(q1, q2) VALUES (288484263558::int8, 29893644334::int8);

--Testcase 227:
INSERT INTO INT8_TBL(q1, q2) VALUES (-288484263558::int8, 29893644334::int8);

--Testcase 228:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, 0::int8);

--Testcase 229:
SELECT q1 AS a, q2 AS b, lcm(q1, q2), lcm(q1, -q2), lcm(q2, q1), lcm(-q2, q1) FROM INT8_TBL;
ROLLBACK;

BEGIN;

--Testcase 230:
DELETE FROM INT8_TBL;

--Testcase 231:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, 1::int8);

--Testcase 232:
SELECT lcm(q1, q2) FROM INT8_TBL;    -- overflow
ROLLBACK;

BEGIN;

--Testcase 233:
DELETE FROM INT8_TBL;

--Testcase 234:
INSERT INTO INT8_TBL(q1, q2) VALUES (9223372036854775807::int8, 9223372036854775806::int8);

--Testcase 235:
SELECT lcm(q1, q2) FROM INT8_TBL;    -- overflow
ROLLBACK;

-- non-decimal literals
BEGIN;
--Testcase 248:
INSERT INTO INT8_TBL(q1) VALUES ('0b100101');
--Testcase 249:
INSERT INTO INT8_TBL(q1) VALUES ('0o273');
--Testcase 250:
INSERT INTO INT8_TBL(q1) VALUES ('0x42F');
--Testcase 251:
SELECT * FROM INT8_TBL;
--Testcase 252:
ROLLBACK;

--Testcase 253:
INSERT INTO INT8_TBL(q1) VALUES ('0b');
--Testcase 254:
INSERT INTO INT8_TBL(q1) VALUES ('0o');
--Testcase 255:
INSERT INTO INT8_TBL(q1) VALUES ('0x');

-- cases near overflow
BEGIN;
--Testcase 256:
INSERT INTO INT8_TBL(q1) VALUES ('0b111111111111111111111111111111111111111111111111111111111111111');
--Testcase 257:
SELECT * FROM INT8_TBL;
ROLLBACK;

--Testcase 258:
INSERT INTO INT8_TBL(q1) VALUES ('0b1000000000000000000000000000000000000000000000000000000000000000');
--Testcase 259:

BEGIN;
INSERT INTO INT8_TBL(q1) VALUES ('0o777777777777777777777');
--Testcase 260:
SELECT * FROM INT8_TBL;
ROLLBACK;

--Testcase 261:
INSERT INTO INT8_TBL(q1) VALUES ('0o1000000000000000000000');

BEGIN;
--Testcase 262:
INSERT INTO INT8_TBL(q1) VALUES ('0x7FFFFFFFFFFFFFFF');
--Testcase 263:
SELECT * FROM INT8_TBL;
ROLLBACK;

--Testcase 264:
INSERT INTO INT8_TBL(q1) VALUES ('0x8000000000000000');

--Testcase 265:
BEGIN;
--Testcase 266:
INSERT INTO INT8_TBL(q1) VALUES ('-0b1000000000000000000000000000000000000000000000000000000000000000');
--Testcase 267:
SELECT * FROM INT8_TBL;
ROLLBACK;

--Testcase 268:
INSERT INTO INT8_TBL(q1) VALUES ('-0b1000000000000000000000000000000000000000000000000000000000000001');

BEGIN;
--Testcase 269:
INSERT INTO INT8_TBL(q1) VALUES ('-0o1000000000000000000000');
--Testcase 270:
SELECT * FROM INT8_TBL;
ROLLBACK;

--Testcase 271:
INSERT INTO INT8_TBL(q1) VALUES ('-0o1000000000000000000001');

BEGIN;
--Testcase 272:
INSERT INTO INT8_TBL(q1) VALUES ('-0x8000000000000000');
--Testcase 273:
SELECT * FROM INT8_TBL;
ROLLBACK;

--Testcase 274:
INSERT INTO INT8_TBL(q1) VALUES ('-0x8000000000000001');

-- underscores
BEGIN;
--Testcase 276:
INSERT INTO INT8_TBL(q1) VALUES ('1_000_000');
--Testcase 277:
INSERT INTO INT8_TBL(q1) VALUES ('1_2_3');
--Testcase 278:
INSERT INTO INT8_TBL(q1) VALUES ('0x1EEE_FFFF');
--Testcase 279:
INSERT INTO INT8_TBL(q1) VALUES ('0o2_73');
--Testcase 280:
INSERT INTO INT8_TBL(q1) VALUES ('0b_10_0101');
--Testcase 281:
SELECT * FROM INT8_TBL;
ROLLBACK;

-- error cases
--Testcase 282:
INSERT INTO INT8_TBL(q1) VALUES ('_100');
--Testcase 283:
INSERT INTO INT8_TBL(q1) VALUES ('100_');
--Testcase 284:
INSERT INTO INT8_TBL(q1) VALUES ('100__000');

--Testcase 236:
DROP FOREIGN TABLE FLOAT8_TBL;

--Testcase 237:
DROP FOREIGN TABLE INT8_TBL;

--Testcase 238:
DROP USER MAPPING FOR public SERVER griddb_svr;

--Testcase 239:
DROP SERVER griddb_svr CASCADE;

--Testcase 240:
DROP EXTENSION griddb_fdw CASCADE;
