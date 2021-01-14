--
-- INT8
-- Test int8 64-bit integers.
--
\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 229:
CREATE EXTENSION griddb_fdw;
--Testcase 230:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');
--Testcase 231:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

--Testcase 232:
CREATE FOREIGN TABLE INT8_TBL(id serial OPTIONS (rowkey 'true'), q1 int8, q2 int8) SERVER griddb_svr; 

--Testcase 1:
INSERT INTO INT8_TBL(q1, q2) VALUES('  123   ','  456');
--Testcase 2:
INSERT INTO INT8_TBL(q1, q2) VALUES('123   ','4567890123456789');
--Testcase 3:
INSERT INTO INT8_TBL(q1, q2) VALUES('4567890123456789','123');
--Testcase 4:
INSERT INTO INT8_TBL(q1, q2) VALUES(+4567890123456789,'4567890123456789');
--Testcase 5:
INSERT INTO INT8_TBL(q1, q2) VALUES('+4567890123456789','-4567890123456789');

-- bad inputs
--Testcase 6:
INSERT INTO INT8_TBL(q1) VALUES ('      ');
--Testcase 7:
INSERT INTO INT8_TBL(q1) VALUES ('xxx');
--Testcase 8:
INSERT INTO INT8_TBL(q1) VALUES ('3908203590239580293850293850329485');
--Testcase 9:
INSERT INTO INT8_TBL(q1) VALUES ('-1204982019841029840928340329840934');
--Testcase 10:
INSERT INTO INT8_TBL(q1) VALUES ('- 123');
--Testcase 11:
INSERT INTO INT8_TBL(q1) VALUES ('  345     5');
--Testcase 12:
INSERT INTO INT8_TBL(q1) VALUES ('');

--Testcase 13:
SELECT q1, q2 FROM INT8_TBL;

-- int8/int8 cmp
--Testcase 14:
SELECT q1, q2 FROM INT8_TBL WHERE q2 = 4567890123456789;
--Testcase 15:
SELECT q1, q2 FROM INT8_TBL WHERE q2 <> 4567890123456789;
--Testcase 16:
SELECT q1, q2 FROM INT8_TBL WHERE q2 < 4567890123456789;
--Testcase 17:
SELECT q1, q2 FROM INT8_TBL WHERE q2 > 4567890123456789;
--Testcase 18:
SELECT q1, q2 FROM INT8_TBL WHERE q2 <= 4567890123456789;
--Testcase 19:
SELECT q1, q2 FROM INT8_TBL WHERE q2 >= 4567890123456789;

-- int8/int4 cmp
--Testcase 20:
SELECT q1, q2 FROM INT8_TBL WHERE q2 = 456;
--Testcase 21:
SELECT q1, q2 FROM INT8_TBL WHERE q2 <> 456;
--Testcase 22:
SELECT q1, q2 FROM INT8_TBL WHERE q2 < 456;
--Testcase 23:
SELECT q1, q2 FROM INT8_TBL WHERE q2 > 456;
--Testcase 24:
SELECT q1, q2 FROM INT8_TBL WHERE q2 <= 456;
--Testcase 25:
SELECT q1, q2 FROM INT8_TBL WHERE q2 >= 456;

-- int4/int8 cmp
--Testcase 26:
SELECT q1, q2 FROM INT8_TBL WHERE 123 = q1;
--Testcase 27:
SELECT q1, q2 FROM INT8_TBL WHERE 123 <> q1;
--Testcase 28:
SELECT q1, q2 FROM INT8_TBL WHERE 123 < q1;
--Testcase 29:
SELECT q1, q2 FROM INT8_TBL WHERE 123 > q1;
--Testcase 30:
SELECT q1, q2 FROM INT8_TBL WHERE 123 <= q1;
--Testcase 31:
SELECT q1, q2 FROM INT8_TBL WHERE 123 >= q1;

-- int8/int2 cmp
--Testcase 32:
SELECT q1, q2 FROM INT8_TBL WHERE q2 = '456'::int2;
--Testcase 33:
SELECT q1, q2 FROM INT8_TBL WHERE q2 <> '456'::int2;
--Testcase 34:
SELECT q1, q2 FROM INT8_TBL WHERE q2 < '456'::int2;
--Testcase 35:
SELECT q1, q2 FROM INT8_TBL WHERE q2 > '456'::int2;
--Testcase 36:
SELECT q1, q2 FROM INT8_TBL WHERE q2 <= '456'::int2;
--Testcase 37:
SELECT q1, q2 FROM INT8_TBL WHERE q2 >= '456'::int2;

-- int2/int8 cmp
--Testcase 38:
SELECT q1, q2 FROM INT8_TBL WHERE '123'::int2 = q1;
--Testcase 39:
SELECT q1, q2 FROM INT8_TBL WHERE '123'::int2 <> q1;
--Testcase 40:
SELECT q1, q2 FROM INT8_TBL WHERE '123'::int2 < q1;
--Testcase 41:
SELECT q1, q2 FROM INT8_TBL WHERE '123'::int2 > q1;
--Testcase 42:
SELECT q1, q2 FROM INT8_TBL WHERE '123'::int2 <= q1;
--Testcase 43:
SELECT q1, q2 FROM INT8_TBL WHERE '123'::int2 >= q1;


--Testcase 44:
SELECT '' AS five, q1 AS plus, -q1 AS minus FROM INT8_TBL;

--Testcase 45:
SELECT '' AS five, q1, q2, q1 + q2 AS plus FROM INT8_TBL;
--Testcase 46:
SELECT '' AS five, q1, q2, q1 - q2 AS minus FROM INT8_TBL;
--Testcase 47:
SELECT '' AS three, q1, q2, q1 * q2 AS multiply FROM INT8_TBL;
--Testcase 48:
SELECT '' AS three, q1, q2, q1 * q2 AS multiply FROM INT8_TBL
 WHERE q1 < 1000 or (q2 > 0 and q2 < 1000);
--Testcase 49:
SELECT '' AS five, q1, q2, q1 / q2 AS divide, q1 % q2 AS mod FROM INT8_TBL;

--Testcase 50:
SELECT '' AS five, q1, float8(q1) FROM INT8_TBL;
--Testcase 51:
SELECT '' AS five, q2, float8(q2) FROM INT8_TBL;

--Testcase 52:
SELECT 37 + q1 AS plus4 FROM INT8_TBL;
--Testcase 53:
SELECT 37 - q1 AS minus4 FROM INT8_TBL;
--Testcase 54:
SELECT '' AS five, 2 * q1 AS "twice int4" FROM INT8_TBL;
--Testcase 55:
SELECT '' AS five, q1 * 2 AS "twice int4" FROM INT8_TBL;

-- int8 op int4
--Testcase 56:
SELECT q1 + 42::int4 AS "8plus4", q1 - 42::int4 AS "8minus4", q1 * 42::int4 AS "8mul4", q1 / 42::int4 AS "8div4" FROM INT8_TBL;
-- int4 op int8
--Testcase 57:
SELECT 246::int4 + q1 AS "4plus8", 246::int4 - q1 AS "4minus8", 246::int4 * q1 AS "4mul8", 246::int4 / q1 AS "4div8" FROM INT8_TBL;

-- int8 op int2
--Testcase 58:
SELECT q1 + 42::int2 AS "8plus2", q1 - 42::int2 AS "8minus2", q1 * 42::int2 AS "8mul2", q1 / 42::int2 AS "8div2" FROM INT8_TBL;
-- int2 op int8
--Testcase 59:
SELECT 246::int2 + q1 AS "2plus8", 246::int2 - q1 AS "2minus8", 246::int2 * q1 AS "2mul8", 246::int2 / q1 AS "2div8" FROM INT8_TBL;

--Testcase 60:
SELECT q2, abs(q2) FROM INT8_TBL;
--Testcase 61:
SELECT min(q1), min(q2) FROM INT8_TBL;
--Testcase 62:
SELECT max(q1), max(q2) FROM INT8_TBL;


-- TO_CHAR()
--
--Testcase 63:
SELECT '' AS to_char_1, to_char(q1, '9G999G999G999G999G999'), to_char(q2, '9,999,999,999,999,999')
	FROM INT8_TBL;

--Testcase 64:
SELECT '' AS to_char_2, to_char(q1, '9G999G999G999G999G999D999G999'), to_char(q2, '9,999,999,999,999,999.999,999')
	FROM INT8_TBL;

--Testcase 65:
SELECT '' AS to_char_3, to_char( (q1 * -1), '9999999999999999PR'), to_char( (q2 * -1), '9999999999999999.999PR')
	FROM INT8_TBL;

--Testcase 66:
SELECT '' AS to_char_4, to_char( (q1 * -1), '9999999999999999S'), to_char( (q2 * -1), 'S9999999999999999')
	FROM INT8_TBL;

--Testcase 67:
SELECT '' AS to_char_5,  to_char(q2, 'MI9999999999999999')     FROM INT8_TBL;
--Testcase 68:
SELECT '' AS to_char_6,  to_char(q2, 'FMS9999999999999999')    FROM INT8_TBL;
--Testcase 69:
SELECT '' AS to_char_7,  to_char(q2, 'FM9999999999999999THPR') FROM INT8_TBL;
--Testcase 70:
SELECT '' AS to_char_8,  to_char(q2, 'SG9999999999999999th')   FROM INT8_TBL;
--Testcase 71:
SELECT '' AS to_char_9,  to_char(q2, '0999999999999999')       FROM INT8_TBL;
--Testcase 72:
SELECT '' AS to_char_10, to_char(q2, 'S0999999999999999')      FROM INT8_TBL;
--Testcase 73:
SELECT '' AS to_char_11, to_char(q2, 'FM0999999999999999')     FROM INT8_TBL;
--Testcase 74:
SELECT '' AS to_char_12, to_char(q2, 'FM9999999999999999.000') FROM INT8_TBL;
--Testcase 75:
SELECT '' AS to_char_13, to_char(q2, 'L9999999999999999.000')  FROM INT8_TBL;
--Testcase 76:
SELECT '' AS to_char_14, to_char(q2, 'FM9999999999999999.999') FROM INT8_TBL;
--Testcase 77:
SELECT '' AS to_char_15, to_char(q2, 'S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9') FROM INT8_TBL;
--Testcase 78:
SELECT '' AS to_char_16, to_char(q2, E'99999 "text" 9999 "9999" 999 "\\"text between quote marks\\"" 9999') FROM INT8_TBL;
--Testcase 79:
SELECT '' AS to_char_17, to_char(q2, '999999SG9999999999')     FROM INT8_TBL;

-- check min/max values and overflow behavior
BEGIN;
--Testcase 80:
DELETE FROM INT8_TBL;
--Testcase 81:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775808'::int8);
--Testcase 82:
SELECT q1 FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 83:
DELETE FROM INT8_TBL;
--Testcase 84:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775809'::int8);
ROLLBACK;

BEGIN;
--Testcase 86:
DELETE FROM INT8_TBL;
--Testcase 87:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775807'::int8);
--Testcase 88:
SELECT q1 FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 89:
DELETE FROM INT8_TBL;
--Testcase 90:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775808'::int8);
ROLLBACK;

BEGIN;
--Testcase 92:
DELETE FROM INT8_TBL;
--Testcase 93:
INSERT INTO INT8_TBL(q1) VALUES (-('-9223372036854775807'::int8));
--Testcase 94:
SELECT q1 FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 95:
DELETE FROM INT8_TBL;
--Testcase 96:
INSERT INTO INT8_TBL(q1) VALUES (-('-9223372036854775808'::int8));
ROLLBACK;

BEGIN;
--Testcase 98:
DELETE FROM INT8_TBL;
--Testcase 99:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 + '9223372036854775800'::int8);
ROLLBACK;

BEGIN;
--Testcase 101:
DELETE FROM INT8_TBL;
--Testcase 102:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775800'::int8 + '-9223372036854775800'::int8);
ROLLBACK;

BEGIN;
--Testcase 104:
DELETE FROM INT8_TBL;
--Testcase 105:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 - '-9223372036854775800'::int8);
ROLLBACK;

BEGIN;
--Testcase 107:
DELETE FROM INT8_TBL;
--Testcase 108:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775800'::int8 - '9223372036854775800'::int8);
ROLLBACK;

BEGIN;
--Testcase 110:
DELETE FROM INT8_TBL;
--Testcase 111:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 * '9223372036854775800'::int8);
ROLLBACK;

BEGIN;
--Testcase 113:
DELETE FROM INT8_TBL;
--Testcase 114:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 / '0'::int8);
ROLLBACK;

BEGIN;
--Testcase 116:
DELETE FROM INT8_TBL;
--Testcase 117:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 % '0'::int8);
ROLLBACK;

BEGIN;
--Testcase 119:
DELETE FROM INT8_TBL;
--Testcase 120:
INSERT INTO INT8_TBL(q1) VALUES (abs('-9223372036854775808'::int8));
ROLLBACK;

BEGIN;
--Testcase 122:
DELETE FROM INT8_TBL;
--Testcase 123:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 + '100'::int4);
ROLLBACK;

BEGIN;
--Testcase 125:
DELETE FROM INT8_TBL;
--Testcase 126:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775800'::int8 - '100'::int4);
ROLLBACK;

BEGIN;
--Testcase 128:
DELETE FROM INT8_TBL;
--Testcase 129:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 * '100'::int4);
ROLLBACK;

BEGIN;
--Testcase 131:
DELETE FROM INT8_TBL;
--Testcase 132:
INSERT INTO INT8_TBL(q1) VALUES ('100'::int4 + '9223372036854775800'::int8);
ROLLBACK;

BEGIN;
--Testcase 134:
DELETE FROM INT8_TBL;
--Testcase 135:
INSERT INTO INT8_TBL(q1) VALUES ('-100'::int4 - '9223372036854775800'::int8);
ROLLBACK;

BEGIN;
--Testcase 137:
DELETE FROM INT8_TBL;
--Testcase 138:
INSERT INTO INT8_TBL(q1) VALUES ('100'::int4 * '9223372036854775800'::int8);
ROLLBACK;

BEGIN;
--Testcase 140:
DELETE FROM INT8_TBL;
--Testcase 141:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 + '100'::int2);
ROLLBACK;

BEGIN;
--Testcase 143:
DELETE FROM INT8_TBL;
--Testcase 144:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775800'::int8 - '100'::int2);
ROLLBACK;

BEGIN;
--Testcase 146:
DELETE FROM INT8_TBL;
--Testcase 147:
INSERT INTO INT8_TBL(q1) VALUES ('9223372036854775800'::int8 * '100'::int2);
ROLLBACK;

BEGIN;
--Testcase 149:
DELETE FROM INT8_TBL;
--Testcase 150:
INSERT INTO INT8_TBL(q1) VALUES ('-9223372036854775808'::int8 / '0'::int2);
ROLLBACK;

BEGIN;
--Testcase 152:
DELETE FROM INT8_TBL;
--Testcase 153:
INSERT INTO INT8_TBL(q1) VALUES ('100'::int2 + '9223372036854775800'::int8);
ROLLBACK;

BEGIN;
--Testcase 155:
DELETE FROM INT8_TBL;
--Testcase 156:
INSERT INTO INT8_TBL(q1) VALUES ('-100'::int2 - '9223372036854775800'::int8);
ROLLBACK;

BEGIN;
--Testcase 158:
DELETE FROM INT8_TBL;
--Testcase 159:
INSERT INTO INT8_TBL(q1) VALUES ('100'::int2 * '9223372036854775800'::int8);
ROLLBACK;

BEGIN;
--Testcase 161:
DELETE FROM INT8_TBL;
--Testcase 162:
INSERT INTO INT8_TBL(q1) VALUES ('100'::int2 / '0'::int8);
ROLLBACK;

--Testcase 164:
SELECT CAST(q1 AS int4) FROM int8_tbl WHERE q2 = 456;
--Testcase 165:
SELECT CAST(q1 AS int4) FROM int8_tbl WHERE q2 <> 456;

--Testcase 166:
SELECT CAST(q1 AS int2) FROM int8_tbl WHERE q2 = 456;
--Testcase 167:
SELECT CAST(q1 AS int2) FROM int8_tbl WHERE q2 <> 456;

BEGIN;
--Testcase 168:
DELETE FROM INT8_TBL;
--Testcase 169:
INSERT INTO INT8_TBL(q1,q2) VALUES ('42'::int2, '-37'::int2);
--Testcase 170:
SELECT CAST(q1 AS int8), CAST(q2 AS int8) FROM INT8_TBL;
ROLLBACK;

--Testcase 171:
SELECT CAST(q1 AS float4), CAST(q2 AS float8) FROM INT8_TBL;

BEGIN;
--Testcase 172:
DELETE FROM INT8_TBL;
--Testcase 173:
INSERT INTO INT8_TBL(q1) VALUES ('36854775807.0'::float4);
--Testcase 174:
SELECT CAST(q1::float4 AS int8) FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 175:
DELETE FROM INT8_TBL;
--Testcase 176:
INSERT INTO INT8_TBL(q1) VALUES ('922337203685477580700.0'::float8);
--Testcase 177:
SELECT CAST(q1::float8 AS int8) FROM INT8_TBL;
ROLLBACK;

--Testcase 178:
SELECT CAST(q1 AS oid) FROM INT8_TBL;

-- bit operations

--Testcase 179:
SELECT q1, q2, q1 & q2 AS "and", q1 | q2 AS "or", q1 # q2 AS "xor", ~q1 AS "not" FROM INT8_TBL;
--Testcase 180:
SELECT q1, q1 << 2 AS "shl", q1 >> 3 AS "shr" FROM INT8_TBL;

-- generate_series

BEGIN;
--Testcase 181:
DELETE FROM INT8_TBL;
--Testcase 182:
INSERT INTO INT8_TBL(q1) SELECT q1 FROM generate_series('+4567890123456789'::int8, '+4567890123456799'::int8) q1;
--Testcase 183:
SELECT q1 AS generate_series FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 184:
DELETE FROM INT8_TBL;
--Testcase 185:
INSERT INTO INT8_TBL(q1) SELECT q1 FROM generate_series('+4567890123456789'::int8, '+4567890123456799'::int8, 0) q1; -- should error
--Testcase 186:
SELECT q1 AS generate_series FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 187:
DELETE FROM INT8_TBL;
--Testcase 188:
INSERT INTO INT8_TBL(q1) SELECT q1 FROM generate_series('+4567890123456789'::int8, '+4567890123456799'::int8, 2) q1;
--Testcase 189:
SELECT q1 AS generate_series FROM INT8_TBL;
ROLLBACK;

-- corner case
BEGIN;
--Testcase 190:
DELETE FROM INT8_TBL;
--Testcase 191:
INSERT INTO INT8_TBL(q1) VALUES(-1::int8<<63);
--Testcase 192:
SELECT q1::text AS text FROM INT8_TBL;
--Testcase 195:
SELECT (q1+1)::text FROM INT8_TBL;
ROLLBACK;

-- check sane handling of INT64_MIN overflow cases
BEGIN;
--Testcase 196:
DELETE FROM INT8_TBL;
--Testcase 197:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);
--Testcase 198:
SELECT (q1 * (-1)::int8) FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 199:
DELETE FROM INT8_TBL;
--Testcase 200:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);
--Testcase 201:
SELECT (q1 / (-1)::int8) FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 202:
DELETE FROM INT8_TBL;
--Testcase 203:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);
--Testcase 204:
SELECT (q1 % (-1)::int8) FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 205:
DELETE FROM INT8_TBL;
--Testcase 206:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);
--Testcase 207:
SELECT (q1 * (-1)::int4) FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 208:
DELETE FROM INT8_TBL;
--Testcase 209:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);
--Testcase 210:
SELECT (q1 / (-1)::int4) FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 211:
DELETE FROM INT8_TBL;
--Testcase 212:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);
--Testcase 213:
SELECT (q1 % (-1)::int4) FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 214:
DELETE FROM INT8_TBL;
--Testcase 215:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);
--Testcase 216:
SELECT (q1 * (-1)::int2) FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 217:
DELETE FROM INT8_TBL;
--Testcase 218:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);
--Testcase 219:
SELECT (q1 / (-1)::int2) FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 220:
DELETE FROM INT8_TBL;
--Testcase 221:
INSERT INTO INT8_TBL(q1) VALUES ((-9223372036854775808)::int8);
--Testcase 222:
SELECT (q1 % (-1)::int2) FROM INT8_TBL;
ROLLBACK;

-- check rounding when casting from float
--Testcase 233:
CREATE FOREIGN TABLE FLOAT8_TBL(id serial OPTIONS (rowkey 'true'), f1 float8) SERVER griddb_svr;
BEGIN;
--Testcase 223:
DELETE FROM FLOAT8_TBL;
--Testcase 224:
INSERT INTO FLOAT8_TBL(f1) VALUES
	     (-2.5::float8),
             (-1.5::float8),
             (-0.5::float8),
             (0.0::float8),
             (0.5::float8),
             (1.5::float8),
             (2.5::float8);
--Testcase 225:
SELECT f1 as x, f1::int8 AS int8_value FROM FLOAT8_TBL;
ROLLBACK;
 
-- check rounding when casting from numeric
BEGIN;
--Testcase 226:
DELETE FROM FLOAT8_TBL;
--Testcase 227:
INSERT INTO FLOAT8_TBL(f1) VALUES
	     (-2.5::numeric),
             (-1.5::numeric),
             (-0.5::numeric),
             (0.0::numeric),
             (0.5::numeric),
             (1.5::numeric),
             (2.5::numeric);
--Testcase 228:
SELECT f1::numeric as x, f1::numeric::int8 AS int8_value FROM FLOAT8_TBL;
ROLLBACK;

-- test gcd()
BEGIN;
--Testcase 234:
DELETE FROM INT8_TBL;
--Testcase 235:
INSERT INTO INT8_TBL(q1, q2) VALUES (0::int8, 0::int8);
--Testcase 236:
INSERT INTO INT8_TBL(q1, q2) VALUES (0::int8, 29893644334::int8);
--Testcase 237:
INSERT INTO INT8_TBL(q1, q2) VALUES (288484263558::int8, 29893644334::int8);
--Testcase 238:
INSERT INTO INT8_TBL(q1, q2) VALUES (-288484263558::int8, 29893644334::int8);
--Testcase 239:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, 1::int8);
--Testcase 240:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, 9223372036854775807::int8);
--Testcase 241:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, 4611686018427387904::int8);
--Testcase 242:
SELECT q1 AS a, q2 AS b, gcd(q1, q2), gcd(q1, -q2), gcd(q2, q1), gcd(-q2, q1) FROM INT8_TBL;
ROlLBACK;

BEGIN;
--Testcase 243:
DELETE FROM INT8_TBL;
--Testcase 244:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, 0::int8);
--Testcase 245:
SELECT gcd(q1, q2) FROM INT8_TBL;    -- overflow
ROLLBACK;

BEGIN;
--Testcase 246:
DELETE FROM INT8_TBL;
--Testcase 247:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, (-9223372036854775808)::int8);
--Testcase 248:
SELECT gcd(q1, q2) FROM INT8_TBL;    -- overflow
ROLLBACK;

-- test lcm()
BEGIN;
--Testcase 249:
DELETE FROM INT8_TBL;
--Testcase 250:
INSERT INTO INT8_TBL(q1, q2) VALUES (0::int8, 0::int8);
--Testcase 251:
INSERT INTO INT8_TBL(q1, q2) VALUES (0::int8, 29893644334::int8);
--Testcase 252:
INSERT INTO INT8_TBL(q1, q2) VALUES (29893644334::int8, 29893644334::int8);
--Testcase 253:
INSERT INTO INT8_TBL(q1, q2) VALUES (288484263558::int8, 29893644334::int8);
--Testcase 254:
INSERT INTO INT8_TBL(q1, q2) VALUES (-288484263558::int8, 29893644334::int8);
--Testcase 255:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, 0::int8);
--Testcase 256:
SELECT q1 AS a, q2 AS b, lcm(q1, q2), lcm(q1, -q2), lcm(q2, q1), lcm(-q2, q1) FROM INT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 257:
DELETE FROM INT8_TBL;
--Testcase 258:
INSERT INTO INT8_TBL(q1, q2) VALUES ((-9223372036854775808)::int8, 1::int8);
--Testcase 259:
SELECT lcm(q1, q2) FROM INT8_TBL;    -- overflow
ROLLBACK;

BEGIN;
--Testcase 260:
DELETE FROM INT8_TBL;
--Testcase 261:
INSERT INTO INT8_TBL(q1, q2) VALUES (9223372036854775807::int8, 9223372036854775806::int8);
--Testcase 262:
SELECT lcm(q1, q2) FROM INT8_TBL;    -- overflow
ROLLBACK;

--Testcase 263:
DROP FOREIGN TABLE FLOAT8_TBL;
--Testcase 264:
DROP FOREIGN TABLE INT8_TBL;
--Testcase 265:
DROP USER MAPPING FOR public SERVER griddb_svr;
--Testcase 266:
DROP SERVER griddb_svr;
--Testcase 267:
DROP EXTENSION griddb_fdw CASCADE;
