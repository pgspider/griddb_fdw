--
-- FLOAT8
--
\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 204:
CREATE EXTENSION griddb_fdw;
--Testcase 205:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');
--Testcase 206:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);
--Testcase 207:
CREATE FOREIGN TABLE FLOAT8_TBL(id serial OPTIONS (rowkey 'true'), f1 float8) SERVER griddb_svr;
--Testcase 208:
CREATE FOREIGN TABLE FLOAT8_TMP(id serial OPTIONS (rowkey 'true'), f1 float8, f2 float8) SERVER griddb_svr;

--Testcase 1:
INSERT INTO FLOAT8_TBL(f1) VALUES ('    0.0   ');
--Testcase 2:
INSERT INTO FLOAT8_TBL(f1) VALUES ('1004.30  ');
--Testcase 3:
INSERT INTO FLOAT8_TBL(f1) VALUES ('   -34.84');
--Testcase 4:
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e+200');
--Testcase 5:
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e-200');

-- test for underflow and overflow handling
--Testcase 6:
INSERT INTO FLOAT8_TBL(f1) VALUES ('10e400'::float8);
--Testcase 7:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-10e400'::float8);
--Testcase 8:
INSERT INTO FLOAT8_TBL(f1) VALUES ('10e-400'::float8);
--Testcase 9:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-10e-400'::float8);

-- test smallest normalized input
BEGIN;
--Testcase 10:
DELETE FROM FLOAT8_TBL;
--Testcase 11:
INSERT INTO FLOAT8_TBL(f1) VALUES ('2.2250738585072014E-308'::float8);
--Testcase 12:
SELECT float8send(f1) FROM FLOAT8_TBL;
ROLLBACK;

-- bad input
--Testcase 13:
INSERT INTO FLOAT8_TBL(f1) VALUES ('');
--Testcase 14:
INSERT INTO FLOAT8_TBL(f1) VALUES ('     ');
--Testcase 15:
INSERT INTO FLOAT8_TBL(f1) VALUES ('xyz');
--Testcase 16:
INSERT INTO FLOAT8_TBL(f1) VALUES ('5.0.0');
--Testcase 17:
INSERT INTO FLOAT8_TBL(f1) VALUES ('5 . 0');
--Testcase 18:
INSERT INTO FLOAT8_TBL(f1) VALUES ('5.   0');
--Testcase 19:
INSERT INTO FLOAT8_TBL(f1) VALUES ('    - 3');
--Testcase 20:
INSERT INTO FLOAT8_TBL(f1) VALUES ('123           5');

-- special inputs
--Testcase 21:
BEGIN;
--Testcase 209:
DELETE FROM FLOAT8_TBL;
--Testcase 210:
INSERT INTO FLOAT8_TBL(f1) VALUES ('NaN'::float8);
--Testcase 211:
SELECT f1 AS float8 FROM FLOAT8_TBL;
ROLLBACK;
--Testcase 23:
BEGIN;
--Testcase 212:
DELETE FROM FLOAT8_TBL;
--Testcase 213:
INSERT INTO FLOAT8_TBL(f1) VALUES ('nan'::float8);
--Testcase 214:
SELECT f1 AS float8 FROM FLOAT8_TBL;
ROLLBACK;
--Testcase 24:
BEGIN;
--Testcase 215:
DELETE FROM FLOAT8_TBL;
--Testcase 216:
INSERT INTO FLOAT8_TBL(f1) VALUES ('   NAN  '::float8);
--Testcase 217:
SELECT f1 AS float8 FROM FLOAT8_TBL;
ROLLBACK;
--Testcase 25:
BEGIN;
--Testcase 218:
DELETE FROM FLOAT8_TBL;
--Testcase 219:
INSERT INTO FLOAT8_TBL(f1) VALUES ('infinity'::float8);
--Testcase 220:
SELECT f1 AS float8 FROM FLOAT8_TBL;
ROLLBACK;
--Testcase 26:
BEGIN;
--Testcase 221:
DELETE FROM FLOAT8_TBL;
--Testcase 222:
INSERT INTO FLOAT8_TBL(f1) VALUES ('          -INFINiTY   '::float8);
--Testcase 223:
SELECT f1 AS float8 FROM FLOAT8_TBL;
ROLLBACK;

-- bad special inputs
--Testcase 28:
INSERT INTO FLOAT8_TBL(f1) VALUES ('N A N'::float8);
--Testcase 29:
INSERT INTO FLOAT8_TBL(f1) VALUES ('NaN x'::float8);
--Testcase 30:
INSERT INTO FLOAT8_TBL(f1) VALUES (' INFINITY    x'::float8);

--Testcase 31:
BEGIN;
--Testcase 224:
DELETE FROM FLOAT8_TBL;
--Testcase 225:
INSERT INTO FLOAT8_TBL(f1) VALUES ('Infinity'::float8);
--Testcase 226:
SELECT (f1::float8 + 100.0) FROM FLOAT8_TBL;
ROLLBACK;
--Testcase 33:
BEGIN;
--Testcase 227:
DELETE FROM FLOAT8_TBL;
--Testcase 228:
INSERT INTO FLOAT8_TBL(f1) VALUES ('Infinity'::float8);
--Testcase 229:
SELECT (f1::float8 / 'Infinity'::float8) FROM FLOAT8_TBL;
ROLLBACK;
--Testcase 34:
BEGIN;
--Testcase 230:
DELETE FROM FLOAT8_TBL;
--Testcase 231:
INSERT INTO FLOAT8_TBL(f1) VALUES ('nan'::float8);
--Testcase 232:
SELECT (f1::float8 / 'nan'::float8) FROM FLOAT8_TBL;
ROLLBACK;
--Testcase 35:
BEGIN;
--Testcase 233:
DELETE FROM FLOAT8_TBL;
--Testcase 234:
INSERT INTO FLOAT8_TBL(f1) VALUES ('nan'::numeric);
--Testcase 235:
SELECT (f1::float8) AS float8 FROM FLOAT8_TBL;
ROLLBACK;

--Testcase 37:
SELECT '' AS five, f1 FROM FLOAT8_TBL;

--Testcase 38:
SELECT '' AS four, f.f1 FROM FLOAT8_TBL f WHERE f.f1 <> '1004.3';

--Testcase 39:
SELECT '' AS one, f.f1 FROM FLOAT8_TBL f WHERE f.f1 = '1004.3';

--Testcase 40:
SELECT '' AS three, f.f1 FROM FLOAT8_TBL f WHERE '1004.3' > f.f1;

--Testcase 41:
SELECT '' AS three, f.f1 FROM FLOAT8_TBL f WHERE  f.f1 < '1004.3';

--Testcase 42:
SELECT '' AS four, f.f1 FROM FLOAT8_TBL f WHERE '1004.3' >= f.f1;

--Testcase 43:
SELECT '' AS four, f.f1 FROM FLOAT8_TBL f WHERE  f.f1 <= '1004.3';

--Testcase 44:
SELECT '' AS three, f.f1, f.f1 * '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

--Testcase 45:
SELECT '' AS three, f.f1, f.f1 + '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

--Testcase 46:
SELECT '' AS three, f.f1, f.f1 / '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

--Testcase 47:
SELECT '' AS three, f.f1, f.f1 - '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

--Testcase 48:
SELECT '' AS one, f.f1 ^ '2.0' AS square_f1
   FROM FLOAT8_TBL f where f.f1 = '1004.3';

-- absolute value
--Testcase 49:
SELECT '' AS five, f.f1, @f.f1 AS abs_f1
   FROM FLOAT8_TBL f;

-- truncate
--Testcase 50:
SELECT '' AS five, f.f1, trunc(f.f1) AS trunc_f1
   FROM FLOAT8_TBL f;

-- round
--Testcase 51:
SELECT '' AS five, f.f1, round(f.f1) AS round_f1
   FROM FLOAT8_TBL f;

-- ceil / ceiling
--Testcase 52:
select ceil(f1) as ceil_f1 from float8_tbl f;
--Testcase 53:
select ceiling(f1) as ceiling_f1 from float8_tbl f;

-- floor
--Testcase 54:
select floor(f1) as floor_f1 from float8_tbl f;

-- sign
--Testcase 55:
select sign(f1) as sign_f1 from float8_tbl f;

-- avoid bit-exact output here because operations may not be bit-exact.
--Testcase 278:
SET extra_float_digits = 0;

-- square root
--Testcase 56:
BEGIN;
--Testcase 236:
DELETE FROM FLOAT8_TBL;
--Testcase 237:
INSERT INTO FLOAT8_TBL(f1) VALUES (float8 '64');
--Testcase 238:
SELECT sqrt(f1) AS eight FROM FLOAT8_TBL;
ROLLBACK;
--Testcase 58:
BEGIN;
--Testcase 239:
DELETE FROM FLOAT8_TBL;
--Testcase 240:
INSERT INTO FLOAT8_TBL(f1) VALUES (float8 '64');
--Testcase 241:
SELECT (|/ f1) AS eight FROM FLOAT8_TBL;
ROLLBACK;

--Testcase 60:
SELECT '' AS three, f.f1, |/f.f1 AS sqrt_f1
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

-- power
--Testcase 61:
BEGIN;
--Testcase 242:
DELETE FROM FLOAT8_TMP;
--Testcase 243:
INSERT INTO FLOAT8_TMP(f1, f2) VALUES (float8 '144', float8 '0.5');
--Testcase 244:
SELECT power(f1, f2) FROM FLOAT8_TMP;
ROLLBACK;
--Testcase 63:
BEGIN;
--Testcase 245:
DELETE FROM FLOAT8_TMP;
--Testcase 246:
INSERT INTO FLOAT8_TMP(f1, f2) VALUES (float8 'NaN', float8 '0.5');
--Testcase 247:
SELECT power(f1, f2) FROM FLOAT8_TMP;
ROLLBACK;
--Testcase 64:
BEGIN;
--Testcase 248:
DELETE FROM FLOAT8_TMP;
--Testcase 249:
INSERT INTO FLOAT8_TMP(f1, f2) VALUES (float8 '144', float8 'NaN');
--Testcase 250:
SELECT power(f1, f2) FROM FLOAT8_TMP;
ROLLBACK;
--Testcase 65:
BEGIN;
--Testcase 251:
DELETE FROM FLOAT8_TMP;
--Testcase 252:
INSERT INTO FLOAT8_TMP(f1, f2) VALUES (float8 'NaN', float8 'NaN');
--Testcase 253:
SELECT power(f1, f2) FROM FLOAT8_TMP;
ROLLBACK;
--Testcase 66:
BEGIN;
--Testcase 254:
DELETE FROM FLOAT8_TMP;
--Testcase 255:
INSERT INTO FLOAT8_TMP(f1, f2) VALUES (float8 '-1', float8 'NaN');
--Testcase 256:
SELECT power(f1, f2) FROM FLOAT8_TMP;
ROLLBACK;
--Testcase 67:
BEGIN;
--Testcase 257:
DELETE FROM FLOAT8_TMP;
--Testcase 258:
INSERT INTO FLOAT8_TMP(f1, f2) VALUES (float8 '1', float8 'NaN');
--Testcase 259:
SELECT power(f1, f2) FROM FLOAT8_TMP;
ROLLBACK;
--Testcase 68:
BEGIN;
--Testcase 260:
DELETE FROM FLOAT8_TMP;
--Testcase 261:
INSERT INTO FLOAT8_TMP(f1, f2) VALUES (float8 'NaN', float8 '0');
--Testcase 262:
SELECT power(f1, f2) FROM FLOAT8_TMP;
ROLLBACK;

-- take exp of ln(f.f1)
--Testcase 70:
SELECT '' AS three, f.f1, exp(ln(f.f1)) AS exp_ln_f1
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

-- cube root
BEGIN;
--Testcase 71:
DELETE FROM FLOAT8_TBL;
--Testcase 72:
INSERT INTO FLOAT8_TBL(f1) VALUES (float8 '27');
--Testcase 73:
SELECT (||/ f1) as three FROM FLOAT8_TBL;
ROLLBACK;

--Testcase 74:
SELECT '' AS five, f.f1, ||/f.f1 AS cbrt_f1 FROM FLOAT8_TBL f;


--Testcase 75:
SELECT '' AS five, f1 FROM FLOAT8_TBL;

--Testcase 76:
UPDATE FLOAT8_TBL
   SET f1 = FLOAT8_TBL.f1 * '-1'
   WHERE FLOAT8_TBL.f1 > '0.0';

--Testcase 77:
SELECT '' AS bad, f.f1 * '1e200' from FLOAT8_TBL f;

--Testcase 78:
SELECT '' AS bad, f.f1 ^ '1e200' from FLOAT8_TBL f;

BEGIN;
--Testcase 79:
DELETE FROM FLOAT8_TBL;
--Testcase 80:
INSERT INTO FLOAT8_TBL(f1) VALUES (0);
--Testcase 81:
SELECT (f1 ^ 0 + f1 ^ 1 + f1 ^ 0.0 + f1 ^ 0.5) FROM FLOAT8_TBL;
ROLLBACK;

--Testcase 82:
SELECT '' AS bad, ln(f.f1) from FLOAT8_TBL f where f.f1 = '0.0' ;

--Testcase 83:
SELECT '' AS bad, ln(f.f1) from FLOAT8_TBL f where f.f1 < '0.0' ;

--Testcase 84:
SELECT '' AS bad, exp(f.f1) from FLOAT8_TBL f;

--Testcase 85:
SELECT '' AS bad, f.f1 / '0.0' from FLOAT8_TBL f;

--Testcase 86:
SELECT '' AS five, f1 FROM FLOAT8_TBL;

-- hyperbolic functions
-- we run these with extra_float_digits = 0 too, since different platforms
-- tend to produce results that vary in the last place.
BEGIN;
--Testcase 87:
DELETE FROM FLOAT8_TBL;
--Testcase 88:
INSERT INTO FLOAT8_TBL(f1) VALUES (1);
--Testcase 89:
SELECT sinh(f1) FROM FLOAT8_TBL;
--Testcase 90:
SELECT cosh(f1) FROM FLOAT8_TBL;
--Testcase 91:
SELECT tanh(f1) FROM FLOAT8_TBL;
--Testcase 92:
SELECT asinh(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 93:
DELETE FROM FLOAT8_TBL;
--Testcase 94:
INSERT INTO FLOAT8_TBL(f1) VALUES (2);
--Testcase 95:
SELECT acosh(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 96:
DELETE FROM FLOAT8_TBL;
--Testcase 97:
INSERT INTO FLOAT8_TBL(f1) VALUES (0.5);
--Testcase 98:
SELECT atanh(f1) FROM FLOAT8_TBL;
ROLLBACK;

-- test Inf/NaN cases for hyperbolic functions
BEGIN;
--Testcase 99:
DELETE FROM FLOAT8_TBL;
--Testcase 100:
INSERT INTO FLOAT8_TBL(f1) VALUES ((float8 'infinity'));
--Testcase 101:
INSERT INTO FLOAT8_TBL(f1) VALUES ((float8 '-infinity'));
--Testcase 102:
INSERT INTO FLOAT8_TBL(f1) VALUES ((float8 'nan'));
--Testcase 103:
SELECT sinh(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 104:
DELETE FROM FLOAT8_TBL;
--Testcase 105:
INSERT INTO FLOAT8_TBL(f1) VALUES ((float8 'infinity'));
--Testcase 106:
INSERT INTO FLOAT8_TBL(f1) VALUES ((float8 '-infinity'));
--Testcase 107:
INSERT INTO FLOAT8_TBL(f1) VALUES ((float8 'nan'));
--Testcase 108:
SELECT cosh(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 109:
DELETE FROM FLOAT8_TBL;
--Testcase 110:
INSERT INTO FLOAT8_TBL(f1) VALUES ((float8 'infinity'));
--Testcase 111:
INSERT INTO FLOAT8_TBL(f1) VALUES ((float8 '-infinity'));
--Testcase 112:
INSERT INTO FLOAT8_TBL(f1) VALUES ((float8 'nan'));
--Testcase 113:
SELECT tanh(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 114:
DELETE FROM FLOAT8_TBL;
--Testcase 115:
INSERT INTO FLOAT8_TBL(f1) VALUES ((float8 'infinity'));
--Testcase 116:
INSERT INTO FLOAT8_TBL(f1) VALUES ((float8 '-infinity'));
--Testcase 117:
INSERT INTO FLOAT8_TBL(f1) VALUES ((float8 'nan'));
--Testcase 118:
SELECT asinh(f1) FROM FLOAT8_TBL;
ROLLBACK;

-- 
-- acosh(Inf) should be Inf, but some mingw versions produce NaN, so skip test
-- SELECT acosh(float8 'infinity');
BEGIN;
--Testcase 119:
DELETE FROM FLOAT8_TBL;
--Testcase 120:
INSERT INTO FLOAT8_TBL(f1) VALUES (float8 '-infinity');
--Testcase 121:
SELECT acosh(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 122:
DELETE FROM FLOAT8_TBL;
--Testcase 123:
INSERT INTO FLOAT8_TBL(f1) VALUES (float8 'nan');
--Testcase 124:
SELECT acosh(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 125:
DELETE FROM FLOAT8_TBL;
--Testcase 126:
INSERT INTO FLOAT8_TBL(f1) VALUES (float8 'infinity');
--Testcase 127:
SELECT atanh(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 128:
DELETE FROM FLOAT8_TBL;
--Testcase 129:
INSERT INTO FLOAT8_TBL(f1) VALUES (float8 '-infinity');
--Testcase 130:
SELECT atanh(f1) FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 131:
DELETE FROM FLOAT8_TBL;
--Testcase 132:
INSERT INTO FLOAT8_TBL(f1) VALUES (float8 'nan');
--Testcase 133:
SELECT atanh(f1) FROM FLOAT8_TBL;
ROLLBACK;

--Testcase 279:
RESET extra_float_digits;

-- test for over- and underflow
--Testcase 134:
INSERT INTO FLOAT8_TBL(f1) VALUES ('10e400');

--Testcase 135:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-10e400');

--Testcase 136:
INSERT INTO FLOAT8_TBL(f1) VALUES ('10e-400');

--Testcase 137:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-10e-400');

-- maintain external table consistency across platforms
-- delete all values and reinsert well-behaved ones

--Testcase 138:
DELETE FROM FLOAT8_TBL;

--Testcase 139:
INSERT INTO FLOAT8_TBL(f1) VALUES ('0.0');

--Testcase 140:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-34.84');

--Testcase 141:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-1004.30');

--Testcase 142:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-1.2345678901234e+200');

--Testcase 143:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-1.2345678901234e-200');

--Testcase 144:
SELECT '' AS five, f1 FROM FLOAT8_TBL;

-- test edge-case coercions to integer
BEGIN;
--Testcase 145:
DELETE FROM FLOAT8_TBL;
--Testcase 146:
INSERT INTO FLOAT8_TBL(f1) VALUES ('32767.4'::float8);
--Testcase 147:
SELECT f1::int2 AS int2 FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 148:
DELETE FROM FLOAT8_TBL;
--Testcase 149:
INSERT INTO FLOAT8_TBL(f1) VALUES ('32767.6'::float8);
--Testcase 150:
SELECT f1::int2 AS int2 FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 151:
DELETE FROM FLOAT8_TBL;
--Testcase 152:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-32768.4'::float8);
--Testcase 153:
SELECT f1::int2 AS int2 FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 154:
DELETE FROM FLOAT8_TBL;
--Testcase 155:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-32768.6'::float8);
--Testcase 156:
SELECT f1::int2 FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 157:
DELETE FROM FLOAT8_TBL;
--Testcase 158:
INSERT INTO FLOAT8_TBL(f1) VALUES ('2147483647.4'::float8);
--Testcase 159:
SELECT f1::int4 AS int4 FROM FLOAT8_TBL;
ROLLBACK;
BEGIN;
--Testcase 160:
DELETE FROM FLOAT8_TBL;
--Testcase 161:
INSERT INTO FLOAT8_TBL(f1) VALUES ('2147483647.6'::float8);
--Testcase 162:
SELECT f1::int4 FROM FLOAT8_TBL;
ROLLBACK;
BEGIN;
--Testcase 163:
DELETE FROM FLOAT8_TBL;
--Testcase 164:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-2147483648.4'::float8);
--Testcase 165:
SELECT f1::int4 AS int4 FROM FLOAT8_TBL;
ROLLBACK;
BEGIN;
--Testcase 166:
DELETE FROM FLOAT8_TBL;
--Testcase 167:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-2147483648.6'::float8);
--Testcase 168:
SELECT f1::int4 FROM FLOAT8_TBL;
ROLLBACK;

BEGIN;
--Testcase 169:
DELETE FROM FLOAT8_TBL;
--Testcase 170:
INSERT INTO FLOAT8_TBL(f1) VALUES ('9223372036854773760'::float8);
--Testcase 171:
SELECT f1::int8 AS int8 FROM FLOAT8_TBL;
ROLLBACK;
BEGIN;
--Testcase 172:
DELETE FROM FLOAT8_TBL;
--Testcase 173:
INSERT INTO FLOAT8_TBL(f1) VALUES ('9223372036854775807'::float8);
--Testcase 174:
SELECT f1::int8 FROM FLOAT8_TBL;
ROLLBACK;
BEGIN;
--Testcase 175:
DELETE FROM FLOAT8_TBL;
--Testcase 176:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-9223372036854775808.5'::float8);
--Testcase 177:
SELECT f1::int8 AS int8 FROM FLOAT8_TBL;
ROLLBACK;
BEGIN;
--Testcase 178:
DELETE FROM FLOAT8_TBL;
--Testcase 179:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-9223372036854780000'::float8);
--Testcase 180:
SELECT f1::int8 FROM FLOAT8_TBL;
ROLLBACK;
-- test exact cases for trigonometric functions in degrees

BEGIN;
--Testcase 181:
DELETE FROM FLOAT8_TBL;
--Testcase 182:
INSERT INTO FLOAT8_TBL(f1) VALUES (0), (30), (90), (150), (180),
      (210), (270), (330), (360);
--Testcase 183:
SELECT f1 AS x,
       sind(f1),
       sind(f1) IN (-1,-0.5,0,0.5,1) AS sind_exact
       FROM FLOAT8_TBL;

--Testcase 184:
DELETE FROM FLOAT8_TBL;
--Testcase 185:
INSERT INTO FLOAT8_TBL(f1) VALUES (0), (60), (90), (120), (180),
      (240), (270), (300), (360);
--Testcase 186:
SELECT f1 AS x,
       cosd(f1),
       cosd(f1) IN (-1,-0.5,0,0.5,1) AS cosd_exact
       FROM FLOAT8_TBL;

--Testcase 187:
DELETE FROM FLOAT8_TBL;
--Testcase 188:
INSERT INTO FLOAT8_TBL(f1) VALUES (0), (45), (90), (135), (180),
      (225), (270), (315), (360);
--Testcase 189:
SELECT f1 AS x,
       tand(f1),
       tand(f1) IN ('-Infinity'::float8,-1,0,
                   1,'Infinity'::float8) AS tand_exact,
       cotd(f1),
       cotd(f1) IN ('-Infinity'::float8,-1,0,
                   1,'Infinity'::float8) AS cotd_exact
          FROM FLOAT8_TBL;

--Testcase 190:
DELETE FROM FLOAT8_TBL;
--Testcase 191:
INSERT INTO FLOAT8_TBL(f1) VALUES (-1), (-0.5), (0), (0.5), (1);
--Testcase 192:
SELECT f1 AS x,
       asind(f1),
       asind(f1) IN (-90,-30,0,30,90) AS asind_exact,
       acosd(f1),
       acosd(f1) IN (0,60,90,120,180) AS acosd_exact
          FROM FLOAT8_TBL;

--Testcase 193:
DELETE FROM FLOAT8_TBL;
--Testcase 194:
INSERT INTO FLOAT8_TBL(f1) VALUES ('-Infinity'::float8), (-1), (0), (1),
      ('Infinity'::float8);
--Testcase 195:
SELECT f1,
       atand(f1),
       atand(f1) IN (-90,-45,0,45,90) AS atand_exact
          FROM FLOAT8_TBL;

--Testcase 196:
DELETE FROM FLOAT8_TBL;
--Testcase 197:
INSERT INTO FLOAT8_TBL(f1) SELECT * FROM generate_series(0, 360, 90);
--Testcase 198:
SELECT x, y,
       atan2d(y, x),
       atan2d(y, x) IN (-90,0,90,180) AS atan2d_exact
FROM (SELECT 10*cosd(f1), 10*sind(f1)
          FROM FLOAT8_TBL) AS t(x,y);

ROLLBACK;

-- 
-- test output (and round-trip safety) of various values.
-- To ensure we're testing what we think we're testing, start with
-- float values specified by bit patterns (as a useful side effect,
-- this means we'll fail on non-IEEE platforms).

--Testcase 263:
create type xfloat8;
--Testcase 264:
create function xfloat8in(cstring) returns xfloat8 immutable strict
  language internal as 'int8in';
--Testcase 265:
create function xfloat8out(xfloat8) returns cstring immutable strict
  language internal as 'int8out';
--Testcase 266:
create type xfloat8 (input = xfloat8in, output = xfloat8out, like = float8);
--Testcase 267:
create cast (xfloat8 as float8) without function;
--Testcase 268:
create cast (float8 as xfloat8) without function;
--Testcase 269:
create cast (xfloat8 as bigint) without function;
--Testcase 270:
create cast (bigint as xfloat8) without function;

-- float8: seeeeeee eeeeeeee eeeeeeee mmmmmmmm mmmmmmmm(x4)

-- we don't care to assume the platform's strtod() handles subnormals
-- correctly; those are "use at your own risk". However we do test
-- subnormal outputs, since those are under our control.

--Testcase 271:
create foreign table test_data(id serial OPTIONS (rowkey 'true'),
        bits text) server griddb_svr;
begin;
--Testcase 199:
insert into test_data(bits) values
  -- small subnormals
  (x'0000000000000001'),
  (x'0000000000000002'), (x'0000000000000003'),
  (x'0000000000001000'), (x'0000000100000000'),
  (x'0000010000000000'), (x'0000010100000000'),
  (x'0000400000000000'), (x'0000400100000000'),
  (x'0000800000000000'), (x'0000800000000001'),
  -- these values taken from upstream testsuite
  (x'00000000000f4240'),
  (x'00000000016e3600'),
  (x'0000008cdcdea440'),
  -- borderline between subnormal and normal
  (x'000ffffffffffff0'), (x'000ffffffffffff1'),
  (x'000ffffffffffffe'), (x'000fffffffffffff');
--Testcase 200:
select float8send(flt) as ibits,
       flt
  from (select bits::bit(64)::bigint::xfloat8::float8 as flt
          from test_data
	offset 0) s;
rollback;

-- round-trip tests

begin;
--Testcase 201:
delete from test_data;
--Testcase 202:
insert into test_data(bits) values
  (x'0000000000000000'),
  -- smallest normal values
  (x'0010000000000000'), (x'0010000000000001'),
  (x'0010000000000002'), (x'0018000000000000'),
  --
  (x'3ddb7cdfd9d7bdba'), (x'3ddb7cdfd9d7bdbb'), (x'3ddb7cdfd9d7bdbc'),
  (x'3e112e0be826d694'), (x'3e112e0be826d695'), (x'3e112e0be826d696'),
  (x'3e45798ee2308c39'), (x'3e45798ee2308c3a'), (x'3e45798ee2308c3b'),
  (x'3e7ad7f29abcaf47'), (x'3e7ad7f29abcaf48'), (x'3e7ad7f29abcaf49'),
  (x'3eb0c6f7a0b5ed8c'), (x'3eb0c6f7a0b5ed8d'), (x'3eb0c6f7a0b5ed8e'),
  (x'3ee4f8b588e368ef'), (x'3ee4f8b588e368f0'), (x'3ee4f8b588e368f1'),
  (x'3f1a36e2eb1c432c'), (x'3f1a36e2eb1c432d'), (x'3f1a36e2eb1c432e'),
  (x'3f50624dd2f1a9fb'), (x'3f50624dd2f1a9fc'), (x'3f50624dd2f1a9fd'),
  (x'3f847ae147ae147a'), (x'3f847ae147ae147b'), (x'3f847ae147ae147c'),
  (x'3fb9999999999999'), (x'3fb999999999999a'), (x'3fb999999999999b'),
  -- values very close to 1
  (x'3feffffffffffff0'), (x'3feffffffffffff1'), (x'3feffffffffffff2'),
  (x'3feffffffffffff3'), (x'3feffffffffffff4'), (x'3feffffffffffff5'),
  (x'3feffffffffffff6'), (x'3feffffffffffff7'), (x'3feffffffffffff8'),
  (x'3feffffffffffff9'), (x'3feffffffffffffa'), (x'3feffffffffffffb'),
  (x'3feffffffffffffc'), (x'3feffffffffffffd'), (x'3feffffffffffffe'),
  (x'3fefffffffffffff'),
  (x'3ff0000000000000'),
  (x'3ff0000000000001'), (x'3ff0000000000002'), (x'3ff0000000000003'),
  (x'3ff0000000000004'), (x'3ff0000000000005'), (x'3ff0000000000006'),
  (x'3ff0000000000007'), (x'3ff0000000000008'), (x'3ff0000000000009'),
  --
  (x'3ff921fb54442d18'),
  (x'4005bf0a8b14576a'),
  (x'400921fb54442d18'),
  --
  (x'4023ffffffffffff'), (x'4024000000000000'), (x'4024000000000001'),
  (x'4058ffffffffffff'), (x'4059000000000000'), (x'4059000000000001'),
  (x'408f3fffffffffff'), (x'408f400000000000'), (x'408f400000000001'),
  (x'40c387ffffffffff'), (x'40c3880000000000'), (x'40c3880000000001'),
  (x'40f869ffffffffff'), (x'40f86a0000000000'), (x'40f86a0000000001'),
  (x'412e847fffffffff'), (x'412e848000000000'), (x'412e848000000001'),
  (x'416312cfffffffff'), (x'416312d000000000'), (x'416312d000000001'),
  (x'4197d783ffffffff'), (x'4197d78400000000'), (x'4197d78400000001'),
  (x'41cdcd64ffffffff'), (x'41cdcd6500000000'), (x'41cdcd6500000001'),
  (x'4202a05f1fffffff'), (x'4202a05f20000000'), (x'4202a05f20000001'),
  (x'42374876e7ffffff'), (x'42374876e8000000'), (x'42374876e8000001'),
  (x'426d1a94a1ffffff'), (x'426d1a94a2000000'), (x'426d1a94a2000001'),
  (x'42a2309ce53fffff'), (x'42a2309ce5400000'), (x'42a2309ce5400001'),
  (x'42d6bcc41e8fffff'), (x'42d6bcc41e900000'), (x'42d6bcc41e900001'),
  (x'430c6bf52633ffff'), (x'430c6bf526340000'), (x'430c6bf526340001'),
  (x'4341c37937e07fff'), (x'4341c37937e08000'), (x'4341c37937e08001'),
  (x'4376345785d89fff'), (x'4376345785d8a000'), (x'4376345785d8a001'),
  (x'43abc16d674ec7ff'), (x'43abc16d674ec800'), (x'43abc16d674ec801'),
  (x'43e158e460913cff'), (x'43e158e460913d00'), (x'43e158e460913d01'),
  (x'4415af1d78b58c3f'), (x'4415af1d78b58c40'), (x'4415af1d78b58c41'),
  (x'444b1ae4d6e2ef4f'), (x'444b1ae4d6e2ef50'), (x'444b1ae4d6e2ef51'),
  (x'4480f0cf064dd591'), (x'4480f0cf064dd592'), (x'4480f0cf064dd593'),
  (x'44b52d02c7e14af5'), (x'44b52d02c7e14af6'), (x'44b52d02c7e14af7'),
  (x'44ea784379d99db3'), (x'44ea784379d99db4'), (x'44ea784379d99db5'),
  (x'45208b2a2c280290'), (x'45208b2a2c280291'), (x'45208b2a2c280292'),
  --
  (x'7feffffffffffffe'), (x'7fefffffffffffff'),
  -- round to even tests (+ve)
  (x'4350000000000002'),
  (x'4350000000002e06'),
  (x'4352000000000003'),
  (x'4352000000000004'),
  (x'4358000000000003'),
  (x'4358000000000004'),
  (x'435f000000000020'),
  -- round to even tests (-ve)
  (x'c350000000000002'),
  (x'c350000000002e06'),
  (x'c352000000000003'),
  (x'c352000000000004'),
  (x'c358000000000003'),
  (x'c358000000000004'),
  (x'c35f000000000020'),
  -- exercise fixed-point memmoves
  (x'42dc12218377de66'),
  (x'42a674e79c5fe51f'),
  (x'4271f71fb04cb74c'),
  (x'423cbe991a145879'),
  (x'4206fee0e1a9e061'),
  (x'41d26580b487e6b4'),
  (x'419d6f34540ca453'),
  (x'41678c29dcd6e9dc'),
  (x'4132d687e3df217d'),
  (x'40fe240c9fcb68c8'),
  (x'40c81cd6e63c53d3'),
  (x'40934a4584fd0fdc'),
  (x'405edd3c07fb4c93'),
  (x'4028b0fcd32f7076'),
  (x'3ff3c0ca428c59f8'),
  -- these cases come from the upstream's testsuite
  -- LotsOfTrailingZeros)
  (x'3e60000000000000'),
  -- Regression
  (x'c352bd2668e077c4'),
  (x'434018601510c000'),
  (x'43d055dc36f24000'),
  (x'43e052961c6f8000'),
  (x'3ff3c0ca2a5b1d5d'),
  -- LooksLikePow5
  (x'4830f0cf064dd592'),
  (x'4840f0cf064dd592'),
  (x'4850f0cf064dd592'),
  -- OutputLength
  (x'3ff3333333333333'),
  (x'3ff3ae147ae147ae'),
  (x'3ff3be76c8b43958'),
  (x'3ff3c083126e978d'),
  (x'3ff3c0c1fc8f3238'),
  (x'3ff3c0c9539b8887'),
  (x'3ff3c0ca2a5b1d5d'),
  (x'3ff3c0ca4283de1b'),
  (x'3ff3c0ca43db770a'),
  (x'3ff3c0ca428abd53'),
  (x'3ff3c0ca428c1d2b'),
  (x'3ff3c0ca428c51f2'),
  (x'3ff3c0ca428c58fc'),
  (x'3ff3c0ca428c59dd'),
  (x'3ff3c0ca428c59f8'),
  (x'3ff3c0ca428c59fb'),
  -- 32-bit chunking
  (x'40112e0be8047a7d'),
  (x'40112e0be815a889'),
  (x'40112e0be826d695'),
  (x'40112e0be83804a1'),
  (x'40112e0be84932ad'),
  -- MinMaxShift
  (x'0040000000000000'),
  (x'007fffffffffffff'),
  (x'0290000000000000'),
  (x'029fffffffffffff'),
  (x'4350000000000000'),
  (x'435fffffffffffff'),
  (x'1330000000000000'),
  (x'133fffffffffffff'),
  (x'3a6fa7161a4d6e0c');
--Testcase 203:
select float8send(flt) as ibits,
       flt,
       flt::text::float8 as r_flt,
       float8send(flt::text::float8) as obits,
       float8send(flt::text::float8) = float8send(flt) as correct
  from (select bits::bit(64)::bigint::xfloat8::float8 as flt
          from test_data
	offset 0) s;

-- clean up, lest opr_sanity complain
--Testcase 272:
drop type xfloat8 cascade;
--Testcase 273:
drop foreign table test_data;
--Testcase 274:
DROP FOREIGN TABLE FLOAT8_TBL;
--Testcase 275:
DROP USER MAPPING FOR public SERVER griddb_svr;
--Testcase 276:
DROP SERVER griddb_svr CASCADE;
--Testcase 277:
DROP EXTENSION griddb_fdw CASCADE;
