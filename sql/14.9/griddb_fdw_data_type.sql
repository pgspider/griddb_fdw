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
-- GridDB containers type_XXX must be created for this test on GridDB server
/*
CREATE TABLE type_string (col1 text primary key, col2 text)
CREATE TABLE type_boolean (col1 integer primary key, col2 boolean)
CREATE TABLE type_byte (col1 integer primary key, col2 char)
CREATE TABLE type_short (col1 integer primary key, col2 short)
CREATE TABLE type_integer (col1 integer primary key, col2 integer)
CREATE TABLE type_long (col1 long primary key, col2 long)
CREATE TABLE type_float (col1 integer primary key, col2 float)
CREATE TABLE type_double (col1 integer primary key, col2 double)
CREATE TABLE type_timestamp (col1 timestamp primary key, col2 timestamp)
CREATE TABLE type_blob (col1 integer primary key, col2 blob)
CREATE TABLE type_string_array (col1 integer primary key, col2 text[])
CREATE TABLE type_bool_array (col1 integer primary key, col2 boolean[])
CREATE TABLE type_byte_array (col1 integer primary key, col2 char[])
CREATE TABLE type_short_array (col1 integer primary key, col2 short[])
CREATE TABLE type_integer_array (col1 integer primary key, col2 integer[])
CREATE TABLE type_long_array (col1 integer primary key, col2 long[])
CREATE TABLE type_float_array (col1 integer primary key, col2 float[])
CREATE TABLE type_double_array (col1 integer primary key, col2 double[])
CREATE TABLE type_timestamp_array (col1 integer primary key, col2 timestamp[])
-- CREATE TABLE type_geometry (col1 integer primary key, col2 geometry)
*/

--Testcase 4:
CREATE TABLE tbl_string(col1 text);

--Testcase 5:
INSERT INTO tbl_string VALUES('stringba');

--Testcase 6:
INSERT INTO tbl_string VALUES('stringaaa');

--Testcase 7:
CREATE TABLE tbl_integer(col1 integer);

--Testcase 8:
INSERT INTO tbl_integer VALUES(32771);

--Testcase 9:
INSERT INTO tbl_integer VALUES(32769);

--Testcase 10:
CREATE TABLE tbl_long(col1 bigint);

--Testcase 11:
INSERT INTO tbl_long VALUES(2147483651);

--Testcase 12:
INSERT INTO tbl_long VALUES(2147483648);

--Testcase 13:
CREATE TABLE tbl_timestamp(col1 timestamp);

--Testcase 14:
INSERT INTO tbl_timestamp VALUES(to_timestamp('2010.07.01 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 15:
INSERT INTO tbl_timestamp VALUES(to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 16:
DELETE FROM type_string;

--Testcase 17:
DELETE FROM type_boolean;

--Testcase 18:
DELETE FROM type_byte;

--Testcase 19:
DELETE FROM type_short;

--Testcase 20:
DELETE FROM type_integer;

--Testcase 21:
DELETE FROM type_long;

--Testcase 22:
DELETE FROM type_float;

--Testcase 23:
DELETE FROM type_double;

--Testcase 24:
DELETE FROM type_timestamp;

--Testcase 25:
DELETE FROM type_blob;

--Testcase 26:
DELETE FROM type_string_array;

--Testcase 27:
DELETE FROM type_bool_array;

--Testcase 28:
DELETE FROM type_byte_array;

--Testcase 29:
DELETE FROM type_short_array;

--Testcase 30:
DELETE FROM type_integer_array;

--Testcase 31:
DELETE FROM type_long_array;

--Testcase 32:
DELETE FROM type_float_array;

--Testcase 33:
DELETE FROM type_double_array;

--Testcase 34:
DELETE FROM type_timestamp_array;
-- DELETE FROM type_geometry;

--Testcase 35:
INSERT INTO type_string(col1,col2) VALUES ('stringbaaa', 'STRINGBAAA');

--Testcase 36:
INSERT INTO type_string(col1,col2) VALUES ('stringaaa', 'STRINGAAA');

--Testcase 37:
INSERT INTO type_string(col1,col2) VALUES ('stringab', 'STRINGAB');

--Testcase 38:
INSERT INTO type_string(col1,col2) VALUES ('stringba', 'STRINGBA');

--Testcase 39:
INSERT INTO type_boolean(col1,col2) VALUES (1, TRUE);

--Testcase 40:
INSERT INTO type_boolean(col1,col2) VALUES (2, FALSE);

--Testcase 41:
INSERT INTO type_byte(col1,col2) VALUES (1, -128);

--Testcase 42:
INSERT INTO type_byte(col1,col2) VALUES (2, 127);

--Testcase 43:
INSERT INTO type_short(col1,col2) VALUES (1, 1);

--Testcase 44:
INSERT INTO type_short(col1,col2) VALUES (2, 2);

--Testcase 45:
INSERT INTO type_short(col1,col2) VALUES (3, 3);

--Testcase 46:
INSERT INTO type_short(col1,col2) VALUES (4, 4);

--Testcase 47:
INSERT INTO type_integer(col1,col2) VALUES (32769, -32772);

--Testcase 48:
INSERT INTO type_integer(col1,col2) VALUES (32772, -32769);

--Testcase 49:
INSERT INTO type_integer(col1,col2) VALUES (32770, -32771);

--Testcase 50:
INSERT INTO type_integer(col1,col2) VALUES (32768, -32773);

--Testcase 51:
INSERT INTO type_integer(col1,col2) VALUES (32771, -32770);

--Testcase 52:
INSERT INTO type_long(col1,col2) VALUES (2147483649, -2147483652);

--Testcase 53:
INSERT INTO type_long(col1,col2) VALUES (2147483652, -2147483649);

--Testcase 54:
INSERT INTO type_long(col1,col2) VALUES (2147483650, -2147483651);

--Testcase 55:
INSERT INTO type_long(col1,col2) VALUES (2147483648, -2147483653);

--Testcase 56:
INSERT INTO type_long(col1,col2) VALUES (2147483651, -2147483650);

--Testcase 57:
INSERT INTO type_float(col1,col2) VALUES (1, 1.58);

--Testcase 58:
INSERT INTO type_float(col1,col2) VALUES (2, 3.14);

--Testcase 59:
INSERT INTO type_double(col1,col2) VALUES (1, 3.14159265);

--Testcase 60:
INSERT INTO type_double(col1,col2) VALUES (2, 5.67890123);

--Testcase 61:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2000.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2200.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 62:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 63:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.45', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.45', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 64:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 65:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2199.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 66:
INSERT INTO type_blob(col1,col2) VALUES (1, bytea('\xDEADBEEF'));

--Testcase 67:
INSERT INTO type_string_array(col1,col2) VALUES (1, ARRAY['s1', 's2', 's3']);

--Testcase 68:
INSERT INTO type_bool_array(col1,col2) VALUES (1, ARRAY[TRUE, FALSE, TRUE, FALSE]);

--Testcase 69:
INSERT INTO type_byte_array(col1,col2) VALUES (1, ARRAY[-128, 0, 127]);

--Testcase 70:
INSERT INTO type_short_array(col1,col2) VALUES (1, ARRAY[100, 200, 300]);

--Testcase 71:
INSERT INTO type_integer_array(col1,col2) VALUES (1, ARRAY[1, 32768, 65537]);

--Testcase 72:
INSERT INTO type_long_array(col1,col2) VALUES (1, ARRAY[1, 2147483648, 4294967297]);

--Testcase 73:
INSERT INTO type_float_array(col1,col2) VALUES (1, ARRAY[3.14, 3.149, 3.1492]);

--Testcase 74:
INSERT INTO type_double_array(col1,col2) VALUES (1, ARRAY[3.14926, 3.149265, 3.1492653]);

--Testcase 75:
INSERT INTO type_timestamp_array(col1,col2) VALUES (1, ARRAY[to_timestamp('2017.11.06 12:34:56.789', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2017.11.07 12:34:56.789', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2017.11.08 12:34:56.789', 'YYYY.MM.DD HH24:MI:SS.MS')]);
-- INSERT INTO type_geometry(col1,col2) VALUES (1, '');

--Testcase 76:
SELECT * FROM type_string;

--Testcase 77:
SELECT * FROM type_boolean;

--Testcase 78:
SELECT * FROM type_byte;

--Testcase 79:
SELECT * FROM type_short;

--Testcase 80:
SELECT * FROM type_integer;

--Testcase 81:
SELECT * FROM type_long;

--Testcase 82:
SELECT * FROM type_float;

--Testcase 83:
SELECT * FROM type_double;

--Testcase 84:
SELECT * FROM type_timestamp;

--Testcase 85:
SELECT * FROM type_blob;

--Testcase 86:
SELECT * FROM type_string_array;

--Testcase 87:
SELECT * FROM type_bool_array;

--Testcase 88:
SELECT * FROM type_byte_array;

--Testcase 89:
SELECT * FROM type_short_array;

--Testcase 90:
SELECT * FROM type_integer_array;

--Testcase 91:
SELECT * FROM type_long_array;

--Testcase 92:
SELECT * FROM type_float_array;

--Testcase 93:
SELECT * FROM type_double_array;

--Testcase 94:
SELECT * FROM type_timestamp_array;
-- SELECT * FROM type_geometry;

-- test of inequality in where clause 

--Testcase 95:
SELECT * FROM type_string WHERE col2 < 'STRINGBA';

--Testcase 96:
SELECT * FROM type_string WHERE col2 >= 'STRINGAB';

-- function test

--Testcase 97:
SELECT * FROM type_string WHERE char_length(col1) > 8;

--Testcase 98:
SELECT * FROM type_string WHERE concat(col1,col2) = 'stringabSTRINGAB';

--Testcase 99:
SELECT * FROM type_string WHERE upper(col1) = 'STRINGAB';

--Testcase 100:
SELECT * FROM type_string WHERE lower(col2) = 'stringab';

--Testcase 101:
SELECT * FROM type_string WHERE substring(col1 from 5 for 3) = 'nga';

--Testcase 102:
SELECT * FROM type_float WHERE round(col2) = 3;

--Testcase 103:
SELECT * FROM type_double WHERE round(col2) = 3;

--Testcase 104:
SELECT * FROM type_float WHERE ceiling(col2) = 4;

--Testcase 105:
SELECT * FROM type_double WHERE ceiling(col2) = 4;

--Testcase 106:
SELECT * FROM type_float WHERE ceil(col2) = 4;

--Testcase 107:
SELECT * FROM type_double WHERE ceil(col2) = 4;

--Testcase 108:
SELECT * FROM type_float WHERE floor(col2) = 3;

--Testcase 109:
SELECT * FROM type_double WHERE floor(col2) = 3;

--Testcase 110:
SELECT * FROM type_timestamp WHERE col2 > now();

-- UPDATE test1 (Not rowkey column is updated)

--Testcase 111:
UPDATE type_string SET col2 = 'stringX' WHERE col1 = 'stringba';

--Testcase 112:
SELECT * FROM type_string;

--Testcase 113:
UPDATE type_string SET col2 = 'stringY' WHERE col1 = 'stringbaaa' OR col1 = 'stringab';

--Testcase 114:
SELECT * FROM type_string;

--Testcase 115:
UPDATE type_string SET col2 = col1 || 'Z' WHERE col1 = 'stringbaaa' OR col1 = 'stringaaa';

--Testcase 116:
SELECT * FROM type_string;

--Testcase 117:
UPDATE type_integer SET col2 = 100 WHERE col1 = 32769;

--Testcase 118:
SELECT * FROM type_integer;

--Testcase 119:
UPDATE type_integer SET col2 = 200 WHERE col1 = 32771 OR col1 = 32770;

--Testcase 120:
SELECT * FROM type_integer;

--Testcase 121:
UPDATE type_integer SET col2 = col1 + 100 WHERE col1 = 32768 OR col1 = 32770;

--Testcase 122:
SELECT * FROM type_integer;

--Testcase 123:
UPDATE type_long SET col2 = 123456789 WHERE col1 = 2147483652;

--Testcase 124:
SELECT * FROM type_long;

--Testcase 125:
UPDATE type_long SET col2 = 1000 WHERE col1 = 2147483650 OR col1 = 2147483649;

--Testcase 126:
SELECT * FROM type_long;

--Testcase 127:
UPDATE type_long SET col2 = col1 + 111 WHERE col1 = 2147483651 OR col1 = 2147483649;

--Testcase 128:
SELECT * FROM type_long;

--Testcase 129:
UPDATE type_timestamp SET col2 = to_timestamp('2010.08.01 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS') WHERE col1 = to_timestamp('2010.07.01 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS');

--Testcase 130:
SELECT * FROM type_timestamp;

--Testcase 131:
SELECT * FROM type_timestamp WHERE col1 = timestamp '2010-07-01 08:15:00.123';
-- Push donw timestamp as ISO format

--Testcase 132:
explain (verbose,costs off) SELECT * FROM type_timestamp WHERE col1 = timestamp '2010-07-01 08:15:00.123';

--Testcase 133:
UPDATE type_timestamp SET col2 = to_timestamp('2100.01.02 10:20:30.400', 'YYYY.MM.DD HH24:MI:SS.MS') WHERE col1 = to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS') OR col1 = to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS');

--Testcase 134:
SELECT * FROM type_timestamp;

--Testcase 135:
UPDATE type_timestamp SET col2 = (col1 + INTERVAL '1 DAY') WHERE col1 = to_timestamp('2010.07.02 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS') OR col1 = to_timestamp('2010.07.02 00:01:23.45', 'YYYY.MM.DD HH24:MI:SS.MS');

--Testcase 136:
SELECT * FROM type_timestamp;
-- Reset modified records

--Testcase 137:
DELETE FROM type_string;

--Testcase 138:
INSERT INTO type_string(col1,col2) VALUES ('stringbaaa', 'STRINGBAAA');

--Testcase 139:
INSERT INTO type_string(col1,col2) VALUES ('stringaaa', 'STRINGAAA');

--Testcase 140:
INSERT INTO type_string(col1,col2) VALUES ('stringab', 'STRINGAB');

--Testcase 141:
INSERT INTO type_string(col1,col2) VALUES ('stringba', 'STRINGBA');

--Testcase 142:
DELETE FROM type_integer;

--Testcase 143:
INSERT INTO type_integer(col1,col2) VALUES (32769, -32772);

--Testcase 144:
INSERT INTO type_integer(col1,col2) VALUES (32772, -32769);

--Testcase 145:
INSERT INTO type_integer(col1,col2) VALUES (32770, -32771);

--Testcase 146:
INSERT INTO type_integer(col1,col2) VALUES (32768, -32773);

--Testcase 147:
INSERT INTO type_integer(col1,col2) VALUES (32771, -32770);

--Testcase 148:
DELETE FROM type_long;

--Testcase 149:
INSERT INTO type_long(col1,col2) VALUES (2147483649, -2147483652);

--Testcase 150:
INSERT INTO type_long(col1,col2) VALUES (2147483652, -2147483649);

--Testcase 151:
INSERT INTO type_long(col1,col2) VALUES (2147483650, -2147483651);

--Testcase 152:
INSERT INTO type_long(col1,col2) VALUES (2147483648, -2147483653);

--Testcase 153:
INSERT INTO type_long(col1,col2) VALUES (2147483651, -2147483650);

--Testcase 154:
DELETE FROM type_timestamp;

--Testcase 155:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2200.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 156:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 157:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 158:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 159:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2199.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'));

-- UPDATE test2 (random order)

--Testcase 160:
SET ENABLE_HASHJOIN TO OFF;

--Testcase 161:
UPDATE type_string SET col2 = 'stringX' WHERE col1 IN (SELECT col1 FROM tbl_string);

--Testcase 162:
SELECT * FROM type_string;

--Testcase 163:
UPDATE type_string SET col2 = col1 || 'Z' WHERE col1 IN (SELECT col1 FROM tbl_string);

--Testcase 164:
SELECT * FROM type_string;

--Testcase 165:
UPDATE type_integer SET col2 = 100 WHERE col1 IN (SELECT col1 FROM tbl_integer);

--Testcase 166:
SELECT * FROM type_integer;

--Testcase 167:
UPDATE type_integer SET col2 = col1 + 100 WHERE col1 IN (SELECT col1 FROM tbl_integer);

--Testcase 168:
SELECT * FROM type_integer;

--Testcase 169:
UPDATE type_long SET col2 = 123456789 WHERE col1 IN (SELECT col1 FROM tbl_long);

--Testcase 170:
SELECT * FROM type_long;

--Testcase 171:
UPDATE type_long SET col2 = col1 + 111 WHERE col1 IN (SELECT col1 FROM tbl_long);

--Testcase 172:
SELECT * FROM type_long;

--Testcase 173:
UPDATE type_timestamp SET col2 = to_timestamp('2010.8.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS') WHERE col1 IN (SELECT col1 FROM tbl_timestamp);

--Testcase 174:
SELECT * FROM type_timestamp;

--Testcase 175:
UPDATE type_timestamp SET col2 = (col1 + INTERVAL '10 DAY') WHERE col1 IN (SELECT col1 FROM tbl_timestamp);

--Testcase 176:
SELECT * FROM type_timestamp;

--Testcase 177:
SET ENABLE_HASHJOIN TO ON;
-- Reset modified records

--Testcase 178:
DELETE FROM type_string;

--Testcase 179:
INSERT INTO type_string(col1,col2) VALUES ('stringbaaa', 'STRINGBAAA');

--Testcase 180:
INSERT INTO type_string(col1,col2) VALUES ('stringaaa', 'STRINGAAA');

--Testcase 181:
INSERT INTO type_string(col1,col2) VALUES ('stringab', 'STRINGAB');

--Testcase 182:
INSERT INTO type_string(col1,col2) VALUES ('stringba', 'STRINGBA');

--Testcase 183:
DELETE FROM type_integer;

--Testcase 184:
INSERT INTO type_integer(col1,col2) VALUES (32769, -32772);

--Testcase 185:
INSERT INTO type_integer(col1,col2) VALUES (32772, -32769);

--Testcase 186:
INSERT INTO type_integer(col1,col2) VALUES (32770, -32771);

--Testcase 187:
INSERT INTO type_integer(col1,col2) VALUES (32768, -32773);

--Testcase 188:
INSERT INTO type_integer(col1,col2) VALUES (32771, -32770);

--Testcase 189:
DELETE FROM type_long;

--Testcase 190:
INSERT INTO type_long(col1,col2) VALUES (2147483649, -2147483652);

--Testcase 191:
INSERT INTO type_long(col1,col2) VALUES (2147483652, -2147483649);

--Testcase 192:
INSERT INTO type_long(col1,col2) VALUES (2147483650, -2147483651);

--Testcase 193:
INSERT INTO type_long(col1,col2) VALUES (2147483648, -2147483653);

--Testcase 194:
INSERT INTO type_long(col1,col2) VALUES (2147483651, -2147483650);

--Testcase 195:
DELETE FROM type_timestamp;

--Testcase 196:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2200.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 197:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 198:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 199:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 200:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2199.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'));

-- UPDATE test3 (Update rowkey column -> error)

--Testcase 201:
UPDATE type_string SET col1 = 'stringX' WHERE col1 = 'stringba';

--Testcase 202:
SELECT * FROM type_string;

-- DELETE test1

--Testcase 203:
DELETE FROM type_string WHERE col1 = 'stringba';

--Testcase 204:
SELECT * FROM type_string;

--Testcase 205:
DELETE FROM type_string WHERE col1 = 'stringbaaa' OR col1 = 'stringab';

--Testcase 206:
SELECT * FROM type_string;

--Testcase 207:
DELETE FROM type_integer WHERE col1 = 32769;

--Testcase 208:
SELECT * FROM type_integer;

--Testcase 209:
DELETE FROM type_integer WHERE col1 = 32771 OR col1 = 32770;

--Testcase 210:
SELECT * FROM type_integer;

--Testcase 211:
DELETE FROM type_long WHERE col1 = 2147483652;

--Testcase 212:
SELECT * FROM type_long;

--Testcase 213:
DELETE FROM type_long WHERE col1 = 2147483650 OR col1 = 2147483648;

--Testcase 214:
SELECT * FROM type_long;

--Testcase 215:
DELETE FROM type_timestamp WHERE col1 = to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS');

--Testcase 216:
SELECT * FROM type_timestamp;

--Testcase 217:
DELETE FROM type_timestamp WHERE col1 = to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS') OR col1 = to_timestamp('2000.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS');

--Testcase 218:
SELECT * FROM type_timestamp;
-- Reset modified records

--Testcase 219:
DELETE FROM type_string;

--Testcase 220:
INSERT INTO type_string(col1,col2) VALUES ('stringbaaa', 'STRINGBAAA');

--Testcase 221:
INSERT INTO type_string(col1,col2) VALUES ('stringaaa', 'STRINGAAA');

--Testcase 222:
INSERT INTO type_string(col1,col2) VALUES ('stringab', 'STRINGAB');

--Testcase 223:
INSERT INTO type_string(col1,col2) VALUES ('stringba', 'STRINGBA');

--Testcase 224:
DELETE FROM type_integer;

--Testcase 225:
INSERT INTO type_integer(col1,col2) VALUES (32769, -32772);

--Testcase 226:
INSERT INTO type_integer(col1,col2) VALUES (32772, -32769);

--Testcase 227:
INSERT INTO type_integer(col1,col2) VALUES (32770, -32771);

--Testcase 228:
INSERT INTO type_integer(col1,col2) VALUES (32768, -32773);

--Testcase 229:
INSERT INTO type_integer(col1,col2) VALUES (32771, -32770);

--Testcase 230:
DELETE FROM type_long;

--Testcase 231:
INSERT INTO type_long(col1,col2) VALUES (2147483649, -2147483652);

--Testcase 232:
INSERT INTO type_long(col1,col2) VALUES (2147483652, -2147483649);

--Testcase 233:
INSERT INTO type_long(col1,col2) VALUES (2147483650, -2147483651);

--Testcase 234:
INSERT INTO type_long(col1,col2) VALUES (2147483648, -2147483653);

--Testcase 235:
INSERT INTO type_long(col1,col2) VALUES (2147483651, -2147483650);

--Testcase 236:
DELETE FROM type_timestamp;

--Testcase 237:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2200.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 238:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 239:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 240:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 241:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2199.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'));

-- DELETE test2 (random order)

--Testcase 242:
SET ENABLE_HASHJOIN TO OFF;

--Testcase 243:
DELETE FROM type_string WHERE col1 IN (SELECT col1 FROM tbl_string);

--Testcase 244:
SELECT * FROM type_string;

--Testcase 245:
DELETE FROM type_integer WHERE col1 IN (SELECT col1 FROM tbl_integer);

--Testcase 246:
SELECT * FROM type_integer;

--Testcase 247:
DELETE FROM type_long WHERE col1 IN (SELECT col1 FROM tbl_long);

--Testcase 248:
SELECT * FROM type_long;

--Testcase 249:
DELETE FROM type_timestamp WHERE col1 IN (SELECT col1 FROM tbl_timestamp);

--Testcase 250:
SELECT * FROM type_timestamp;

--Testcase 251:
SET ENABLE_HASHJOIN TO ON;

-- Clean up

--Testcase 252:
DELETE FROM type_string;

--Testcase 253:
DELETE FROM type_boolean;

--Testcase 254:
DELETE FROM type_byte;

--Testcase 255:
DELETE FROM type_short;

--Testcase 256:
DELETE FROM type_integer;

--Testcase 257:
DELETE FROM type_long;

--Testcase 258:
DELETE FROM type_float;

--Testcase 259:
DELETE FROM type_double;

--Testcase 260:
DELETE FROM type_timestamp;

--Testcase 261:
DELETE FROM type_blob;

--Testcase 262:
DELETE FROM type_string_array;

--Testcase 263:
DELETE FROM type_bool_array;

--Testcase 264:
DELETE FROM type_byte_array;

--Testcase 265:
DELETE FROM type_short_array;

--Testcase 266:
DELETE FROM type_integer_array;

--Testcase 267:
DELETE FROM type_long_array;

--Testcase 268:
DELETE FROM type_float_array;

--Testcase 269:
DELETE FROM type_double_array;

--Testcase 270:
DELETE FROM type_timestamp_array;
-- DELETE FROM type_geometry;

--Testcase 271:
DROP FOREIGN TABLE type_string;

--Testcase 272:
DROP FOREIGN TABLE type_boolean;

--Testcase 273:
DROP FOREIGN TABLE type_byte;

--Testcase 274:
DROP FOREIGN TABLE type_short;

--Testcase 275:
DROP FOREIGN TABLE type_integer;

--Testcase 276:
DROP FOREIGN TABLE type_long;

--Testcase 277:
DROP FOREIGN TABLE type_float;

--Testcase 278:
DROP FOREIGN TABLE type_double;

--Testcase 279:
DROP FOREIGN TABLE type_timestamp;

--Testcase 280:
DROP FOREIGN TABLE type_blob;

--Testcase 281:
DROP FOREIGN TABLE type_string_array;

--Testcase 282:
DROP FOREIGN TABLE type_bool_array;

--Testcase 283:
DROP FOREIGN TABLE type_byte_array;

--Testcase 284:
DROP FOREIGN TABLE type_short_array;

--Testcase 285:
DROP FOREIGN TABLE type_integer_array;

--Testcase 286:
DROP FOREIGN TABLE type_long_array;

--Testcase 287:
DROP FOREIGN TABLE type_float_array;

--Testcase 288:
DROP FOREIGN TABLE type_double_array;

--Testcase 289:
DROP FOREIGN TABLE type_timestamp_array;
-- DROP FOREIGN TABLE type_geometry;

--Testcase 290:
CREATE OR REPLACE FUNCTION drop_all_foreign_tables() RETURNS void AS $$
DECLARE
  tbl_name varchar;
  cmd varchar;
BEGIN
  FOR tbl_name IN SELECT foreign_table_name FROM information_schema._pg_foreign_tables LOOP
    cmd := 'DROP FOREIGN TABLE ' || quote_ident(tbl_name);

--Testcase 291:
    EXECUTE cmd;
  END LOOP;
  RETURN;
END
$$ LANGUAGE plpgsql;

--Testcase 292:
SELECT drop_all_foreign_tables();

--Testcase 293:
DROP USER MAPPING FOR public SERVER griddb_svr;

--Testcase 294:
DROP SERVER griddb_svr CASCADE;

--Testcase 295:
DROP EXTENSION griddb_fdw CASCADE;
