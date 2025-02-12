\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 261:
CREATE EXTENSION griddb_fdw;
--Testcase 262:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername :CLUSTER_NAME);
--Testcase 263:
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

--Testcase 264:
CREATE TABLE tbl_string(col1 text);
--Testcase 1:
INSERT INTO tbl_string VALUES('stringba');
--Testcase 2:
INSERT INTO tbl_string VALUES('stringaaa');
--Testcase 265:
CREATE TABLE tbl_integer(col1 integer);
--Testcase 3:
INSERT INTO tbl_integer VALUES(32771);
--Testcase 4:
INSERT INTO tbl_integer VALUES(32769);
--Testcase 266:
CREATE TABLE tbl_long(col1 bigint);
--Testcase 5:
INSERT INTO tbl_long VALUES(2147483651);
--Testcase 6:
INSERT INTO tbl_long VALUES(2147483648);
--Testcase 267:
CREATE TABLE tbl_timestamp(col1 timestamp);
--Testcase 7:
INSERT INTO tbl_timestamp VALUES(to_timestamp('2010.07.01 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 8:
INSERT INTO tbl_timestamp VALUES(to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));

--Testcase 9:
DELETE FROM type_string;
--Testcase 10:
DELETE FROM type_boolean;
--Testcase 11:
DELETE FROM type_byte;
--Testcase 12:
DELETE FROM type_short;
--Testcase 13:
DELETE FROM type_integer;
--Testcase 14:
DELETE FROM type_long;
--Testcase 15:
DELETE FROM type_float;
--Testcase 16:
DELETE FROM type_double;
--Testcase 17:
DELETE FROM type_timestamp;
--Testcase 18:
DELETE FROM type_blob;
--Testcase 19:
DELETE FROM type_string_array;
--Testcase 20:
DELETE FROM type_bool_array;
--Testcase 21:
DELETE FROM type_byte_array;
--Testcase 22:
DELETE FROM type_short_array;
--Testcase 23:
DELETE FROM type_integer_array;
--Testcase 24:
DELETE FROM type_long_array;
--Testcase 25:
DELETE FROM type_float_array;
--Testcase 26:
DELETE FROM type_double_array;
--Testcase 27:
DELETE FROM type_timestamp_array;
-- DELETE FROM type_geometry;

--Testcase 28:
INSERT INTO type_string(col1,col2) VALUES ('stringbaaa', 'STRINGBAAA');
--Testcase 29:
INSERT INTO type_string(col1,col2) VALUES ('stringaaa', 'STRINGAAA');
--Testcase 30:
INSERT INTO type_string(col1,col2) VALUES ('stringab', 'STRINGAB');
--Testcase 31:
INSERT INTO type_string(col1,col2) VALUES ('stringba', 'STRINGBA');
--Testcase 32:
INSERT INTO type_boolean(col1,col2) VALUES (1, TRUE);
--Testcase 33:
INSERT INTO type_boolean(col1,col2) VALUES (2, FALSE);
--Testcase 34:
INSERT INTO type_byte(col1,col2) VALUES (1, -128);
--Testcase 35:
INSERT INTO type_byte(col1,col2) VALUES (2, 127);
--Testcase 36:
INSERT INTO type_short(col1,col2) VALUES (1, 1);
--Testcase 37:
INSERT INTO type_short(col1,col2) VALUES (2, 2);
--Testcase 38:
INSERT INTO type_short(col1,col2) VALUES (3, 3);
--Testcase 39:
INSERT INTO type_short(col1,col2) VALUES (4, 4);
--Testcase 40:
INSERT INTO type_integer(col1,col2) VALUES (32769, -32772);
--Testcase 41:
INSERT INTO type_integer(col1,col2) VALUES (32772, -32769);
--Testcase 42:
INSERT INTO type_integer(col1,col2) VALUES (32770, -32771);
--Testcase 43:
INSERT INTO type_integer(col1,col2) VALUES (32768, -32773);
--Testcase 44:
INSERT INTO type_integer(col1,col2) VALUES (32771, -32770);
--Testcase 45:
INSERT INTO type_long(col1,col2) VALUES (2147483649, -2147483652);
--Testcase 46:
INSERT INTO type_long(col1,col2) VALUES (2147483652, -2147483649);
--Testcase 47:
INSERT INTO type_long(col1,col2) VALUES (2147483650, -2147483651);
--Testcase 48:
INSERT INTO type_long(col1,col2) VALUES (2147483648, -2147483653);
--Testcase 49:
INSERT INTO type_long(col1,col2) VALUES (2147483651, -2147483650);
--Testcase 50:
INSERT INTO type_float(col1,col2) VALUES (1, 1.58);
--Testcase 51:
INSERT INTO type_float(col1,col2) VALUES (2, 3.14);
--Testcase 52:
INSERT INTO type_double(col1,col2) VALUES (1, 3.14159265);
--Testcase 53:
INSERT INTO type_double(col1,col2) VALUES (2, 5.67890123);
--Testcase 54:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2000.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2200.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 55:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 56:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.45', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.45', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 57:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 58:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2199.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 59:
INSERT INTO type_blob(col1,col2) VALUES (1, bytea('\xDEADBEEF'));
--Testcase 60:
INSERT INTO type_string_array(col1,col2) VALUES (1, ARRAY['s1', 's2', 's3']);
--Testcase 61:
INSERT INTO type_bool_array(col1,col2) VALUES (1, ARRAY[TRUE, FALSE, TRUE, FALSE]);
--Testcase 62:
INSERT INTO type_byte_array(col1,col2) VALUES (1, ARRAY[-128, 0, 127]);
--Testcase 63:
INSERT INTO type_short_array(col1,col2) VALUES (1, ARRAY[100, 200, 300]);
--Testcase 64:
INSERT INTO type_integer_array(col1,col2) VALUES (1, ARRAY[1, 32768, 65537]);
--Testcase 65:
INSERT INTO type_long_array(col1,col2) VALUES (1, ARRAY[1, 2147483648, 4294967297]);
--Testcase 66:
INSERT INTO type_float_array(col1,col2) VALUES (1, ARRAY[3.14, 3.149, 3.1492]);
--Testcase 67:
INSERT INTO type_double_array(col1,col2) VALUES (1, ARRAY[3.14926, 3.149265, 3.1492653]);
--Testcase 68:
INSERT INTO type_timestamp_array(col1,col2) VALUES (1, ARRAY[to_timestamp('2017.11.06 12:34:56.789', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2017.11.07 12:34:56.789', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2017.11.08 12:34:56.789', 'YYYY.MM.DD HH24:MI:SS.MS')]);
-- INSERT INTO type_geometry(col1,col2) VALUES (1, '');

--Testcase 69:
SELECT * FROM type_string;
--Testcase 70:
SELECT * FROM type_boolean;
--Testcase 71:
SELECT * FROM type_byte;
--Testcase 72:
SELECT * FROM type_short;
--Testcase 73:
SELECT * FROM type_integer;
--Testcase 74:
SELECT * FROM type_long;
--Testcase 75:
SELECT * FROM type_float;
--Testcase 76:
SELECT * FROM type_double;
--Testcase 77:
SELECT * FROM type_timestamp;
--Testcase 78:
SELECT * FROM type_blob;
--Testcase 79:
SELECT * FROM type_string_array;
--Testcase 80:
SELECT * FROM type_bool_array;
--Testcase 81:
SELECT * FROM type_byte_array;
--Testcase 82:
SELECT * FROM type_short_array;
--Testcase 83:
SELECT * FROM type_integer_array;
--Testcase 84:
SELECT * FROM type_long_array;
--Testcase 85:
SELECT * FROM type_float_array;
--Testcase 86:
SELECT * FROM type_double_array;
--Testcase 87:
SELECT * FROM type_timestamp_array;
-- SELECT * FROM type_geometry;

-- test of inequality in where clause 
--Testcase 88:
SELECT * FROM type_string WHERE col2 < 'STRINGBA';
--Testcase 89:
SELECT * FROM type_string WHERE col2 >= 'STRINGAB';

-- function test
--Testcase 90:
SELECT * FROM type_string WHERE char_length(col1) > 8;
--Testcase 91:
SELECT * FROM type_string WHERE concat(col1,col2) = 'stringabSTRINGAB';
--Testcase 92:
SELECT * FROM type_string WHERE upper(col1) = 'STRINGAB';
--Testcase 93:
SELECT * FROM type_string WHERE lower(col2) = 'stringab';
--Testcase 94:
SELECT * FROM type_string WHERE substring(col1 from 5 for 3) = 'nga';
--Testcase 95:
SELECT * FROM type_float WHERE round(col2) = 3;
--Testcase 96:
SELECT * FROM type_double WHERE round(col2) = 3;
--Testcase 97:
SELECT * FROM type_float WHERE ceiling(col2) = 4;
--Testcase 98:
SELECT * FROM type_double WHERE ceiling(col2) = 4;
--Testcase 99:
SELECT * FROM type_float WHERE ceil(col2) = 4;
--Testcase 100:
SELECT * FROM type_double WHERE ceil(col2) = 4;
--Testcase 101:
SELECT * FROM type_float WHERE floor(col2) = 3;
--Testcase 102:
SELECT * FROM type_double WHERE floor(col2) = 3;
--Testcase 103:
SELECT * FROM type_timestamp WHERE col2 > now();

-- UPDATE test1 (Not rowkey column is updated)
--Testcase 104:
UPDATE type_string SET col2 = 'stringX' WHERE col1 = 'stringba';
--Testcase 105:
SELECT * FROM type_string;
--Testcase 106:
UPDATE type_string SET col2 = 'stringY' WHERE col1 = 'stringbaaa' OR col1 = 'stringab';
--Testcase 107:
SELECT * FROM type_string;
--Testcase 108:
UPDATE type_string SET col2 = col1 || 'Z' WHERE col1 = 'stringbaaa' OR col1 = 'stringaaa';
--Testcase 109:
SELECT * FROM type_string;
--Testcase 110:
UPDATE type_integer SET col2 = 100 WHERE col1 = 32769;
--Testcase 111:
SELECT * FROM type_integer;
--Testcase 112:
UPDATE type_integer SET col2 = 200 WHERE col1 = 32771 OR col1 = 32770;
--Testcase 113:
SELECT * FROM type_integer;
--Testcase 114:
UPDATE type_integer SET col2 = col1 + 100 WHERE col1 = 32768 OR col1 = 32770;
--Testcase 115:
SELECT * FROM type_integer;
--Testcase 116:
UPDATE type_long SET col2 = 123456789 WHERE col1 = 2147483652;
--Testcase 117:
SELECT * FROM type_long;
--Testcase 118:
UPDATE type_long SET col2 = 1000 WHERE col1 = 2147483650 OR col1 = 2147483649;
--Testcase 119:
SELECT * FROM type_long;
--Testcase 120:
UPDATE type_long SET col2 = col1 + 111 WHERE col1 = 2147483651 OR col1 = 2147483649;
--Testcase 121:
SELECT * FROM type_long;
--Testcase 122:
UPDATE type_timestamp SET col2 = to_timestamp('2010.08.01 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS') WHERE col1 = to_timestamp('2010.07.01 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS');
--Testcase 123:
SELECT * FROM type_timestamp;
--Testcase 124:
SELECT * FROM type_timestamp WHERE col1 = timestamp '2010-07-01 08:15:00.123';
-- Push donw timestamp as ISO format
--Testcase 125:
explain (verbose,costs off) SELECT * FROM type_timestamp WHERE col1 = timestamp '2010-07-01 08:15:00.123';
--Testcase 126:
UPDATE type_timestamp SET col2 = to_timestamp('2100.01.02 10:20:30.400', 'YYYY.MM.DD HH24:MI:SS.MS') WHERE col1 = to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS') OR col1 = to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS');
--Testcase 127:
SELECT * FROM type_timestamp;
--Testcase 128:
UPDATE type_timestamp SET col2 = (col1 + INTERVAL '1 DAY') WHERE col1 = to_timestamp('2010.07.02 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS') OR col1 = to_timestamp('2010.07.02 00:01:23.45', 'YYYY.MM.DD HH24:MI:SS.MS');
--Testcase 129:
SELECT * FROM type_timestamp;
-- Reset modified records
--Testcase 130:
DELETE FROM type_string;
--Testcase 131:
INSERT INTO type_string(col1,col2) VALUES ('stringbaaa', 'STRINGBAAA');
--Testcase 132:
INSERT INTO type_string(col1,col2) VALUES ('stringaaa', 'STRINGAAA');
--Testcase 133:
INSERT INTO type_string(col1,col2) VALUES ('stringab', 'STRINGAB');
--Testcase 134:
INSERT INTO type_string(col1,col2) VALUES ('stringba', 'STRINGBA');
--Testcase 135:
DELETE FROM type_integer;
--Testcase 136:
INSERT INTO type_integer(col1,col2) VALUES (32769, -32772);
--Testcase 137:
INSERT INTO type_integer(col1,col2) VALUES (32772, -32769);
--Testcase 138:
INSERT INTO type_integer(col1,col2) VALUES (32770, -32771);
--Testcase 139:
INSERT INTO type_integer(col1,col2) VALUES (32768, -32773);
--Testcase 140:
INSERT INTO type_integer(col1,col2) VALUES (32771, -32770);
--Testcase 141:
DELETE FROM type_long;
--Testcase 142:
INSERT INTO type_long(col1,col2) VALUES (2147483649, -2147483652);
--Testcase 143:
INSERT INTO type_long(col1,col2) VALUES (2147483652, -2147483649);
--Testcase 144:
INSERT INTO type_long(col1,col2) VALUES (2147483650, -2147483651);
--Testcase 145:
INSERT INTO type_long(col1,col2) VALUES (2147483648, -2147483653);
--Testcase 146:
INSERT INTO type_long(col1,col2) VALUES (2147483651, -2147483650);
--Testcase 147:
DELETE FROM type_timestamp;
--Testcase 148:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2200.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 149:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 150:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 151:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 152:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2199.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'));

-- UPDATE test2 (random order)
--Testcase 292:
SET ENABLE_HASHJOIN TO OFF;
--Testcase 153:
UPDATE type_string SET col2 = 'stringX' WHERE col1 IN (SELECT col1 FROM tbl_string);
--Testcase 154:
SELECT * FROM type_string;
--Testcase 155:
UPDATE type_string SET col2 = col1 || 'Z' WHERE col1 IN (SELECT col1 FROM tbl_string);
--Testcase 156:
SELECT * FROM type_string;
--Testcase 157:
UPDATE type_integer SET col2 = 100 WHERE col1 IN (SELECT col1 FROM tbl_integer);
--Testcase 158:
SELECT * FROM type_integer;
--Testcase 159:
UPDATE type_integer SET col2 = col1 + 100 WHERE col1 IN (SELECT col1 FROM tbl_integer);
--Testcase 160:
SELECT * FROM type_integer;
--Testcase 161:
UPDATE type_long SET col2 = 123456789 WHERE col1 IN (SELECT col1 FROM tbl_long);
--Testcase 162:
SELECT * FROM type_long;
--Testcase 163:
UPDATE type_long SET col2 = col1 + 111 WHERE col1 IN (SELECT col1 FROM tbl_long);
--Testcase 164:
SELECT * FROM type_long;
--Testcase 165:
UPDATE type_timestamp SET col2 = to_timestamp('2010.8.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS') WHERE col1 IN (SELECT col1 FROM tbl_timestamp);
--Testcase 166:
SELECT * FROM type_timestamp;
--Testcase 167:
UPDATE type_timestamp SET col2 = (col1 + INTERVAL '10 DAY') WHERE col1 IN (SELECT col1 FROM tbl_timestamp);
--Testcase 168:
SELECT * FROM type_timestamp;
--Testcase 293:
SET ENABLE_HASHJOIN TO ON;
-- Reset modified records
--Testcase 169:
DELETE FROM type_string;
--Testcase 170:
INSERT INTO type_string(col1,col2) VALUES ('stringbaaa', 'STRINGBAAA');
--Testcase 171:
INSERT INTO type_string(col1,col2) VALUES ('stringaaa', 'STRINGAAA');
--Testcase 172:
INSERT INTO type_string(col1,col2) VALUES ('stringab', 'STRINGAB');
--Testcase 173:
INSERT INTO type_string(col1,col2) VALUES ('stringba', 'STRINGBA');
--Testcase 174:
DELETE FROM type_integer;
--Testcase 175:
INSERT INTO type_integer(col1,col2) VALUES (32769, -32772);
--Testcase 176:
INSERT INTO type_integer(col1,col2) VALUES (32772, -32769);
--Testcase 177:
INSERT INTO type_integer(col1,col2) VALUES (32770, -32771);
--Testcase 178:
INSERT INTO type_integer(col1,col2) VALUES (32768, -32773);
--Testcase 179:
INSERT INTO type_integer(col1,col2) VALUES (32771, -32770);
--Testcase 180:
DELETE FROM type_long;
--Testcase 181:
INSERT INTO type_long(col1,col2) VALUES (2147483649, -2147483652);
--Testcase 182:
INSERT INTO type_long(col1,col2) VALUES (2147483652, -2147483649);
--Testcase 183:
INSERT INTO type_long(col1,col2) VALUES (2147483650, -2147483651);
--Testcase 184:
INSERT INTO type_long(col1,col2) VALUES (2147483648, -2147483653);
--Testcase 185:
INSERT INTO type_long(col1,col2) VALUES (2147483651, -2147483650);
--Testcase 186:
DELETE FROM type_timestamp;
--Testcase 187:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2200.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 188:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 189:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 190:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 191:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2199.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'));

-- UPDATE test3 (Update rowkey column -> error)
--Testcase 192:
UPDATE type_string SET col1 = 'stringX' WHERE col1 = 'stringba';
--Testcase 193:
SELECT * FROM type_string;

-- DELETE test1
--Testcase 194:
DELETE FROM type_string WHERE col1 = 'stringba';
--Testcase 195:
SELECT * FROM type_string;
--Testcase 196:
DELETE FROM type_string WHERE col1 = 'stringbaaa' OR col1 = 'stringab';
--Testcase 197:
SELECT * FROM type_string;
--Testcase 198:
DELETE FROM type_integer WHERE col1 = 32769;
--Testcase 199:
SELECT * FROM type_integer;
--Testcase 200:
DELETE FROM type_integer WHERE col1 = 32771 OR col1 = 32770;
--Testcase 201:
SELECT * FROM type_integer;
--Testcase 202:
DELETE FROM type_long WHERE col1 = 2147483652;
--Testcase 203:
SELECT * FROM type_long;
--Testcase 204:
DELETE FROM type_long WHERE col1 = 2147483650 OR col1 = 2147483648;
--Testcase 205:
SELECT * FROM type_long;
--Testcase 206:
DELETE FROM type_timestamp WHERE col1 = to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS');
--Testcase 207:
SELECT * FROM type_timestamp;
--Testcase 208:
DELETE FROM type_timestamp WHERE col1 = to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS') OR col1 = to_timestamp('2000.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS');
--Testcase 209:
SELECT * FROM type_timestamp;
-- Reset modified records
--Testcase 210:
DELETE FROM type_string;
--Testcase 211:
INSERT INTO type_string(col1,col2) VALUES ('stringbaaa', 'STRINGBAAA');
--Testcase 212:
INSERT INTO type_string(col1,col2) VALUES ('stringaaa', 'STRINGAAA');
--Testcase 213:
INSERT INTO type_string(col1,col2) VALUES ('stringab', 'STRINGAB');
--Testcase 214:
INSERT INTO type_string(col1,col2) VALUES ('stringba', 'STRINGBA');
--Testcase 215:
DELETE FROM type_integer;
--Testcase 216:
INSERT INTO type_integer(col1,col2) VALUES (32769, -32772);
--Testcase 217:
INSERT INTO type_integer(col1,col2) VALUES (32772, -32769);
--Testcase 218:
INSERT INTO type_integer(col1,col2) VALUES (32770, -32771);
--Testcase 219:
INSERT INTO type_integer(col1,col2) VALUES (32768, -32773);
--Testcase 220:
INSERT INTO type_integer(col1,col2) VALUES (32771, -32770);
--Testcase 221:
DELETE FROM type_long;
--Testcase 222:
INSERT INTO type_long(col1,col2) VALUES (2147483649, -2147483652);
--Testcase 223:
INSERT INTO type_long(col1,col2) VALUES (2147483652, -2147483649);
--Testcase 224:
INSERT INTO type_long(col1,col2) VALUES (2147483650, -2147483651);
--Testcase 225:
INSERT INTO type_long(col1,col2) VALUES (2147483648, -2147483653);
--Testcase 226:
INSERT INTO type_long(col1,col2) VALUES (2147483651, -2147483650);
--Testcase 227:
DELETE FROM type_timestamp;
--Testcase 228:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2200.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 229:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 230:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 231:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));
--Testcase 232:
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2199.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'));

-- DELETE test2 (random order)
--Testcase 294:
SET ENABLE_HASHJOIN TO OFF;
--Testcase 233:
DELETE FROM type_string WHERE col1 IN (SELECT col1 FROM tbl_string);
--Testcase 234:
SELECT * FROM type_string;
--Testcase 235:
DELETE FROM type_integer WHERE col1 IN (SELECT col1 FROM tbl_integer);
--Testcase 236:
SELECT * FROM type_integer;
--Testcase 237:
DELETE FROM type_long WHERE col1 IN (SELECT col1 FROM tbl_long);
--Testcase 238:
SELECT * FROM type_long;
--Testcase 239:
DELETE FROM type_timestamp WHERE col1 IN (SELECT col1 FROM tbl_timestamp);
--Testcase 240:
SELECT * FROM type_timestamp;
--Testcase 295:
SET ENABLE_HASHJOIN TO ON;

-- Clean up
--Testcase 241:
DELETE FROM type_string;
--Testcase 242:
DELETE FROM type_boolean;
--Testcase 243:
DELETE FROM type_byte;
--Testcase 244:
DELETE FROM type_short;
--Testcase 245:
DELETE FROM type_integer;
--Testcase 246:
DELETE FROM type_long;
--Testcase 247:
DELETE FROM type_float;
--Testcase 248:
DELETE FROM type_double;
--Testcase 249:
DELETE FROM type_timestamp;
--Testcase 250:
DELETE FROM type_blob;
--Testcase 251:
DELETE FROM type_string_array;
--Testcase 252:
DELETE FROM type_bool_array;
--Testcase 253:
DELETE FROM type_byte_array;
--Testcase 254:
DELETE FROM type_short_array;
--Testcase 255:
DELETE FROM type_integer_array;
--Testcase 256:
DELETE FROM type_long_array;
--Testcase 257:
DELETE FROM type_float_array;
--Testcase 258:
DELETE FROM type_double_array;
--Testcase 259:
DELETE FROM type_timestamp_array;
-- DELETE FROM type_geometry;

--Testcase 268:
DROP FOREIGN TABLE type_string;
--Testcase 269:
DROP FOREIGN TABLE type_boolean;
--Testcase 270:
DROP FOREIGN TABLE type_byte;
--Testcase 271:
DROP FOREIGN TABLE type_short;
--Testcase 272:
DROP FOREIGN TABLE type_integer;
--Testcase 273:
DROP FOREIGN TABLE type_long;
--Testcase 274:
DROP FOREIGN TABLE type_float;
--Testcase 275:
DROP FOREIGN TABLE type_double;
--Testcase 276:
DROP FOREIGN TABLE type_timestamp;
--Testcase 277:
DROP FOREIGN TABLE type_blob;
--Testcase 278:
DROP FOREIGN TABLE type_string_array;
--Testcase 279:
DROP FOREIGN TABLE type_bool_array;
--Testcase 280:
DROP FOREIGN TABLE type_byte_array;
--Testcase 281:
DROP FOREIGN TABLE type_short_array;
--Testcase 282:
DROP FOREIGN TABLE type_integer_array;
--Testcase 283:
DROP FOREIGN TABLE type_long_array;
--Testcase 284:
DROP FOREIGN TABLE type_float_array;
--Testcase 285:
DROP FOREIGN TABLE type_double_array;
--Testcase 286:
DROP FOREIGN TABLE type_timestamp_array;
-- DROP FOREIGN TABLE type_geometry;

--Testcase 287:
CREATE OR REPLACE FUNCTION drop_all_foreign_tables() RETURNS void AS $$
DECLARE
  tbl_name varchar;
  cmd varchar;
BEGIN
  FOR tbl_name IN SELECT foreign_table_name FROM information_schema._pg_foreign_tables LOOP
    cmd := 'DROP FOREIGN TABLE ' || quote_ident(tbl_name);
--Testcase 288:
    EXECUTE cmd;
  END LOOP;
  RETURN;
END
$$ LANGUAGE plpgsql;
--Testcase 260:
SELECT drop_all_foreign_tables();

--Testcase 289:
DROP USER MAPPING FOR public SERVER griddb_svr;
--Testcase 290:
DROP SERVER griddb_svr CASCADE;
--Testcase 291:
DROP EXTENSION griddb_fdw CASCADE;
