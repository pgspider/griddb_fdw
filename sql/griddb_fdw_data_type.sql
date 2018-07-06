CREATE EXTENSION griddb_fdw;
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS(host '239.0.0.1', port '31999', clustername 'ktymCluster');
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS(username 'admin', password 'testadmin');

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

CREATE TABLE tbl_string(col1 text);
INSERT INTO tbl_string VALUES('stringba');
INSERT INTO tbl_string VALUES('stringaaa');
CREATE TABLE tbl_integer(col1 integer);
INSERT INTO tbl_integer VALUES(32771);
INSERT INTO tbl_integer VALUES(32769);
CREATE TABLE tbl_long(col1 bigint);
INSERT INTO tbl_long VALUES(2147483651);
INSERT INTO tbl_long VALUES(2147483648);
CREATE TABLE tbl_timestamp(col1 timestamp);
INSERT INTO tbl_timestamp VALUES(to_timestamp('2010.07.01 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO tbl_timestamp VALUES(to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));

DELETE FROM type_string;
DELETE FROM type_boolean;
DELETE FROM type_byte;
DELETE FROM type_short;
DELETE FROM type_integer;
DELETE FROM type_long;
DELETE FROM type_float;
DELETE FROM type_double;
DELETE FROM type_timestamp;
DELETE FROM type_blob;
DELETE FROM type_string_array;
DELETE FROM type_bool_array;
DELETE FROM type_byte_array;
DELETE FROM type_short_array;
DELETE FROM type_integer_array;
DELETE FROM type_long_array;
DELETE FROM type_float_array;
DELETE FROM type_double_array;
DELETE FROM type_timestamp_array;
-- DELETE FROM type_geometry;

INSERT INTO type_string(col1,col2) VALUES ('stringbaaa', 'STRINGBAAA');
INSERT INTO type_string(col1,col2) VALUES ('stringaaa', 'STRINGAAA');
INSERT INTO type_string(col1,col2) VALUES ('stringab', 'STRINGAB');
INSERT INTO type_string(col1,col2) VALUES ('stringba', 'STRINGBA');
INSERT INTO type_boolean(col1,col2) VALUES (1, TRUE);
INSERT INTO type_boolean(col1,col2) VALUES (2, FALSE);
INSERT INTO type_byte(col1,col2) VALUES (1, 'g');
INSERT INTO type_byte(col1,col2) VALUES (2, 's');
INSERT INTO type_short(col1,col2) VALUES (1, 1);
INSERT INTO type_short(col1,col2) VALUES (2, 2);
INSERT INTO type_short(col1,col2) VALUES (3, 3);
INSERT INTO type_short(col1,col2) VALUES (4, 4);
INSERT INTO type_integer(col1,col2) VALUES (32769, -32772);
INSERT INTO type_integer(col1,col2) VALUES (32772, -32769);
INSERT INTO type_integer(col1,col2) VALUES (32770, -32771);
INSERT INTO type_integer(col1,col2) VALUES (32768, -32773);
INSERT INTO type_integer(col1,col2) VALUES (32771, -32770);
INSERT INTO type_long(col1,col2) VALUES (2147483649, -2147483652);
INSERT INTO type_long(col1,col2) VALUES (2147483652, -2147483649);
INSERT INTO type_long(col1,col2) VALUES (2147483650, -2147483651);
INSERT INTO type_long(col1,col2) VALUES (2147483648, -2147483653);
INSERT INTO type_long(col1,col2) VALUES (2147483651, -2147483650);
INSERT INTO type_float(col1,col2) VALUES (1, 1.58);
INSERT INTO type_float(col1,col2) VALUES (2, 3.14);
INSERT INTO type_double(col1,col2) VALUES (1, 3.14159265);
INSERT INTO type_double(col1,col2) VALUES (2, 5.67890123);
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2000.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2200.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.45', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.45', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2199.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_blob(col1,col2) VALUES (1, bytea('\xDEADBEEF'));
INSERT INTO type_string_array(col1,col2) VALUES (1, ARRAY['s1', 's2', 's3']);
INSERT INTO type_bool_array(col1,col2) VALUES (1, ARRAY[TRUE, FALSE, TRUE, FALSE]);
INSERT INTO type_byte_array(col1,col2) VALUES (1, ARRAY['a', 'b', 'c']);
INSERT INTO type_short_array(col1,col2) VALUES (1, ARRAY[100, 200, 300]);
INSERT INTO type_integer_array(col1,col2) VALUES (1, ARRAY[1, 32768, 65537]);
INSERT INTO type_long_array(col1,col2) VALUES (1, ARRAY[1, 2147483648, 4294967297]);
INSERT INTO type_float_array(col1,col2) VALUES (1, ARRAY[3.14, 3.149, 3.1492]);
INSERT INTO type_double_array(col1,col2) VALUES (1, ARRAY[3.14926, 3.149265, 3.1492653]);
INSERT INTO type_timestamp_array(col1,col2) VALUES (1, ARRAY[to_timestamp('2017.11.06 12:34:56.789', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2017.11.07 12:34:56.789', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2017.11.08 12:34:56.789', 'YYYY.MM.DD HH24:MI:SS.MS')]);
-- INSERT INTO type_geometry(col1,col2) VALUES (1, '');

SELECT * FROM type_string;
SELECT * FROM type_boolean;
SELECT * FROM type_byte;
SELECT * FROM type_short;
SELECT * FROM type_integer;
SELECT * FROM type_long;
SELECT * FROM type_float;
SELECT * FROM type_double;
SELECT * FROM type_timestamp;
SELECT * FROM type_blob;
SELECT * FROM type_string_array;
SELECT * FROM type_bool_array;
SELECT * FROM type_byte_array;
SELECT * FROM type_short_array;
SELECT * FROM type_integer_array;
SELECT * FROM type_long_array;
SELECT * FROM type_float_array;
SELECT * FROM type_double_array;
SELECT * FROM type_timestamp_array;
-- SELECT * FROM type_geometry;

-- function test
SELECT * FROM type_string WHERE char_length(col1) > 8;
SELECT * FROM type_string WHERE concat(col1,col2) = 'stringabSTRINGAB';
SELECT * FROM type_string WHERE upper(col1) = 'STRINGAB';
SELECT * FROM type_string WHERE lower(col2) = 'stringab';
SELECT * FROM type_string WHERE substring(col1 from 5 for 3) = 'nga';
SELECT * FROM type_float WHERE round(col2) = 3;
SELECT * FROM type_double WHERE round(col2) = 3;
SELECT * FROM type_float WHERE ceiling(col2) = 4;
SELECT * FROM type_double WHERE ceiling(col2) = 4;
SELECT * FROM type_float WHERE ceil(col2) = 4;
SELECT * FROM type_double WHERE ceil(col2) = 4;
SELECT * FROM type_float WHERE floor(col2) = 3;
SELECT * FROM type_double WHERE floor(col2) = 3;
SELECT * FROM type_timestamp WHERE col2 > now();

-- UPDATE test1 (Not rowkey column is updated)
UPDATE type_string SET col2 = 'stringX' WHERE col1 = 'stringba';
SELECT * FROM type_string;
UPDATE type_string SET col2 = 'stringY' WHERE col1 = 'stringbaaa' OR col1 = 'stringab';
SELECT * FROM type_string;
UPDATE type_string SET col2 = col1 || 'Z' WHERE col1 = 'stringbaaa' OR col1 = 'stringaaa';
SELECT * FROM type_string;
UPDATE type_integer SET col2 = 100 WHERE col1 = 32769;
SELECT * FROM type_integer;
UPDATE type_integer SET col2 = 200 WHERE col1 = 32771 OR col1 = 32770;
SELECT * FROM type_integer;
UPDATE type_integer SET col2 = col1 + 100 WHERE col1 = 32768 OR col1 = 32770;
SELECT * FROM type_integer;
UPDATE type_long SET col2 = 123456789 WHERE col1 = 2147483652;
SELECT * FROM type_long;
UPDATE type_long SET col2 = 1000 WHERE col1 = 2147483650 OR col1 = 2147483649;
SELECT * FROM type_long;
UPDATE type_long SET col2 = col1 + 111 WHERE col1 = 2147483651 OR col1 = 2147483649;
SELECT * FROM type_long;
UPDATE type_timestamp SET col2 = to_timestamp('2010.08.01 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS') WHERE col1 = to_timestamp('2010.07.01 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS');
SELECT * FROM type_timestamp;
UPDATE type_timestamp SET col2 = to_timestamp('2100.01.02 10:20:30.400', 'YYYY.MM.DD HH24:MI:SS.MS') WHERE col1 = to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS') OR col1 = to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS');
SELECT * FROM type_timestamp;
UPDATE type_timestamp SET col2 = (col1 + INTERVAL '1 DAY') WHERE col1 = to_timestamp('2010.07.02 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS') OR col1 = to_timestamp('2010.07.02 00:01:23.45', 'YYYY.MM.DD HH24:MI:SS.MS');
SELECT * FROM type_timestamp;
-- Reset modified records
DELETE FROM type_string;
INSERT INTO type_string(col1,col2) VALUES ('stringbaaa', 'STRINGBAAA');
INSERT INTO type_string(col1,col2) VALUES ('stringaaa', 'STRINGAAA');
INSERT INTO type_string(col1,col2) VALUES ('stringab', 'STRINGAB');
INSERT INTO type_string(col1,col2) VALUES ('stringba', 'STRINGBA');
DELETE FROM type_integer;
INSERT INTO type_integer(col1,col2) VALUES (32769, -32772);
INSERT INTO type_integer(col1,col2) VALUES (32772, -32769);
INSERT INTO type_integer(col1,col2) VALUES (32770, -32771);
INSERT INTO type_integer(col1,col2) VALUES (32768, -32773);
INSERT INTO type_integer(col1,col2) VALUES (32771, -32770);
DELETE FROM type_long;
INSERT INTO type_long(col1,col2) VALUES (2147483649, -2147483652);
INSERT INTO type_long(col1,col2) VALUES (2147483652, -2147483649);
INSERT INTO type_long(col1,col2) VALUES (2147483650, -2147483651);
INSERT INTO type_long(col1,col2) VALUES (2147483648, -2147483653);
INSERT INTO type_long(col1,col2) VALUES (2147483651, -2147483650);
DELETE FROM type_timestamp;
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2200.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2199.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'));

-- UPDATE test2 (random order)
SET ENABLE_HASHJOIN TO OFF;
UPDATE type_string SET col2 = 'stringX' WHERE col1 IN (SELECT col1 FROM tbl_string);
SELECT * FROM type_string;
UPDATE type_string SET col2 = col1 || 'Z' WHERE col1 IN (SELECT col1 FROM tbl_string);
SELECT * FROM type_string;
UPDATE type_integer SET col2 = 100 WHERE col1 IN (SELECT col1 FROM tbl_integer);
SELECT * FROM type_integer;
UPDATE type_integer SET col2 = col1 + 100 WHERE col1 IN (SELECT col1 FROM tbl_integer);
SELECT * FROM type_integer;
UPDATE type_long SET col2 = 123456789 WHERE col1 IN (SELECT col1 FROM tbl_long);
SELECT * FROM type_long;
UPDATE type_long SET col2 = col1 + 111 WHERE col1 IN (SELECT col1 FROM tbl_long);
SELECT * FROM type_long;
UPDATE type_timestamp SET col2 = to_timestamp('2010.8.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS') WHERE col1 IN (SELECT col1 FROM tbl_timestamp);
SELECT * FROM type_timestamp;
UPDATE type_timestamp SET col2 = (col1 + INTERVAL '10 DAY') WHERE col1 IN (SELECT col1 FROM tbl_timestamp);
SELECT * FROM type_timestamp;
SET ENABLE_HASHJOIN TO ON;
-- Reset modified records
DELETE FROM type_string;
INSERT INTO type_string(col1,col2) VALUES ('stringbaaa', 'STRINGBAAA');
INSERT INTO type_string(col1,col2) VALUES ('stringaaa', 'STRINGAAA');
INSERT INTO type_string(col1,col2) VALUES ('stringab', 'STRINGAB');
INSERT INTO type_string(col1,col2) VALUES ('stringba', 'STRINGBA');
DELETE FROM type_integer;
INSERT INTO type_integer(col1,col2) VALUES (32769, -32772);
INSERT INTO type_integer(col1,col2) VALUES (32772, -32769);
INSERT INTO type_integer(col1,col2) VALUES (32770, -32771);
INSERT INTO type_integer(col1,col2) VALUES (32768, -32773);
INSERT INTO type_integer(col1,col2) VALUES (32771, -32770);
DELETE FROM type_long;
INSERT INTO type_long(col1,col2) VALUES (2147483649, -2147483652);
INSERT INTO type_long(col1,col2) VALUES (2147483652, -2147483649);
INSERT INTO type_long(col1,col2) VALUES (2147483650, -2147483651);
INSERT INTO type_long(col1,col2) VALUES (2147483648, -2147483653);
INSERT INTO type_long(col1,col2) VALUES (2147483651, -2147483650);
DELETE FROM type_timestamp;
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2200.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2199.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'));

-- UPDATE test3 (Update rowkey column -> error)
UPDATE type_string SET col1 = 'stringX' WHERE col1 = 'stringba';
SELECT * FROM type_string;

-- DELETE test1
DELETE FROM type_string WHERE col1 = 'stringba';
SELECT * FROM type_string;
DELETE FROM type_string WHERE col1 = 'stringbaaa' OR col1 = 'stringab';
SELECT * FROM type_string;
DELETE FROM type_integer WHERE col1 = 32769;
SELECT * FROM type_integer;
DELETE FROM type_integer WHERE col1 = 32771 OR col1 = 32770;
SELECT * FROM type_integer;
DELETE FROM type_long WHERE col1 = 2147483652;
SELECT * FROM type_long;
DELETE FROM type_long WHERE col1 = 2147483650 OR col1 = 2147483648;
SELECT * FROM type_long;
DELETE FROM type_timestamp WHERE col1 = to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS');
SELECT * FROM type_timestamp;
DELETE FROM type_timestamp WHERE col1 = to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS') OR col1 = to_timestamp('2000.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS');
SELECT * FROM type_timestamp;
-- Reset modified records
DELETE FROM type_string;
INSERT INTO type_string(col1,col2) VALUES ('stringbaaa', 'STRINGBAAA');
INSERT INTO type_string(col1,col2) VALUES ('stringaaa', 'STRINGAAA');
INSERT INTO type_string(col1,col2) VALUES ('stringab', 'STRINGAB');
INSERT INTO type_string(col1,col2) VALUES ('stringba', 'STRINGBA');
DELETE FROM type_integer;
INSERT INTO type_integer(col1,col2) VALUES (32769, -32772);
INSERT INTO type_integer(col1,col2) VALUES (32772, -32769);
INSERT INTO type_integer(col1,col2) VALUES (32770, -32771);
INSERT INTO type_integer(col1,col2) VALUES (32768, -32773);
INSERT INTO type_integer(col1,col2) VALUES (32771, -32770);
DELETE FROM type_long;
INSERT INTO type_long(col1,col2) VALUES (2147483649, -2147483652);
INSERT INTO type_long(col1,col2) VALUES (2147483652, -2147483649);
INSERT INTO type_long(col1,col2) VALUES (2147483650, -2147483651);
INSERT INTO type_long(col1,col2) VALUES (2147483648, -2147483653);
INSERT INTO type_long(col1,col2) VALUES (2147483651, -2147483650);
DELETE FROM type_timestamp;
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2000.01.01 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2200.1.1 00:00:00.000', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.456', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.2 00:01:23.450', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('2010.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2210.7.1 08:15:00.123', 'YYYY.MM.DD HH24:MI:SS.MS'));
INSERT INTO type_timestamp(col1,col2) VALUES (to_timestamp('1999.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'), to_timestamp('2199.12.31 23:59:59.999', 'YYYY.MM.DD HH24:MI:SS.MS'));

-- DELETE test2 (random order)
SET ENABLE_HASHJOIN TO OFF;
DELETE FROM type_string WHERE col1 IN (SELECT col1 FROM tbl_string);
SELECT * FROM type_string;
DELETE FROM type_integer WHERE col1 IN (SELECT col1 FROM tbl_integer);
SELECT * FROM type_integer;
DELETE FROM type_long WHERE col1 IN (SELECT col1 FROM tbl_long);
SELECT * FROM type_long;
DELETE FROM type_timestamp WHERE col1 IN (SELECT col1 FROM tbl_timestamp);
SELECT * FROM type_timestamp;
SET ENABLE_HASHJOIN TO ON;

-- Clean up
DELETE FROM type_string;
DELETE FROM type_boolean;
DELETE FROM type_byte;
DELETE FROM type_short;
DELETE FROM type_integer;
DELETE FROM type_long;
DELETE FROM type_float;
DELETE FROM type_double;
DELETE FROM type_timestamp;
DELETE FROM type_blob;
DELETE FROM type_string_array;
DELETE FROM type_bool_array;
DELETE FROM type_byte_array;
DELETE FROM type_short_array;
DELETE FROM type_integer_array;
DELETE FROM type_long_array;
DELETE FROM type_float_array;
DELETE FROM type_double_array;
DELETE FROM type_timestamp_array;
-- DELETE FROM type_geometry;

DROP FOREIGN TABLE type_string;
DROP FOREIGN TABLE type_boolean;
DROP FOREIGN TABLE type_byte;
DROP FOREIGN TABLE type_short;
DROP FOREIGN TABLE type_integer;
DROP FOREIGN TABLE type_long;
DROP FOREIGN TABLE type_float;
DROP FOREIGN TABLE type_double;
DROP FOREIGN TABLE type_timestamp;
DROP FOREIGN TABLE type_blob;
DROP FOREIGN TABLE type_string_array;
DROP FOREIGN TABLE type_bool_array;
DROP FOREIGN TABLE type_byte_array;
DROP FOREIGN TABLE type_short_array;
DROP FOREIGN TABLE type_integer_array;
DROP FOREIGN TABLE type_long_array;
DROP FOREIGN TABLE type_float_array;
DROP FOREIGN TABLE type_double_array;
DROP FOREIGN TABLE type_timestamp_array;
-- DROP FOREIGN TABLE type_geometry;

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
