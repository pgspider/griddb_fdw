--
-- Test for push down functions in target list
--

--Testcase 1:
SET datestyle TO "ISO, YMD";

--Testcase 2:
SET timezone TO +00;

--Testcase 3:
SET intervalstyle to "postgres";

\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 4:
CREATE EXTENSION griddb_fdw;

--Testcase 5:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');

--Testcase 6:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);
IMPORT FOREIGN SCHEMA public LIMIT TO (student, time_series, time_series2) FROM SERVER griddb_svr INTO public;

--Select all

--Testcase 7:
SELECT * FROM student;

--
-- Test for non-unique functions of GridDB in WHERE clause
--
-- char_length

--Testcase 8:
EXPLAIN VERBOSE
SELECT * FROM student WHERE char_length(name) > 4 ;

--Testcase 9:
SELECT * FROM student WHERE char_length(name) > 4 ;

--Testcase 10:
EXPLAIN VERBOSE
SELECT * FROM student WHERE char_length(name) < 6 ;

--Testcase 11:
SELECT * FROM student WHERE char_length(name) < 6 ;

--Testcase 12:
EXPLAIN VERBOSE
SELECT * FROM student WHERE concat(name,' and george') = 'fred and george';

--Testcase 13:
SELECT * FROM student WHERE concat(name,' and george') = 'fred and george';

--substr

--Testcase 14:
EXPLAIN VERBOSE
SELECT * FROM student WHERE substr(name,2,3) = 'red';

--Testcase 15:
SELECT * FROM student WHERE substr(name,2,3) = 'red';

--Testcase 16:
EXPLAIN VERBOSE
SELECT * FROM student WHERE substr(name,1,3) <> 'fre';

--Testcase 17:
SELECT * FROM student WHERE substr(name,1,3) <> 'fre';

--upper

--Testcase 18:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE upper(name) = 'FRED';

--Testcase 19:
SELECT * FROM student WHERE upper(name) = 'FRED';

--Testcase 20:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE upper(name) <> 'FRED';

--Testcase 21:
SELECT * FROM student WHERE upper(name) <> 'FRED';

--lower

--Testcase 22:
INSERT INTO student VALUES ('GEORGE',30,'(1.2,-3.5)',3.8);

--Testcase 23:
INSERT INTO student VALUES ('BOB',35,'(5.2,3.8)',2.5);

--Testcase 24:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE lower(name) = 'george';

--Testcase 25:
SELECT * FROM student WHERE lower(name) = 'george';

--Testcase 26:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE lower(name) <> 'bob';

--Testcase 27:
SELECT * FROM student WHERE lower(name) <> 'bob';

--round

--Testcase 28:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE round(gpa) > 3.5;

--Testcase 29:
SELECT * FROM student WHERE round(gpa) > 3.5;

--Testcase 30:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE round(gpa) <= 3;

--Testcase 31:
SELECT * FROM student WHERE round(gpa) <= 3;

--floor

--Testcase 32:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE floor(gpa) = 3;

--Testcase 33:
SELECT * FROM student WHERE floor(gpa) = 3;

--Testcase 34:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE floor(gpa) < 2;

--Testcase 35:
SELECT * FROM student WHERE floor(gpa) < 3;

--ceiling

--Testcase 36:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE ceiling(gpa) >= 3;

--Testcase 37:
SELECT * FROM student WHERE ceiling(gpa) >= 3;

--Testcase 38:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE ceiling(gpa) = 4;

--Testcase 39:
SELECT * FROM student WHERE ceiling(gpa) = 4;

--
--Test for unique functions of GridDB in WHERE clause: time functions
--

--Testcase 40:
\d time_series2

--Testcase 41:
INSERT INTO time_series2 VALUES ('2020-12-29 04:40:00', '2000-12-29 04:40:00', '2020-01-05T20:30:00Z', 't', 0, 5, 100, 2000, 65.4, 2391.5, '7C8C893C8087F07883AF', ARRAY['aaa','bbb','ccc'],
ARRAY['t'::boolean,'f'::boolean,'t'::boolean],ARRAY[1,2,3,4],ARRAY[1,2,3],ARRAY[444,333,222],ARRAY[44444,22222,45555],ARRAY[2.3,4.2,62.1],ARRAY[444.2,554.3,5432.5],
ARRAY['2020-12-29 04:45:00'::timestamp,'2020-12-29 04:46:00'::timestamp]);

--Testcase 42:
INSERT INTO time_series2 VALUES ('2020-12-29 04:50:00', '2020-03-17 04:40:00', '2020-01-05T23:30:00Z', 'f', 20, 120, 1000, 2500, 44.5, 2432.78, '09C4A3E91E60BCD22357', ARRAY['abcdef','ghijkl','mnopqs'],
ARRAY['f'::boolean,'f'::boolean,'t'::boolean],ARRAY[2,9,11,25],ARRAY[45,22,35,50],ARRAY[4445,33,2221],ARRAY[25413,77548,36251],ARRAY[4.2,24.54,70.55],ARRAY[4431.63,-200.14,3265.1],
ARRAY['2020-12-31 14:00:00'::timestamp,'2020-12-31 14:45:00'::timestamp, '2020-01-01 15:00:00']);

--Testcase 43:
INSERT INTO time_series2 VALUES ('2020-12-29 05:00:30', '2020-12-11 02:30:30', '2020-01-05T22:00:00Z', 'f', 11, 175, 1234, 7705, 15.72, 1435.22, '2F63A64D987344F83AC8', ARRAY['7777a32ebea96a918b0f','40ee382083b987e94dd1','d417cf517eca8c2a709a'],
ARRAY['t'::boolean,'t'::boolean,'f'::boolean,'f'::boolean],ARRAY[12,29,1,14,16],ARRAY[255,124,77,51],ARRAY[2697,2641,7777],ARRAY[12475,12346,12654],ARRAY[22.5,12.11,23.54],ARRAY[3567.21,2124.23,-1254.11],
ARRAY['2020-05-19 14:15:20'::timestamp,'2020-11-14 17:45:14'::timestamp, '2020-09-05 01:24:06']);

--griddb_timestamp: push down timestamp function to GridDB

--Testcase 44:
EXPLAIN VERBOSE
SELECT date, strcol, booleancol, bytecol, shortcol, intcol, longcol, floatcol, doublecol FROM time_series2 WHERE griddb_timestamp(strcol) > '2020-01-05 21:00:00';

--Testcase 45:
SELECT date, strcol, booleancol, bytecol, shortcol, intcol, longcol, floatcol, doublecol FROM time_series2 WHERE griddb_timestamp(strcol) > '2020-01-05 21:00:00';

--Testcase 46:
EXPLAIN VERBOSE
SELECT date, strcol FROM time_series2 WHERE date < griddb_timestamp(strcol);

--Testcase 47:
SELECT date, strcol FROM time_series2 WHERE date < griddb_timestamp(strcol);
--griddb_timestamp: push down timestamp function to GridDB and gets error because GridDB only support YYYY-MM-DDThh:mm:ss.SSSZ format for timestamp function

--Testcase 48:
UPDATE time_series2 SET strcol = '2020-01-05 21:00:00';

--Testcase 49:
EXPLAIN VERBOSE
SELECT date, strcol FROM time_series2 WHERE griddb_timestamp(strcol) = '2020-01-05 21:00:00';

--Testcase 50:
SELECT date, strcol FROM time_series2 WHERE griddb_timestamp(strcol) = '2020-01-05 21:00:00';

--timestampadd
--YEAR

--Testcase 51:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('YEAR', date, -1) > '2019-12-29 05:00:00';

--Testcase 52:
SELECT date FROM time_series2 WHERE timestampadd('YEAR', date, -1) > '2019-12-29 05:00:00';

--Testcase 53:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('YEAR', date, 5) >= '2025-12-29 04:50:00';

--Testcase 54:
SELECT date FROM time_series2 WHERE timestampadd('YEAR', date, 5) >= '2025-12-29 04:50:00';

--Testcase 55:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('YEAR', date, 5) >= '2025-12-29';

--Testcase 56:
SELECT date FROM time_series2 WHERE timestampadd('YEAR', date, 5) >= '2025-12-29';
--MONTH

--Testcase 57:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MONTH', date, -3) > '2020-06-29 05:00:00';

--Testcase 58:
SELECT date FROM time_series2 WHERE timestampadd('MONTH', date, -3) > '2020-06-29 05:00:00';

--Testcase 59:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MONTH', date, 3) = '2021-03-29 05:00:30';

--Testcase 60:
SELECT date FROM time_series2 WHERE timestampadd('MONTH', date, 3) = '2021-03-29 05:00:30';

--Testcase 61:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MONTH', date, 3) >= '2021-03-29';

--Testcase 62:
SELECT date FROM time_series2 WHERE timestampadd('MONTH', date, 3) >= '2021-03-29';
--DAY

--Testcase 63:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('DAY', date, -3) > '2020-06-29 05:00:00';

--Testcase 64:
SELECT date FROM time_series2 WHERE timestampadd('DAY', date, -3) > '2020-06-29 05:00:00';

--Testcase 65:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('DAY', date, 3) = '2021-01-01 05:00:30';

--Testcase 66:
SELECT date FROM time_series2 WHERE timestampadd('DAY', date, 3) = '2021-01-01 05:00:30';

--Testcase 67:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('DAY', date, 3) >= '2021-01-01';

--Testcase 68:
SELECT date FROM time_series2 WHERE timestampadd('DAY', date, 3) >= '2021-01-01';
--HOUR

--Testcase 69:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('HOUR', date, -1) > '2020-12-29 04:00:00';

--Testcase 70:
SELECT date FROM time_series2 WHERE timestampadd('HOUR', date, -1) > '2020-12-29 04:00:00';

--Testcase 71:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('HOUR', date, 2) >= '2020-12-29 06:50:00';

--Testcase 72:
SELECT date FROM time_series2 WHERE timestampadd('HOUR', date, 2) >= '2020-12-29 06:50:00';
--MINUTE

--Testcase 73:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MINUTE', date, 20) = '2020-12-29 05:00:00';

--Testcase 74:
SELECT date FROM time_series2 WHERE timestampadd('MINUTE', date, 20) = '2020-12-29 05:00:00';

--Testcase 75:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MINUTE', date, -50) <= '2020-12-29 04:00:00';

--Testcase 76:
SELECT date FROM time_series2 WHERE timestampadd('MINUTE', date, -50) <= '2020-12-29 04:00:00';
--SECOND

--Testcase 77:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('SECOND', date, 25) >= '2020-12-29 04:40:30';

--Testcase 78:
SELECT date FROM time_series2 WHERE timestampadd('SECOND', date, 25) >= '2020-12-29 04:40:30';

--Testcase 79:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('SECOND', date, -50) <= '2020-12-29 04:00:00';

--Testcase 80:
SELECT date FROM time_series2 WHERE timestampadd('SECOND', date, -30) = '2020-12-29 05:00:00';
--MILLISECOND

--Testcase 81:
INSERT INTO time_series2 VALUES ('2020-12-29 05:10:00.120', '2020-12-29 05:10:00.563', '2020-01-05T20:30:30Z', 't', 0, 5, 30000000, 2000, 65.4, 2391.5, '7731b23fa1437ab784e3', ARRAY['aaa','bbb','ccc'],
ARRAY['t'::boolean,'f'::boolean,'t'::boolean],ARRAY[1,2,3,4],ARRAY[1,2,3],ARRAY[444,333,222],ARRAY[44444,22222,45555],ARRAY[2.3,4.2,62.1],ARRAY[444.2,554.3,5432.5],
ARRAY['2020-12-29 04:45:00'::timestamp,'2020-12-29 04:46:00'::timestamp]);

--Testcase 82:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MILLISECOND', date, 300) = '2020-12-29 05:10:00.420';

--Testcase 83:
SELECT date FROM time_series2 WHERE timestampadd('MILLISECOND', date, 300) = '2020-12-29 05:10:00.420';

--Testcase 84:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MILLISECOND', date, -55) = '2020-12-29 05:10:00.065';

--Testcase 85:
SELECT date FROM time_series2 WHERE timestampadd('MILLISECOND', date, -55) = '2020-12-29 05:10:00.065';
--Input wrong unit

--Testcase 86:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MICROSECOND', date, -55) = '2020-12-29 05:10:00.065';

--timestampdiff
--YEAR

--Testcase 87:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampdiff('YEAR', date, '2018-01-04 08:48:00') > 0;

--Testcase 88:
SELECT date FROM time_series2 WHERE timestampdiff('YEAR', date, '2018-01-04 08:48:00') > 0;

--Testcase 89:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('YEAR', '2015-07-15 08:48:00', date2) < 5;

--Testcase 90:
SELECT date2 FROM time_series2 WHERE timestampdiff('YEAR', '2015-07-15 08:48:00', date2) < 5;

--Testcase 91:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('YEAR', date, date2) > 10;

--Testcase 92:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('YEAR', date, date2) > 10;
--MONTH

--Testcase 93:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampdiff('MONTH', date, '2020-11-04 08:48:00') = 1;

--Testcase 94:
SELECT date FROM time_series2 WHERE timestampdiff('MONTH', date, '2020-11-04 08:48:00') = 1;

--Testcase 95:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('YEAR', '2020-02-15 08:48:00', date2) < 5;

--Testcase 96:
SELECT date2 FROM time_series2 WHERE timestampdiff('YEAR', '2020-02-15 08:48:00', date2) < 5;

--Testcase 97:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('MONTH', date, date2) < 10;

--Testcase 98:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('MONTH', date, date2) < 10;
--DAY

--Testcase 99:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('DAY', date2, '2020-12-04 08:48:00') > 20;

--Testcase 100:
SELECT date2 FROM time_series2 WHERE timestampdiff('DAY', date2, '2020-12-04 08:48:00') > 20;

--Testcase 101:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('DAY', '2020-02-15 08:48:00', date2) < 5;

--Testcase 102:
SELECT date2 FROM time_series2 WHERE timestampdiff('DAY', '2020-02-15 08:48:00', date2) < 5;

--Testcase 103:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('DAY', date, date2) > 10;

--Testcase 104:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('DAY', date, date2) > 10;
--HOUR

--Testcase 105:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampdiff('HOUR', date, '2020-12-29 07:40:00') < 0;

--Testcase 106:
SELECT date FROM time_series2 WHERE timestampdiff('HOUR', date, '2020-12-29 07:40:00') < 0;

--Testcase 107:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('HOUR', '2020-12-15 08:48:00', date2) > 3.5;

--Testcase 108:
SELECT date2 FROM time_series2 WHERE timestampdiff('HOUR', '2020-12-15 08:48:00', date2) > 3.5;

--Testcase 109:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('HOUR', date, date2) > 10;

--Testcase 110:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('HOUR', date, date2) > 10;
--MINUTE

--Testcase 111:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('MINUTE', date2, '2020-12-04 08:48:00') > 20;

--Testcase 112:
SELECT date2 FROM time_series2 WHERE timestampdiff('MINUTE', date2, '2020-12-04 08:48:00') > 20;

--Testcase 113:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('MINUTE', '2020-02-15 08:48:00', date2) < 5;

--Testcase 114:
SELECT date2 FROM time_series2 WHERE timestampdiff('MINUTE', '2020-02-15 08:48:00', date2) < 5;

--Testcase 115:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('MINUTE', date, date2) > 10;

--Testcase 116:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('MINUTE', date, date2) > 10;
--SECOND

--Testcase 117:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('SECOND', date2, '2020-12-04 08:48:00') > 1000;

--Testcase 118:
SELECT date2 FROM time_series2 WHERE timestampdiff('SECOND', date2, '2020-12-04 08:48:00') > 1000;

--Testcase 119:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('SECOND', '2020-03-17 04:50:00', date2) < 100;

--Testcase 120:
SELECT date2 FROM time_series2 WHERE timestampdiff('SECOND', '2020-03-17 04:50:00', date2) < 100;

--Testcase 121:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('SECOND', date, date2) > 1600000;

--Testcase 122:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('SECOND', date, date2) > 1600000;
--MILLISECOND

--Testcase 123:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('MILLISECOND', date2, '2020-12-04 08:48:00') > 200;

--Testcase 124:
SELECT date2 FROM time_series2 WHERE timestampdiff('MILLISECOND', date2, '2020-12-04 08:48:00') > 200;

--Testcase 125:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('MILLISECOND', '2020-03-17 08:48:00', date2) < 0;

--Testcase 126:
SELECT date2 FROM time_series2 WHERE timestampdiff('MILLISECOND', '2020-03-17 08:48:00', date2) < 0;

--Testcase 127:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('MILLISECOND', date, date2) = -443;

--Testcase 128:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('MILLISECOND', date, date2) = -443;
--Input wrong unit

--Testcase 129:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('MICROSECOND', date2, '2020-12-04 08:48:00') > 20;

--Testcase 130:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('DECADE', '2020-02-15 08:48:00', date2) < 5;

--Testcase 131:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('NANOSECOND', date, date2) > 10;

--to_timestamp_ms
--Normal case

--Testcase 132:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE to_timestamp_ms(intcol) > '1970-01-01 1:00:00';

--Testcase 133:
SELECT date FROM time_series2 WHERE to_timestamp_ms(intcol) > '1970-01-01 1:00:00';
--Return error if column contains -1 value

--Testcase 134:
INSERT INTO time_series2 VALUES ('2020-12-29 05:20:00.120', '2020-12-29 05:10:00.563', '2020-01-05T20:30:30Z', 't', 0, 5, -1, 2000, 65.4, 2391.5, '7731b23fa1437ab784e3', ARRAY['aaa','bbb','ccc'],
ARRAY['t'::boolean,'f'::boolean,'t'::boolean],ARRAY[1,2,3,4],ARRAY[1,2,3],ARRAY[444,333,222],ARRAY[44444,22222,45555],ARRAY[2.3,4.2,62.1],ARRAY[444.2,554.3,5432.5],
ARRAY['2020-12-29 04:45:00'::timestamp,'2020-12-29 04:46:00'::timestamp]);

--Testcase 135:
SELECT date FROM time_series2 WHERE to_timestamp_ms(intcol) > '1970-01-01 1:00:00';

--to_epoch_ms

--Testcase 136:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE intcol < to_epoch_ms(date);

--Testcase 137:
SELECT date FROM time_series2 WHERE intcol < to_epoch_ms(date);

--Testcase 138:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE to_epoch_ms(date2) < 1000000000000;

--
--Test for unique functions of GridDB in WHERE clause: array functions
--
--array_length

--Testcase 139:
EXPLAIN VERBOSE
SELECT boolarray FROM time_series2 WHERE array_length(boolarray) = 3;

--Testcase 140:
SELECT boolarray FROM time_series2 WHERE array_length(boolarray) = 3;

--Testcase 141:
EXPLAIN VERBOSE
SELECT stringarray FROM time_series2 WHERE array_length(stringarray) = 3;

--Testcase 142:
SELECT stringarray FROM time_series2 WHERE array_length(stringarray) = 3;

--Testcase 143:
EXPLAIN VERBOSE
SELECT bytearray, shortarray FROM time_series2 WHERE array_length(bytearray) > array_length(shortarray);

--Testcase 144:
SELECT bytearray, shortarray FROM time_series2 WHERE array_length(bytearray) > array_length(shortarray);

--Testcase 145:
EXPLAIN VERBOSE
SELECT integerarray, longarray FROM time_series2 WHERE array_length(integerarray) = array_length(longarray);

--Testcase 146:
SELECT integerarray, longarray FROM time_series2 WHERE array_length(integerarray) = array_length(longarray);

--Testcase 147:
EXPLAIN VERBOSE
SELECT floatarray, doublearray FROM time_series2 WHERE array_length(floatarray) - array_length(doublearray) = 0;

--Testcase 148:
SELECT floatarray, doublearray FROM time_series2 WHERE array_length(floatarray) - array_length(doublearray) = 0;

--Testcase 149:
EXPLAIN VERBOSE
SELECT timestamparray FROM time_series2 WHERE array_length(timestamparray) < 3;

--Testcase 150:
SELECT timestamparray FROM time_series2 WHERE array_length(timestamparray) < 3;

--element
--Normal case

--Testcase 151:
EXPLAIN VERBOSE
SELECT boolarray FROM time_series2 WHERE element(1, boolarray) = 'f';

--Testcase 152:
SELECT boolarray FROM time_series2 WHERE element(1, boolarray) = 'f';

--Testcase 153:
EXPLAIN VERBOSE
SELECT stringarray FROM time_series2 WHERE element(1, stringarray) != 'bbb';

--Testcase 154:
SELECT stringarray FROM time_series2 WHERE element(1, stringarray) != 'bbb';

--Testcase 155:
EXPLAIN VERBOSE
SELECT bytearray, shortarray FROM time_series2 WHERE element(0, bytearray) = element(0, shortarray);

--Testcase 156:
SELECT bytearray, shortarray FROM time_series2 WHERE element(0, bytearray) = element(0, shortarray);

--Testcase 157:
EXPLAIN VERBOSE
SELECT integerarray, longarray FROM time_series2 WHERE element(0, integerarray)*100+44 = element(0,longarray);

--Testcase 158:
SELECT integerarray, longarray FROM time_series2 WHERE element(0, integerarray)*100+44 = element(0,longarray);

--Testcase 159:
EXPLAIN VERBOSE
SELECT floatarray, doublearray FROM time_series2 WHERE element(2, floatarray)*10 < element(0,doublearray);

--Testcase 160:
SELECT floatarray, doublearray FROM time_series2 WHERE element(2, floatarray)*10 < element(0,doublearray);

--Testcase 161:
EXPLAIN VERBOSE
SELECT timestamparray FROM time_series2 WHERE element(1,timestamparray) > '2020-12-29 04:00:00';

--Testcase 162:
SELECT timestamparray FROM time_series2 WHERE element(1,timestamparray) > '2020-12-29 04:00:00';
--Return error when getting non-existent element

--Testcase 163:
EXPLAIN VERBOSE
SELECT timestamparray FROM time_series2 WHERE element(2,timestamparray) > '2020-12-29 04:00:00';

--Testcase 164:
SELECT timestamparray FROM time_series2 WHERE element(2,timestamparray) > '2020-12-29 04:00:00';

--
--if user selects non-unique functions which Griddb only supports in WHERE clause => do not push down
--if user selects unique functions which Griddb only supports in WHERE clause => still push down, return error of Griddb
--

--Testcase 165:
EXPLAIN VERBOSE
SELECT char_length(name) FROM student;

--Testcase 166:
SELECT char_length(name) FROM student;

--Testcase 167:
EXPLAIN VERBOSE
SELECT concat(name,'abc') FROM student;

--Testcase 168:
SELECT concat(name,'abc') FROM student;

--Testcase 169:
EXPLAIN VERBOSE
SELECT substr(name,2,3) FROM student;

--Testcase 170:
SELECT substr(name,2,3) FROM student;

--Testcase 171:
EXPLAIN VERBOSE
SELECT element(1, timestamparray) FROM time_series2;

--Testcase 172:
SELECT element(1, timestamparray) FROM time_series2;

--Testcase 173:
EXPLAIN VERBOSE
SELECT upper(name) FROM student;

--Testcase 174:
SELECT upper(name) FROM student;

--Testcase 175:
EXPLAIN VERBOSE
SELECT lower(name) FROM student;

--Testcase 176:
SELECT lower(name) FROM student;

--Testcase 177:
EXPLAIN VERBOSE
SELECT round(gpa) FROM student;

--Testcase 178:
SELECT round(gpa) FROM student;

--Testcase 179:
EXPLAIN VERBOSE
SELECT floor(gpa) FROM student;

--Testcase 180:
SELECT floor(gpa) FROM student;

--Testcase 181:
EXPLAIN VERBOSE
SELECT ceiling(gpa) FROM student;

--Testcase 182:
SELECT ceiling(gpa) FROM student;

--Testcase 183:
EXPLAIN VERBOSE
SELECT griddb_timestamp(strcol) FROM time_series2;

--Testcase 184:
SELECT griddb_timestamp(strcol) FROM time_series2;

--Testcase 185:
EXPLAIN VERBOSE
SELECT timestampadd('YEAR', date, -1) FROM time_series2;

--Testcase 186:
SELECT timestampadd('YEAR', date, -1) FROM time_series2;

--Testcase 187:
EXPLAIN VERBOSE
SELECT timestampdiff('YEAR', date, '2018-01-04 08:48:00') FROM time_series2;

--Testcase 188:
SELECT timestampdiff('YEAR', date, '2018-01-04 08:48:00') FROM time_series2;

--Testcase 189:
EXPLAIN VERBOSE
SELECT to_timestamp_ms(intcol) FROM time_series2;

--Testcase 190:
SELECT to_timestamp_ms(intcol) FROM time_series2;

--Testcase 191:
EXPLAIN VERBOSE
SELECT to_epoch_ms(date) FROM time_series2;

--Testcase 192:
SELECT to_epoch_ms(date) FROM time_series2;

--Testcase 193:
EXPLAIN VERBOSE
SELECT array_length(boolarray) FROM time_series2;

--Testcase 194:
SELECT array_length(boolarray) FROM time_series2;

--Testcase 195:
EXPLAIN VERBOSE
SELECT element(1, stringarray) FROM time_series2;

--Testcase 196:
SELECT element(1, stringarray) FROM time_series2;

--
--Test for unique functions of GridDB in SELECT clause: time-series functions
--
--time_next
--specified time exist => return that row

--Testcase 197:
EXPLAIN VERBOSE
SELECT time_next('2018-12-01 10:00:00') FROM time_series;

--Testcase 198:
SELECT time_next('2018-12-01 10:00:00') FROM time_series;
--specified time does not exist => return the row whose time  is immediately after the specified time

--Testcase 199:
EXPLAIN VERBOSE
SELECT time_next('2018-12-01 10:05:00') FROM time_series;

--Testcase 200:
SELECT time_next('2018-12-01 10:05:00') FROM time_series;
--specified time does not exist, there is no time after the specified time => return no row

--Testcase 201:
EXPLAIN VERBOSE
SELECT time_next('2018-12-01 10:45:00') FROM time_series;

--Testcase 202:
SELECT time_next('2018-12-01 10:45:00') FROM time_series;

--time_next_only
--even though specified time exist, still return the row whose time is immediately after the specified time

--Testcase 203:
EXPLAIN VERBOSE
SELECT time_next_only('2018-12-01 10:00:00') FROM time_series;

--Testcase 204:
SELECT time_next_only('2018-12-01 10:00:00') FROM time_series;
--specified time does not exist => return the row whose time  is immediately after the specified time

--Testcase 205:
EXPLAIN VERBOSE
SELECT time_next_only('2018-12-01 10:05:00') FROM time_series;

--Testcase 206:
SELECT time_next_only('2018-12-01 10:05:00') FROM time_series;
--there is no time after the specified time => return no row

--Testcase 207:
EXPLAIN VERBOSE
SELECT time_next_only('2018-12-01 10:45:00') FROM time_series;

--Testcase 208:
SELECT time_next_only('2018-12-01 10:45:00') FROM time_series;

--time_prev
--specified time exist => return that row

--Testcase 209:
EXPLAIN VERBOSE
SELECT time_prev('2018-12-01 10:10:00') FROM time_series;

--Testcase 210:
SELECT time_prev('2018-12-01 10:10:00') FROM time_series;
--specified time does not exist => return the row whose time  is immediately before the specified time

--Testcase 211:
EXPLAIN VERBOSE
SELECT time_prev('2018-12-01 10:05:00') FROM time_series;

--Testcase 212:
SELECT time_prev('2018-12-01 10:05:00') FROM time_series;
--specified time does not exist, there is no time before the specified time => return no row

--Testcase 213:
EXPLAIN VERBOSE
SELECT time_prev('2018-12-01 09:45:00') FROM time_series;

--Testcase 214:
SELECT time_prev('2018-12-01 09:45:00') FROM time_series;

--time_prev_only
--even though specified time exist, still return the row whose time is immediately before the specified time

--Testcase 215:
EXPLAIN VERBOSE
SELECT time_prev_only('2018-12-01 10:10:00') FROM time_series;

--Testcase 216:
SELECT time_prev_only('2018-12-01 10:10:00') FROM time_series;
--specified time does not exist => return the row whose time  is immediately before the specified time

--Testcase 217:
EXPLAIN VERBOSE
SELECT time_prev_only('2018-12-01 10:05:00') FROM time_series;

--Testcase 218:
SELECT time_prev_only('2018-12-01 10:05:00') FROM time_series;
--there is no time before the specified time => return no row

--Testcase 219:
EXPLAIN VERBOSE
SELECT time_prev_only('2018-12-01 09:45:00') FROM time_series;

--Testcase 220:
SELECT time_prev_only('2018-12-01 09:45:00') FROM time_series;

--time_interpolated
--specified time exist => return that row

--Testcase 221:
EXPLAIN VERBOSE
SELECT time_interpolated(value1, '2018-12-01 10:10:00') FROM time_series;

--Testcase 222:
SELECT time_interpolated(value1, '2018-12-01 10:10:00') FROM time_series;
--specified time does not exist => return the row which has interpolated value.
--The column which is specified as the 1st parameter will be calculated by linearly interpolating the value of the previous and next rows.
--Other values will be equal to the values of rows previous to the specified time.

--Testcase 223:
EXPLAIN VERBOSE
SELECT time_interpolated(value1, '2018-12-01 10:05:00') FROM time_series;

--Testcase 224:
SELECT time_interpolated(value1, '2018-12-01 10:05:00') FROM time_series;
--specified time does not exist. There is no row before or after the specified time => can not calculate interpolated value, return no row.

--Testcase 225:
EXPLAIN VERBOSE
SELECT time_interpolated(value1, '2018-12-01 09:05:00') FROM time_series;

--Testcase 226:
SELECT time_interpolated(value1, '2018-12-01 09:05:00') FROM time_series;

--Testcase 227:
EXPLAIN VERBOSE
SELECT time_interpolated(value1, '2018-12-01 10:45:00') FROM time_series;

--Testcase 228:
SELECT time_interpolated(value1, '2018-12-01 10:45:00') FROM time_series;

--time_sampling by MINUTE
--rows for sampling exists => return those rows

--Testcase 229:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:20:00', 10, 'MINUTE') FROM time_series;

--Testcase 230:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:20:00', 10, 'MINUTE') FROM time_series;
--rows for sampling does not exist => return rows that contains interpolated values.
--The column which is specified as the 1st parameter will be calculated by linearly interpolating the value of the previous and next rows.
--Other values will be equal to the values of rows previous to the specified time.

--Testcase 231:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:05:00', '2018-12-01 10:35:00', 10, 'MINUTE') FROM time_series;

--Testcase 232:
SELECT time_sampling(value1, '2018-12-01 10:05:00', '2018-12-01 10:35:00', 10, 'MINUTE') FROM time_series;
--mix exist and non-exist sampling

--Testcase 233:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:40:00', 10, 'MINUTE') FROM time_series;

--Testcase 234:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:40:00', 10, 'MINUTE') FROM time_series;
--In linearly interpolating the value of the previous and next rows, if one of the values does not exist => the sampling row will not be returned

--Testcase 235:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 09:30:00', '2018-12-01 11:00:00', 10, 'MINUTE') FROM time_series;

--Testcase 236:
SELECT time_sampling(value1, '2018-12-01 09:30:00', '2018-12-01 11:00:00', 10, 'MINUTE') FROM time_series;
--if the first parameter is not set, * will be added as the first parameter.
--When specified time does not exist, all columns (except timestamp key column) will be equal to the values of rows previous to the specified time.

--Testcase 237:
UPDATE time_series SET value1 = 5 where date = '2018-12-01 10:40:00';

--Testcase 238:
EXPLAIN VERBOSE
SELECT time_sampling('2018-12-01 10:00:00', '2018-12-01 10:40:00', 10, 'MINUTE') FROM time_series;

--Testcase 239:
SELECT time_sampling('2018-12-01 10:00:00', '2018-12-01 10:40:00', 10, 'MINUTE') FROM time_series;

--time_sampling by HOUR

--Testcase 240:
DELETE FROM time_series;

--Testcase 241:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00', 1, 10.5);

--Testcase 242:
INSERT INTO time_series VALUES ('2018-12-01 12:00:00', 2, 9.4);

--Testcase 243:
INSERT INTO time_series VALUES ('2018-12-01 16:00:00', 3, 8);

--Testcase 244:
INSERT INTO time_series VALUES ('2018-12-01 17:00:00', 4, 7.2);

--Testcase 245:
INSERT INTO time_series VALUES ('2018-12-01 20:00:00', 5, 5.6);
--rows for sampling exists => return those rows

--Testcase 246:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 12:00:00', 2, 'HOUR') FROM time_series;

--Testcase 247:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 12:00:00', 2, 'HOUR') FROM time_series;
--rows for sampling does not exist => return rows that contains interpolated values.
--The column which is specified as the 1st parameter will be calculated by linearly interpolating the value of the previous and next rows.
--Other values will be equal to the values of rows previous to the specified time.

--Testcase 248:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:05:00', '2018-12-01 21:00:00', 3, 'HOUR') FROM time_series;

--Testcase 249:
SELECT time_sampling(value1, '2018-12-01 10:05:00', '2018-12-01 21:00:00', 3, 'HOUR') FROM time_series;
--mix exist and non-exist sampling

--Testcase 250:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 21:40:00', 2, 'HOUR') FROM time_series;

--Testcase 251:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 21:40:00', 2, 'HOUR') FROM time_series;
--In linearly interpolating the value of the previous and next rows, if one of the values does not exist => the sampling row will not be returned

--Testcase 252:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 6:00:00', '2018-12-01 23:00:00', 3, 'HOUR') FROM time_series;

--Testcase 253:
SELECT time_sampling(value1, '2018-12-01 6:00:00', '2018-12-01 23:00:00', 3, 'HOUR') FROM time_series;
--if the first parameter is not set, * will be added as the first parameter.
--When specified time does not exist, all columns (except timestamp key column) will be equal to the values of rows previous to the specified time.

--Testcase 254:
DELETE FROM time_series WHERE value1 = 4;

--Testcase 255:
EXPLAIN VERBOSE
SELECT time_sampling('2018-12-01 10:00:00', '2018-12-01 21:40:00', 2, 'HOUR') FROM time_series;

--Testcase 256:
SELECT time_sampling('2018-12-01 10:00:00', '2018-12-01 21:40:00', 2, 'HOUR') FROM time_series;

--time_sampling by DAY

--Testcase 257:
DELETE FROM time_series;

--Testcase 258:
INSERT INTO time_series VALUES ('2018-12-01 11:00:00', 4, 4);

--Testcase 259:
INSERT INTO time_series VALUES ('2018-12-02 11:00:00', 5, 3.2);

--Testcase 260:
INSERT INTO time_series VALUES ('2018-12-02 12:00:30', 6, 3);

--Testcase 261:
INSERT INTO time_series VALUES ('2018-12-03 12:00:30', 7, 2.8);
--rows for sampling exists => return those rows

--Testcase 262:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 11:00:00', '2018-12-02 11:00:00', 1, 'DAY') FROM time_series;

--Testcase 263:
SELECT time_sampling(value1, '2018-12-01 11:00:00', '2018-12-02 11:00:00', 1, 'DAY') FROM time_series;
--rows for sampling does not exist => return rows that contains interpolated values.
--The column which is specified as the 1st parameter will be calculated by linearly interpolating the value of the previous and next rows.
--Other values will be equal to the values of rows previous to the specified time.

--Testcase 264:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 09:00:00', '2018-12-03 12:00:00', 1, 'DAY') FROM time_series;

--Testcase 265:
SELECT time_sampling(value1, '2018-12-01 09:00:00', '2018-12-03 12:00:00', 1, 'DAY') FROM time_series;
--mix exist and non-exist sampling

--Testcase 266:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 11:00:00', '2018-12-03 12:00:00', 1, 'DAY') FROM time_series;

--Testcase 267:
SELECT time_sampling(value1, '2018-12-01 11:00:00', '2018-12-03 12:00:00', 1, 'DAY') FROM time_series;
--In linearly interpolating the value of the previous and next rows, if one of the values does not exist => the sampling row will not be returned

--Testcase 268:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 09:30:00', '2018-12-01 11:00:00', 1, 'DAY') FROM time_series;

--Testcase 269:
SELECT time_sampling(value1, '2018-12-01 09:30:00', '2018-12-05 11:00:00', 1, 'DAY') FROM time_series;
--if the first parameter is not set, * will be added as the first parameter.
--When specified time does not exist, all columns (except timestamp key column) will be equal to the values of rows previous to the specified time.

--Testcase 270:
DELETE FROM time_series WHERE value1 = 6;

--Testcase 271:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 11:00:00', '2018-12-03 12:00:00', 1, 'DAY') FROM time_series;

--Testcase 272:
SELECT time_sampling(value1, '2018-12-01 11:00:00', '2018-12-03 12:00:00', 1, 'DAY') FROM time_series;

--time_sampling by SECOND

--Testcase 273:
DELETE FROM time_series;

--Testcase 274:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00', 1, 1.5);

--Testcase 275:
INSERT INTO time_series VALUES ('2018-12-01 10:00:10', 2, 3.2);

--Testcase 276:
INSERT INTO time_series VALUES ('2018-12-01 10:00:20', 4, 3.5);

--Testcase 277:
INSERT INTO time_series VALUES ('2018-12-01 10:00:40', 6, 5.2);

--Testcase 278:
INSERT INTO time_series VALUES ('2018-12-01 10:01:10', 7, 6.7);
--rows for sampling exists => return those rows

--Testcase 279:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:00:20', 10, 'SECOND') FROM time_series;

--Testcase 280:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:00:20', 10, 'SECOND') FROM time_series;
--rows for sampling does not exist => return rows that contains interpolated values.
--The column which is specified as the 1st parameter will be calculated by linearly interpolating the value of the previous and next rows.
--Other values will be equal to the values of rows previous to the specified time.

--Testcase 281:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:03', '2018-12-01 10:00:35', 15, 'SECOND') FROM time_series;

--Testcase 282:
SELECT time_sampling(value1, '2018-12-01 10:00:03', '2018-12-01 10:00:35', 15, 'SECOND') FROM time_series;
--mix exist and non-exist sampling

--Testcase 283:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 11:00:00', 10, 'SECOND') FROM time_series;

--Testcase 284:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 11:00:00', 10, 'SECOND') FROM time_series;
--In linearly interpolating the value of the previous and next rows, if one of the values does not exist => the sampling row will not be returned

--Testcase 285:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 08:30:00', '2018-12-01 11:00:00', 20, 'SECOND') FROM time_series;

--Testcase 286:
SELECT time_sampling(value1, '2018-12-01 08:30:00', '2018-12-01 11:00:00', 20, 'SECOND') FROM time_series;
--if the first parameter is not set, * will be added as the first parameter.
--When specified time does not exist, all columns (except timestamp key column) will be equal to the values of rows previous to the specified time.

--Testcase 287:
DELETE FROM time_series WHERE value1 = 4;

--Testcase 288:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 11:00:00', 10, 'SECOND') FROM time_series;

--Testcase 289:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 11:00:00', 10, 'SECOND') FROM time_series;

--time_sampling by MILLISECOND

--Testcase 290:
DELETE FROM time_series;

--Testcase 291:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00.100', 1, 1.5);

--Testcase 292:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00.120', 2, 3.2);

--Testcase 293:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00.140', 4, 3.5);

--Testcase 294:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00.150', 6, 5.2);

--Testcase 295:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00.160', 7, 6.7);
--rows for sampling exists => return those rows

--Testcase 296:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00.100', '2018-12-01 10:00:00.140', 20, 'MILLISECOND') FROM time_series;

--Testcase 297:
SELECT time_sampling(value1, '2018-12-01 10:00:00.100', '2018-12-01 10:00:00.140', 20, 'MILLISECOND') FROM time_series;
--rows for sampling does not exist => return rows that contains interpolated values.
--The column which is specified as the 1st parameter will be calculated by linearly interpolating the value of the previous and next rows.
--Other values will be equal to the values of rows previous to the specified time.

--Testcase 298:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00.115', '2018-12-01 10:00:00.155', 15, 'MILLISECOND') FROM time_series;

--Testcase 299:
SELECT time_sampling(value1, '2018-12-01 10:00:00.115', '2018-12-01 10:00:00.155', 15, 'MILLISECOND') FROM time_series;
--mix exist and non-exist sampling

--Testcase 300:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00.100', '2018-12-01 10:00:00.150', 5, 'MILLISECOND') FROM time_series;

--Testcase 301:
SELECT time_sampling(value1, '2018-12-01 10:00:00.100', '2018-12-01 10:00:00.150', 5, 'MILLISECOND') FROM time_series;
--In linearly interpolating the value of the previous and next rows, if one of the values does not exist => the sampling row will not be returned

--Testcase 302:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00.002', '2018-12-01 10:00:00.500', 20, 'MILLISECOND') FROM time_series;

--Testcase 303:
SELECT time_sampling(value1, '2018-12-01 10:00:00.002', '2018-12-01 10:00:00.500', 20, 'MILLISECOND') FROM time_series;
--if the first parameter is not set, * will be added as the first parameter.
--When specified time does not exist, all columns (except timestamp key column) will be equal to the values of rows previous to the specified time.

--Testcase 304:
DELETE FROM time_series WHERE value1 = 4;

--Testcase 305:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00.100', '2018-12-01 10:00:00.150', 5, 'MILLISECOND') FROM time_series;

--Testcase 306:
SELECT time_sampling(value1, '2018-12-01 10:00:00.100', '2018-12-01 10:00:00.150', 5, 'MILLISECOND') FROM time_series;

--max_rows

--Testcase 307:
DELETE FROM time_series;

--Testcase 308:
INSERT INTO time_series VALUES ('2018-12-01 11:00:00', 4, 4);

--Testcase 309:
INSERT INTO time_series VALUES ('2018-12-02 11:00:00', 5, 3.2);

--Testcase 310:
INSERT INTO time_series VALUES ('2018-12-02 12:00:30', 6, 3);

--Testcase 311:
INSERT INTO time_series VALUES ('2018-12-03 12:00:30', 7, 2.8);

--Testcase 312:
EXPLAIN VERBOSE
SELECT max_rows(value2) FROM time_series;

--Testcase 313:
SELECT max_rows(value2) FROM time_series;

--Testcase 314:
EXPLAIN VERBOSE
SELECT max_rows(date) FROM time_series;

--Testcase 315:
SELECT max_rows(date) FROM time_series;

--min_rows

--Testcase 316:
EXPLAIN VERBOSE
SELECT min_rows(value2) FROM time_series;

--Testcase 317:
SELECT min_rows(value2) FROM time_series;

--Testcase 318:
EXPLAIN VERBOSE
SELECT min_rows(date) FROM time_series;

--Testcase 319:
SELECT min_rows(date) FROM time_series;

--
--if WHERE clause contains functions which Griddb only supports in SELECT clause => still push down, return error of Griddb
--

--Testcase 320:
EXPLAIN VERBOSE
SELECT * FROM time_series2 WHERE time_next('2018-12-01 10:00:00') = '"2020-01-05 21:00:00,{t,f,t}"';

--Testcase 321:
SELECT * FROM time_series2 WHERE time_next('2018-12-01 10:00:00') = '"2020-01-05 21:00:00,{t,f,t}"';

--Testcase 322:
EXPLAIN VERBOSE
SELECT date FROM time_series WHERE time_next_only('2018-12-01 10:00:00') = time_interpolated(value1, '2018-12-01 10:10:00');

--Testcase 323:
SELECT date FROM time_series WHERE time_next_only('2018-12-01 10:00:00') = time_interpolated(value1, '2018-12-01 10:10:00');

--Testcase 324:
EXPLAIN VERBOSE
SELECT * FROM time_series2 WHERE time_prev('2018-12-01 10:00:00') = '"2020-01-05 21:00:00,{t,f,t}"';

--Testcase 325:
SELECT * FROM time_series2 WHERE time_prev('2018-12-01 10:00:00') = '"2020-01-05 21:00:00,{t,f,t}"';

--Testcase 326:
EXPLAIN VERBOSE
SELECT date FROM time_series WHERE time_prev_only('2018-12-01 10:00:00') = time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:40:00', 10, 'MINUTE');

--Testcase 327:
SELECT date FROM time_series WHERE time_prev_only('2018-12-01 10:00:00') = time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:40:00', 10, 'MINUTE');

--Testcase 328:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE max_rows(date) = min_rows(value2);

--Testcase 329:
SELECT * FROM time_series WHERE max_rows(date) = min_rows(value2);

--
-- Test syntax (xxx()::time_series).*
--

--Testcase 330:
EXPLAIN VERBOSE
SELECT (time_sampling(value1, '2018-12-01 11:00:00', '2018-12-01 12:00:00', 20, 'MINUTE')::time_series).* FROM time_series;

--Testcase 331:
SELECT (time_sampling(value1, '2018-12-01 11:00:00', '2018-12-01 12:00:00', 20, 'MINUTE')::time_series).* FROM time_series;

--Testcase 332:
EXPLAIN VERBOSE
SELECT (time_sampling(value1, '2018-12-01 11:00:00', '2018-12-01 12:00:00', 20, 'MINUTE')::time_series).date FROM time_series;

--Testcase 333:
SELECT (time_sampling(value1, '2018-12-01 11:00:00', '2018-12-01 12:00:00', 20, 'MINUTE')::time_series).date FROM time_series;

--Testcase 334:
EXPLAIN VERBOSE
SELECT (time_sampling(value1, '2018-12-01 11:00:00', '2018-12-01 12:00:00', 20, 'MINUTE')::time_series).value1 FROM time_series;

--Testcase 335:
SELECT (time_sampling(value1, '2018-12-01 11:00:00', '2018-12-01 12:00:00', 20, 'MINUTE')::time_series).value1 FROM time_series;

--
-- Test syntax (xxx()::time_series2).*
--

--Testcase 336:
DELETE FROM time_series2;

--Testcase 337:
INSERT INTO time_series2 VALUES ('2020-12-20 05:00:00', '2020-12-20 08:00:00', '(3.1, 2.3)', 'f', 1, 175, 1234, 7705, 15.72, 1435.22, '2F63A64D987344F83AC8', ARRAY['7777a32ebea96a918b0f','40ee382083b987e94dd1','d417cf517eca8c2a709a'],
ARRAY['t'::boolean,'t'::boolean,'f'::boolean,'f'::boolean],ARRAY[12,29,1,14,16],ARRAY[255,124,77,51],ARRAY[2697,2641,7777],ARRAY[12475,12346,12654],ARRAY[22.5,12.11,23.54],ARRAY[3567.21,2124.23,-1254.11],
ARRAY['2020-05-19 14:15:20'::timestamp,'2020-11-14 17:45:14'::timestamp, '2020-09-05 01:24:06']);

--Testcase 338:
INSERT INTO time_series2 VALUES ('2020-12-20 06:00:00', '2020-12-20 09:00:00', '(1.3, 3.2)', 'f', 2, 175, 1234, 7705, 15.72, 1435.22, '2F63A64D987344F83AC8', ARRAY['7777a32ebea96a918b0f','40ee382083b987e94dd1','d417cf517eca8c2a709a'],
ARRAY['t'::boolean,'t'::boolean,'f'::boolean,'f'::boolean],ARRAY[12,29,1,14,16],ARRAY[255,124,77,51],ARRAY[2697,2641,7777],ARRAY[12475,12346,12654],ARRAY[22.5,12.11,23.54],ARRAY[3567.21,2124.23,-1254.11],
ARRAY['2020-05-19 14:15:20'::timestamp,'2020-11-14 17:45:14'::timestamp, '2020-09-05 01:24:06']);

--Testcase 339:
EXPLAIN VERBOSE
SELECT (time_sampling(bytecol, '2020-12-20 05:00:00', '2020-12-20 05:20:00', 20, 'MINUTE')::time_series2).* FROM time_series2;

--Testcase 340:
SELECT (time_sampling(bytecol, '2020-12-20 05:00:00', '2020-12-20 05:20:00', 20, 'MINUTE')::time_series2).* FROM time_series2;

--
-- Test aggregate function time_avg
--

--Testcase 341:
EXPLAIN VERBOSE
SELECT time_avg(value1) FROM time_series;

--Testcase 342:
SELECT time_avg(value1) FROM time_series;

--Testcase 343:
EXPLAIN VERBOSE
SELECT time_avg(value2) FROM time_series;

--Testcase 344:
SELECT time_avg(value2) FROM time_series;
-- GridDB does not support select multiple target in a query => do not push down, raise stub function error

--Testcase 345:
EXPLAIN VERBOSE
SELECT time_avg(value1), time_avg(value2) FROM time_series;

--Testcase 346:
SELECT time_avg(value1), time_avg(value2) FROM time_series;
-- Do not push down when expected type is not correct, raise stub function error

--Testcase 347:
EXPLAIN VERBOSE
SELECT time_avg(date) FROM time_series;

--Testcase 348:
SELECT time_avg(date) FROM time_series;

--Testcase 349:
EXPLAIN VERBOSE
SELECT time_avg(blobcol) FROM time_series2;

--Testcase 350:
SELECT time_avg(blobcol) FROM time_series2;

--
-- Test aggregate function min, max, count, sum, avg, variance, stddev
--

--Testcase 351:
EXPLAIN VERBOSE
SELECT min(age) FROM student;

--Testcase 352:
SELECT min(age) FROM student;

--Testcase 353:
EXPLAIN VERBOSE
SELECT max(gpa) FROM student;

--Testcase 354:
SELECT max(gpa) FROM student;

--Testcase 355:
EXPLAIN VERBOSE
SELECT count(*) FROM student;

--Testcase 356:
SELECT count(*) FROM student;

--Testcase 357:
EXPLAIN VERBOSE
SELECT count(*) FROM student WHERE gpa < 3.5 OR age < 42;

--Testcase 358:
SELECT count(*) FROM student WHERE gpa < 3.5 OR age < 42;

--Testcase 359:
EXPLAIN VERBOSE
SELECT sum(age) FROM student;

--Testcase 360:
SELECT sum(age) FROM student;

--Testcase 361:
EXPLAIN VERBOSE
SELECT sum(age) FROM student WHERE round(gpa) > 3.5;

--Testcase 362:
SELECT sum(age) FROM student WHERE round(gpa) > 3.5;

--Testcase 363:
EXPLAIN VERBOSE
SELECT avg(gpa) FROM student;

--Testcase 364:
SELECT avg(gpa) FROM student;

--Testcase 365:
EXPLAIN VERBOSE
SELECT avg(gpa) FROM student WHERE lower(name) = 'george';

--Testcase 366:
SELECT avg(gpa) FROM student WHERE lower(name) = 'george';

--Testcase 367:
EXPLAIN VERBOSE
SELECT variance(gpa) FROM student;

--Testcase 368:
SELECT variance(gpa) FROM student;

--Testcase 369:
EXPLAIN VERBOSE
SELECT variance(gpa) FROM student WHERE gpa > 3.5;

--Testcase 370:
SELECT variance(gpa) FROM student WHERE gpa > 3.5;

--Testcase 371:
EXPLAIN VERBOSE
SELECT stddev(age) FROM student;

--Testcase 372:
SELECT stddev(age) FROM student;

--Testcase 373:
EXPLAIN VERBOSE
SELECT stddev(age) FROM student WHERE char_length(name) > 4;

--Testcase 374:
SELECT stddev(age) FROM student WHERE char_length(name) > 4;

--Testcase 375:
EXPLAIN VERBOSE
SELECT max(gpa), min(age) FROM student;

--Testcase 376:
SELECT max(gpa), min(age) FROM student;
--Delete inserted values

--Testcase 377:
DELETE FROM student WHERE name = 'GEORGE' or name = 'BOB';

--Drop all foreign tables

--Testcase 378:
DROP FOREIGN TABLE student;

--Testcase 379:
DROP FOREIGN TABLE time_series;

--Testcase 380:
DROP FOREIGN TABLE time_series2;

--Testcase 381:
DROP USER MAPPING FOR public SERVER griddb_svr;

--Testcase 382:
DROP SERVER griddb_svr CASCADE;

--Testcase 383:
DROP EXTENSION griddb_fdw CASCADE;
