--
-- Test for push down functions in target list
--
--Testcase 380:
SET datestyle TO "ISO, YMD";
--Testcase 381:
SET timezone TO +00;
--Testcase 382:
SET intervalstyle to "postgres";

\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 1:
CREATE EXTENSION griddb_fdw;
--Testcase 2:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');
--Testcase 3:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);
IMPORT FOREIGN SCHEMA public LIMIT TO (student, time_series, time_series2) FROM SERVER griddb_svr INTO public;

--Select all
--Testcase 4:
SELECT * FROM student;

--
-- Test for non-unique functions of GridDB in WHERE clause
--
-- char_length
--Testcase 5:
EXPLAIN VERBOSE
SELECT * FROM student WHERE char_length(name) > 4 ;
--Testcase 6:
SELECT * FROM student WHERE char_length(name) > 4 ;
--Testcase 7:
EXPLAIN VERBOSE
SELECT * FROM student WHERE char_length(name) < 6 ;
--Testcase 8:
SELECT * FROM student WHERE char_length(name) < 6 ;

--Testcase 336:
EXPLAIN VERBOSE
SELECT * FROM student WHERE concat(name,' and george') = 'fred and george';
--Testcase 337:
SELECT * FROM student WHERE concat(name,' and george') = 'fred and george';

--substr
--Testcase 9:
EXPLAIN VERBOSE
SELECT * FROM student WHERE substr(name,2,3) = 'red';
--Testcase 10:
SELECT * FROM student WHERE substr(name,2,3) = 'red';
--Testcase 11:
EXPLAIN VERBOSE
SELECT * FROM student WHERE substr(name,1,3) <> 'fre';
--Testcase 12:
SELECT * FROM student WHERE substr(name,1,3) <> 'fre';

--upper
--Testcase 13:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE upper(name) = 'FRED';
--Testcase 14:
SELECT * FROM student WHERE upper(name) = 'FRED';
--Testcase 15:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE upper(name) <> 'FRED';
--Testcase 16:
SELECT * FROM student WHERE upper(name) <> 'FRED';

--lower
--Testcase 17:
INSERT INTO student VALUES ('GEORGE',30,'(1.2,-3.5)',3.8);
--Testcase 18:
INSERT INTO student VALUES ('BOB',35,'(5.2,3.8)',2.5);
--Testcase 19:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE lower(name) = 'george';
--Testcase 20:
SELECT * FROM student WHERE lower(name) = 'george';
--Testcase 21:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE lower(name) <> 'bob';
--Testcase 22:
SELECT * FROM student WHERE lower(name) <> 'bob';

--round
--Testcase 23:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE round(gpa) > 3.5;
--Testcase 24:
SELECT * FROM student WHERE round(gpa) > 3.5;
--Testcase 25:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE round(gpa) <= 3;
--Testcase 26:
SELECT * FROM student WHERE round(gpa) <= 3;

--floor
--Testcase 27:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE floor(gpa) = 3;
--Testcase 28:
SELECT * FROM student WHERE floor(gpa) = 3;
--Testcase 29:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE floor(gpa) < 2;
--Testcase 30:
SELECT * FROM student WHERE floor(gpa) < 3;

--ceiling
--Testcase 31:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE ceiling(gpa) >= 3;
--Testcase 32:
SELECT * FROM student WHERE ceiling(gpa) >= 3;
--Testcase 33:
EXPLAIN VERBOSE 
SELECT * FROM student WHERE ceiling(gpa) = 4;
--Testcase 34:
SELECT * FROM student WHERE ceiling(gpa) = 4;

--
--Test for unique functions of GridDB in WHERE clause: time functions
--
--Testcase 35:
\d time_series2
--Testcase 36:
INSERT INTO time_series2 VALUES ('2020-12-29 04:40:00', '2000-12-29 04:40:00', '2020-01-05T20:30:00Z', 't', 0, 5, 100, 2000, 65.4, 2391.5, '7C8C893C8087F07883AF', ARRAY['aaa','bbb','ccc'],
ARRAY['t'::boolean,'f'::boolean,'t'::boolean],ARRAY[1,2,3,4],ARRAY[1,2,3],ARRAY[444,333,222],ARRAY[44444,22222,45555],ARRAY[2.3,4.2,62.1],ARRAY[444.2,554.3,5432.5],
ARRAY['2020-12-29 04:45:00'::timestamp,'2020-12-29 04:46:00'::timestamp]);
--Testcase 37:
INSERT INTO time_series2 VALUES ('2020-12-29 04:50:00', '2020-03-17 04:40:00', '2020-01-05T23:30:00Z', 'f', 20, 120, 1000, 2500, 44.5, 2432.78, '09C4A3E91E60BCD22357', ARRAY['abcdef','ghijkl','mnopqs'],
ARRAY['f'::boolean,'f'::boolean,'t'::boolean],ARRAY[2,9,11,25],ARRAY[45,22,35,50],ARRAY[4445,33,2221],ARRAY[25413,77548,36251],ARRAY[4.2,24.54,70.55],ARRAY[4431.63,-200.14,3265.1],
ARRAY['2020-12-31 14:00:00'::timestamp,'2020-12-31 14:45:00'::timestamp, '2020-01-01 15:00:00']);
--Testcase 38:
INSERT INTO time_series2 VALUES ('2020-12-29 05:00:30', '2020-12-11 02:30:30', '2020-01-05T22:00:00Z', 'f', 11, 175, 1234, 7705, 15.72, 1435.22, '2F63A64D987344F83AC8', ARRAY['7777a32ebea96a918b0f','40ee382083b987e94dd1','d417cf517eca8c2a709a'],
ARRAY['t'::boolean,'t'::boolean,'f'::boolean,'f'::boolean],ARRAY[12,29,1,14,16],ARRAY[255,124,77,51],ARRAY[2697,2641,7777],ARRAY[12475,12346,12654],ARRAY[22.5,12.11,23.54],ARRAY[3567.21,2124.23,-1254.11],
ARRAY['2020-05-19 14:15:20'::timestamp,'2020-11-14 17:45:14'::timestamp, '2020-09-05 01:24:06']);

--griddb_timestamp: push down timestamp function to GridDB
--Testcase 39:
EXPLAIN VERBOSE
SELECT date, strcol, booleancol, bytecol, shortcol, intcol, longcol, floatcol, doublecol FROM time_series2 WHERE griddb_timestamp(strcol) > '2020-01-05 21:00:00';
--Testcase 40:
SELECT date, strcol, booleancol, bytecol, shortcol, intcol, longcol, floatcol, doublecol FROM time_series2 WHERE griddb_timestamp(strcol) > '2020-01-05 21:00:00';
--Testcase 41:
EXPLAIN VERBOSE
SELECT date, strcol FROM time_series2 WHERE date < griddb_timestamp(strcol);
--Testcase 42:
SELECT date, strcol FROM time_series2 WHERE date < griddb_timestamp(strcol);
--griddb_timestamp: push down timestamp function to GridDB and gets error because GridDB only support YYYY-MM-DDThh:mm:ss.SSSZ format for timestamp function
--Testcase 43:
UPDATE time_series2 SET strcol = '2020-01-05 21:00:00';
--Testcase 44:
EXPLAIN VERBOSE
SELECT date, strcol FROM time_series2 WHERE griddb_timestamp(strcol) = '2020-01-05 21:00:00';
--Testcase 45:
SELECT date, strcol FROM time_series2 WHERE griddb_timestamp(strcol) = '2020-01-05 21:00:00';

--timestampadd
--YEAR
--Testcase 46:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('YEAR', date, -1) > '2019-12-29 05:00:00';
--Testcase 47:
SELECT date FROM time_series2 WHERE timestampadd('YEAR', date, -1) > '2019-12-29 05:00:00';
--Testcase 48:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('YEAR', date, 5) >= '2025-12-29 04:50:00';
--Testcase 49:
SELECT date FROM time_series2 WHERE timestampadd('YEAR', date, 5) >= '2025-12-29 04:50:00';
--Testcase 50:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('YEAR', date, 5) >= '2025-12-29';
--Testcase 51:
SELECT date FROM time_series2 WHERE timestampadd('YEAR', date, 5) >= '2025-12-29';
--MONTH
--Testcase 52:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MONTH', date, -3) > '2020-06-29 05:00:00';
--Testcase 53:
SELECT date FROM time_series2 WHERE timestampadd('MONTH', date, -3) > '2020-06-29 05:00:00';
--Testcase 54:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MONTH', date, 3) = '2021-03-29 05:00:30';
--Testcase 55:
SELECT date FROM time_series2 WHERE timestampadd('MONTH', date, 3) = '2021-03-29 05:00:30';
--Testcase 56:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MONTH', date, 3) >= '2021-03-29';
--Testcase 57:
SELECT date FROM time_series2 WHERE timestampadd('MONTH', date, 3) >= '2021-03-29';
--DAY
--Testcase 58:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('DAY', date, -3) > '2020-06-29 05:00:00';
--Testcase 59:
SELECT date FROM time_series2 WHERE timestampadd('DAY', date, -3) > '2020-06-29 05:00:00';
--Testcase 60:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('DAY', date, 3) = '2021-01-01 05:00:30';
--Testcase 61:
SELECT date FROM time_series2 WHERE timestampadd('DAY', date, 3) = '2021-01-01 05:00:30';
--Testcase 62:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('DAY', date, 3) >= '2021-01-01';
--Testcase 63:
SELECT date FROM time_series2 WHERE timestampadd('DAY', date, 3) >= '2021-01-01';
--HOUR
--Testcase 64:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('HOUR', date, -1) > '2020-12-29 04:00:00';
--Testcase 65:
SELECT date FROM time_series2 WHERE timestampadd('HOUR', date, -1) > '2020-12-29 04:00:00';
--Testcase 66:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('HOUR', date, 2) >= '2020-12-29 06:50:00';
--Testcase 67:
SELECT date FROM time_series2 WHERE timestampadd('HOUR', date, 2) >= '2020-12-29 06:50:00';
--MINUTE
--Testcase 68:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MINUTE', date, 20) = '2020-12-29 05:00:00';
--Testcase 69:
SELECT date FROM time_series2 WHERE timestampadd('MINUTE', date, 20) = '2020-12-29 05:00:00';
--Testcase 70:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MINUTE', date, -50) <= '2020-12-29 04:00:00';
--Testcase 71:
SELECT date FROM time_series2 WHERE timestampadd('MINUTE', date, -50) <= '2020-12-29 04:00:00';
--SECOND
--Testcase 72:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('SECOND', date, 25) >= '2020-12-29 04:40:30';
--Testcase 73:
SELECT date FROM time_series2 WHERE timestampadd('SECOND', date, 25) >= '2020-12-29 04:40:30';
--Testcase 74:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('SECOND', date, -50) <= '2020-12-29 04:00:00';
--Testcase 75:
SELECT date FROM time_series2 WHERE timestampadd('SECOND', date, -30) = '2020-12-29 05:00:00';
--MILLISECOND
--Testcase 76:
INSERT INTO time_series2 VALUES ('2020-12-29 05:10:00.120', '2020-12-29 05:10:00.563', '2020-01-05T20:30:30Z', 't', 0, 5, 30000000, 2000, 65.4, 2391.5, '7731b23fa1437ab784e3', ARRAY['aaa','bbb','ccc'],
ARRAY['t'::boolean,'f'::boolean,'t'::boolean],ARRAY[1,2,3,4],ARRAY[1,2,3],ARRAY[444,333,222],ARRAY[44444,22222,45555],ARRAY[2.3,4.2,62.1],ARRAY[444.2,554.3,5432.5],
ARRAY['2020-12-29 04:45:00'::timestamp,'2020-12-29 04:46:00'::timestamp]);
--Testcase 77:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MILLISECOND', date, 300) = '2020-12-29 05:10:00.420';
--Testcase 78:
SELECT date FROM time_series2 WHERE timestampadd('MILLISECOND', date, 300) = '2020-12-29 05:10:00.420';
--Testcase 79:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MILLISECOND', date, -55) = '2020-12-29 05:10:00.065';
--Testcase 80:
SELECT date FROM time_series2 WHERE timestampadd('MILLISECOND', date, -55) = '2020-12-29 05:10:00.065';
--Input wrong unit
--Testcase 81:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampadd('MICROSECOND', date, -55) = '2020-12-29 05:10:00.065';

--timestampdiff
--YEAR
--Testcase 82:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampdiff('YEAR', date, '2018-01-04 08:48:00') > 0;
--Testcase 83:
SELECT date FROM time_series2 WHERE timestampdiff('YEAR', date, '2018-01-04 08:48:00') > 0;
--Testcase 84:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('YEAR', '2015-07-15 08:48:00', date2) < 5;
--Testcase 85:
SELECT date2 FROM time_series2 WHERE timestampdiff('YEAR', '2015-07-15 08:48:00', date2) < 5;
--Testcase 86:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('YEAR', date, date2) > 10;
--Testcase 87:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('YEAR', date, date2) > 10;
--MONTH
--Testcase 88:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampdiff('MONTH', date, '2020-11-04 08:48:00') = 1;
--Testcase 89:
SELECT date FROM time_series2 WHERE timestampdiff('MONTH', date, '2020-11-04 08:48:00') = 1;
--Testcase 90:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('YEAR', '2020-02-15 08:48:00', date2) < 5;
--Testcase 91:
SELECT date2 FROM time_series2 WHERE timestampdiff('YEAR', '2020-02-15 08:48:00', date2) < 5;
--Testcase 92:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('MONTH', date, date2) < 10;
--Testcase 93:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('MONTH', date, date2) < 10;
--DAY
--Testcase 94:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('DAY', date2, '2020-12-04 08:48:00') > 20;
--Testcase 95:
SELECT date2 FROM time_series2 WHERE timestampdiff('DAY', date2, '2020-12-04 08:48:00') > 20;
--Testcase 96:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('DAY', '2020-02-15 08:48:00', date2) < 5;
--Testcase 97:
SELECT date2 FROM time_series2 WHERE timestampdiff('DAY', '2020-02-15 08:48:00', date2) < 5;
--Testcase 98:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('DAY', date, date2) > 10;
--Testcase 99:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('DAY', date, date2) > 10;
--HOUR
--Testcase 100:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE timestampdiff('HOUR', date, '2020-12-29 07:40:00') < 0;
--Testcase 101:
SELECT date FROM time_series2 WHERE timestampdiff('HOUR', date, '2020-12-29 07:40:00') < 0;
--Testcase 102:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('HOUR', '2020-12-15 08:48:00', date2) > 3.5;
--Testcase 103:
SELECT date2 FROM time_series2 WHERE timestampdiff('HOUR', '2020-12-15 08:48:00', date2) > 3.5;
--Testcase 104:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('HOUR', date, date2) > 10;
--Testcase 105:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('HOUR', date, date2) > 10;
--MINUTE
--Testcase 106:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('MINUTE', date2, '2020-12-04 08:48:00') > 20;
--Testcase 107:
SELECT date2 FROM time_series2 WHERE timestampdiff('MINUTE', date2, '2020-12-04 08:48:00') > 20;
--Testcase 108:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('MINUTE', '2020-02-15 08:48:00', date2) < 5;
--Testcase 109:
SELECT date2 FROM time_series2 WHERE timestampdiff('MINUTE', '2020-02-15 08:48:00', date2) < 5;
--Testcase 110:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('MINUTE', date, date2) > 10;
--Testcase 111:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('MINUTE', date, date2) > 10;
--SECOND
--Testcase 112:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('SECOND', date2, '2020-12-04 08:48:00') > 1000;
--Testcase 113:
SELECT date2 FROM time_series2 WHERE timestampdiff('SECOND', date2, '2020-12-04 08:48:00') > 1000;
--Testcase 114:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('SECOND', '2020-03-17 04:50:00', date2) < 100;
--Testcase 115:
SELECT date2 FROM time_series2 WHERE timestampdiff('SECOND', '2020-03-17 04:50:00', date2) < 100;
--Testcase 116:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('SECOND', date, date2) > 1600000;
--Testcase 117:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('SECOND', date, date2) > 1600000;
--MILLISECOND
--Testcase 118:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('MILLISECOND', date2, '2020-12-04 08:48:00') > 200;
--Testcase 119:
SELECT date2 FROM time_series2 WHERE timestampdiff('MILLISECOND', date2, '2020-12-04 08:48:00') > 200;
--Testcase 120:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('MILLISECOND', '2020-03-17 08:48:00', date2) < 0;
--Testcase 121:
SELECT date2 FROM time_series2 WHERE timestampdiff('MILLISECOND', '2020-03-17 08:48:00', date2) < 0;
--Testcase 122:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('MILLISECOND', date, date2) = -443;
--Testcase 123:
SELECT date, date2 FROM time_series2 WHERE timestampdiff('MILLISECOND', date, date2) = -443;
--Input wrong unit
--Testcase 124:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('MICROSECOND', date2, '2020-12-04 08:48:00') > 20;
--Testcase 125:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE timestampdiff('DECADE', '2020-02-15 08:48:00', date2) < 5;
--Testcase 126:
EXPLAIN VERBOSE
SELECT date, date2 FROM time_series2 WHERE timestampdiff('NANOSECOND', date, date2) > 10;

--to_timestamp_ms
--Normal case
--Testcase 127:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE to_timestamp_ms(intcol) > '1970-01-01 1:00:00';
--Testcase 128:
SELECT date FROM time_series2 WHERE to_timestamp_ms(intcol) > '1970-01-01 1:00:00';
--Return error if column contains -1 value
--Testcase 129:
INSERT INTO time_series2 VALUES ('2020-12-29 05:20:00.120', '2020-12-29 05:10:00.563', '2020-01-05T20:30:30Z', 't', 0, 5, -1, 2000, 65.4, 2391.5, '7731b23fa1437ab784e3', ARRAY['aaa','bbb','ccc'],
ARRAY['t'::boolean,'f'::boolean,'t'::boolean],ARRAY[1,2,3,4],ARRAY[1,2,3],ARRAY[444,333,222],ARRAY[44444,22222,45555],ARRAY[2.3,4.2,62.1],ARRAY[444.2,554.3,5432.5],
ARRAY['2020-12-29 04:45:00'::timestamp,'2020-12-29 04:46:00'::timestamp]);
--Testcase 130:
SELECT date FROM time_series2 WHERE to_timestamp_ms(intcol) > '1970-01-01 1:00:00';

--to_epoch_ms
--Testcase 131:
EXPLAIN VERBOSE
SELECT date FROM time_series2 WHERE intcol < to_epoch_ms(date);
--Testcase 132:
SELECT date FROM time_series2 WHERE intcol < to_epoch_ms(date);
--Testcase 133:
EXPLAIN VERBOSE
SELECT date2 FROM time_series2 WHERE to_epoch_ms(date2) < 1000000000000;

-- Test for now() pushdown function of griddb
-- griddb_now as parameter of timestampdiff()
--Testcase 383:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE timestampdiff('YEAR', date, griddb_now()) < 0;
--Testcase 384:
SELECT * FROM time_series WHERE timestampdiff('YEAR', date, griddb_now()) < 0;

--Testcase 385:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE timestampdiff('YEAR', date, griddb_now()) > 0;
--Testcase 386:
SELECT * FROM time_series WHERE timestampdiff('YEAR', date, griddb_now()) > 0;

--Testcase 387:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE timestampdiff('HOUR', griddb_now(), '2020-12-04 08:48:00') > 0;
--Testcase 388:
SELECT * FROM time_series WHERE timestampdiff('HOUR', griddb_now(), '2020-12-04 08:48:00') > 0;

--Testcase 389:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE timestampdiff('YEAR', griddb_now(), '2032-12-04 08:48:00') < 0;
--Testcase 390:
SELECT * FROM time_series WHERE timestampdiff('YEAR', griddb_now(), '2032-12-04 08:48:00') < 0;

-- griddb_now as parameter of timestampadd()
--Testcase 391:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE date > timestampadd('YEAR', griddb_now(), -1);
--Testcase 392:
SELECT * FROM time_series WHERE date > timestampadd('YEAR', griddb_now(), -1);

--Testcase 393:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE date < timestampadd('YEAR', griddb_now(), -1);
--Testcase 394:
SELECT * FROM time_series WHERE date < timestampadd('YEAR', griddb_now(), -1);

-- griddb_now() in expression
--Testcase 395:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE date < griddb_now();
--Testcase 396:
SELECT * FROM time_series WHERE date < griddb_now();

--Testcase 397:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE date > griddb_now();
--Testcase 398:
SELECT * FROM time_series WHERE date > griddb_now();

--Testcase 399:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE date <= griddb_now();
--Testcase 400:
SELECT * FROM time_series WHERE date <= griddb_now();

-- griddb_now() to_epoch_ms()
--Testcase 401:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE to_epoch_ms(griddb_now()) > 0;
--Testcase 402:
SELECT * FROM time_series WHERE to_epoch_ms(griddb_now()) > 0;

-- griddb_now() other cases
--Testcase 403:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE griddb_now() IS NOT NULL;
--Testcase 404:
SELECT * FROM time_series WHERE griddb_now() IS NOT NULL;

--Testcase 405:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE timestampdiff('YEAR', date, griddb_now()) > 0 OR timestampdiff('YEAR', date, griddb_now()) < 0;
--Testcase 406:
SELECT * FROM time_series WHERE timestampdiff('YEAR', date, griddb_now()) > 0 OR timestampdiff('YEAR', date, griddb_now()) < 0;

--Testcase 407:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE timestampdiff('YEAR', date, griddb_now()) < 0 ORDER BY value1 ASC;

--Testcase 408:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE timestampdiff('YEAR', date, griddb_now()) < 0 ORDER BY value1 DESC;

--Testcase 409:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE timestampdiff('YEAR', date, griddb_now()) < 0 ORDER BY value2 ASC;

--Testcase 410:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE timestampdiff('YEAR', date, griddb_now()) < 0 ORDER BY value2 DESC;

--
--Test for unique functions of GridDB in WHERE clause: array functions
--
--array_length
--Testcase 134:
EXPLAIN VERBOSE
SELECT boolarray FROM time_series2 WHERE array_length(boolarray) = 3;
--Testcase 135:
SELECT boolarray FROM time_series2 WHERE array_length(boolarray) = 3;
--Testcase 136:
EXPLAIN VERBOSE
SELECT stringarray FROM time_series2 WHERE array_length(stringarray) = 3;
--Testcase 137:
SELECT stringarray FROM time_series2 WHERE array_length(stringarray) = 3;
--Testcase 138:
EXPLAIN VERBOSE
SELECT bytearray, shortarray FROM time_series2 WHERE array_length(bytearray) > array_length(shortarray);
--Testcase 139:
SELECT bytearray, shortarray FROM time_series2 WHERE array_length(bytearray) > array_length(shortarray);
--Testcase 140:
EXPLAIN VERBOSE
SELECT integerarray, longarray FROM time_series2 WHERE array_length(integerarray) = array_length(longarray);
--Testcase 141:
SELECT integerarray, longarray FROM time_series2 WHERE array_length(integerarray) = array_length(longarray);
--Testcase 142:
EXPLAIN VERBOSE
SELECT floatarray, doublearray FROM time_series2 WHERE array_length(floatarray) - array_length(doublearray) = 0;
--Testcase 143:
SELECT floatarray, doublearray FROM time_series2 WHERE array_length(floatarray) - array_length(doublearray) = 0;
--Testcase 144:
EXPLAIN VERBOSE
SELECT timestamparray FROM time_series2 WHERE array_length(timestamparray) < 3;
--Testcase 145:
SELECT timestamparray FROM time_series2 WHERE array_length(timestamparray) < 3;

--element
--Normal case
--Testcase 146:
EXPLAIN VERBOSE
SELECT boolarray FROM time_series2 WHERE element(1, boolarray) = 'f';
--Testcase 147:
SELECT boolarray FROM time_series2 WHERE element(1, boolarray) = 'f';
--Testcase 148:
EXPLAIN VERBOSE
SELECT stringarray FROM time_series2 WHERE element(1, stringarray) != 'bbb';
--Testcase 149:
SELECT stringarray FROM time_series2 WHERE element(1, stringarray) != 'bbb';
--Testcase 150:
EXPLAIN VERBOSE
SELECT bytearray, shortarray FROM time_series2 WHERE element(0, bytearray) = element(0, shortarray);
--Testcase 151:
SELECT bytearray, shortarray FROM time_series2 WHERE element(0, bytearray) = element(0, shortarray);
--Testcase 152:
EXPLAIN VERBOSE
SELECT integerarray, longarray FROM time_series2 WHERE element(0, integerarray)*100+44 = element(0,longarray);
--Testcase 153:
SELECT integerarray, longarray FROM time_series2 WHERE element(0, integerarray)*100+44 = element(0,longarray);
--Testcase 154:
EXPLAIN VERBOSE
SELECT floatarray, doublearray FROM time_series2 WHERE element(2, floatarray)*10 < element(0,doublearray);
--Testcase 155:
SELECT floatarray, doublearray FROM time_series2 WHERE element(2, floatarray)*10 < element(0,doublearray);
--Testcase 156:
EXPLAIN VERBOSE
SELECT timestamparray FROM time_series2 WHERE element(1,timestamparray) > '2020-12-29 04:00:00';
--Testcase 157:
SELECT timestamparray FROM time_series2 WHERE element(1,timestamparray) > '2020-12-29 04:00:00';
--Return error when getting non-existent element
--Testcase 158:
EXPLAIN VERBOSE
SELECT timestamparray FROM time_series2 WHERE element(2,timestamparray) > '2020-12-29 04:00:00';
--Testcase 159:
SELECT timestamparray FROM time_series2 WHERE element(2,timestamparray) > '2020-12-29 04:00:00';

--
--if user selects non-unique functions which Griddb only supports in WHERE clause => do not push down
--if user selects unique functions which Griddb only supports in WHERE clause => still push down, return error of Griddb
--
--Testcase 160:
EXPLAIN VERBOSE
SELECT char_length(name) FROM student;
--Testcase 161:
SELECT char_length(name) FROM student;
--Testcase 338:
EXPLAIN VERBOSE
SELECT concat(name,'abc') FROM student;
--Testcase 339:
SELECT concat(name,'abc') FROM student;
--Testcase 162:
EXPLAIN VERBOSE
SELECT substr(name,2,3) FROM student;
--Testcase 163:
SELECT substr(name,2,3) FROM student;
--Testcase 164:
EXPLAIN VERBOSE
SELECT element(1, timestamparray) FROM time_series2;
--Testcase 165:
SELECT element(1, timestamparray) FROM time_series2;
--Testcase 166:
EXPLAIN VERBOSE
SELECT upper(name) FROM student;
--Testcase 167:
SELECT upper(name) FROM student;
--Testcase 168:
EXPLAIN VERBOSE
SELECT lower(name) FROM student;
--Testcase 169:
SELECT lower(name) FROM student;
--Testcase 170:
EXPLAIN VERBOSE
SELECT round(gpa) FROM student;
--Testcase 171:
SELECT round(gpa) FROM student;
--Testcase 172:
EXPLAIN VERBOSE
SELECT floor(gpa) FROM student;
--Testcase 173:
SELECT floor(gpa) FROM student;
--Testcase 174:
EXPLAIN VERBOSE
SELECT ceiling(gpa) FROM student;
--Testcase 175:
SELECT ceiling(gpa) FROM student;
--Testcase 176:
EXPLAIN VERBOSE
SELECT griddb_timestamp(strcol) FROM time_series2;
--Testcase 177:
SELECT griddb_timestamp(strcol) FROM time_series2;
--Testcase 178:
EXPLAIN VERBOSE
SELECT timestampadd('YEAR', date, -1) FROM time_series2;
--Testcase 179:
SELECT timestampadd('YEAR', date, -1) FROM time_series2;
--Testcase 180:
EXPLAIN VERBOSE
SELECT timestampdiff('YEAR', date, '2018-01-04 08:48:00') FROM time_series2;
--Testcase 181:
SELECT timestampdiff('YEAR', date, '2018-01-04 08:48:00') FROM time_series2;
--Testcase 182:
EXPLAIN VERBOSE
SELECT to_timestamp_ms(intcol) FROM time_series2;
--Testcase 183:
SELECT to_timestamp_ms(intcol) FROM time_series2;
--Testcase 184:
EXPLAIN VERBOSE
SELECT to_epoch_ms(date) FROM time_series2;
--Testcase 185:
SELECT to_epoch_ms(date) FROM time_series2;
--Testcase 186:
EXPLAIN VERBOSE
SELECT array_length(boolarray) FROM time_series2;
--Testcase 187:
SELECT array_length(boolarray) FROM time_series2;
--Testcase 188:
EXPLAIN VERBOSE
SELECT element(1, stringarray) FROM time_series2;
--Testcase 189:
SELECT element(1, stringarray) FROM time_series2;

--
--Test for unique functions of GridDB in SELECT clause: time-series functions
--
--time_next
--specified time exist => return that row
--Testcase 190:
EXPLAIN VERBOSE
SELECT time_next('2018-12-01 10:00:00') FROM time_series;
--Testcase 191:
SELECT time_next('2018-12-01 10:00:00') FROM time_series;
--specified time does not exist => return the row whose time  is immediately after the specified time
--Testcase 192:
EXPLAIN VERBOSE
SELECT time_next('2018-12-01 10:05:00') FROM time_series;
--Testcase 193:
SELECT time_next('2018-12-01 10:05:00') FROM time_series;
--specified time does not exist, there is no time after the specified time => return no row
--Testcase 194:
EXPLAIN VERBOSE
SELECT time_next('2018-12-01 10:45:00') FROM time_series;
--Testcase 195:
SELECT time_next('2018-12-01 10:45:00') FROM time_series;

--time_next_only
--even though specified time exist, still return the row whose time is immediately after the specified time
--Testcase 196:
EXPLAIN VERBOSE
SELECT time_next_only('2018-12-01 10:00:00') FROM time_series;
--Testcase 197:
SELECT time_next_only('2018-12-01 10:00:00') FROM time_series;
--specified time does not exist => return the row whose time  is immediately after the specified time
--Testcase 198:
EXPLAIN VERBOSE
SELECT time_next_only('2018-12-01 10:05:00') FROM time_series;
--Testcase 199:
SELECT time_next_only('2018-12-01 10:05:00') FROM time_series;
--there is no time after the specified time => return no row
--Testcase 200:
EXPLAIN VERBOSE
SELECT time_next_only('2018-12-01 10:45:00') FROM time_series;
--Testcase 201:
SELECT time_next_only('2018-12-01 10:45:00') FROM time_series;

--time_prev
--specified time exist => return that row
--Testcase 202:
EXPLAIN VERBOSE
SELECT time_prev('2018-12-01 10:10:00') FROM time_series;
--Testcase 203:
SELECT time_prev('2018-12-01 10:10:00') FROM time_series;
--specified time does not exist => return the row whose time  is immediately before the specified time
--Testcase 204:
EXPLAIN VERBOSE
SELECT time_prev('2018-12-01 10:05:00') FROM time_series;
--Testcase 205:
SELECT time_prev('2018-12-01 10:05:00') FROM time_series;
--specified time does not exist, there is no time before the specified time => return no row
--Testcase 206:
EXPLAIN VERBOSE
SELECT time_prev('2018-12-01 09:45:00') FROM time_series;
--Testcase 207:
SELECT time_prev('2018-12-01 09:45:00') FROM time_series;

--time_prev_only
--even though specified time exist, still return the row whose time is immediately before the specified time
--Testcase 208:
EXPLAIN VERBOSE
SELECT time_prev_only('2018-12-01 10:10:00') FROM time_series;
--Testcase 209:
SELECT time_prev_only('2018-12-01 10:10:00') FROM time_series;
--specified time does not exist => return the row whose time  is immediately before the specified time
--Testcase 210:
EXPLAIN VERBOSE
SELECT time_prev_only('2018-12-01 10:05:00') FROM time_series;
--Testcase 211:
SELECT time_prev_only('2018-12-01 10:05:00') FROM time_series;
--there is no time before the specified time => return no row
--Testcase 212:
EXPLAIN VERBOSE
SELECT time_prev_only('2018-12-01 09:45:00') FROM time_series;
--Testcase 213:
SELECT time_prev_only('2018-12-01 09:45:00') FROM time_series;

--time_interpolated
--specified time exist => return that row
--Testcase 214:
EXPLAIN VERBOSE
SELECT time_interpolated(value1, '2018-12-01 10:10:00') FROM time_series;
--Testcase 215:
SELECT time_interpolated(value1, '2018-12-01 10:10:00') FROM time_series;
--specified time does not exist => return the row which has interpolated value.
--The column which is specified as the 1st parameter will be calculated by linearly interpolating the value of the previous and next rows.
--Other values will be equal to the values of rows previous to the specified time.
--Testcase 216:
EXPLAIN VERBOSE
SELECT time_interpolated(value1, '2018-12-01 10:05:00') FROM time_series;
--Testcase 217:
SELECT time_interpolated(value1, '2018-12-01 10:05:00') FROM time_series;
--specified time does not exist. There is no row before or after the specified time => can not calculate interpolated value, return no row.
--Testcase 218:
EXPLAIN VERBOSE
SELECT time_interpolated(value1, '2018-12-01 09:05:00') FROM time_series;
--Testcase 219:
SELECT time_interpolated(value1, '2018-12-01 09:05:00') FROM time_series;
--Testcase 220:
EXPLAIN VERBOSE
SELECT time_interpolated(value1, '2018-12-01 10:45:00') FROM time_series;
--Testcase 221:
SELECT time_interpolated(value1, '2018-12-01 10:45:00') FROM time_series;

--time_sampling by MINUTE
--rows for sampling exists => return those rows
--Testcase 222:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:20:00', 10, 'MINUTE') FROM time_series;
--Testcase 223:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:20:00', 10, 'MINUTE') FROM time_series;
--rows for sampling does not exist => return rows that contains interpolated values.
--The column which is specified as the 1st parameter will be calculated by linearly interpolating the value of the previous and next rows.
--Other values will be equal to the values of rows previous to the specified time.
--Testcase 224:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:05:00', '2018-12-01 10:35:00', 10, 'MINUTE') FROM time_series;
--Testcase 225:
SELECT time_sampling(value1, '2018-12-01 10:05:00', '2018-12-01 10:35:00', 10, 'MINUTE') FROM time_series;
--mix exist and non-exist sampling
--Testcase 226:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:40:00', 10, 'MINUTE') FROM time_series;
--Testcase 227:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:40:00', 10, 'MINUTE') FROM time_series;
--In linearly interpolating the value of the previous and next rows, if one of the values does not exist => the sampling row will not be returned
--Testcase 228:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 09:30:00', '2018-12-01 11:00:00', 10, 'MINUTE') FROM time_series;
--Testcase 229:
SELECT time_sampling(value1, '2018-12-01 09:30:00', '2018-12-01 11:00:00', 10, 'MINUTE') FROM time_series;
--if the first parameter is not set, * will be added as the first parameter.
--When specified time does not exist, all columns (except timestamp key column) will be equal to the values of rows previous to the specified time.
--Testcase 230:
UPDATE time_series SET value1 = 5 where date = '2018-12-01 10:40:00';
--Testcase 231:
EXPLAIN VERBOSE
SELECT time_sampling('2018-12-01 10:00:00', '2018-12-01 10:40:00', 10, 'MINUTE') FROM time_series;
--Testcase 232:
SELECT time_sampling('2018-12-01 10:00:00', '2018-12-01 10:40:00', 10, 'MINUTE') FROM time_series;

--time_sampling by HOUR
--Testcase 233:
DELETE FROM time_series;
--Testcase 234:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00', 1, 10.5);
--Testcase 235:
INSERT INTO time_series VALUES ('2018-12-01 12:00:00', 2, 9.4);
--Testcase 236:
INSERT INTO time_series VALUES ('2018-12-01 16:00:00', 3, 8);
--Testcase 237:
INSERT INTO time_series VALUES ('2018-12-01 17:00:00', 4, 7.2);
--Testcase 238:
INSERT INTO time_series VALUES ('2018-12-01 20:00:00', 5, 5.6);
--rows for sampling exists => return those rows
--Testcase 239:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 12:00:00', 2, 'HOUR') FROM time_series;
--Testcase 240:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 12:00:00', 2, 'HOUR') FROM time_series;
--rows for sampling does not exist => return rows that contains interpolated values.
--The column which is specified as the 1st parameter will be calculated by linearly interpolating the value of the previous and next rows.
--Other values will be equal to the values of rows previous to the specified time.
--Testcase 241:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:05:00', '2018-12-01 21:00:00', 3, 'HOUR') FROM time_series;
--Testcase 242:
SELECT time_sampling(value1, '2018-12-01 10:05:00', '2018-12-01 21:00:00', 3, 'HOUR') FROM time_series;
--mix exist and non-exist sampling
--Testcase 243:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 21:40:00', 2, 'HOUR') FROM time_series;
--Testcase 244:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 21:40:00', 2, 'HOUR') FROM time_series;
--In linearly interpolating the value of the previous and next rows, if one of the values does not exist => the sampling row will not be returned
--Testcase 245:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 6:00:00', '2018-12-01 23:00:00', 3, 'HOUR') FROM time_series;
--Testcase 246:
SELECT time_sampling(value1, '2018-12-01 6:00:00', '2018-12-01 23:00:00', 3, 'HOUR') FROM time_series;
--if the first parameter is not set, * will be added as the first parameter.
--When specified time does not exist, all columns (except timestamp key column) will be equal to the values of rows previous to the specified time.
--Testcase 247:
DELETE FROM time_series WHERE value1 = 4;
--Testcase 248:
EXPLAIN VERBOSE
SELECT time_sampling('2018-12-01 10:00:00', '2018-12-01 21:40:00', 2, 'HOUR') FROM time_series;
--Testcase 249:
SELECT time_sampling('2018-12-01 10:00:00', '2018-12-01 21:40:00', 2, 'HOUR') FROM time_series;

--time_sampling by DAY
--Testcase 250:
DELETE FROM time_series;
--Testcase 251:
INSERT INTO time_series VALUES ('2018-12-01 11:00:00', 4, 4);
--Testcase 252:
INSERT INTO time_series VALUES ('2018-12-02 11:00:00', 5, 3.2);
--Testcase 253:
INSERT INTO time_series VALUES ('2018-12-02 12:00:30', 6, 3);
--Testcase 254:
INSERT INTO time_series VALUES ('2018-12-03 12:00:30', 7, 2.8);
--rows for sampling exists => return those rows
--Testcase 255:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 11:00:00', '2018-12-02 11:00:00', 1, 'DAY') FROM time_series;
--Testcase 256:
SELECT time_sampling(value1, '2018-12-01 11:00:00', '2018-12-02 11:00:00', 1, 'DAY') FROM time_series;
--rows for sampling does not exist => return rows that contains interpolated values.
--The column which is specified as the 1st parameter will be calculated by linearly interpolating the value of the previous and next rows.
--Other values will be equal to the values of rows previous to the specified time.
--Testcase 257:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 09:00:00', '2018-12-03 12:00:00', 1, 'DAY') FROM time_series;
--Testcase 258:
SELECT time_sampling(value1, '2018-12-01 09:00:00', '2018-12-03 12:00:00', 1, 'DAY') FROM time_series;
--mix exist and non-exist sampling
--Testcase 259:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 11:00:00', '2018-12-03 12:00:00', 1, 'DAY') FROM time_series;
--Testcase 260:
SELECT time_sampling(value1, '2018-12-01 11:00:00', '2018-12-03 12:00:00', 1, 'DAY') FROM time_series;
--In linearly interpolating the value of the previous and next rows, if one of the values does not exist => the sampling row will not be returned
--Testcase 261:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 09:30:00', '2018-12-01 11:00:00', 1, 'DAY') FROM time_series;
--Testcase 262:
SELECT time_sampling(value1, '2018-12-01 09:30:00', '2018-12-05 11:00:00', 1, 'DAY') FROM time_series;
--if the first parameter is not set, * will be added as the first parameter.
--When specified time does not exist, all columns (except timestamp key column) will be equal to the values of rows previous to the specified time.
--Testcase 263:
DELETE FROM time_series WHERE value1 = 6;
--Testcase 264:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 11:00:00', '2018-12-03 12:00:00', 1, 'DAY') FROM time_series;
--Testcase 265:
SELECT time_sampling(value1, '2018-12-01 11:00:00', '2018-12-03 12:00:00', 1, 'DAY') FROM time_series;

--time_sampling by SECOND
--Testcase 266:
DELETE FROM time_series;
--Testcase 267:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00', 1, 1.5);
--Testcase 268:
INSERT INTO time_series VALUES ('2018-12-01 10:00:10', 2, 3.2);
--Testcase 269:
INSERT INTO time_series VALUES ('2018-12-01 10:00:20', 4, 3.5);
--Testcase 270:
INSERT INTO time_series VALUES ('2018-12-01 10:00:40', 6, 5.2);
--Testcase 271:
INSERT INTO time_series VALUES ('2018-12-01 10:01:10', 7, 6.7);
--rows for sampling exists => return those rows
--Testcase 272:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:00:20', 10, 'SECOND') FROM time_series;
--Testcase 273:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:00:20', 10, 'SECOND') FROM time_series;
--rows for sampling does not exist => return rows that contains interpolated values.
--The column which is specified as the 1st parameter will be calculated by linearly interpolating the value of the previous and next rows.
--Other values will be equal to the values of rows previous to the specified time.
--Testcase 274:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:03', '2018-12-01 10:00:35', 15, 'SECOND') FROM time_series;
--Testcase 275:
SELECT time_sampling(value1, '2018-12-01 10:00:03', '2018-12-01 10:00:35', 15, 'SECOND') FROM time_series;
--mix exist and non-exist sampling
--Testcase 276:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 11:00:00', 10, 'SECOND') FROM time_series;
--Testcase 277:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 11:00:00', 10, 'SECOND') FROM time_series;
--In linearly interpolating the value of the previous and next rows, if one of the values does not exist => the sampling row will not be returned
--Testcase 278:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 08:30:00', '2018-12-01 11:00:00', 20, 'SECOND') FROM time_series;
--Testcase 279:
SELECT time_sampling(value1, '2018-12-01 08:30:00', '2018-12-01 11:00:00', 20, 'SECOND') FROM time_series;
--if the first parameter is not set, * will be added as the first parameter.
--When specified time does not exist, all columns (except timestamp key column) will be equal to the values of rows previous to the specified time.
--Testcase 280:
DELETE FROM time_series WHERE value1 = 4;
--Testcase 281:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 11:00:00', 10, 'SECOND') FROM time_series;
--Testcase 282:
SELECT time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 11:00:00', 10, 'SECOND') FROM time_series;

--time_sampling by MILLISECOND
--Testcase 283:
DELETE FROM time_series;
--Testcase 284:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00.100', 1, 1.5);
--Testcase 285:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00.120', 2, 3.2);
--Testcase 286:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00.140', 4, 3.5);
--Testcase 287:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00.150', 6, 5.2);
--Testcase 288:
INSERT INTO time_series VALUES ('2018-12-01 10:00:00.160', 7, 6.7);
--rows for sampling exists => return those rows
--Testcase 289:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00.100', '2018-12-01 10:00:00.140', 20, 'MILLISECOND') FROM time_series;
--Testcase 290:
SELECT time_sampling(value1, '2018-12-01 10:00:00.100', '2018-12-01 10:00:00.140', 20, 'MILLISECOND') FROM time_series;
--rows for sampling does not exist => return rows that contains interpolated values.
--The column which is specified as the 1st parameter will be calculated by linearly interpolating the value of the previous and next rows.
--Other values will be equal to the values of rows previous to the specified time.
--Testcase 291:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00.115', '2018-12-01 10:00:00.155', 15, 'MILLISECOND') FROM time_series;
--Testcase 292:
SELECT time_sampling(value1, '2018-12-01 10:00:00.115', '2018-12-01 10:00:00.155', 15, 'MILLISECOND') FROM time_series;
--mix exist and non-exist sampling
--Testcase 293:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00.100', '2018-12-01 10:00:00.150', 5, 'MILLISECOND') FROM time_series;
--Testcase 294:
SELECT time_sampling(value1, '2018-12-01 10:00:00.100', '2018-12-01 10:00:00.150', 5, 'MILLISECOND') FROM time_series;
--In linearly interpolating the value of the previous and next rows, if one of the values does not exist => the sampling row will not be returned
--Testcase 295:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00.002', '2018-12-01 10:00:00.500', 20, 'MILLISECOND') FROM time_series;
--Testcase 296:
SELECT time_sampling(value1, '2018-12-01 10:00:00.002', '2018-12-01 10:00:00.500', 20, 'MILLISECOND') FROM time_series;
--if the first parameter is not set, * will be added as the first parameter.
--When specified time does not exist, all columns (except timestamp key column) will be equal to the values of rows previous to the specified time.
--Testcase 297:
DELETE FROM time_series WHERE value1 = 4;
--Testcase 298:
EXPLAIN VERBOSE
SELECT time_sampling(value1, '2018-12-01 10:00:00.100', '2018-12-01 10:00:00.150', 5, 'MILLISECOND') FROM time_series;
--Testcase 299:
SELECT time_sampling(value1, '2018-12-01 10:00:00.100', '2018-12-01 10:00:00.150', 5, 'MILLISECOND') FROM time_series;

--max_rows
--Testcase 300:
DELETE FROM time_series;
--Testcase 301:
INSERT INTO time_series VALUES ('2018-12-01 11:00:00', 4, 4);
--Testcase 302:
INSERT INTO time_series VALUES ('2018-12-02 11:00:00', 5, 3.2);
--Testcase 303:
INSERT INTO time_series VALUES ('2018-12-02 12:00:30', 6, 3);
--Testcase 304:
INSERT INTO time_series VALUES ('2018-12-03 12:00:30', 7, 2.8);
--Testcase 305:
EXPLAIN VERBOSE
SELECT max_rows(value2) FROM time_series;
--Testcase 306:
SELECT max_rows(value2) FROM time_series;
--Testcase 307:
EXPLAIN VERBOSE
SELECT max_rows(date) FROM time_series;
--Testcase 308:
SELECT max_rows(date) FROM time_series;

--min_rows
--Testcase 309:
EXPLAIN VERBOSE
SELECT min_rows(value2) FROM time_series;
--Testcase 310:
SELECT min_rows(value2) FROM time_series;
--Testcase 311:
EXPLAIN VERBOSE
SELECT min_rows(date) FROM time_series;
--Testcase 312:
SELECT min_rows(date) FROM time_series;

--
--if WHERE clause contains functions which Griddb only supports in SELECT clause => still push down, return error of Griddb
--
--Testcase 313:
EXPLAIN VERBOSE
SELECT * FROM time_series2 WHERE time_next('2018-12-01 10:00:00') = '"2020-01-05 21:00:00,{t,f,t}"';
--Testcase 314:
SELECT * FROM time_series2 WHERE time_next('2018-12-01 10:00:00') = '"2020-01-05 21:00:00,{t,f,t}"';
--Testcase 315:
EXPLAIN VERBOSE
SELECT date FROM time_series WHERE time_next_only('2018-12-01 10:00:00') = time_interpolated(value1, '2018-12-01 10:10:00');
--Testcase 316:
SELECT date FROM time_series WHERE time_next_only('2018-12-01 10:00:00') = time_interpolated(value1, '2018-12-01 10:10:00');
--Testcase 317:
EXPLAIN VERBOSE
SELECT * FROM time_series2 WHERE time_prev('2018-12-01 10:00:00') = '"2020-01-05 21:00:00,{t,f,t}"';
--Testcase 318:
SELECT * FROM time_series2 WHERE time_prev('2018-12-01 10:00:00') = '"2020-01-05 21:00:00,{t,f,t}"';
--Testcase 319:
EXPLAIN VERBOSE
SELECT date FROM time_series WHERE time_prev_only('2018-12-01 10:00:00') = time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:40:00', 10, 'MINUTE');
--Testcase 320:
SELECT date FROM time_series WHERE time_prev_only('2018-12-01 10:00:00') = time_sampling(value1, '2018-12-01 10:00:00', '2018-12-01 10:40:00', 10, 'MINUTE');
--Testcase 321:
EXPLAIN VERBOSE
SELECT * FROM time_series WHERE max_rows(date) = min_rows(value2);
--Testcase 322:
SELECT * FROM time_series WHERE max_rows(date) = min_rows(value2);

--
-- Test syntax (xxx()::time_series).*
--
--Testcase 323:
EXPLAIN VERBOSE
SELECT (time_sampling(value1, '2018-12-01 11:00:00', '2018-12-01 12:00:00', 20, 'MINUTE')::time_series).* FROM time_series;
--Testcase 324:
SELECT (time_sampling(value1, '2018-12-01 11:00:00', '2018-12-01 12:00:00', 20, 'MINUTE')::time_series).* FROM time_series;
--Testcase 325:
EXPLAIN VERBOSE
SELECT (time_sampling(value1, '2018-12-01 11:00:00', '2018-12-01 12:00:00', 20, 'MINUTE')::time_series).date FROM time_series;
--Testcase 326:
SELECT (time_sampling(value1, '2018-12-01 11:00:00', '2018-12-01 12:00:00', 20, 'MINUTE')::time_series).date FROM time_series;
--Testcase 327:
EXPLAIN VERBOSE
SELECT (time_sampling(value1, '2018-12-01 11:00:00', '2018-12-01 12:00:00', 20, 'MINUTE')::time_series).value1 FROM time_series;
--Testcase 328:
SELECT (time_sampling(value1, '2018-12-01 11:00:00', '2018-12-01 12:00:00', 20, 'MINUTE')::time_series).value1 FROM time_series;

--
-- Test syntax (xxx()::time_series2).*
--
--Testcase 376:
DELETE FROM time_series2;
--Testcase 377:
INSERT INTO time_series2 VALUES ('2020-12-20 05:00:00', '2020-12-20 08:00:00', '(3.1, 2.3)', 'f', 1, 175, 1234, 7705, 15.72, 1435.22, '2F63A64D987344F83AC8', ARRAY['7777a32ebea96a918b0f','40ee382083b987e94dd1','d417cf517eca8c2a709a'],
ARRAY['t'::boolean,'t'::boolean,'f'::boolean,'f'::boolean],ARRAY[12,29,1,14,16],ARRAY[255,124,77,51],ARRAY[2697,2641,7777],ARRAY[12475,12346,12654],ARRAY[22.5,12.11,23.54],ARRAY[3567.21,2124.23,-1254.11],
ARRAY['2020-05-19 14:15:20'::timestamp,'2020-11-14 17:45:14'::timestamp, '2020-09-05 01:24:06']);
--Testcase 377:
INSERT INTO time_series2 VALUES ('2020-12-20 06:00:00', '2020-12-20 09:00:00', '(1.3, 3.2)', 'f', 2, 175, 1234, 7705, 15.72, 1435.22, '2F63A64D987344F83AC8', ARRAY['7777a32ebea96a918b0f','40ee382083b987e94dd1','d417cf517eca8c2a709a'],
ARRAY['t'::boolean,'t'::boolean,'f'::boolean,'f'::boolean],ARRAY[12,29,1,14,16],ARRAY[255,124,77,51],ARRAY[2697,2641,7777],ARRAY[12475,12346,12654],ARRAY[22.5,12.11,23.54],ARRAY[3567.21,2124.23,-1254.11],
ARRAY['2020-05-19 14:15:20'::timestamp,'2020-11-14 17:45:14'::timestamp, '2020-09-05 01:24:06']);
--Testcase 378:
EXPLAIN VERBOSE
SELECT (time_sampling(bytecol, '2020-12-20 05:00:00', '2020-12-20 05:20:00', 20, 'MINUTE')::time_series2).* FROM time_series2;
--Testcase 379:
SELECT (time_sampling(bytecol, '2020-12-20 05:00:00', '2020-12-20 05:20:00', 20, 'MINUTE')::time_series2).* FROM time_series2;

--
-- Test aggregate function time_avg
--
--Testcase 340:
EXPLAIN VERBOSE
SELECT time_avg(value1) FROM time_series;
--Testcase 341:
SELECT time_avg(value1) FROM time_series;
--Testcase 342:
EXPLAIN VERBOSE
SELECT time_avg(value2) FROM time_series;
--Testcase 343:
SELECT time_avg(value2) FROM time_series;
-- GridDB does not support select multiple target in a query => do not push down, raise stub function error
--Testcase 344:
EXPLAIN VERBOSE
SELECT time_avg(value1), time_avg(value2) FROM time_series;
--Testcase 345:
SELECT time_avg(value1), time_avg(value2) FROM time_series;
-- Do not push down when expected type is not correct, raise stub function error
--Testcase 346:
EXPLAIN VERBOSE
SELECT time_avg(date) FROM time_series;
--Testcase 347:
SELECT time_avg(date) FROM time_series;
--Testcase 348:
EXPLAIN VERBOSE
SELECT time_avg(blobcol) FROM time_series2;
--Testcase 349:
SELECT time_avg(blobcol) FROM time_series2;

--
-- Test aggregate function min, max, count, sum, avg, variance, stddev
--
--Testcase 350:
EXPLAIN VERBOSE
SELECT min(age) FROM student;
--Testcase 351:
SELECT min(age) FROM student;

--Testcase 352:
EXPLAIN VERBOSE
SELECT max(gpa) FROM student;
--Testcase 353:
SELECT max(gpa) FROM student;

--Testcase 354:
EXPLAIN VERBOSE
SELECT count(*) FROM student;
--Testcase 355:
SELECT count(*) FROM student;
--Testcase 356:
EXPLAIN VERBOSE
SELECT count(*) FROM student WHERE gpa < 3.5 OR age < 42;
--Testcase 357:
SELECT count(*) FROM student WHERE gpa < 3.5 OR age < 42;

--Testcase 358:
EXPLAIN VERBOSE
SELECT sum(age) FROM student;
--Testcase 359:
SELECT sum(age) FROM student;
--Testcase 360:
EXPLAIN VERBOSE
SELECT sum(age) FROM student WHERE round(gpa) > 3.5;
--Testcase 361:
SELECT sum(age) FROM student WHERE round(gpa) > 3.5;

--Testcase 362:
EXPLAIN VERBOSE
SELECT avg(gpa) FROM student;
--Testcase 363:
SELECT avg(gpa) FROM student;
--Testcase 364:
EXPLAIN VERBOSE
SELECT avg(gpa) FROM student WHERE lower(name) = 'george';
--Testcase 365:
SELECT avg(gpa) FROM student WHERE lower(name) = 'george';

--Testcase 366:
EXPLAIN VERBOSE
SELECT variance(gpa) FROM student;
--Testcase 367:
SELECT variance(gpa) FROM student;
--Testcase 368:
EXPLAIN VERBOSE
SELECT variance(gpa) FROM student WHERE gpa > 3.5;
--Testcase 369:
SELECT variance(gpa) FROM student WHERE gpa > 3.5;

--Testcase 370:
EXPLAIN VERBOSE
SELECT stddev(age) FROM student;
--Testcase 371:
SELECT stddev(age) FROM student;
--Testcase 372:
EXPLAIN VERBOSE
SELECT stddev(age) FROM student WHERE char_length(name) > 4;
--Testcase 373:
SELECT stddev(age) FROM student WHERE char_length(name) > 4;

--Testcase 374:
EXPLAIN VERBOSE
SELECT max(gpa), min(age) FROM student;
--Testcase 375:
SELECT max(gpa), min(age) FROM student;
--Delete inserted values
--Testcase 329:
DELETE FROM student WHERE name = 'GEORGE' or name = 'BOB';

--Drop all foreign tables
--Testcase 330:
DROP FOREIGN TABLE student;
--Testcase 331:
DROP FOREIGN TABLE time_series;
--Testcase 332:
DROP FOREIGN TABLE time_series2;
--Testcase 333:
DROP USER MAPPING FOR public SERVER griddb_svr;
--Testcase 334:
DROP SERVER griddb_svr CASCADE;
--Testcase 335:
DROP EXTENSION griddb_fdw CASCADE;
