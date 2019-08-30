--
-- FLOAT4
--
CREATE EXTENSION griddb_fdw;
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS(host '239.0.0.1', port '31999', clustername 'griddbfdwTestCluster');
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS(username 'admin', password 'testadmin');
CREATE FOREIGN TABLE FLOAT4_TBL(id serial OPTIONS (rowkey 'true'), f1 float4) SERVER griddb_svr;

INSERT INTO FLOAT4_TBL(f1) VALUES ('    0.0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('1004.30   ');
INSERT INTO FLOAT4_TBL(f1) VALUES ('     -34.84    ');
INSERT INTO FLOAT4_TBL(f1) VALUES ('1.2345678901234e+20');
INSERT INTO FLOAT4_TBL(f1) VALUES ('1.2345678901234e-20');

-- test for over and under flow
INSERT INTO FLOAT4_TBL(f1) VALUES ('10e70');
INSERT INTO FLOAT4_TBL(f1) VALUES ('-10e70');
INSERT INTO FLOAT4_TBL(f1) VALUES ('10e-70');
INSERT INTO FLOAT4_TBL(f1) VALUES ('-10e-70');

-- bad input
INSERT INTO FLOAT4_TBL(f1) VALUES ('');
INSERT INTO FLOAT4_TBL(f1) VALUES ('       ');
INSERT INTO FLOAT4_TBL(f1) VALUES ('xyz');
INSERT INTO FLOAT4_TBL(f1) VALUES ('5.0.0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('5 . 0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('5.   0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('     - 3.0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('123            5');

-- special inputs
BEGIN;
DELETE FROM FLOAT4_TBL;
INSERT INTO FLOAT4_TBL(f1) VALUES ('NaN'::float4);
INSERT INTO FLOAT4_TBL(f1) VALUES ('nan'::float4);
INSERT INTO FLOAT4_TBL(f1) VALUES ('   NAN  '::float4);
INSERT INTO FLOAT4_TBL(f1) VALUES ('infinity'::float4);
INSERT INTO FLOAT4_TBL(f1) VALUES ('          -INFINiTY   '::float4);
SELECT f1 AS float4 FROM FLOAT4_TBL;
ROLLBACK;
-- bad special inputs
INSERT INTO FLOAT4_TBL(f1) VALUES ('N A N'::float4);
INSERT INTO FLOAT4_TBL(f1) VALUES ('NaN x'::float4);
INSERT INTO FLOAT4_TBL(f1) VALUES (' INFINITY    x'::float4);

BEGIN;
DELETE FROM FLOAT4_TBL;
INSERT INTO FLOAT4_TBL(f1) VALUES ('Infinity'::float4 + 100.0);
INSERT INTO FLOAT4_TBL(f1) VALUES ('Infinity'::float4 / 'Infinity'::float4);
INSERT INTO FLOAT4_TBL(f1) VALUES ('nan'::float4 / 'nan'::float4);
INSERT INTO FLOAT4_TBL(f1) VALUES ('nan'::numeric::float4);
SELECT f1 AS float4 FROM FLOAT4_TBL;
ROLLBACK;

SELECT '' AS five, * FROM FLOAT4_TBL;

SELECT '' AS four, f.* FROM FLOAT4_TBL f WHERE f.f1 <> '1004.3';

SELECT '' AS one, f.* FROM FLOAT4_TBL f WHERE f.f1 = '1004.3';

SELECT '' AS three, f.* FROM FLOAT4_TBL f WHERE '1004.3' > f.f1;

SELECT '' AS three, f.* FROM FLOAT4_TBL f WHERE  f.f1 < '1004.3';

SELECT '' AS four, f.* FROM FLOAT4_TBL f WHERE '1004.3' >= f.f1;

SELECT '' AS four, f.* FROM FLOAT4_TBL f WHERE  f.f1 <= '1004.3';

SELECT '' AS three, f.f1, f.f1 * '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0';

SELECT '' AS three, f.f1, f.f1 + '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0';

SELECT '' AS three, f.f1, f.f1 / '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0';

SELECT '' AS three, f.f1, f.f1 - '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0';

-- test divide by zero
SELECT '' AS bad, f.f1 / '0.0' from FLOAT4_TBL f;

SELECT '' AS five, * FROM FLOAT4_TBL;

-- test the unary float4abs operator
SELECT '' AS five, f.f1, @f.f1 AS abs_f1 FROM FLOAT4_TBL f;

UPDATE FLOAT4_TBL
   SET f1 = FLOAT4_TBL.f1 * '-1'
   WHERE FLOAT4_TBL.f1 > '0.0';

SELECT '' AS five, * FROM FLOAT4_TBL;

DROP FOREIGN TABLE FLOAT4_TBL;
DROP USER MAPPING FOR public SERVER griddb_svr;
DROP SERVER griddb_svr;
DROP EXTENSION griddb_fdw CASCADE;