--
-- NUMERIC
--
CREATE EXTENSION griddb_fdw;
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS(host '239.0.0.1', port '31999', clustername 'griddbfdwTestCluster');
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS(username 'admin', password 'testadmin');

CREATE FOREIGN TABLE num_data (idx serial, id int4, val float8) SERVER griddb_svr;
CREATE FOREIGN TABLE num_exp_add (idx serial, id1 int4, id2 int4, expected float8) SERVER griddb_svr;
CREATE FOREIGN TABLE num_exp_sub (idx serial, id1 int4, id2 int4, expected float8) SERVER griddb_svr;
CREATE FOREIGN TABLE num_exp_div (idx serial, id1 int4, id2 int4, expected float8) SERVER griddb_svr;
CREATE FOREIGN TABLE num_exp_mul (idx serial, id1 int4, id2 int4, expected float8) SERVER griddb_svr;
CREATE FOREIGN TABLE num_exp_sqrt (idx serial, id int4, expected float8) SERVER griddb_svr;
CREATE FOREIGN TABLE num_exp_ln (idx serial, id int4, expected float8) SERVER griddb_svr;
CREATE FOREIGN TABLE num_exp_log10 (idx serial, id int4, expected float8) SERVER griddb_svr;
CREATE FOREIGN TABLE num_exp_power_10_ln (idx serial, id int4, expected float8) SERVER griddb_svr;

CREATE FOREIGN TABLE num_result (idx serial OPTIONS (rowkey 'true'), id1 int4, id2 int4, result float8) SERVER griddb_svr;


-- ******************************
-- * The following EXPECTED results are computed by bc(1)
-- * with a scale of 200
-- ******************************

BEGIN TRANSACTION;
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,0,'0');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,0,'0');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,0,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,0,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,1,'0');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,1,'0');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,1,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,1,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,2,'-34338492.215397047');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,2,'34338492.215397047');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,2,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,2,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,3,'4.31');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,3,'-4.31');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,3,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,3,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,4,'7799461.4119');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,4,'-7799461.4119');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,4,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,4,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,5,'16397.038491');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,5,'-16397.038491');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,5,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,5,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,6,'93901.57763026');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,6,'-93901.57763026');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,6,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,6,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,7,'-83028485');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,7,'83028485');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,7,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,7,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,8,'74881');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,8,'-74881');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,8,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,8,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,9,'-24926804.045047420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,9,'24926804.045047420');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,9,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,9,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,0,'0');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,0,'0');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,0,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,0,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,1,'0');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,1,'0');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,1,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,1,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,2,'-34338492.215397047');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,2,'34338492.215397047');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,2,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,2,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,3,'4.31');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,3,'-4.31');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,3,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,3,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,4,'7799461.4119');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,4,'-7799461.4119');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,4,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,4,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,5,'16397.038491');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,5,'-16397.038491');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,5,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,5,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,6,'93901.57763026');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,6,'-93901.57763026');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,6,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,6,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,7,'-83028485');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,7,'83028485');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,7,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,7,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,8,'74881');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,8,'-74881');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,8,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,8,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,9,'-24926804.045047420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,9,'24926804.045047420');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,9,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,9,'0');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,0,'-34338492.215397047');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,0,'-34338492.215397047');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,0,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,0,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,1,'-34338492.215397047');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,1,'-34338492.215397047');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,1,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,1,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,2,'-68676984.430794094');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,2,'0');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,2,'1179132047626883.596862135856320209');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,2,'1.00000000000000000000');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,3,'-34338487.905397');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,3,'-34338496.525397047');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,3,'-147998901.44836127257');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,3,'-7967167.56737751');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,4,'-26539030.803497047');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,4,'-42137953.627297047');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,4,'-267821744976817.8111137106593');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,4,'-4.40267480046830116685');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,5,'-34322095.176906047');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,5,'-34354889.253888047');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,5,'-563049578578.769242506736077');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,5,'-2094.18866914563535496429');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,6,'-34244590.637766787');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,6,'-34432393.793027307');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,6,'-3224438592470.18449811926184222');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,6,'-365.68599891479766440940');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,7,'-117366977.215397047');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,7,'48689992.784602953');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,7,'2851072985828710.485883795');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,7,'.41357483778485235518');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,8,'-34263611.215397047');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,8,'-34413373.215397047');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,8,'-2571300635581.146276407');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,8,'-458.57416721727870888476');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,9,'-59265296.260444467');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,9,'-9411688.17034962');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,9,'855948866655588.453741509242968740');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,9,'1.37757299946438931811');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,0,'4.31');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,0,'4.31');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,0,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,0,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,1,'4.31');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,1,'4.31');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,1,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,1,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,2,'-34338487.905397');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,2,'34338496.525397047');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,2,'-147998901.44836127257');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,2,'-.000000125515120843525');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,3,'8.62');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,3,'0');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,3,'18.5761');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,3,'1.00000000000000000000');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,4,'7799465.7219');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,4,'-7799457.1019');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,4,'33615678.685289');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,4,'.000000552602259615521');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,5,'16401.348491');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,5,'-16392.728491');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,5,'70671.23589621');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,5,'.00026285234387695504');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,6,'93905.88763026');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,6,'-93897.26763026');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,6,'404715.7995864206');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,6,'.0000458991223445759');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,7,'-83028480.69');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,7,'83028489.31');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,7,'-357852770.35');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,7,'-.000000051909895742407');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,8,'74885.31');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,8,'-74876.69');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,8,'322737.11');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,8,'.00005755799201399553');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,9,'-24926799.735047420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,9,'24926808.355047420');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,9,'-107434525.43415438020');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,9,'-.00000017290624149855');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,0,'7799461.4119');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,0,'7799461.4119');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,0,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,0,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,1,'7799461.4119');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,1,'7799461.4119');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,1,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,1,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,2,'-26539030.803497047');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,2,'42137953.627297047');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,2,'-267821744976817.8111137106593');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,2,'-.22713465002993920385');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,3,'7799465.7219');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,3,'7799457.1019');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,3,'33615678.685289');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,3,'1809619.81714617169373549883');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,4,'15598922.8238');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,4,'0');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,4,'60831598315717.14146161');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,4,'1.00000000000000000000');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,5,'7815858.450391');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,5,'7783064.373409');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,5,'127888068979.9935054429');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,5,'475.66281046305802686061');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,6,'7893362.98953026');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,6,'7705559.83426974');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,6,'732381731243.745115764094');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,6,'83.05996138436129499606');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,7,'-75229023.5881');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,7,'90827946.4119');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,7,'-647577464846017.9715');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,7,'-.09393717604145131637');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,8,'7874342.4119');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,8,'7724580.4119');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,8,'584031469984.4839');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,8,'104.15808298366741897143');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,9,'-17127342.633147420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,9,'32726265.456947420');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,9,'-194415646271340.1815956522980');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,9,'-.31289456112403769409');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,0,'16397.038491');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,0,'16397.038491');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,0,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,0,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,1,'16397.038491');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,1,'16397.038491');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,1,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,1,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,2,'-34322095.176906047');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,2,'34354889.253888047');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,2,'-563049578578.769242506736077');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,2,'-.00047751189505192446');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,3,'16401.348491');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,3,'16392.728491');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,3,'70671.23589621');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,3,'3804.41728329466357308584');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,4,'7815858.450391');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,4,'-7783064.373409');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,4,'127888068979.9935054429');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,4,'.00210232958726897192');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,5,'32794.076982');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,5,'0');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,5,'268862871.275335557081');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,5,'1.00000000000000000000');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,6,'110298.61612126');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,6,'-77504.53913926');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,6,'1539707782.76899778633766');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,6,'.17461941433576102689');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,7,'-83012087.961509');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,7,'83044882.038491');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,7,'-1361421264394.416135');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,7,'-.00019748690453643710');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,8,'91278.038491');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,8,'-58483.961509');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,8,'1227826639.244571');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,8,'.21897461960978085228');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,9,'-24910407.006556420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,9,'24943201.083538420');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,9,'-408725765384.257043660243220');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,9,'-.00065780749354660427');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,0,'93901.57763026');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,0,'93901.57763026');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,0,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,0,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,1,'93901.57763026');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,1,'93901.57763026');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,1,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,1,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,2,'-34244590.637766787');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,2,'34432393.793027307');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,2,'-3224438592470.18449811926184222');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,2,'-.00273458651128995823');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,3,'93905.88763026');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,3,'93897.26763026');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,3,'404715.7995864206');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,3,'21786.90896293735498839907');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,4,'7893362.98953026');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,4,'-7705559.83426974');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,4,'732381731243.745115764094');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,4,'.01203949512295682469');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,5,'110298.61612126');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,5,'77504.53913926');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,5,'1539707782.76899778633766');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,5,'5.72674008674192359679');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,6,'187803.15526052');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,6,'0');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,6,'8817506281.45174');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,6,'1.00000000000000000000');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,7,'-82934583.42236974');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,7,'83122386.57763026');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,7,'-7796505729750.37795610');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,7,'-.00113095617281538980');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,8,'168782.57763026');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,8,'19020.57763026');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,8,'7031444034.53149906');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,8,'1.25401073209839612184');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,9,'-24832902.467417160');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,9,'25020705.622677680');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,9,'-2340666225110.29929521292692920');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,9,'-.00376709254265256789');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,0,'-83028485');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,0,'-83028485');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,0,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,0,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,1,'-83028485');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,1,'-83028485');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,1,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,1,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,2,'-117366977.215397047');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,2,'-48689992.784602953');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,2,'2851072985828710.485883795');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,2,'2.41794207151503385700');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,3,'-83028480.69');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,3,'-83028489.31');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,3,'-357852770.35');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,3,'-19264149.65197215777262180974');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,4,'-75229023.5881');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,4,'-90827946.4119');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,4,'-647577464846017.9715');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,4,'-10.64541262725136247686');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,5,'-83012087.961509');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,5,'-83044882.038491');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,5,'-1361421264394.416135');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,5,'-5063.62688881730941836574');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,6,'-82934583.42236974');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,6,'-83122386.57763026');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,6,'-7796505729750.37795610');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,6,'-884.20756174009028770294');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,7,'-166056970');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,7,'0');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,7,'6893729321395225');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,7,'1.00000000000000000000');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,8,'-82953604');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,8,'-83103366');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,8,'-6217255985285');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,8,'-1108.80577182462841041118');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,9,'-107955289.045047420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,9,'-58101680.954952580');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,9,'2069634775752159.035758700');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,9,'3.33089171198810413382');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,0,'74881');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,0,'74881');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,0,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,0,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,1,'74881');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,1,'74881');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,1,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,1,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,2,'-34263611.215397047');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,2,'34413373.215397047');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,2,'-2571300635581.146276407');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,2,'-.00218067233500788615');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,3,'74885.31');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,3,'74876.69');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,3,'322737.11');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,3,'17373.78190255220417633410');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,4,'7874342.4119');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,4,'-7724580.4119');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,4,'584031469984.4839');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,4,'.00960079113741758956');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,5,'91278.038491');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,5,'58483.961509');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,5,'1227826639.244571');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,5,'4.56673929509287019456');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,6,'168782.57763026');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,6,'-19020.57763026');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,6,'7031444034.53149906');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,6,'.79744134113322314424');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,7,'-82953604');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,7,'83103366');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,7,'-6217255985285');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,7,'-.00090187120721280172');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,8,'149762');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,8,'0');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,8,'5607164161');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,8,'1.00000000000000000000');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,9,'-24851923.045047420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,9,'25001685.045047420');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,9,'-1866544013697.195857020');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,9,'-.00300403532938582735');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,0,'-24926804.045047420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,0,'-24926804.045047420');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,0,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,0,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,1,'-24926804.045047420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,1,'-24926804.045047420');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,1,'0');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,1,'NaN');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,2,'-59265296.260444467');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,2,'9411688.17034962');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,2,'855948866655588.453741509242968740');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,2,'.72591434384152961526');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,3,'-24926799.735047420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,3,'-24926808.355047420');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,3,'-107434525.43415438020');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,3,'-5783481.21694835730858468677');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,4,'-17127342.633147420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,4,'-32726265.456947420');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,4,'-194415646271340.1815956522980');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,4,'-3.19596478892958416484');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,5,'-24910407.006556420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,5,'-24943201.083538420');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,5,'-408725765384.257043660243220');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,5,'-1520.20159364322004505807');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,6,'-24832902.467417160');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,6,'-25020705.622677680');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,6,'-2340666225110.29929521292692920');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,6,'-265.45671195426965751280');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,7,'-107955289.045047420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,7,'58101680.954952580');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,7,'2069634775752159.035758700');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,7,'.30021990699995814689');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,8,'-24851923.045047420');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,8,'-25001685.045047420');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,8,'-1866544013697.195857020');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,8,'-332.88556569820675471748');
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,9,'-49853608.090094840');
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,9,'0');
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,9,'621345559900192.420120630048656400');
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,9,'1.00000000000000000000');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
INSERT INTO num_exp_sqrt(id, expected) VALUES (0,'0');
INSERT INTO num_exp_sqrt(id, expected) VALUES (1,'0');
INSERT INTO num_exp_sqrt(id, expected) VALUES (2,'5859.90547836712524903505');
INSERT INTO num_exp_sqrt(id, expected) VALUES (3,'2.07605394920266944396');
INSERT INTO num_exp_sqrt(id, expected) VALUES (4,'2792.75158435189147418923');
INSERT INTO num_exp_sqrt(id, expected) VALUES (5,'128.05092147657509145473');
INSERT INTO num_exp_sqrt(id, expected) VALUES (6,'306.43364311096782703406');
INSERT INTO num_exp_sqrt(id, expected) VALUES (7,'9111.99676251039939975230');
INSERT INTO num_exp_sqrt(id, expected) VALUES (8,'273.64392922189960397542');
INSERT INTO num_exp_sqrt(id, expected) VALUES (9,'4992.67503899937593364766');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
INSERT INTO num_exp_ln(id, expected) VALUES (0,'NaN');
INSERT INTO num_exp_ln(id, expected) VALUES (1,'NaN');
INSERT INTO num_exp_ln(id, expected) VALUES (2,'17.35177750493897715514');
INSERT INTO num_exp_ln(id, expected) VALUES (3,'1.46093790411565641971');
INSERT INTO num_exp_ln(id, expected) VALUES (4,'15.86956523951936572464');
INSERT INTO num_exp_ln(id, expected) VALUES (5,'9.70485601768871834038');
INSERT INTO num_exp_ln(id, expected) VALUES (6,'11.45000246622944403127');
INSERT INTO num_exp_ln(id, expected) VALUES (7,'18.23469429965478772991');
INSERT INTO num_exp_ln(id, expected) VALUES (8,'11.22365546576315513668');
INSERT INTO num_exp_ln(id, expected) VALUES (9,'17.03145425013166006962');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
INSERT INTO num_exp_log10(id, expected) VALUES (0,'NaN');
INSERT INTO num_exp_log10(id, expected) VALUES (1,'NaN');
INSERT INTO num_exp_log10(id, expected) VALUES (2,'7.53578122160797276459');
INSERT INTO num_exp_log10(id, expected) VALUES (3,'.63447727016073160075');
INSERT INTO num_exp_log10(id, expected) VALUES (4,'6.89206461372691743345');
INSERT INTO num_exp_log10(id, expected) VALUES (5,'4.21476541614777768626');
INSERT INTO num_exp_log10(id, expected) VALUES (6,'4.97267288886207207671');
INSERT INTO num_exp_log10(id, expected) VALUES (7,'7.91922711353275546914');
INSERT INTO num_exp_log10(id, expected) VALUES (8,'4.87437163556421004138');
INSERT INTO num_exp_log10(id, expected) VALUES (9,'7.39666659961986567059');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (0,'NaN');
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (1,'NaN');
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (2,'224790267919917455.13261618583642653184');
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (3,'28.90266599445155957393');
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (4,'7405685069594999.07733999469386277636');
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (5,'5068226527.32127265408584640098');
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (6,'281839893606.99372343357047819067');
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (7,'1716699575118597095.42330819910640247627');
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (8,'167361463828.07491320069016125952');
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (9,'107511333880052007.04141124673540337457');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
INSERT INTO num_data(id, val) VALUES (0, '0');
INSERT INTO num_data(id, val) VALUES (1, '0');
INSERT INTO num_data(id, val) VALUES (2, '-34338492.215397047');
INSERT INTO num_data(id, val) VALUES (3, '4.31');
INSERT INTO num_data(id, val) VALUES (4, '7799461.4119');
INSERT INTO num_data(id, val) VALUES (5, '16397.038491');
INSERT INTO num_data(id, val) VALUES (6, '93901.57763026');
INSERT INTO num_data(id, val) VALUES (7, '-83028485');
INSERT INTO num_data(id, val) VALUES (8, '74881');
INSERT INTO num_data(id, val) VALUES (9, '-24926804.045047420');
COMMIT TRANSACTION;

-- ******************************
-- * Now check the behaviour of the NUMERIC type
-- ******************************

-- ******************************
-- * Addition check
-- ******************************
DELETE FROM num_result;
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, t1.val + t2.val
    FROM num_data t1, num_data t2;
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_add t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

DELETE FROM num_result;
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, round((t1.val + t2.val)::numeric, 10)
    FROM num_data t1, num_data t2;
SELECT t1.id1, t1.id2, t1.result, round(t2.expected::numeric, 10) as expected
    FROM num_result t1, num_exp_add t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != round(t2.expected::numeric, 10);

-- ******************************
-- * Subtraction check
-- ******************************
DELETE FROM num_result;
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, t1.val - t2.val
    FROM num_data t1, num_data t2;
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_sub t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

DELETE FROM num_result;
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, round((t1.val - t2.val)::numeric, 40)
    FROM num_data t1, num_data t2;
SELECT t1.id1, t1.id2, t1.result, round(t2.expected::numeric, 40)
    FROM num_result t1, num_exp_sub t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != round(t2.expected::numeric, 40);

-- ******************************
-- * Multiply check
-- ******************************
DELETE FROM num_result;
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, t1.val * t2.val
    FROM num_data t1, num_data t2;
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_mul t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

DELETE FROM num_result;
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, round((t1.val * t2.val)::numeric, 30)
    FROM num_data t1, num_data t2;
SELECT t1.id1, t1.id2, t1.result, round(t2.expected::numeric, 30) as expected
    FROM num_result t1, num_exp_mul t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != round(t2.expected::numeric, 30);

-- ******************************
-- * Division check
-- ******************************
DELETE FROM num_result;
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, t1.val / t2.val
    FROM num_data t1, num_data t2
    WHERE t2.val != '0.0';
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_div t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

DELETE FROM num_result;
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, round((t1.val / t2.val)::numeric, 80)
    FROM num_data t1, num_data t2
    WHERE t2.val != '0.0';
SELECT t1.id1, t1.id2, t1.result::numeric(210,80), round(t2.expected::numeric, 80) as expected
    FROM num_result t1, num_exp_div t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected::numeric, 80);

-- ******************************
-- * Square root check
-- ******************************
DELETE FROM num_result;
INSERT INTO num_result(id1, id2, result) SELECT id, 0, SQRT(ABS(val))
    FROM num_data;
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_sqrt t2
    WHERE t1.id1 = t2.id
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

-- ******************************
-- * Natural logarithm check
-- ******************************
DELETE FROM num_result;
INSERT INTO num_result(id1, id2, result) SELECT id, 0, LN(ABS(val))
    FROM num_data
    WHERE val != '0.0';
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_ln t2
    WHERE t1.id1 = t2.id
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

-- ******************************
-- * Logarithm base 10 check
-- ******************************
DELETE FROM num_result;
INSERT INTO num_result(id1, id2, result) SELECT id, 0, LOG(numeric '10', ABS(val::numeric))
    FROM num_data
    WHERE val != '0.0';
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_log10 t2
    WHERE t1.id1 = t2.id
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

-- ******************************
-- * POWER(10, LN(value)) check
-- ******************************
DELETE FROM num_result;
INSERT INTO num_result(id1, id2, result) SELECT id, 0, POWER(numeric '10', LN(ABS(round(val::numeric,200))))
    FROM num_data
    WHERE val != '0.0';
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_power_10_ln t2
    WHERE t1.id1 = t2.id
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

-- ******************************
-- * miscellaneous checks for things that have been broken in the past...
-- ******************************
-- numeric AVG used to fail on some platforms
SELECT AVG(val) FROM num_data;
SELECT STDDEV(val) FROM num_data;
SELECT VARIANCE(val) FROM num_data;

-- Check for appropriate rounding and overflow
CREATE FOREIGN TABLE fract_only (id serial OPTIONS (rowkey 'true'), val float8) server griddb_svr;
INSERT INTO fract_only VALUES (1, '0.0'::numeric(4,4));
INSERT INTO fract_only VALUES (2, '0.1'::numeric(4,4));
INSERT INTO fract_only VALUES (3, '1.0'::numeric(4,4));	-- should fail
INSERT INTO fract_only VALUES (4, '-0.9999'::numeric(4,4));
INSERT INTO fract_only VALUES (5, '0.99994'::numeric(4,4));
INSERT INTO fract_only VALUES (6, '0.99995'::numeric(4,4));  -- should fail
INSERT INTO fract_only VALUES (7, '0.00001'::numeric(4,4));
INSERT INTO fract_only VALUES (8, '0.00017'::numeric(4,4));
SELECT id, val::numeric(4,4) FROM fract_only;

-- Check inf/nan conversion behavior
DELETE FROM fract_only;
INSERT INTO fract_only(val) VALUES ('NaN'::float8);
SELECT val::numeric FROM fract_only;
DELETE FROM fract_only;
INSERT INTO fract_only(val) VALUES ('Infinity'::float8);
SELECT val::numeric FROM fract_only;
DELETE FROM fract_only;
INSERT INTO fract_only(val) VALUES ('-Infinity'::float8);
SELECT val::numeric FROM fract_only;
DELETE FROM fract_only;
INSERT INTO fract_only(val) VALUES ('NaN'::float8);
SELECT val::numeric FROM fract_only;
DELETE FROM fract_only;
INSERT INTO fract_only(val) VALUES ('Infinity'::float4);
SELECT val::numeric FROM fract_only;
DELETE FROM fract_only;
INSERT INTO fract_only(val) VALUES ('-Infinity'::float4);
SELECT val::numeric FROM fract_only;
DELETE FROM fract_only;
DROP FOREIGN TABLE fract_only;

-- Simple check that ceil(), floor(), and round() work correctly
CREATE FOREIGN TABLE ceil_floor_round (id serial options (rowkey 'true'), a float8) SERVER griddb_svr;
INSERT INTO ceil_floor_round(a) VALUES ('-5.5');
INSERT INTO ceil_floor_round(a) VALUES ('-5.499999');
INSERT INTO ceil_floor_round(a) VALUES ('9.5');
INSERT INTO ceil_floor_round(a) VALUES ('9.4999999');
INSERT INTO ceil_floor_round(a) VALUES ('0.0');
INSERT INTO ceil_floor_round(a) VALUES ('0.0000001');
INSERT INTO ceil_floor_round(a) VALUES ('-0.000001');
SELECT a::numeric, ceil(a::numeric), ceiling(a::numeric), floor(a::numeric), round(a::numeric) FROM ceil_floor_round;

-- Check rounding, it should round ties away from zero.
DELETE FROM ceil_floor_round;
INSERT INTO ceil_floor_round(a) SELECT * FROM generate_series(-5,5);
SELECT a as pow, 
	round((-2.5 * 10 ^ a)::numeric, -a::int),
	round((-1.5 * 10 ^ a)::numeric, -a::int),
	round((-0.5 * 10 ^ a)::numeric, -a::int),
	round((0.5 * 10 ^ a)::numeric, -a::int),
	round((1.5 * 10 ^ a)::numeric, -a::int),
	round((2.5 * 10 ^ a)::numeric, -a::int)
FROM ceil_floor_round;

-- Testing for width_bucket(). For convenience, we test both the
-- numeric and float8 versions of the function in this file.

-- errors
SELECT width_bucket(5.0, 3.0, 4.0, 0);
SELECT width_bucket(5.0, 3.0, 4.0, -5);
SELECT width_bucket(3.5, 3.0, 3.0, 888);
SELECT width_bucket(5.0::float8, 3.0::float8, 4.0::float8, 0);
SELECT width_bucket(5.0::float8, 3.0::float8, 4.0::float8, -5);
SELECT width_bucket(3.5::float8, 3.0::float8, 3.0::float8, 888);
SELECT width_bucket('NaN', 3.0, 4.0, 888);
SELECT width_bucket(0::float8, 'NaN', 4.0::float8, 888);

-- normal operation
CREATE FOREIGN TABLE width_bucket_test (
	id serial OPTIONS (rowkey 'true'),
	operand_num float8,
	operand_f8 float8
) SERVER griddb_svr;

COPY width_bucket_test (operand_num) FROM stdin;
-5.2
-0.0000000001
0.000000000001
1
1.99999999999999
2
2.00000000000001
3
4
4.5
5
5.5
6
7
8
9
9.99999999999999
10
10.0000000000001
\.

UPDATE width_bucket_test SET operand_f8 = operand_num::float8;

SELECT
    operand_num::numeric,
    width_bucket(operand_num, 0, 10, 5) AS wb_1,
    width_bucket(operand_f8, 0, 10, 5) AS wb_1f,
    width_bucket(operand_num, 10, 0, 5) AS wb_2,
    width_bucket(operand_f8, 10, 0, 5) AS wb_2f,
    width_bucket(operand_num, 2, 8, 4) AS wb_3,
    width_bucket(operand_f8, 2, 8, 4) AS wb_3f,
    width_bucket(operand_num, 5.0, 5.5, 20) AS wb_4,
    width_bucket(operand_f8, 5.0, 5.5, 20) AS wb_4f,
    width_bucket(operand_num, -25, 25, 10) AS wb_5,
    width_bucket(operand_f8, -25, 25, 10) AS wb_5f
    FROM width_bucket_test;

-- for float8 only, check positive and negative infinity: we require
-- finite bucket bounds, but allow an infinite operand
SELECT width_bucket(0.0::float8, 'Infinity'::float8, 5, 10); -- error
SELECT width_bucket(0.0::float8, 5, '-Infinity'::float8, 20); -- error
SELECT width_bucket('Infinity'::float8, 1, 10, 10),
       width_bucket('-Infinity'::float8, 1, 10, 10);

DROP FOREIGN TABLE width_bucket_test;

-- TO_CHAR()
--
SELECT '' AS to_char_1, to_char(val::numeric(210,10), '9G999G999G999G999G999')
	FROM num_data;

SELECT '' AS to_char_2, to_char(val::numeric(210,10), '9G999G999G999G999G999D999G999G999G999G999')
	FROM num_data;

SELECT '' AS to_char_3, to_char(val::numeric(210,10), '9999999999999999.999999999999999PR')
	FROM num_data;

SELECT '' AS to_char_4, to_char(val::numeric(210,10), '9999999999999999.999999999999999S')
	FROM num_data;

SELECT '' AS to_char_5,  to_char(val::numeric(210,10), 'MI9999999999999999.999999999999999')     FROM num_data;
SELECT '' AS to_char_6,  to_char(val::numeric(210,10), 'FMS9999999999999999.999999999999999')    FROM num_data;
SELECT '' AS to_char_7,  to_char(val::numeric(210,10), 'FM9999999999999999.999999999999999THPR') FROM num_data;
SELECT '' AS to_char_8,  to_char(val::numeric(210,10), 'SG9999999999999999.999999999999999th')   FROM num_data;
SELECT '' AS to_char_9,  to_char(val::numeric(210,10), '0999999999999999.999999999999999')       FROM num_data;
SELECT '' AS to_char_10, to_char(val::numeric(210,10), 'S0999999999999999.999999999999999')      FROM num_data;
SELECT '' AS to_char_11, to_char(val::numeric(210,10), 'FM0999999999999999.999999999999999')     FROM num_data;
SELECT '' AS to_char_12, to_char(val::numeric(210,10), 'FM9999999999999999.099999999999999') 	FROM num_data;
SELECT '' AS to_char_13, to_char(val::numeric(210,10), 'FM9999999999990999.990999999999999') 	FROM num_data;
SELECT '' AS to_char_14, to_char(val::numeric(210,10), 'FM0999999999999999.999909999999999') 	FROM num_data;
SELECT '' AS to_char_15, to_char(val::numeric(210,10), 'FM9999999990999999.099999999999999') 	FROM num_data;
SELECT '' AS to_char_16, to_char(val::numeric(210,10), 'L9999999999999999.099999999999999')	FROM num_data;
SELECT '' AS to_char_17, to_char(val::numeric(210,10), 'FM9999999999999999.99999999999999')	FROM num_data;
SELECT '' AS to_char_18, to_char(val::numeric(210,10), 'S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9') FROM num_data;
SELECT '' AS to_char_19, to_char(val::numeric(210,10), 'FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9') FROM num_data;
SELECT '' AS to_char_20, to_char(val::numeric(210,10), E'99999 "text" 9999 "9999" 999 "\\"text between quote marks\\"" 9999') FROM num_data;
SELECT '' AS to_char_21, to_char(val::numeric(210,10), '999999SG9999999999')			FROM num_data;
SELECT '' AS to_char_22, to_char(val::numeric(210,10), 'FM9999999999999999.999999999999999')	FROM num_data;
SELECT '' AS to_char_23, to_char(val::numeric(210,10), '9.999EEEE')				FROM num_data;

DELETE FROM ceil_floor_round;
INSERT INTO ceil_floor_round(a) VALUES ('100'::numeric);
SELECT '' AS to_char_24, to_char(a::numeric, 'FM999.9') FROM ceil_floor_round;
SELECT '' AS to_char_25, to_char(a::numeric, 'FM999.') FROM ceil_floor_round;
SELECT '' AS to_char_26, to_char(a::numeric, 'FM999') FROM ceil_floor_round;

-- Check parsing of literal text in a format string
SELECT '' AS to_char_27, to_char(a::numeric, 'foo999') FROM ceil_floor_round;
SELECT '' AS to_char_28, to_char(a::numeric, 'f\oo999') FROM ceil_floor_round;
SELECT '' AS to_char_29, to_char(a::numeric, 'f\\oo999') FROM ceil_floor_round;
SELECT '' AS to_char_30, to_char(a::numeric, 'f\"oo999') FROM ceil_floor_round;
SELECT '' AS to_char_31, to_char(a::numeric, 'f\\"oo999') FROM ceil_floor_round;
SELECT '' AS to_char_32, to_char(a::numeric, 'f"ool"999') FROM ceil_floor_round;
SELECT '' AS to_char_33, to_char(a::numeric, 'f"\ool"999') FROM ceil_floor_round;
SELECT '' AS to_char_34, to_char(a::numeric, 'f"\\ool"999') FROM ceil_floor_round;
SELECT '' AS to_char_35, to_char(a::numeric, 'f"ool\"999') FROM ceil_floor_round;
SELECT '' AS to_char_36, to_char(a::numeric, 'f"ool\\"999') FROM ceil_floor_round;

-- TO_NUMBER()
--
SET lc_numeric = 'C';
CREATE FOREIGN TABLE to_number_test (
	id serial OPTIONS (rowkey 'true'),
	val text,
	fmt text
) SERVER griddb_svr;

INSERT INTO to_number_test(val, fmt) VALUES
	('-34,338,492', '99G999G999'),
	('-34,338,492.654,878', '99G999G999D999G999'),
	('<564646.654564>', '999999.999999PR'),
	('0.00001-', '9.999999S'),
	('5.01-', 'FM9.999999S'),
	('5.01-', 'FM9.999999MI'),
	('5 4 4 4 4 8 . 7 8', '9 9 9 9 9 9 . 9 9'),
	('.01', 'FM9.99'),
	('.0', '99999999.99999999'),
	('0', '99.99'),
	('.-01', 'S99.99'),
	('.01-', '99.99S'),
	(' . 0 1-', ' 9 9 . 9 9 S'),
	('34,50','999,99'),
	('123,000','999G'),
	('123456','999G999'),
	('$1234.56','L9,999.99'),
	('$1234.56','L99,999.99'),
	('$1,234.56','L99,999.99'),
	('1234.56','L99,999.99'),
	('1,234.56','L99,999.99'),
	('42nd', '99th');
SELECT id AS to_number,  to_number(val, fmt) from to_number_test;
RESET lc_numeric;
DROP FOREIGN TABLE to_number_test;

--
-- Input syntax
--

CREATE FOREIGN TABLE num_input_test (id serial options (rowkey 'true'), n1 float8) SERVER griddb_svr;

-- good inputs
INSERT INTO num_input_test(n1) VALUES (' 123');
INSERT INTO num_input_test(n1) VALUES ('   3245874    ');
INSERT INTO num_input_test(n1) VALUES ('  -93853');
INSERT INTO num_input_test(n1) VALUES ('555.50');
INSERT INTO num_input_test(n1) VALUES ('-555.50');
INSERT INTO num_input_test(n1) VALUES ('NaN ');
INSERT INTO num_input_test(n1) VALUES ('        nan');

-- bad inputs
INSERT INTO num_input_test(n1) VALUES ('     ');
INSERT INTO num_input_test(n1) VALUES ('   1234   %');
INSERT INTO num_input_test(n1) VALUES ('xyz');
INSERT INTO num_input_test(n1) VALUES ('- 1234');
INSERT INTO num_input_test(n1) VALUES ('5 . 0');
INSERT INTO num_input_test(n1) VALUES ('5. 0   ');
INSERT INTO num_input_test(n1) VALUES ('');
INSERT INTO num_input_test(n1) VALUES (' N aN ');

SELECT * FROM num_input_test;

--
-- Test some corner cases for multiplication
--
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES (4790999999999999999999999999999999999999999999999999999999999999999999999999999999999999 * 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
INSERT INTO num_input_test(n1) VALUES (4790999999999999999999999999999999999999999999999999999999999999999999999999999999999999 * 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
INSERT INTO num_input_test(n1) VALUES (4789999999999999999999999999999999999999999999999999999999999999999999999999999999999999 * 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
INSERT INTO num_input_test(n1) VALUES (4770999999999999999999999999999999999999999999999999999999999999999999999999999999999999 * 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
INSERT INTO num_input_test(n1) VALUES (4769999999999999999999999999999999999999999999999999999999999999999999999999999999999999 * 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
SELECT n1::numeric FROM num_input_test;
--
-- Test some corner cases for division
--
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES ('999999999999999999999.00');
SELECT n1::numeric/1000000000000000000000 FROM num_input_test;
SELECT div(n1::numeric,1000000000000000000000) FROM num_input_test;
SELECT mod(n1::numeric,1000000000000000000000) FROM num_input_test;
SELECT div(-n1::numeric,1000000000000000000000) FROM num_input_test;
SELECT mod(-n1::numeric,1000000000000000000000) FROM num_input_test;
SELECT div(-n1::numeric,1000000000000000000000)*1000000000000000000000 + 
	mod(-n1::numeric,1000000000000000000000) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES ('70.0');
SELECT mod (n1::numeric,70) FROM num_input_test;
SELECT div (n1::numeric,70)) FROM num_input_test;
SELECT n1::numeric / 70 FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES ('12345678901234567890');
SELECT n1::numeric % 123 FROM num_input_test;
SELECT n1::numeric / 123 FROM num_input_test;
SELECT div(n1::numeric, 123) FROM num_input_test;
SELECT div(n1::numeric, 123) * 123 + (n1::numeric % 123) FROM num_input_test;

--
-- Test code path for raising to integer powers
--
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES (10.0 ^ -2147483648);
INSERT INTO num_input_test(n1) VALUES (10.0 ^ -2147483647);
INSERT INTO num_input_test(n1) VALUES (10.0 ^ 2147483647);
INSERT INTO num_input_test(n1) VALUES (117743296169.0 ^ 1000000000);
SELECT n1::numeric FROM num_input_test;

-- cases that used to return inaccurate results
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES (3.789 ^ 21);
INSERT INTO num_input_test(n1) VALUES (3.789 ^ 35);
INSERT INTO num_input_test(n1) VALUES (1.2 ^ 345);
INSERT INTO num_input_test(n1) VALUES (0.12 ^ (-20));
SELECT n1::numeric FROM num_input_test;

-- cases that used to error out
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES (0.12 ^ (-25));
INSERT INTO num_input_test(n1) VALUES (0.5678 ^ (-85));
SELECT n1::numeric FROM num_input_test;

--
-- Tests for raising to non-integer powers
--

-- special cases
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES (0.0 ^ 0.0);
INSERT INTO num_input_test(n1) VALUES ((-12.34) ^ 0.0);
INSERT INTO num_input_test(n1) VALUES (12.34 ^ 0.0);
INSERT INTO num_input_test(n1) VALUES (0.0 ^ 12.34);
SELECT n1::numeric(17,16) FROM num_input_test;

-- NaNs
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES ('NaN'::numeric ^ 'NaN'::numeric);
INSERT INTO num_input_test(n1) VALUES ('NaN'::numeric ^ 0);
INSERT INTO num_input_test(n1) VALUES ('NaN'::numeric ^ 1);
INSERT INTO num_input_test(n1) VALUES (0 ^ 'NaN'::numeric);
INSERT INTO num_input_test(n1) VALUES (1 ^ 'NaN'::numeric);
SELECT n1::numeric FROM num_input_test;

-- invalid inputs
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES (0.0 ^ (-12.34));
INSERT INTO num_input_test(n1) VALUES ((-12.34) ^ 1.2);
SELECT n1::numeric FROM num_input_test;

-- cases that used to generate inaccurate results
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES (32.1 ^ 9.8);
INSERT INTO num_input_test(n1) VALUES (32.1 ^ (-9.8));
INSERT INTO num_input_test(n1) VALUES (12.3 ^ 45.6);
INSERT INTO num_input_test(n1) VALUES (12.3 ^ (-45.6));
SELECT n1::numeric FROM num_input_test;

-- big test
-- out of range
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES (1.234 & 5678);
SELECT n1::numeric FROM num_input_test;

--
-- Tests for EXP()
--

-- special cases
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES ('0.0');
SELECT exp(n1::numeric) from num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES ('1.0');
SELECT exp(n1::numeric) from num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES ('1.0');
SELECT exp(n1::numeric(71, 70)) from num_input_test;

-- cases that used to generate inaccurate results
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES ('32.999');
SELECT exp(n1::numeric) from num_input_test;
SELECT exp(-n1::numeric) from num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES ('123.456');
SELECT exp(n1::numeric) from num_input_test;
SELECT exp(-n1::numeric) from num_input_test;

-- big test
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) VALUES ('1234.5678');
SELECT exp(n1::numeric) from num_input_test;

--
-- Tests for generate_series
--
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) select * from generate_series(0.0, 4.0);
SELECT n1::numeric(2,1) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) select * from generate_series(0.1, 4.0, 1.3);
SELECT n1::numeric(2,1) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) select * from generate_series(4.0, -1.5, -2.2);
SELECT n1::numeric(2,1) FROM num_input_test;

-- skip, cannot insert out of range value
-- Trigger errors
select * from generate_series(-100::numeric, 100::numeric, 0::numeric);
select * from generate_series(-100::numeric, 100::numeric, 'nan'::numeric);
select * from generate_series('nan'::numeric, 100::numeric, 10::numeric);
select * from generate_series(0::numeric, 'nan'::numeric, 10::numeric);
-- Checks maximum, output is truncated
select (i / (10::numeric ^ 131071))::numeric(1,0)
	from generate_series(6 * (10::numeric ^ 131071),
			     9 * (10::numeric ^ 131071),
			     10::numeric ^ 131071) as a(i);
-- Check usage with variables
select * from generate_series(1::numeric, 3::numeric) i, generate_series(i,3) j;
select * from generate_series(1::numeric, 3::numeric) i, generate_series(1,i) j;
select * from generate_series(1::numeric, 3::numeric) i, generate_series(1,5,i) j;

--
-- Tests for LN()
--

-- Invalid inputs
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values('-12.34');
SELECT ln(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values('0.0');
SELECT ln(n1::numeric) FROM num_input_test;

-- Some random tests
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(1.2345678e-28);
SELECT ln(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(0.0456789);
SELECT ln(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(0.349873948359354029493948309745709580730482050975);
SELECT ln(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(0.99949452);
SELECT ln(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(1.00049687395);
SELECT ln(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(1234.567890123456789);
SELECT ln(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(5.80397490724e5);
SELECT ln(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(9.342536355e34);
SELECT ln(n1::numeric) FROM num_input_test;

-- 
-- Tests for LOG() (base 10)
--

-- invalid inputs
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values('-12.34');
SELECT ln(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values('0.0');
SELECT ln(n1::numeric) FROM num_input_test;

-- some random tests
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(1.234567e-89);
SELECT log(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(3.4634998359873254962349856073435545);
SELECT log(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(9.999999999999999999);
SELECT log(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(10.00000000000000000);
SELECT log(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(10.00000000000000001);
SELECT log(n1::numeric) FROM num_input_test;

DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(590489.45235237);
SELECT log(n1::numeric) FROM num_input_test;

-- similar as above test. Basically, we can get float8 value and 
-- convert to numeric
-- Tests for LOG() (arbitrary base)
--

-- invalid inputs
select log(-12.34, 56.78);
select log(-12.34, -56.78);
select log(12.34, -56.78);
select log(0.0, 12.34);
select log(12.34, 0.0);
select log(1.0, 12.34);

-- some random tests
select log(1.23e-89, 6.4689e45);
select log(0.99923, 4.58934e34);
select log(1.000016, 8.452010e18);
select log(3.1954752e47, 9.4792021e-73);

--
-- Tests for scale()
--

select scale(numeric 'NaN');
select scale(NULL::numeric);
select scale(1.12);
select scale(0);
select scale(0.00);
select scale(1.12345);
select scale(110123.12475871856128);
select scale(-1123.12471856128);
select scale(-13.000000000000000);

--
-- Tests for SUM()
--

-- cases that need carry propagation
DELETE FROM num_input_test;
INSERT INTO num_input_test(n1) values(generate_series(1, 100000));
SELECT SUM(999::numeric) FROM num_input_test;
SELECT SUM((-999)::numeric) FROM num_input_test;

DROP FOREIGN TABLE num_data;
DROP FOREIGN TABLE num_exp_add;
DROP FOREIGN TABLE num_exp_sub;
DROP FOREIGN TABLE num_exp_div;
DROP FOREIGN TABLE num_exp_mul;
DROP FOREIGN TABLE num_exp_sqrt;
DROP FOREIGN TABLE num_exp_ln;
DROP FOREIGN TABLE num_exp_log10;
DROP FOREIGN TABLE num_exp_power_10_ln;
DROP FOREIGN TABLE num_result;
DROP FOREIGN TABLE num_input_test;
DROP FOREIGN TABLE ceil_floor_round;
DROP USER MAPPING FOR public SERVER griddb_svr;
DROP SERVER griddb_svr;
DROP EXTENSION griddb_fdw CASCADE;
