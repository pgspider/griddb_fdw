--
-- NUMERIC
--
\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 780:
CREATE EXTENSION griddb_fdw;
--Testcase 781:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');
--Testcase 782:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

--Testcase 783:
CREATE FOREIGN TABLE num_data (idx serial, id int4, val float8) SERVER griddb_svr;
--Testcase 784:
CREATE FOREIGN TABLE num_exp_add (idx serial, id1 int4, id2 int4, expected float8) SERVER griddb_svr;
--Testcase 785:
CREATE FOREIGN TABLE num_exp_sub (idx serial, id1 int4, id2 int4, expected float8) SERVER griddb_svr;
--Testcase 786:
CREATE FOREIGN TABLE num_exp_div (idx serial, id1 int4, id2 int4, expected float8) SERVER griddb_svr;
--Testcase 787:
CREATE FOREIGN TABLE num_exp_mul (idx serial, id1 int4, id2 int4, expected float8) SERVER griddb_svr;
--Testcase 788:
CREATE FOREIGN TABLE num_exp_sqrt (idx serial, id int4, expected float8) SERVER griddb_svr;
--Testcase 789:
CREATE FOREIGN TABLE num_exp_ln (idx serial, id int4, expected float8) SERVER griddb_svr;
--Testcase 790:
CREATE FOREIGN TABLE num_exp_log10 (idx serial, id int4, expected float8) SERVER griddb_svr;
--Testcase 791:
CREATE FOREIGN TABLE num_exp_power_10_ln (idx serial, id int4, expected float8) SERVER griddb_svr;

--Testcase 792:
CREATE FOREIGN TABLE num_result (idx serial OPTIONS (rowkey 'true'), id1 int4, id2 int4, result float8) SERVER griddb_svr;


-- ******************************
-- * The following EXPECTED results are computed by bc(1)
-- * with a scale of 200
-- ******************************

BEGIN TRANSACTION;
--Testcase 1:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,0,'0');
--Testcase 2:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,0,'0');
--Testcase 3:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,0,'0');
--Testcase 4:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,0,'NaN');
--Testcase 5:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,1,'0');
--Testcase 6:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,1,'0');
--Testcase 7:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,1,'0');
--Testcase 8:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,1,'NaN');
--Testcase 9:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,2,'-34338492.215397047');
--Testcase 10:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,2,'34338492.215397047');
--Testcase 11:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,2,'0');
--Testcase 12:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,2,'0');
--Testcase 13:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,3,'4.31');
--Testcase 14:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,3,'-4.31');
--Testcase 15:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,3,'0');
--Testcase 16:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,3,'0');
--Testcase 17:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,4,'7799461.4119');
--Testcase 18:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,4,'-7799461.4119');
--Testcase 19:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,4,'0');
--Testcase 20:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,4,'0');
--Testcase 21:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,5,'16397.038491');
--Testcase 22:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,5,'-16397.038491');
--Testcase 23:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,5,'0');
--Testcase 24:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,5,'0');
--Testcase 25:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,6,'93901.57763026');
--Testcase 26:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,6,'-93901.57763026');
--Testcase 27:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,6,'0');
--Testcase 28:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,6,'0');
--Testcase 29:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,7,'-83028485');
--Testcase 30:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,7,'83028485');
--Testcase 31:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,7,'0');
--Testcase 32:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,7,'0');
--Testcase 33:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,8,'74881');
--Testcase 34:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,8,'-74881');
--Testcase 35:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,8,'0');
--Testcase 36:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,8,'0');
--Testcase 37:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,9,'-24926804.045047420');
--Testcase 38:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,9,'24926804.045047420');
--Testcase 39:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,9,'0');
--Testcase 40:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,9,'0');
--Testcase 41:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,0,'0');
--Testcase 42:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,0,'0');
--Testcase 43:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,0,'0');
--Testcase 44:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,0,'NaN');
--Testcase 45:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,1,'0');
--Testcase 46:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,1,'0');
--Testcase 47:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,1,'0');
--Testcase 48:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,1,'NaN');
--Testcase 49:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,2,'-34338492.215397047');
--Testcase 50:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,2,'34338492.215397047');
--Testcase 51:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,2,'0');
--Testcase 52:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,2,'0');
--Testcase 53:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,3,'4.31');
--Testcase 54:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,3,'-4.31');
--Testcase 55:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,3,'0');
--Testcase 56:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,3,'0');
--Testcase 57:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,4,'7799461.4119');
--Testcase 58:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,4,'-7799461.4119');
--Testcase 59:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,4,'0');
--Testcase 60:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,4,'0');
--Testcase 61:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,5,'16397.038491');
--Testcase 62:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,5,'-16397.038491');
--Testcase 63:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,5,'0');
--Testcase 64:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,5,'0');
--Testcase 65:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,6,'93901.57763026');
--Testcase 66:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,6,'-93901.57763026');
--Testcase 67:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,6,'0');
--Testcase 68:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,6,'0');
--Testcase 69:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,7,'-83028485');
--Testcase 70:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,7,'83028485');
--Testcase 71:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,7,'0');
--Testcase 72:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,7,'0');
--Testcase 73:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,8,'74881');
--Testcase 74:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,8,'-74881');
--Testcase 75:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,8,'0');
--Testcase 76:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,8,'0');
--Testcase 77:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,9,'-24926804.045047420');
--Testcase 78:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,9,'24926804.045047420');
--Testcase 79:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,9,'0');
--Testcase 80:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,9,'0');
--Testcase 81:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,0,'-34338492.215397047');
--Testcase 82:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,0,'-34338492.215397047');
--Testcase 83:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,0,'0');
--Testcase 84:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,0,'NaN');
--Testcase 85:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,1,'-34338492.215397047');
--Testcase 86:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,1,'-34338492.215397047');
--Testcase 87:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,1,'0');
--Testcase 88:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,1,'NaN');
--Testcase 89:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,2,'-68676984.430794094');
--Testcase 90:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,2,'0');
--Testcase 91:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,2,'1179132047626883.596862135856320209');
--Testcase 92:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,2,'1.00000000000000000000');
--Testcase 93:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,3,'-34338487.905397');
--Testcase 94:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,3,'-34338496.525397047');
--Testcase 95:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,3,'-147998901.44836127257');
--Testcase 96:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,3,'-7967167.56737751');
--Testcase 97:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,4,'-26539030.803497047');
--Testcase 98:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,4,'-42137953.627297047');
--Testcase 99:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,4,'-267821744976817.8111137106593');
--Testcase 100:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,4,'-4.40267480046830116685');
--Testcase 101:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,5,'-34322095.176906047');
--Testcase 102:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,5,'-34354889.253888047');
--Testcase 103:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,5,'-563049578578.769242506736077');
--Testcase 104:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,5,'-2094.18866914563535496429');
--Testcase 105:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,6,'-34244590.637766787');
--Testcase 106:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,6,'-34432393.793027307');
--Testcase 107:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,6,'-3224438592470.18449811926184222');
--Testcase 108:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,6,'-365.68599891479766440940');
--Testcase 109:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,7,'-117366977.215397047');
--Testcase 110:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,7,'48689992.784602953');
--Testcase 111:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,7,'2851072985828710.485883795');
--Testcase 112:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,7,'.41357483778485235518');
--Testcase 113:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,8,'-34263611.215397047');
--Testcase 114:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,8,'-34413373.215397047');
--Testcase 115:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,8,'-2571300635581.146276407');
--Testcase 116:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,8,'-458.57416721727870888476');
--Testcase 117:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,9,'-59265296.260444467');
--Testcase 118:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,9,'-9411688.17034962');
--Testcase 119:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,9,'855948866655588.453741509242968740');
--Testcase 120:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,9,'1.37757299946438931811');
--Testcase 121:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,0,'4.31');
--Testcase 122:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,0,'4.31');
--Testcase 123:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,0,'0');
--Testcase 124:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,0,'NaN');
--Testcase 125:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,1,'4.31');
--Testcase 126:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,1,'4.31');
--Testcase 127:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,1,'0');
--Testcase 128:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,1,'NaN');
--Testcase 129:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,2,'-34338487.905397');
--Testcase 130:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,2,'34338496.525397047');
--Testcase 131:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,2,'-147998901.44836127257');
--Testcase 132:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,2,'-.000000125515120843525');
--Testcase 133:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,3,'8.62');
--Testcase 134:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,3,'0');
--Testcase 135:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,3,'18.5761');
--Testcase 136:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,3,'1.00000000000000000000');
--Testcase 137:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,4,'7799465.7219');
--Testcase 138:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,4,'-7799457.1019');
--Testcase 139:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,4,'33615678.685289');
--Testcase 140:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,4,'.000000552602259615521');
--Testcase 141:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,5,'16401.348491');
--Testcase 142:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,5,'-16392.728491');
--Testcase 143:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,5,'70671.23589621');
--Testcase 144:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,5,'.00026285234387695504');
--Testcase 145:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,6,'93905.88763026');
--Testcase 146:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,6,'-93897.26763026');
--Testcase 147:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,6,'404715.7995864206');
--Testcase 148:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,6,'.0000458991223445759');
--Testcase 149:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,7,'-83028480.69');
--Testcase 150:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,7,'83028489.31');
--Testcase 151:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,7,'-357852770.35');
--Testcase 152:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,7,'-.000000051909895742407');
--Testcase 153:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,8,'74885.31');
--Testcase 154:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,8,'-74876.69');
--Testcase 155:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,8,'322737.11');
--Testcase 156:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,8,'.00005755799201399553');
--Testcase 157:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,9,'-24926799.735047420');
--Testcase 158:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,9,'24926808.355047420');
--Testcase 159:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,9,'-107434525.43415438020');
--Testcase 160:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,9,'-.00000017290624149855');
--Testcase 161:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,0,'7799461.4119');
--Testcase 162:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,0,'7799461.4119');
--Testcase 163:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,0,'0');
--Testcase 164:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,0,'NaN');
--Testcase 165:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,1,'7799461.4119');
--Testcase 166:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,1,'7799461.4119');
--Testcase 167:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,1,'0');
--Testcase 168:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,1,'NaN');
--Testcase 169:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,2,'-26539030.803497047');
--Testcase 170:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,2,'42137953.627297047');
--Testcase 171:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,2,'-267821744976817.8111137106593');
--Testcase 172:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,2,'-.22713465002993920385');
--Testcase 173:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,3,'7799465.7219');
--Testcase 174:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,3,'7799457.1019');
--Testcase 175:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,3,'33615678.685289');
--Testcase 176:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,3,'1809619.81714617169373549883');
--Testcase 177:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,4,'15598922.8238');
--Testcase 178:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,4,'0');
--Testcase 179:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,4,'60831598315717.14146161');
--Testcase 180:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,4,'1.00000000000000000000');
--Testcase 181:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,5,'7815858.450391');
--Testcase 182:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,5,'7783064.373409');
--Testcase 183:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,5,'127888068979.9935054429');
--Testcase 184:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,5,'475.66281046305802686061');
--Testcase 185:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,6,'7893362.98953026');
--Testcase 186:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,6,'7705559.83426974');
--Testcase 187:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,6,'732381731243.745115764094');
--Testcase 188:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,6,'83.05996138436129499606');
--Testcase 189:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,7,'-75229023.5881');
--Testcase 190:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,7,'90827946.4119');
--Testcase 191:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,7,'-647577464846017.9715');
--Testcase 192:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,7,'-.09393717604145131637');
--Testcase 193:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,8,'7874342.4119');
--Testcase 194:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,8,'7724580.4119');
--Testcase 195:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,8,'584031469984.4839');
--Testcase 196:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,8,'104.15808298366741897143');
--Testcase 197:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,9,'-17127342.633147420');
--Testcase 198:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,9,'32726265.456947420');
--Testcase 199:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,9,'-194415646271340.1815956522980');
--Testcase 200:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,9,'-.31289456112403769409');
--Testcase 201:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,0,'16397.038491');
--Testcase 202:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,0,'16397.038491');
--Testcase 203:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,0,'0');
--Testcase 204:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,0,'NaN');
--Testcase 205:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,1,'16397.038491');
--Testcase 206:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,1,'16397.038491');
--Testcase 207:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,1,'0');
--Testcase 208:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,1,'NaN');
--Testcase 209:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,2,'-34322095.176906047');
--Testcase 210:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,2,'34354889.253888047');
--Testcase 211:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,2,'-563049578578.769242506736077');
--Testcase 212:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,2,'-.00047751189505192446');
--Testcase 213:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,3,'16401.348491');
--Testcase 214:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,3,'16392.728491');
--Testcase 215:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,3,'70671.23589621');
--Testcase 216:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,3,'3804.41728329466357308584');
--Testcase 217:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,4,'7815858.450391');
--Testcase 218:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,4,'-7783064.373409');
--Testcase 219:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,4,'127888068979.9935054429');
--Testcase 220:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,4,'.00210232958726897192');
--Testcase 221:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,5,'32794.076982');
--Testcase 222:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,5,'0');
--Testcase 223:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,5,'268862871.275335557081');
--Testcase 224:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,5,'1.00000000000000000000');
--Testcase 225:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,6,'110298.61612126');
--Testcase 226:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,6,'-77504.53913926');
--Testcase 227:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,6,'1539707782.76899778633766');
--Testcase 228:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,6,'.17461941433576102689');
--Testcase 229:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,7,'-83012087.961509');
--Testcase 230:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,7,'83044882.038491');
--Testcase 231:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,7,'-1361421264394.416135');
--Testcase 232:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,7,'-.00019748690453643710');
--Testcase 233:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,8,'91278.038491');
--Testcase 234:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,8,'-58483.961509');
--Testcase 235:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,8,'1227826639.244571');
--Testcase 236:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,8,'.21897461960978085228');
--Testcase 237:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,9,'-24910407.006556420');
--Testcase 238:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,9,'24943201.083538420');
--Testcase 239:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,9,'-408725765384.257043660243220');
--Testcase 240:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,9,'-.00065780749354660427');
--Testcase 241:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,0,'93901.57763026');
--Testcase 242:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,0,'93901.57763026');
--Testcase 243:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,0,'0');
--Testcase 244:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,0,'NaN');
--Testcase 245:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,1,'93901.57763026');
--Testcase 246:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,1,'93901.57763026');
--Testcase 247:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,1,'0');
--Testcase 248:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,1,'NaN');
--Testcase 249:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,2,'-34244590.637766787');
--Testcase 250:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,2,'34432393.793027307');
--Testcase 251:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,2,'-3224438592470.18449811926184222');
--Testcase 252:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,2,'-.00273458651128995823');
--Testcase 253:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,3,'93905.88763026');
--Testcase 254:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,3,'93897.26763026');
--Testcase 255:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,3,'404715.7995864206');
--Testcase 256:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,3,'21786.90896293735498839907');
--Testcase 257:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,4,'7893362.98953026');
--Testcase 258:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,4,'-7705559.83426974');
--Testcase 259:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,4,'732381731243.745115764094');
--Testcase 260:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,4,'.01203949512295682469');
--Testcase 261:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,5,'110298.61612126');
--Testcase 262:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,5,'77504.53913926');
--Testcase 263:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,5,'1539707782.76899778633766');
--Testcase 264:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,5,'5.72674008674192359679');
--Testcase 265:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,6,'187803.15526052');
--Testcase 266:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,6,'0');
--Testcase 267:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,6,'8817506281.45174');
--Testcase 268:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,6,'1.00000000000000000000');
--Testcase 269:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,7,'-82934583.42236974');
--Testcase 270:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,7,'83122386.57763026');
--Testcase 271:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,7,'-7796505729750.37795610');
--Testcase 272:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,7,'-.00113095617281538980');
--Testcase 273:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,8,'168782.57763026');
--Testcase 274:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,8,'19020.57763026');
--Testcase 275:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,8,'7031444034.53149906');
--Testcase 276:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,8,'1.25401073209839612184');
--Testcase 277:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,9,'-24832902.467417160');
--Testcase 278:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,9,'25020705.622677680');
--Testcase 279:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,9,'-2340666225110.29929521292692920');
--Testcase 280:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,9,'-.00376709254265256789');
--Testcase 281:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,0,'-83028485');
--Testcase 282:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,0,'-83028485');
--Testcase 283:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,0,'0');
--Testcase 284:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,0,'NaN');
--Testcase 285:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,1,'-83028485');
--Testcase 286:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,1,'-83028485');
--Testcase 287:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,1,'0');
--Testcase 288:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,1,'NaN');
--Testcase 289:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,2,'-117366977.215397047');
--Testcase 290:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,2,'-48689992.784602953');
--Testcase 291:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,2,'2851072985828710.485883795');
--Testcase 292:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,2,'2.41794207151503385700');
--Testcase 293:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,3,'-83028480.69');
--Testcase 294:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,3,'-83028489.31');
--Testcase 295:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,3,'-357852770.35');
--Testcase 296:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,3,'-19264149.65197215777262180974');
--Testcase 297:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,4,'-75229023.5881');
--Testcase 298:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,4,'-90827946.4119');
--Testcase 299:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,4,'-647577464846017.9715');
--Testcase 300:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,4,'-10.64541262725136247686');
--Testcase 301:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,5,'-83012087.961509');
--Testcase 302:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,5,'-83044882.038491');
--Testcase 303:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,5,'-1361421264394.416135');
--Testcase 304:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,5,'-5063.62688881730941836574');
--Testcase 305:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,6,'-82934583.42236974');
--Testcase 306:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,6,'-83122386.57763026');
--Testcase 307:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,6,'-7796505729750.37795610');
--Testcase 308:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,6,'-884.20756174009028770294');
--Testcase 309:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,7,'-166056970');
--Testcase 310:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,7,'0');
--Testcase 311:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,7,'6893729321395225');
--Testcase 312:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,7,'1.00000000000000000000');
--Testcase 313:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,8,'-82953604');
--Testcase 314:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,8,'-83103366');
--Testcase 315:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,8,'-6217255985285');
--Testcase 316:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,8,'-1108.80577182462841041118');
--Testcase 317:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,9,'-107955289.045047420');
--Testcase 318:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,9,'-58101680.954952580');
--Testcase 319:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,9,'2069634775752159.035758700');
--Testcase 320:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,9,'3.33089171198810413382');
--Testcase 321:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,0,'74881');
--Testcase 322:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,0,'74881');
--Testcase 323:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,0,'0');
--Testcase 324:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,0,'NaN');
--Testcase 325:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,1,'74881');
--Testcase 326:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,1,'74881');
--Testcase 327:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,1,'0');
--Testcase 328:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,1,'NaN');
--Testcase 329:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,2,'-34263611.215397047');
--Testcase 330:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,2,'34413373.215397047');
--Testcase 331:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,2,'-2571300635581.146276407');
--Testcase 332:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,2,'-.00218067233500788615');
--Testcase 333:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,3,'74885.31');
--Testcase 334:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,3,'74876.69');
--Testcase 335:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,3,'322737.11');
--Testcase 336:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,3,'17373.78190255220417633410');
--Testcase 337:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,4,'7874342.4119');
--Testcase 338:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,4,'-7724580.4119');
--Testcase 339:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,4,'584031469984.4839');
--Testcase 340:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,4,'.00960079113741758956');
--Testcase 341:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,5,'91278.038491');
--Testcase 342:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,5,'58483.961509');
--Testcase 343:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,5,'1227826639.244571');
--Testcase 344:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,5,'4.56673929509287019456');
--Testcase 345:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,6,'168782.57763026');
--Testcase 346:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,6,'-19020.57763026');
--Testcase 347:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,6,'7031444034.53149906');
--Testcase 348:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,6,'.79744134113322314424');
--Testcase 349:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,7,'-82953604');
--Testcase 350:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,7,'83103366');
--Testcase 351:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,7,'-6217255985285');
--Testcase 352:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,7,'-.00090187120721280172');
--Testcase 353:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,8,'149762');
--Testcase 354:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,8,'0');
--Testcase 355:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,8,'5607164161');
--Testcase 356:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,8,'1.00000000000000000000');
--Testcase 357:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,9,'-24851923.045047420');
--Testcase 358:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,9,'25001685.045047420');
--Testcase 359:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,9,'-1866544013697.195857020');
--Testcase 360:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,9,'-.00300403532938582735');
--Testcase 361:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,0,'-24926804.045047420');
--Testcase 362:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,0,'-24926804.045047420');
--Testcase 363:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,0,'0');
--Testcase 364:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,0,'NaN');
--Testcase 365:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,1,'-24926804.045047420');
--Testcase 366:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,1,'-24926804.045047420');
--Testcase 367:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,1,'0');
--Testcase 368:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,1,'NaN');
--Testcase 369:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,2,'-59265296.260444467');
--Testcase 370:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,2,'9411688.17034962');
--Testcase 371:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,2,'855948866655588.453741509242968740');
--Testcase 372:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,2,'.72591434384152961526');
--Testcase 373:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,3,'-24926799.735047420');
--Testcase 374:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,3,'-24926808.355047420');
--Testcase 375:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,3,'-107434525.43415438020');
--Testcase 376:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,3,'-5783481.21694835730858468677');
--Testcase 377:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,4,'-17127342.633147420');
--Testcase 378:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,4,'-32726265.456947420');
--Testcase 379:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,4,'-194415646271340.1815956522980');
--Testcase 380:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,4,'-3.19596478892958416484');
--Testcase 381:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,5,'-24910407.006556420');
--Testcase 382:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,5,'-24943201.083538420');
--Testcase 383:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,5,'-408725765384.257043660243220');
--Testcase 384:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,5,'-1520.20159364322004505807');
--Testcase 385:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,6,'-24832902.467417160');
--Testcase 386:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,6,'-25020705.622677680');
--Testcase 387:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,6,'-2340666225110.29929521292692920');
--Testcase 388:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,6,'-265.45671195426965751280');
--Testcase 389:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,7,'-107955289.045047420');
--Testcase 390:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,7,'58101680.954952580');
--Testcase 391:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,7,'2069634775752159.035758700');
--Testcase 392:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,7,'.30021990699995814689');
--Testcase 393:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,8,'-24851923.045047420');
--Testcase 394:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,8,'-25001685.045047420');
--Testcase 395:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,8,'-1866544013697.195857020');
--Testcase 396:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,8,'-332.88556569820675471748');
--Testcase 397:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,9,'-49853608.090094840');
--Testcase 398:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,9,'0');
--Testcase 399:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,9,'621345559900192.420120630048656400');
--Testcase 400:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,9,'1.00000000000000000000');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
--Testcase 401:
INSERT INTO num_exp_sqrt(id, expected) VALUES (0,'0');
--Testcase 402:
INSERT INTO num_exp_sqrt(id, expected) VALUES (1,'0');
--Testcase 403:
INSERT INTO num_exp_sqrt(id, expected) VALUES (2,'5859.90547836712524903505');
--Testcase 404:
INSERT INTO num_exp_sqrt(id, expected) VALUES (3,'2.07605394920266944396');
--Testcase 405:
INSERT INTO num_exp_sqrt(id, expected) VALUES (4,'2792.75158435189147418923');
--Testcase 406:
INSERT INTO num_exp_sqrt(id, expected) VALUES (5,'128.05092147657509145473');
--Testcase 407:
INSERT INTO num_exp_sqrt(id, expected) VALUES (6,'306.43364311096782703406');
--Testcase 408:
INSERT INTO num_exp_sqrt(id, expected) VALUES (7,'9111.99676251039939975230');
--Testcase 409:
INSERT INTO num_exp_sqrt(id, expected) VALUES (8,'273.64392922189960397542');
--Testcase 410:
INSERT INTO num_exp_sqrt(id, expected) VALUES (9,'4992.67503899937593364766');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
--Testcase 411:
INSERT INTO num_exp_ln(id, expected) VALUES (0,'NaN');
--Testcase 412:
INSERT INTO num_exp_ln(id, expected) VALUES (1,'NaN');
--Testcase 413:
INSERT INTO num_exp_ln(id, expected) VALUES (2,'17.35177750493897715514');
--Testcase 414:
INSERT INTO num_exp_ln(id, expected) VALUES (3,'1.46093790411565641971');
--Testcase 415:
INSERT INTO num_exp_ln(id, expected) VALUES (4,'15.86956523951936572464');
--Testcase 416:
INSERT INTO num_exp_ln(id, expected) VALUES (5,'9.70485601768871834038');
--Testcase 417:
INSERT INTO num_exp_ln(id, expected) VALUES (6,'11.45000246622944403127');
--Testcase 418:
INSERT INTO num_exp_ln(id, expected) VALUES (7,'18.23469429965478772991');
--Testcase 419:
INSERT INTO num_exp_ln(id, expected) VALUES (8,'11.22365546576315513668');
--Testcase 420:
INSERT INTO num_exp_ln(id, expected) VALUES (9,'17.03145425013166006962');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
--Testcase 421:
INSERT INTO num_exp_log10(id, expected) VALUES (0,'NaN');
--Testcase 422:
INSERT INTO num_exp_log10(id, expected) VALUES (1,'NaN');
--Testcase 423:
INSERT INTO num_exp_log10(id, expected) VALUES (2,'7.53578122160797276459');
--Testcase 424:
INSERT INTO num_exp_log10(id, expected) VALUES (3,'.63447727016073160075');
--Testcase 425:
INSERT INTO num_exp_log10(id, expected) VALUES (4,'6.89206461372691743345');
--Testcase 426:
INSERT INTO num_exp_log10(id, expected) VALUES (5,'4.21476541614777768626');
--Testcase 427:
INSERT INTO num_exp_log10(id, expected) VALUES (6,'4.97267288886207207671');
--Testcase 428:
INSERT INTO num_exp_log10(id, expected) VALUES (7,'7.91922711353275546914');
--Testcase 429:
INSERT INTO num_exp_log10(id, expected) VALUES (8,'4.87437163556421004138');
--Testcase 430:
INSERT INTO num_exp_log10(id, expected) VALUES (9,'7.39666659961986567059');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
--Testcase 431:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (0,'NaN');
--Testcase 432:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (1,'NaN');
--Testcase 433:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (2,'224790267919917955.13261618583642653184');
--Testcase 434:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (3,'28.90266599445155957393');
--Testcase 435:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (4,'7405685069594999.07733999469386277636');
--Testcase 436:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (5,'5068226527.32127265408584640098');
--Testcase 437:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (6,'281839893606.99372343357047819067');
--Testcase 438:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (7,'1716699575118597095.42330819910640247627');
--Testcase 439:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (8,'167361463828.07491320069016125952');
--Testcase 440:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (9,'107511333880052007.04141124673540337457');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
--Testcase 441:
INSERT INTO num_data(id, val) VALUES (0, '0');
--Testcase 442:
INSERT INTO num_data(id, val) VALUES (1, '0');
--Testcase 443:
INSERT INTO num_data(id, val) VALUES (2, '-34338492.215397047');
--Testcase 444:
INSERT INTO num_data(id, val) VALUES (3, '4.31');
--Testcase 445:
INSERT INTO num_data(id, val) VALUES (4, '7799461.4119');
--Testcase 446:
INSERT INTO num_data(id, val) VALUES (5, '16397.038491');
--Testcase 447:
INSERT INTO num_data(id, val) VALUES (6, '93901.57763026');
--Testcase 448:
INSERT INTO num_data(id, val) VALUES (7, '-83028485');
--Testcase 449:
INSERT INTO num_data(id, val) VALUES (8, '74881');
--Testcase 450:
INSERT INTO num_data(id, val) VALUES (9, '-24926804.045047420');
COMMIT TRANSACTION;

-- ******************************
-- * Create indices for faster checks
-- ******************************
-- Skip these setting, creating foreign table with primary key already covered.

--CREATE UNIQUE INDEX num_exp_add_idx ON num_exp_add (id1, id2);
--CREATE UNIQUE INDEX num_exp_sub_idx ON num_exp_sub (id1, id2);
--CREATE UNIQUE INDEX num_exp_div_idx ON num_exp_div (id1, id2);
--CREATE UNIQUE INDEX num_exp_mul_idx ON num_exp_mul (id1, id2);
--CREATE UNIQUE INDEX num_exp_sqrt_idx ON num_exp_sqrt (id);
--CREATE UNIQUE INDEX num_exp_ln_idx ON num_exp_ln (id);
--CREATE UNIQUE INDEX num_exp_log10_idx ON num_exp_log10 (id);
--CREATE UNIQUE INDEX num_exp_power_10_ln_idx ON num_exp_power_10_ln (id);

--VACUUM ANALYZE num_exp_add;
--VACUUM ANALYZE num_exp_sub;
--VACUUM ANALYZE num_exp_div;
--VACUUM ANALYZE num_exp_mul;
--VACUUM ANALYZE num_exp_sqrt;
--VACUUM ANALYZE num_exp_ln;
--VACUUM ANALYZE num_exp_log10;
--VACUUM ANALYZE num_exp_power_10_ln;

-- ******************************
-- * Now check the behaviour of the NUMERIC type
-- ******************************

-- ******************************
-- * Addition check
-- ******************************
--Testcase 451:
DELETE FROM num_result;
--Testcase 452:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, t1.val + t2.val
    FROM num_data t1, num_data t2;
--Testcase 453:
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_add t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

--Testcase 454:
DELETE FROM num_result;
--Testcase 455:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, round((t1.val + t2.val)::numeric, 10)
    FROM num_data t1, num_data t2;
--Testcase 456:
SELECT t1.id1, t1.id2, t1.result, round(t2.expected::numeric, 10) as expected
    FROM num_result t1, num_exp_add t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != round(t2.expected::numeric, 10);

-- ******************************
-- * Subtraction check
-- ******************************
--Testcase 457:
DELETE FROM num_result;
--Testcase 458:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, t1.val - t2.val
    FROM num_data t1, num_data t2;
--Testcase 459:
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_sub t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

--Testcase 460:
DELETE FROM num_result;
--Testcase 461:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, round((t1.val - t2.val)::numeric, 40)
    FROM num_data t1, num_data t2;
--Testcase 462:
SELECT t1.id1, t1.id2, t1.result, round(t2.expected::numeric, 40)
    FROM num_result t1, num_exp_sub t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != round(t2.expected::numeric, 40);

-- ******************************
-- * Multiply check
-- ******************************
--Testcase 463:
DELETE FROM num_result;
--Testcase 464:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, t1.val * t2.val
    FROM num_data t1, num_data t2;
--Testcase 465:
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_mul t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

--Testcase 466:
DELETE FROM num_result;
--Testcase 467:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, round((t1.val * t2.val)::numeric, 30)
    FROM num_data t1, num_data t2;
--Testcase 468:
SELECT t1.id1, t1.id2, t1.result, round(t2.expected::numeric, 30) as expected
    FROM num_result t1, num_exp_mul t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != round(t2.expected::numeric, 30);

-- ******************************
-- * Division check
-- ******************************
--Testcase 469:
DELETE FROM num_result;
--Testcase 470:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, t1.val / t2.val
    FROM num_data t1, num_data t2
    WHERE t2.val != '0.0';
--Testcase 471:
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_div t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

--Testcase 472:
DELETE FROM num_result;
--Testcase 473:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, round((t1.val / t2.val)::numeric, 80)
    FROM num_data t1, num_data t2
    WHERE t2.val != '0.0';
--Testcase 474:
SELECT t1.id1, t1.id2, t1.result::numeric(210,80), round(t2.expected::numeric, 80) as expected
    FROM num_result t1, num_exp_div t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected::numeric, 80);

-- ******************************
-- * Square root check
-- ******************************
--Testcase 475:
DELETE FROM num_result;
--Testcase 476:
INSERT INTO num_result(id1, id2, result) SELECT id, 0, SQRT(ABS(val))
    FROM num_data;
--Testcase 477:
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_sqrt t2
    WHERE t1.id1 = t2.id
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

-- ******************************
-- * Natural logarithm check
-- ******************************
--Testcase 478:
DELETE FROM num_result;
--Testcase 479:
INSERT INTO num_result(id1, id2, result) SELECT id, 0, LN(ABS(val))
    FROM num_data
    WHERE val != '0.0';
--Testcase 480:
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_ln t2
    WHERE t1.id1 = t2.id
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

-- ******************************
-- * Logarithm base 10 check
-- ******************************
--Testcase 481:
DELETE FROM num_result;
--Testcase 482:
INSERT INTO num_result(id1, id2, result) SELECT id, 0, LOG(numeric '10', ABS(val::numeric))
    FROM num_data
    WHERE val != '0.0';
--Testcase 483:
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_log10 t2
    WHERE t1.id1 = t2.id
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

-- ******************************
-- * POWER(10, LN(value)) check
-- ******************************
--Testcase 484:
DELETE FROM num_result;
--Testcase 485:
INSERT INTO num_result(id1, id2, result) SELECT id, 0, POWER(numeric '10', LN(ABS(round(val::numeric,200))))
    FROM num_data
    WHERE val != '0.0';
--Testcase 486:
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_power_10_ln t2
    WHERE t1.id1 = t2.id
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

-- ******************************
-- * miscellaneous checks for things that have been broken in the past...
-- ******************************
-- numeric AVG used to fail on some platforms
--Testcase 487:
SELECT AVG(val) FROM num_data;
--Testcase 488:
SELECT STDDEV(val) FROM num_data;
--Testcase 489:
SELECT VARIANCE(val) FROM num_data;

-- Check for appropriate rounding and overflow
--Testcase 793:
CREATE FOREIGN TABLE fract_only (id serial OPTIONS (rowkey 'true'), val float8) server griddb_svr;
--Testcase 490:
INSERT INTO fract_only VALUES (1, '0.0'::numeric(4,4));
--Testcase 491:
INSERT INTO fract_only VALUES (2, '0.1'::numeric(4,4));
--Testcase 492:
INSERT INTO fract_only VALUES (3, '1.0'::numeric(4,4));	-- should fail
--Testcase 493:
INSERT INTO fract_only VALUES (4, '-0.9999'::numeric(4,4));
--Testcase 494:
INSERT INTO fract_only VALUES (5, '0.99994'::numeric(4,4));
--Testcase 495:
INSERT INTO fract_only VALUES (6, '0.99995'::numeric(4,4));  -- should fail
--Testcase 496:
INSERT INTO fract_only VALUES (7, '0.00001'::numeric(4,4));
--Testcase 497:
INSERT INTO fract_only VALUES (8, '0.00017'::numeric(4,4));
--Testcase 498:
SELECT id, val::numeric(4,4) FROM fract_only;

-- Check inf/nan conversion behavior
--Testcase 499:
DELETE FROM fract_only;
--Testcase 500:
INSERT INTO fract_only(val) VALUES ('NaN'::float8);
--Testcase 501:
SELECT val::numeric AS numeric FROM fract_only;
--Testcase 502:
DELETE FROM fract_only;
--Testcase 503:
INSERT INTO fract_only(val) VALUES ('Infinity'::float8);
--Testcase 504:
SELECT val::numeric FROM fract_only;
--Testcase 505:
DELETE FROM fract_only;
--Testcase 506:
INSERT INTO fract_only(val) VALUES ('-Infinity'::float8);
--Testcase 507:
SELECT val::numeric FROM fract_only;
--Testcase 508:
DELETE FROM fract_only;
--Testcase 509:
INSERT INTO fract_only(val) VALUES ('NaN'::float8);
--Testcase 510:
SELECT val::numeric AS numeric FROM fract_only;
--Testcase 511:
DELETE FROM fract_only;
--Testcase 512:
INSERT INTO fract_only(val) VALUES ('Infinity'::float4);
--Testcase 513:
SELECT val::numeric FROM fract_only;
--Testcase 514:
DELETE FROM fract_only;
--Testcase 515:
INSERT INTO fract_only(val) VALUES ('-Infinity'::float4);
--Testcase 516:
SELECT val::numeric FROM fract_only;
--Testcase 517:
DELETE FROM fract_only;
--Testcase 794:
DROP FOREIGN TABLE fract_only;

-- Simple check that ceil(), floor(), and round() work correctly
--Testcase 795:
CREATE FOREIGN TABLE ceil_floor_round (id serial options (rowkey 'true'), a float8) SERVER griddb_svr;
--Testcase 518:
INSERT INTO ceil_floor_round(a) VALUES ('-5.5');
--Testcase 519:
INSERT INTO ceil_floor_round(a) VALUES ('-5.499999');
--Testcase 520:
INSERT INTO ceil_floor_round(a) VALUES ('9.5');
--Testcase 521:
INSERT INTO ceil_floor_round(a) VALUES ('9.4999999');
--Testcase 522:
INSERT INTO ceil_floor_round(a) VALUES ('0.0');
--Testcase 523:
INSERT INTO ceil_floor_round(a) VALUES ('0.0000001');
--Testcase 524:
INSERT INTO ceil_floor_round(a) VALUES ('-0.000001');
--Testcase 525:
SELECT a::numeric, ceil(a::numeric), ceiling(a::numeric), floor(a::numeric), round(a::numeric) FROM ceil_floor_round;

-- Check rounding, it should round ties away from zero.
--Testcase 526:
DELETE FROM ceil_floor_round;
--Testcase 527:
INSERT INTO ceil_floor_round(a) SELECT * FROM generate_series(-5,5);
--Testcase 528:
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

--Testcase 796:
CREATE FOREIGN TABLE width_bucket_tbl (
	id serial OPTIONS (rowkey 'true'),
	id1 float8,
	id2 float8,
	id3 float8,
	id4 int
) SERVER griddb_svr;

--Testcase 529:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (5.0, 3.0, 4.0, 0);
--Testcase 797:
SELECT width_bucket(id1, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 530:
DELETE FROM width_bucket_tbl;
--Testcase 798:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (5.0, 3.0, 4.0, -5);
--Testcase 799:
SELECT width_bucket(id1, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 531:
DELETE FROM width_bucket_tbl;
--Testcase 800:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (3.5, 3.0, 3.0, 888);
--Testcase 801:
SELECT width_bucket(id1, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 532:
DELETE FROM width_bucket_tbl;
--Testcase 802:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (5.0, 3.0, 4.0, 0);
--Testcase 803:
SELECT width_bucket(id1::float8, id2::float8, id3::float8, id4) FROM width_bucket_tbl;

--Testcase 533:
DELETE FROM width_bucket_tbl;
--Testcase 804:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (5.0, 3.0, 4.0, -5);
--Testcase 805:
SELECT width_bucket(id1::float8, id2::float8, id3::float8, id4) FROM width_bucket_tbl;

--Testcase 534:
DELETE FROM width_bucket_tbl;
--Testcase 806:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (3.5, 3.0, 3.0, 888);
--Testcase 807:
SELECT width_bucket(id1::float8, id2::float8, id3::float8, id4) FROM width_bucket_tbl;

--Testcase 535:
DELETE FROM width_bucket_tbl;
--Testcase 808:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES ('NaN'::numeric, 3.0, 4.0, 888);
--Testcase 809:
SELECT width_bucket(id1, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 536:
DELETE FROM width_bucket_tbl;
--Testcase 810:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (0, 'NaN'::numeric, 4.0, 888);
--Testcase 811:
SELECT width_bucket(id1::float8, id2, id3::float8, id4) FROM width_bucket_tbl;

-- normal operation
--Testcase 812:
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

--Testcase 537:
UPDATE width_bucket_test SET operand_f8 = operand_num::float8;

--Testcase 538:
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
--Testcase 539:
DELETE FROM width_bucket_tbl;
--Testcase 813:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (0.0, 'Infinity', 5, 10);
--Testcase 814:
SELECT width_bucket(id1::float8, id2::float8, id3, id4) FROM width_bucket_tbl;  -- error
--Testcase 540:
DELETE FROM width_bucket_tbl;
--Testcase 815:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (0.0, 5, '-Infinity', 20);
--Testcase 816:
SELECT width_bucket(id1::float8, id2, id3::float8, id4) FROM width_bucket_tbl; -- error
--Testcase 541:
DELETE FROM width_bucket_tbl;
--Testcase 817:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES ('Infinity', 1, 10, 10);
--Testcase 818:
SELECT width_bucket(id1::float8, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 819:
DELETE FROM width_bucket_tbl;
--Testcase 820:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES ('-Infinity', 1, 10, 10);
--Testcase 821:
SELECT width_bucket(id1::float8, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 822:
DROP FOREIGN TABLE width_bucket_test;

-- TO_CHAR()
--
--Testcase 542:
SELECT '' AS to_char_1, to_char(val::numeric(210,10), '9G999G999G999G999G999')
	FROM num_data;

--Testcase 543:
SELECT '' AS to_char_2, to_char(val::numeric(210,10), '9G999G999G999G999G999D999G999G999G999G999')
	FROM num_data;

--Testcase 544:
SELECT '' AS to_char_3, to_char(val::numeric(210,10), '9999999999999999.999999999999999PR')
	FROM num_data;

--Testcase 545:
SELECT '' AS to_char_4, to_char(val::numeric(210,10), '9999999999999999.999999999999999S')
	FROM num_data;

--Testcase 546:
SELECT '' AS to_char_5,  to_char(val::numeric(210,10), 'MI9999999999999999.999999999999999')     FROM num_data;
--Testcase 547:
SELECT '' AS to_char_6,  to_char(val::numeric(210,10), 'FMS9999999999999999.999999999999999')    FROM num_data;
--Testcase 548:
SELECT '' AS to_char_7,  to_char(val::numeric(210,10), 'FM9999999999999999.999999999999999THPR') FROM num_data;
--Testcase 549:
SELECT '' AS to_char_8,  to_char(val::numeric(210,10), 'SG9999999999999999.999999999999999th')   FROM num_data;
--Testcase 550:
SELECT '' AS to_char_9,  to_char(val::numeric(210,10), '0999999999999999.999999999999999')       FROM num_data;
--Testcase 551:
SELECT '' AS to_char_10, to_char(val::numeric(210,10), 'S0999999999999999.999999999999999')      FROM num_data;
--Testcase 552:
SELECT '' AS to_char_11, to_char(val::numeric(210,10), 'FM0999999999999999.999999999999999')     FROM num_data;
--Testcase 553:
SELECT '' AS to_char_12, to_char(val::numeric(210,10), 'FM9999999999999999.099999999999999') 	FROM num_data;
--Testcase 554:
SELECT '' AS to_char_13, to_char(val::numeric(210,10), 'FM9999999999990999.990999999999999') 	FROM num_data;
--Testcase 555:
SELECT '' AS to_char_14, to_char(val::numeric(210,10), 'FM0999999999999999.999909999999999') 	FROM num_data;
--Testcase 556:
SELECT '' AS to_char_15, to_char(val::numeric(210,10), 'FM9999999990999999.099999999999999') 	FROM num_data;
--Testcase 557:
SELECT '' AS to_char_16, to_char(val::numeric(210,10), 'L9999999999999999.099999999999999')	FROM num_data;
--Testcase 558:
SELECT '' AS to_char_17, to_char(val::numeric(210,10), 'FM9999999999999999.99999999999999')	FROM num_data;
--Testcase 559:
SELECT '' AS to_char_18, to_char(val::numeric(210,10), 'S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9') FROM num_data;
--Testcase 560:
SELECT '' AS to_char_19, to_char(val::numeric(210,10), 'FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9') FROM num_data;
--Testcase 561:
SELECT '' AS to_char_20, to_char(val::numeric(210,10), E'99999 "text" 9999 "9999" 999 "\\"text between quote marks\\"" 9999') FROM num_data;
--Testcase 562:
SELECT '' AS to_char_21, to_char(val::numeric(210,10), '999999SG9999999999')			FROM num_data;
--Testcase 563:
SELECT '' AS to_char_22, to_char(val::numeric(210,10), 'FM9999999999999999.999999999999999')	FROM num_data;
--Testcase 564:
SELECT '' AS to_char_23, to_char(val::numeric(210,10), '9.999EEEE')				FROM num_data;

--Testcase 565:
DELETE FROM ceil_floor_round;
--Testcase 566:
INSERT INTO ceil_floor_round(a) VALUES ('100'::numeric);
--Testcase 567:
SELECT '' AS to_char_24, to_char(a::numeric, 'FM999.9') FROM ceil_floor_round;
--Testcase 568:
SELECT '' AS to_char_25, to_char(a::numeric, 'FM999.') FROM ceil_floor_round;
--Testcase 569:
SELECT '' AS to_char_26, to_char(a::numeric, 'FM999') FROM ceil_floor_round;

-- Check parsing of literal text in a format string
--Testcase 570:
SELECT '' AS to_char_27, to_char(a::numeric, 'foo999') FROM ceil_floor_round;
--Testcase 571:
SELECT '' AS to_char_28, to_char(a::numeric, 'f\oo999') FROM ceil_floor_round;
--Testcase 572:
SELECT '' AS to_char_29, to_char(a::numeric, 'f\\oo999') FROM ceil_floor_round;
--Testcase 573:
SELECT '' AS to_char_30, to_char(a::numeric, 'f\"oo999') FROM ceil_floor_round;
--Testcase 574:
SELECT '' AS to_char_31, to_char(a::numeric, 'f\\"oo999') FROM ceil_floor_round;
--Testcase 575:
SELECT '' AS to_char_32, to_char(a::numeric, 'f"ool"999') FROM ceil_floor_round;
--Testcase 576:
SELECT '' AS to_char_33, to_char(a::numeric, 'f"\ool"999') FROM ceil_floor_round;
--Testcase 577:
SELECT '' AS to_char_34, to_char(a::numeric, 'f"\\ool"999') FROM ceil_floor_round;
--Testcase 578:
SELECT '' AS to_char_35, to_char(a::numeric, 'f"ool\"999') FROM ceil_floor_round;
--Testcase 579:
SELECT '' AS to_char_36, to_char(a::numeric, 'f"ool\\"999') FROM ceil_floor_round;

-- TO_NUMBER()
--
--Testcase 1010:
SET lc_numeric = 'C';
--Testcase 823:
CREATE FOREIGN TABLE to_number_test (
	id serial OPTIONS (rowkey 'true'),
	val text,
	fmt text
) SERVER griddb_svr;

--Testcase 580:
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
--Testcase 581:
SELECT id AS to_number,  to_number(val, fmt) from to_number_test;
--Testcase 1011:
RESET lc_numeric;
--Testcase 824:
DROP FOREIGN TABLE to_number_test;

--
-- Input syntax
--

--Testcase 825:
CREATE FOREIGN TABLE num_input_test (id serial options (rowkey 'true'), n1 float8) SERVER griddb_svr;

-- good inputs
--Testcase 582:
INSERT INTO num_input_test(n1) VALUES (' 123');
--Testcase 583:
INSERT INTO num_input_test(n1) VALUES ('   3245874    ');
--Testcase 584:
INSERT INTO num_input_test(n1) VALUES ('  -93853');
--Testcase 585:
INSERT INTO num_input_test(n1) VALUES ('555.50');
--Testcase 586:
INSERT INTO num_input_test(n1) VALUES ('-555.50');
--Testcase 587:
INSERT INTO num_input_test(n1) VALUES ('NaN ');
--Testcase 588:
INSERT INTO num_input_test(n1) VALUES ('        nan');

-- bad inputs
--Testcase 589:
INSERT INTO num_input_test(n1) VALUES ('     ');
--Testcase 590:
INSERT INTO num_input_test(n1) VALUES ('   1234   %');
--Testcase 591:
INSERT INTO num_input_test(n1) VALUES ('xyz');
--Testcase 592:
INSERT INTO num_input_test(n1) VALUES ('- 1234');
--Testcase 593:
INSERT INTO num_input_test(n1) VALUES ('5 . 0');
--Testcase 594:
INSERT INTO num_input_test(n1) VALUES ('5. 0   ');
--Testcase 595:
INSERT INTO num_input_test(n1) VALUES ('');
--Testcase 596:
INSERT INTO num_input_test(n1) VALUES (' N aN ');

--Testcase 597:
SELECT n1 FROM num_input_test;

--
-- Test some corner cases for multiplication
--

--Testcase 826:
CREATE FOREIGN TABLE num_test_calc (
    id serial options (rowkey 'true'),
    n1 float8,
    n2 float8
) SERVER griddb_svr;

--Testcase 598:
DELETE FROM num_test_calc;
--Testcase 599:
INSERT INTO num_test_calc(n1, n2) VALUES (4790999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
--Testcase 600:
INSERT INTO num_test_calc(n1, n2) VALUES (4790999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
--Testcase 601:
INSERT INTO num_test_calc(n1, n2) VALUES (4789999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
--Testcase 602:
INSERT INTO num_test_calc(n1, n2) VALUES (4770999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
--Testcase 603:
INSERT INTO num_test_calc(n1, n2) VALUES (4769999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);
--Testcase 604:
SELECT n1::numeric * n2::numeric FROM num_test_calc;

--
-- Test some corner cases for division
--
--Testcase 605:
DELETE FROM num_test_calc;
--Testcase 606:
INSERT INTO num_test_calc(n1, n2) VALUES (999999999999999999999, 1000000000000000000000);
--Testcase 607:
SELECT n1::numeric/n2::numeric FROM num_test_calc;
--Testcase 608:
SELECT div(n1::numeric,n2::numeric) FROM num_test_calc;
--Testcase 609:
SELECT mod(n1::numeric,n2::numeric) FROM num_test_calc;
--Testcase 610:
SELECT div(-n1::numeric,n2::numeric) FROM num_test_calc;
--Testcase 611:
SELECT mod(-n1::numeric,n2::numeric) FROM num_test_calc;
--Testcase 612:
SELECT div(-n1::numeric,n2::numeric)*n2 + 
	mod(-n1::numeric,n2::numeric) FROM num_test_calc;

--Testcase 613:
DELETE FROM num_test_calc;
--Testcase 614:
INSERT INTO num_test_calc(n1, n2) VALUES (70.0, 70);
--Testcase 615:
SELECT mod (n1::numeric,n2::numeric) FROM num_test_calc;
--Testcase 616:
SELECT div (n1::numeric,n2::numeric) FROM num_test_calc;
--Testcase 617:
SELECT n1::numeric / n2::numeric FROM num_test_calc;

--Testcase 618:
DELETE FROM num_test_calc;
--Testcase 619:
INSERT INTO num_test_calc(n1, n2) VALUES (12345678901234567890, 123);
--Testcase 620:
SELECT n1::numeric % n2::numeric FROM num_test_calc;
--Testcase 621:
SELECT n1::numeric / n2::numeric FROM num_test_calc;
--Testcase 622:
SELECT div(n1::numeric, n2::numeric) FROM num_test_calc;
--Testcase 623:
SELECT div(n1::numeric, n2::numeric) * n2::numeric + (n1::numeric % n2::numeric) FROM num_test_calc;

--
-- Test some corner cases for square root
--

--Testcase 827:
DELETE FROM num_test_calc;
--Testcase 828:
INSERT INTO num_test_calc(n1, n2) VALUES (1.000000000000003, 0);
--Testcase 829:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 830:
DELETE FROM num_test_calc;
--Testcase 831:
INSERT INTO num_test_calc(n1, n2) VALUES (1.000000000000004, 0);
--Testcase 832:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 833:
DELETE FROM num_test_calc;
--Testcase 834:
INSERT INTO num_test_calc(n1, n2) VALUES (96627521408608.56340355805, 0);
--Testcase 835:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 836:
DELETE FROM num_test_calc;
--Testcase 837:
INSERT INTO num_test_calc(n1, n2) VALUES (96627521408608.56340355806, 0);
--Testcase 838:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 839:
DELETE FROM num_test_calc;
--Testcase 840:
INSERT INTO num_test_calc(n1, n2) VALUES (515549506212297735.073688290367, 0);
--Testcase 841:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 842:
DELETE FROM num_test_calc;
--Testcase 843:
INSERT INTO num_test_calc(n1, n2) VALUES (515549506212297735.073688290368, 0);
--Testcase 844:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 845:
DELETE FROM num_test_calc;
--Testcase 846:
INSERT INTO num_test_calc(n1, n2) VALUES (8015491789940783531003294973900306, 0);
--Testcase 847:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 848:
DELETE FROM num_test_calc;
--Testcase 849:
INSERT INTO num_test_calc(n1, n2) VALUES (8015491789940783531003294973900307, 0);
--Testcase 850:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--
-- Test code path for raising to integer powers
--
--Testcase 624:
DELETE FROM num_test_calc;
--Testcase 625:
INSERT INTO num_test_calc(n1, n2) VALUES (10.0, -2147483648);
--Testcase 851:
SELECT n1::numeric ^ n2::numeric as rounds_to_zero FROM num_test_calc;
--Testcase 626:
DELETE FROM num_test_calc;
--Testcase 852:
INSERT INTO num_test_calc(n1, n2) VALUES (10.0, -2147483647);
--Testcase 853:
SELECT n1::numeric ^ n2::numeric as rounds_to_zero FROM num_test_calc;
--Testcase 627:
DELETE FROM num_test_calc;
--Testcase 854:
INSERT INTO num_test_calc(n1, n2) VALUES (10.0, 2147483647);
--Testcase 855:
SELECT n1::numeric ^ n2::numeric as overflows FROM num_test_calc;
--Testcase 628:
DELETE FROM num_test_calc;
--Testcase 856:
INSERT INTO num_test_calc(n1, n2) VALUES (117743296169.0, 1000000000);
--Testcase 857:
SELECT n1::numeric ^ n2::numeric as overflows FROM num_test_calc;
--Testcase 629:

-- cases that used to return inaccurate results
--Testcase 630:
DELETE FROM num_test_calc;
--Testcase 631:
INSERT INTO num_test_calc(n1, n2) VALUES (3.789, 21);
--Testcase 632:
INSERT INTO num_test_calc(n1, n2) VALUES (3.789, 35);
--Testcase 633:
INSERT INTO num_test_calc(n1, n2) VALUES (1.2, 345);
--Testcase 634:
INSERT INTO num_test_calc(n1, n2) VALUES (0.12, (-20));
--Testcase 635:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

-- cases that used to error out
--Testcase 636:
DELETE FROM num_test_calc;
--Testcase 637:
INSERT INTO num_test_calc(n1, n2) VALUES (0.12, -25);
--Testcase 638:
INSERT INTO num_test_calc(n1, n2) VALUES (0.5678, -85);
--Testcase 639:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

--
-- Tests for raising to non-integer powers
--

-- special cases
--Testcase 640:
DELETE FROM num_test_calc;
--Testcase 641:
INSERT INTO num_test_calc(n1, n2) VALUES (0.0, 0.0);
--Testcase 642:
INSERT INTO num_test_calc(n1, n2) VALUES (-12.34, 0.0);
--Testcase 643:
INSERT INTO num_test_calc(n1, n2) VALUES (12.34, 0.0);
--Testcase 644:
INSERT INTO num_test_calc(n1, n2) VALUES (0.0, 12.34);
--Testcase 645:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

-- NaNs
--Testcase 646:
DELETE FROM num_test_calc;
--Testcase 647:
INSERT INTO num_test_calc(n1, n2) VALUES ('NaN'::numeric, 'NaN'::numeric);
--Testcase 648:
INSERT INTO num_test_calc(n1, n2) VALUES ('NaN'::numeric, 0);
--Testcase 649:
INSERT INTO num_test_calc(n1, n2) VALUES ('NaN'::numeric, 1);
--Testcase 650:
INSERT INTO num_test_calc(n1, n2) VALUES (0, 'NaN'::numeric);
--Testcase 651:
INSERT INTO num_test_calc(n1, n2) VALUES (1, 'NaN'::numeric);
--Testcase 652:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

-- invalid inputs
--Testcase 653:
DELETE FROM num_test_calc;
--Testcase 654:
INSERT INTO num_test_calc(n1, n2) VALUES (0.0, -12.34);
--Testcase 858:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;
--Testcase 655:
DELETE FROM num_test_calc;
--Testcase 859:
INSERT INTO num_test_calc(n1, n2) VALUES (-12.34, 1.2);
--Testcase 860:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

-- cases that used to generate inaccurate results
--Testcase 657:
BEGIN;
--Testcase 861:
DELETE FROM num_test_calc;
--Testcase 658:
INSERT INTO num_test_calc(n1, n2) VALUES (32.1, 9.8);
--Testcase 862:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;
--Testcase 659:
DELETE FROM num_test_calc;
--Testcase 863:
INSERT INTO num_test_calc(n1, n2) VALUES (32.1, -9.8);
--Testcase 864:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;
--Testcase 660:
DELETE FROM num_test_calc;
--Testcase 865:
INSERT INTO num_test_calc(n1, n2) VALUES (12.3, 45.6);
--Testcase 866:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;
--Testcase 661:
DELETE FROM num_test_calc;
--Testcase 867:
INSERT INTO num_test_calc(n1, n2) VALUES (12.3, (-45.6));
--Testcase 868:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;
ROLLBACK;
-- big test
-- out of range
--Testcase 663:
BEGIN;
--Testcase 869:
DELETE FROM num_input_test;
--Testcase 664:
INSERT INTO num_test_calc(n1, n2) VALUES (1.234, 5678);
--Testcase 870:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;
ROLLBACK;
--
-- Tests for EXP()
--

-- special cases
--Testcase 666:
DELETE FROM num_input_test;
--Testcase 667:
INSERT INTO num_input_test(n1) VALUES ('0.0');
--Testcase 668:
SELECT exp(n1::numeric) from num_input_test;

--Testcase 669:
DELETE FROM num_input_test;
--Testcase 670:
INSERT INTO num_input_test(n1) VALUES ('1.0');
--Testcase 671:
SELECT exp(n1::numeric) from num_input_test;

--Testcase 672:
DELETE FROM num_input_test;
--Testcase 673:
INSERT INTO num_input_test(n1) VALUES ('1.0');
--Testcase 674:
SELECT exp(n1::numeric(71, 70)) from num_input_test;

-- cases that used to generate inaccurate results
--Testcase 675:
DELETE FROM num_input_test;
--Testcase 676:
INSERT INTO num_input_test(n1) VALUES ('32.999');
--Testcase 677:
SELECT exp(n1::numeric) from num_input_test;
--Testcase 678:
SELECT exp(-n1::numeric) from num_input_test;

--Testcase 679:
DELETE FROM num_input_test;
--Testcase 680:
INSERT INTO num_input_test(n1) VALUES ('123.456');
--Testcase 681:
SELECT exp(n1::numeric) from num_input_test;
--Testcase 682:
SELECT exp(-n1::numeric) from num_input_test;

-- big test
--Testcase 683:
DELETE FROM num_input_test;
--Testcase 684:
INSERT INTO num_input_test(n1) VALUES ('1234.5678');
--Testcase 685:
SELECT exp(n1::numeric) from num_input_test;

--
-- Tests for generate_series
--
--Testcase 686:
DELETE FROM num_input_test;
--Testcase 687:
INSERT INTO num_input_test(n1) select * from generate_series(0.0, 4.0);
--Testcase 688:
SELECT n1::numeric(2,1) FROM num_input_test;

--Testcase 689:
DELETE FROM num_input_test;
--Testcase 690:
INSERT INTO num_input_test(n1) select * from generate_series(0.1, 4.0, 1.3);
--Testcase 691:
SELECT n1::numeric(2,1) FROM num_input_test;

--Testcase 692:
DELETE FROM num_input_test;
--Testcase 693:
INSERT INTO num_input_test(n1) select * from generate_series(4.0, -1.5, -2.2);
--Testcase 694:
SELECT n1::numeric(2,1) FROM num_input_test;

-- Trigger errors
--Testcase 695:
DELETE FROM num_input_test;
--Testcase 693:
INSERT INTO num_input_test(n1) select * from generate_series(-100::numeric, 100::numeric, 0::numeric);
--Testcase 871:
SELECT n1 FROM num_input_test;
--Testcase 696:
DELETE FROM num_input_test;
--Testcase 872:
INSERT INTO num_input_test(n1) select * from generate_series(-100::numeric, 100::numeric, 'nan'::numeric);
--Testcase 873:
SELECT n1 FROM num_input_test;
--Testcase 697:
DELETE FROM num_input_test;
--Testcase 874:
INSERT INTO num_input_test(n1) select * from generate_series('nan'::numeric, 100::numeric, 10::numeric);
--Testcase 875:
SELECT n1 FROM num_input_test;
--Testcase 698:
DELETE FROM num_input_test;
--Testcase 876:
INSERT INTO num_input_test(n1) select * from generate_series(0::numeric, 'nan'::numeric, 10::numeric);
--Testcase 877:
SELECT n1 FROM num_input_test;
-- Checks maximum, output is truncated
--Testcase 699:
DELETE FROM num_input_test;
--Testcase 878:
INSERT INTO num_input_test(n1) select (i / (10::numeric ^ 131071))::numeric(1,0)
	from generate_series(6 * (10::numeric ^ 131071),
			     9 * (10::numeric ^ 131071),
			     10::numeric ^ 131071) as a(i);
--Testcase 879:
SELECT n1 AS numeric FROM num_input_test;

-- Check usage with variables
--Testcase 700:
DELETE FROM num_test_calc;
--Testcase 880:
INSERT INTO num_test_calc(n1, n2) select * from generate_series(1::numeric, 3::numeric) i, generate_series(i,3) j;
--Testcase 881:
SELECT n1 as i, n2 as j FROM num_test_calc;

--Testcase 701:
DELETE FROM num_test_calc;
--Testcase 882:
INSERT INTO num_test_calc(n1, n2) select * from generate_series(1::numeric, 3::numeric) i, generate_series(1,i) j;
--Testcase 883:
SELECT n1 as i, n2 as j FROM num_test_calc;

--Testcase 702:
DELETE FROM num_test_calc;
--Testcase 884:
INSERT INTO num_test_calc(n1, n2) select * from generate_series(1::numeric, 3::numeric) i, generate_series(1,5,i) j;
--Testcase 885:
SELECT n1 as i, n2 as j FROM num_test_calc;

--
-- Tests for LN()
--

-- Invalid inputs
--Testcase 703:
DELETE FROM num_input_test;
--Testcase 704:
INSERT INTO num_input_test(n1) values('-12.34');
--Testcase 705:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 706:
DELETE FROM num_input_test;
--Testcase 707:
INSERT INTO num_input_test(n1) values('0.0');
--Testcase 708:
SELECT ln(n1::numeric) FROM num_input_test;

-- Some random tests
--Testcase 709:
DELETE FROM num_input_test;
--Testcase 710:
INSERT INTO num_input_test(n1) values(1.2345678e-28);
--Testcase 711:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 712:
DELETE FROM num_input_test;
--Testcase 713:
INSERT INTO num_input_test(n1) values(0.0456789);
--Testcase 714:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 715:
DELETE FROM num_input_test;
--Testcase 716:
INSERT INTO num_input_test(n1) values(0.349873948359354029493948309745709580730482050975);
--Testcase 717:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 718:
DELETE FROM num_input_test;
--Testcase 719:
INSERT INTO num_input_test(n1) values(0.99949452);
--Testcase 720:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 721:
DELETE FROM num_input_test;
--Testcase 722:
INSERT INTO num_input_test(n1) values(1.00049687395);
--Testcase 723:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 724:
DELETE FROM num_input_test;
--Testcase 725:
INSERT INTO num_input_test(n1) values(1234.567890123456789);
--Testcase 726:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 727:
DELETE FROM num_input_test;
--Testcase 728:
INSERT INTO num_input_test(n1) values(5.80397490724e5);
--Testcase 729:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 730:
DELETE FROM num_input_test;
--Testcase 731:
INSERT INTO num_input_test(n1) values(9.342536355e34);
--Testcase 732:
SELECT ln(n1::numeric) FROM num_input_test;

-- 
-- Tests for LOG() (base 10)
--

-- invalid inputs
--Testcase 733:
DELETE FROM num_input_test;
--Testcase 734:
INSERT INTO num_input_test(n1) values('-12.34');
--Testcase 735:
SELECT log(n1::numeric) FROM num_input_test;

--Testcase 736:
DELETE FROM num_input_test;
--Testcase 737:
INSERT INTO num_input_test(n1) values('0.0');
--Testcase 738:
SELECT log(n1::numeric) FROM num_input_test;

-- some random tests
--Testcase 739:
DELETE FROM num_input_test;
--Testcase 740:
INSERT INTO num_input_test(n1) values(1.234567e-89);
--Testcase 741:
SELECT log(n1::numeric) FROM num_input_test;

--Testcase 742:
DELETE FROM num_input_test;
--Testcase 743:
INSERT INTO num_input_test(n1) values(3.4634998359873254962349856073435545);
--Testcase 744:
SELECT log(n1::numeric) FROM num_input_test;

--Testcase 745:
DELETE FROM num_input_test;
--Testcase 746:
INSERT INTO num_input_test(n1) values(9.999999999999999999);
--Testcase 747:
SELECT log(n1::numeric) FROM num_input_test;

--Testcase 748:
DELETE FROM num_input_test;
--Testcase 749:
INSERT INTO num_input_test(n1) values(10.00000000000000000);
--Testcase 750:
SELECT log(n1::numeric) FROM num_input_test;

--Testcase 751:
DELETE FROM num_input_test;
--Testcase 752:
INSERT INTO num_input_test(n1) values(10.00000000000000001);
--Testcase 753:
SELECT log(n1::numeric) FROM num_input_test;

--Testcase 754:
DELETE FROM num_input_test;
--Testcase 755:
INSERT INTO num_input_test(n1) values(590489.45235237);
--Testcase 756:
SELECT log(n1::numeric) FROM num_input_test;

-- similar as above test. Basically, we can get float8 value and 
-- convert to numeric
-- Tests for LOG() (arbitrary base)
--

-- invalid inputs
--Testcase 757:
DELETE FROM num_test_calc;
--Testcase 886:
INSERT INTO num_test_calc(n1, n2) VALUES(-12.34, 56.78);
--Testcase 887:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;
--Testcase 758:
DELETE FROM num_test_calc;
--Testcase 888:
INSERT INTO num_test_calc(n1, n2) VALUES(-12.34, -56.78);
--Testcase 889:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;
--Testcase 759:
DELETE FROM num_test_calc;
--Testcase 890:
INSERT INTO num_test_calc(n1, n2) VALUES(12.34, -56.78);
--Testcase 891:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;
--Testcase 760:
DELETE FROM num_test_calc;
--Testcase 892:
INSERT INTO num_test_calc(n1, n2) VALUES(0.0, 12.34);
--Testcase 893:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;
--Testcase 761:
DELETE FROM num_test_calc;
--Testcase 894:
INSERT INTO num_test_calc(n1, n2) VALUES(12.34, 0.0);
--Testcase 895:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;
--Testcase 762:
DELETE FROM num_test_calc;
--Testcase 896:
INSERT INTO num_test_calc(n1, n2) VALUES(1.0, 12.34);
--Testcase 897:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;

-- some random tests
--Testcase 763:
DELETE FROM num_test_calc;
--Testcase 898:
INSERT INTO num_test_calc(n1, n2) VALUES(1.23e-89, 6.4689e45);
--Testcase 899:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;
--Testcase 764:
DELETE FROM num_test_calc;
--Testcase 900:
INSERT INTO num_test_calc(n1, n2) VALUES(0.99923, 4.58934e34);
--Testcase 901:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;
--Testcase 765:
DELETE FROM num_test_calc;
--Testcase 902:
INSERT INTO num_test_calc(n1, n2) VALUES(1.000016, 8.452010e18);
--Testcase 903:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;
--Testcase 766:
DELETE FROM num_test_calc;
--Testcase 904:
INSERT INTO num_test_calc(n1, n2) VALUES(3.1954752e47, 9.4792021e-73);
--Testcase 905:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;

--
-- Tests for scale()
--

--Testcase 767:
DELETE FROM num_input_test;
--Testcase 906:
INSERT INTO num_input_test(n1) values(numeric 'NaN');
--Testcase 907:
SELECT scale(n1::numeric) FROM num_input_test;
--Testcase 768:
DELETE FROM num_input_test;
--Testcase 908:
INSERT INTO num_input_test(n1) values(NULL::numeric);
--Testcase 909:
SELECT scale(n1::numeric) FROM num_input_test;
--Testcase 769:
DELETE FROM num_input_test;
--Testcase 910:
INSERT INTO num_input_test(n1) values(1.12);
--Testcase 911:
SELECT scale(n1::numeric) FROM num_input_test;
--Testcase 770:
DELETE FROM num_input_test;
--Testcase 912:
INSERT INTO num_input_test(n1) values(0);
--Testcase 913:
SELECT scale(n1::numeric) FROM num_input_test;
--Testcase 771:
DELETE FROM num_input_test;
--Testcase 914:
INSERT INTO num_input_test(n1) values(0.00);
--Testcase 915:
SELECT scale(n1::numeric) FROM num_input_test;
--Testcase 772:
DELETE FROM num_input_test;
--Testcase 916:
INSERT INTO num_input_test(n1) values(1.12345);
--Testcase 917:
SELECT scale(n1::numeric) FROM num_input_test;
--Testcase 773:
DELETE FROM num_input_test;
--Testcase 918:
INSERT INTO num_input_test(n1) values(110123.12475871856128);
--Testcase 919:
SELECT scale(n1::numeric) FROM num_input_test;
--Testcase 774:
DELETE FROM num_input_test;
--Testcase 920:
INSERT INTO num_input_test(n1) values(-1123.12471856128);
--Testcase 921:
SELECT scale(n1::numeric) FROM num_input_test;
--Testcase 775:
DELETE FROM num_input_test;
--Testcase 922:
INSERT INTO num_input_test(n1) values(-13.000000000000000);
--Testcase 923:
SELECT scale(n1::numeric) FROM num_input_test;

--
-- Tests for min_scale()
--

--Testcase 924:
DELETE FROM num_input_test;
--Testcase 925:
INSERT INTO num_input_test(n1) values(numeric 'NaN');
--Testcase 926:
SELECT min_scale(n1::numeric) is NULL FROM num_input_test; -- should be true

--Testcase 927:
DELETE FROM num_input_test;
--Testcase 928:
INSERT INTO num_input_test(n1) values(0);
--Testcase 929:
SELECT min_scale(n1::numeric) FROM num_input_test; -- no digits

--Testcase 930:
DELETE FROM num_input_test;
--Testcase 931:
INSERT INTO num_input_test(n1) values(0.00);
--Testcase 932:
SELECT min_scale(n1::numeric) FROM num_input_test; -- no digits again

--Testcase 933:
DELETE FROM num_input_test;
--Testcase 934:
INSERT INTO num_input_test(n1) values(1.0);
--Testcase 935:
SELECT min_scale(n1::numeric) FROM num_input_test; -- no scale

--Testcase 936:
DELETE FROM num_input_test;
--Testcase 937:
INSERT INTO num_input_test(n1) values(1.1);
--Testcase 938:
SELECT min_scale(n1::numeric) FROM num_input_test; -- scale 1

--Testcase 939:
DELETE FROM num_input_test;
--Testcase 940:
INSERT INTO num_input_test(n1) values(1.12);
--Testcase 941:
SELECT min_scale(n1::numeric) FROM num_input_test; -- scale 2

--Testcase 942:
DELETE FROM num_input_test;
--Testcase 943:
INSERT INTO num_input_test(n1) values(1.123);
--Testcase 944:
SELECT min_scale(n1::numeric) FROM num_input_test; -- scale 3

--Testcase 945:
DELETE FROM num_input_test;
--Testcase 946:
INSERT INTO num_input_test(n1) values(1.1234);
--Testcase 947:
SELECT min_scale(n1::numeric) FROM num_input_test; -- scale 4, filled digit

--Testcase 948:
DELETE FROM num_input_test;
--Testcase 949:
INSERT INTO num_input_test(n1) values(1.12345);
--Testcase 950:
SELECT min_scale(n1::numeric) FROM num_input_test; -- scale 5, 2 NDIGITS

--Testcase 951:
DELETE FROM num_input_test;
--Testcase 952:
INSERT INTO num_input_test(n1) values(1.1000);
--Testcase 953:
SELECT min_scale(n1::numeric) FROM num_input_test; -- 1 pos in NDIGITS

--Testcase 954:
DELETE FROM num_input_test;
--Testcase 955:
INSERT INTO num_input_test(n1) values(1e100);
--Testcase 956:
SELECT min_scale(n1::numeric) FROM num_input_test; -- very big number

--
-- Tests for trim_scale()
--

--Testcase 957:
DELETE FROM num_input_test;
--Testcase 958:
INSERT INTO num_input_test(n1) values(numeric 'NaN');
--Testcase 959:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 960:
DELETE FROM num_input_test;
--Testcase 961:
INSERT INTO num_input_test(n1) values(1.120);
--Testcase 962:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 963:
DELETE FROM num_input_test;
--Testcase 964:
INSERT INTO num_input_test(n1) values(0);
--Testcase 965:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 966:
DELETE FROM num_input_test;
--Testcase 967:
INSERT INTO num_input_test(n1) values(0.00);
--Testcase 968:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 969:
DELETE FROM num_input_test;
--Testcase 970:
INSERT INTO num_input_test(n1) values(1.1234500);
--Testcase 971:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 972:
DELETE FROM num_input_test;
--Testcase 973:
INSERT INTO num_input_test(n1) values(110123.12475871856128000);
--Testcase 974:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 975:
DELETE FROM num_input_test;
--Testcase 976:
INSERT INTO num_input_test(n1) values(-1123.124718561280000000);
--Testcase 977:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 978:
DELETE FROM num_input_test;
--Testcase 979:
INSERT INTO num_input_test(n1) values(-13.00000000000000000000);
--Testcase 980:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 981:
DELETE FROM num_input_test;
--Testcase 982:
INSERT INTO num_input_test(n1) values(1e100);
--Testcase 983:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--
-- Tests for SUM()
--

-- cases that need carry propagation
--Testcase 776:
DELETE FROM num_input_test;
--Testcase 777:
INSERT INTO num_input_test(n1) values(generate_series(1, 100000));
--Testcase 778:
SELECT SUM(9999::numeric) FROM num_input_test;
--Testcase 779:
SELECT SUM((-9999)::numeric) FROM num_input_test;

--
-- Tests for GCD()
--
--Testcase 984:
DELETE FROM num_test_calc;
--Testcase 985:
INSERT INTO num_test_calc(n1, n2) VALUES 
             (0::numeric, 0::numeric),
             (0::numeric, numeric 'NaN'),
             (0::numeric, 46375::numeric),
             (433125::numeric, 46375::numeric),
             (43312.5::numeric, 4637.5::numeric),
             (4331.250::numeric, 463.75000::numeric);
--Testcase 986:
SELECT n1 as a, n2 as b, gcd(n1::numeric, n2::numeric), gcd(n1::numeric, -n2::numeric), gcd(-n2::numeric, n1::numeric), gcd(-n2::numeric, -n1::numeric) FROM num_test_calc;
--
-- Tests for LCM()
--
--Testcase 987:
DELETE FROM num_test_calc;
--Testcase 988:
INSERT INTO num_test_calc(n1, n2) VALUES 
             (0::numeric, 0::numeric),
             (0::numeric, numeric 'NaN'),
             (0::numeric, 13272::numeric),
             (13272::numeric, 13272::numeric),
             (423282::numeric, 13272::numeric),
             (42328.2::numeric, 1327.2::numeric),
             (4232.820::numeric, 132.72000::numeric);
--Testcase 989:
SELECT n1 as a, n2 as b, lcm(n1::numeric, n2::numeric), lcm(n1::numeric, -n2::numeric), lcm(-n2::numeric, n1::numeric), lcm(-n2::numeric, -n1::numeric) FROM num_test_calc;

--Testcase 990:
DELETE FROM num_test_calc;
--Testcase 991:
INSERT INTO num_test_calc(n1, n2) VALUES (10::numeric, 131068); 
--Testcase 992:
SELECT lcm((9999 * (n1::numeric)^n2::numeric + (n1::numeric^n2::numeric - 1)), 2) FROM num_test_calc; -- overflow

--Testcase 993:
DROP FOREIGN TABLE width_bucket_tbl;
--Testcase 994:
DROP FOREIGN TABLE num_test_calc;
--Testcase 995:
DROP FOREIGN TABLE num_data;
--Testcase 996:
DROP FOREIGN TABLE num_exp_add;
--Testcase 997:
DROP FOREIGN TABLE num_exp_sub;
--Testcase 998:
DROP FOREIGN TABLE num_exp_div;
--Testcase 999:
DROP FOREIGN TABLE num_exp_mul;
--Testcase 1000:
DROP FOREIGN TABLE num_exp_sqrt;
--Testcase 1001:
DROP FOREIGN TABLE num_exp_ln;
--Testcase 1002:
DROP FOREIGN TABLE num_exp_log10;
--Testcase 1003:
DROP FOREIGN TABLE num_exp_power_10_ln;
--Testcase 1004:
DROP FOREIGN TABLE num_result;
--Testcase 1005:
DROP FOREIGN TABLE num_input_test;
--Testcase 1006:
DROP FOREIGN TABLE ceil_floor_round;
--Testcase 1007:
DROP USER MAPPING FOR public SERVER griddb_svr;
--Testcase 1008:
DROP SERVER griddb_svr;
--Testcase 1009:
DROP EXTENSION griddb_fdw CASCADE;
