--
-- NUMERIC
--
\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--Testcase 1:
CREATE EXTENSION griddb_fdw;

--Testcase 2:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');

--Testcase 3:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

--Testcase 4:
CREATE FOREIGN TABLE num_data (idx serial, id int4, val float8) SERVER griddb_svr;

--Testcase 5:
CREATE FOREIGN TABLE num_exp_add (idx serial, id1 int4, id2 int4, expected float8) SERVER griddb_svr;

--Testcase 6:
CREATE FOREIGN TABLE num_exp_sub (idx serial, id1 int4, id2 int4, expected float8) SERVER griddb_svr;

--Testcase 7:
CREATE FOREIGN TABLE num_exp_div (idx serial, id1 int4, id2 int4, expected float8) SERVER griddb_svr;

--Testcase 8:
CREATE FOREIGN TABLE num_exp_mul (idx serial, id1 int4, id2 int4, expected float8) SERVER griddb_svr;

--Testcase 9:
CREATE FOREIGN TABLE num_exp_sqrt (idx serial, id int4, expected float8) SERVER griddb_svr;

--Testcase 10:
CREATE FOREIGN TABLE num_exp_ln (idx serial, id int4, expected float8) SERVER griddb_svr;

--Testcase 11:
CREATE FOREIGN TABLE num_exp_log10 (idx serial, id int4, expected float8) SERVER griddb_svr;

--Testcase 12:
CREATE FOREIGN TABLE num_exp_power_10_ln (idx serial, id int4, expected float8) SERVER griddb_svr;

--Testcase 13:
CREATE FOREIGN TABLE num_result (idx serial OPTIONS (rowkey 'true'), id1 int4, id2 int4, result float8) SERVER griddb_svr;

-- ******************************
-- * The following EXPECTED results are computed by bc(1)
-- * with a scale of 200
-- ******************************

BEGIN TRANSACTION;

--Testcase 14:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,0,'0');

--Testcase 15:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,0,'0');

--Testcase 16:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,0,'0');

--Testcase 17:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,0,'NaN');

--Testcase 18:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,1,'0');

--Testcase 19:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,1,'0');

--Testcase 20:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,1,'0');

--Testcase 21:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,1,'NaN');

--Testcase 22:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,2,'-34338492.215397047');

--Testcase 23:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,2,'34338492.215397047');

--Testcase 24:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,2,'0');

--Testcase 25:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,2,'0');

--Testcase 26:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,3,'4.31');

--Testcase 27:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,3,'-4.31');

--Testcase 28:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,3,'0');

--Testcase 29:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,3,'0');

--Testcase 30:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,4,'7799461.4119');

--Testcase 31:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,4,'-7799461.4119');

--Testcase 32:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,4,'0');

--Testcase 33:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,4,'0');

--Testcase 34:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,5,'16397.038491');

--Testcase 35:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,5,'-16397.038491');

--Testcase 36:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,5,'0');

--Testcase 37:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,5,'0');

--Testcase 38:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,6,'93901.57763026');

--Testcase 39:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,6,'-93901.57763026');

--Testcase 40:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,6,'0');

--Testcase 41:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,6,'0');

--Testcase 42:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,7,'-83028485');

--Testcase 43:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,7,'83028485');

--Testcase 44:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,7,'0');

--Testcase 45:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,7,'0');

--Testcase 46:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,8,'74881');

--Testcase 47:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,8,'-74881');

--Testcase 48:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,8,'0');

--Testcase 49:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,8,'0');

--Testcase 50:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (0,9,'-24926804.045047420');

--Testcase 51:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (0,9,'24926804.045047420');

--Testcase 52:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (0,9,'0');

--Testcase 53:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (0,9,'0');

--Testcase 54:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,0,'0');

--Testcase 55:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,0,'0');

--Testcase 56:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,0,'0');

--Testcase 57:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,0,'NaN');

--Testcase 58:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,1,'0');

--Testcase 59:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,1,'0');

--Testcase 60:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,1,'0');

--Testcase 61:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,1,'NaN');

--Testcase 62:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,2,'-34338492.215397047');

--Testcase 63:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,2,'34338492.215397047');

--Testcase 64:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,2,'0');

--Testcase 65:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,2,'0');

--Testcase 66:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,3,'4.31');

--Testcase 67:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,3,'-4.31');

--Testcase 68:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,3,'0');

--Testcase 69:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,3,'0');

--Testcase 70:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,4,'7799461.4119');

--Testcase 71:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,4,'-7799461.4119');

--Testcase 72:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,4,'0');

--Testcase 73:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,4,'0');

--Testcase 74:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,5,'16397.038491');

--Testcase 75:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,5,'-16397.038491');

--Testcase 76:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,5,'0');

--Testcase 77:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,5,'0');

--Testcase 78:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,6,'93901.57763026');

--Testcase 79:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,6,'-93901.57763026');

--Testcase 80:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,6,'0');

--Testcase 81:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,6,'0');

--Testcase 82:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,7,'-83028485');

--Testcase 83:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,7,'83028485');

--Testcase 84:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,7,'0');

--Testcase 85:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,7,'0');

--Testcase 86:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,8,'74881');

--Testcase 87:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,8,'-74881');

--Testcase 88:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,8,'0');

--Testcase 89:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,8,'0');

--Testcase 90:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (1,9,'-24926804.045047420');

--Testcase 91:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (1,9,'24926804.045047420');

--Testcase 92:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (1,9,'0');

--Testcase 93:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (1,9,'0');

--Testcase 94:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,0,'-34338492.215397047');

--Testcase 95:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,0,'-34338492.215397047');

--Testcase 96:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,0,'0');

--Testcase 97:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,0,'NaN');

--Testcase 98:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,1,'-34338492.215397047');

--Testcase 99:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,1,'-34338492.215397047');

--Testcase 100:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,1,'0');

--Testcase 101:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,1,'NaN');

--Testcase 102:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,2,'-68676984.430794094');

--Testcase 103:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,2,'0');

--Testcase 104:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,2,'1179132047626883.596862135856320209');

--Testcase 105:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,2,'1.00000000000000000000');

--Testcase 106:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,3,'-34338487.905397');

--Testcase 107:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,3,'-34338496.525397047');

--Testcase 108:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,3,'-147998901.44836127257');

--Testcase 109:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,3,'-7967167.56737751');

--Testcase 110:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,4,'-26539030.803497047');

--Testcase 111:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,4,'-42137953.627297047');

--Testcase 112:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,4,'-267821744976817.8111137106593');

--Testcase 113:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,4,'-4.40267480046830116685');

--Testcase 114:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,5,'-34322095.176906047');

--Testcase 115:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,5,'-34354889.253888047');

--Testcase 116:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,5,'-563049578578.769242506736077');

--Testcase 117:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,5,'-2094.18866914563535496429');

--Testcase 118:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,6,'-34244590.637766787');

--Testcase 119:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,6,'-34432393.793027307');

--Testcase 120:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,6,'-3224438592470.18449811926184222');

--Testcase 121:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,6,'-365.68599891479766440940');

--Testcase 122:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,7,'-117366977.215397047');

--Testcase 123:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,7,'48689992.784602953');

--Testcase 124:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,7,'2851072985828710.485883795');

--Testcase 125:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,7,'.41357483778485235518');

--Testcase 126:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,8,'-34263611.215397047');

--Testcase 127:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,8,'-34413373.215397047');

--Testcase 128:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,8,'-2571300635581.146276407');

--Testcase 129:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,8,'-458.57416721727870888476');

--Testcase 130:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (2,9,'-59265296.260444467');

--Testcase 131:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (2,9,'-9411688.17034962');

--Testcase 132:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (2,9,'855948866655588.453741509242968740');

--Testcase 133:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (2,9,'1.37757299946438931811');

--Testcase 134:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,0,'4.31');

--Testcase 135:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,0,'4.31');

--Testcase 136:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,0,'0');

--Testcase 137:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,0,'NaN');

--Testcase 138:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,1,'4.31');

--Testcase 139:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,1,'4.31');

--Testcase 140:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,1,'0');

--Testcase 141:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,1,'NaN');

--Testcase 142:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,2,'-34338487.905397');

--Testcase 143:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,2,'34338496.525397047');

--Testcase 144:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,2,'-147998901.44836127257');

--Testcase 145:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,2,'-.000000125515120843525');

--Testcase 146:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,3,'8.62');

--Testcase 147:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,3,'0');

--Testcase 148:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,3,'18.5761');

--Testcase 149:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,3,'1.00000000000000000000');

--Testcase 150:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,4,'7799465.7219');

--Testcase 151:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,4,'-7799457.1019');

--Testcase 152:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,4,'33615678.685289');

--Testcase 153:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,4,'.000000552602259615521');

--Testcase 154:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,5,'16401.348491');

--Testcase 155:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,5,'-16392.728491');

--Testcase 156:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,5,'70671.23589621');

--Testcase 157:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,5,'.00026285234387695504');

--Testcase 158:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,6,'93905.88763026');

--Testcase 159:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,6,'-93897.26763026');

--Testcase 160:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,6,'404715.7995864206');

--Testcase 161:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,6,'.0000458991223445759');

--Testcase 162:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,7,'-83028480.69');

--Testcase 163:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,7,'83028489.31');

--Testcase 164:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,7,'-357852770.35');

--Testcase 165:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,7,'-.000000051909895742407');

--Testcase 166:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,8,'74885.31');

--Testcase 167:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,8,'-74876.69');

--Testcase 168:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,8,'322737.11');

--Testcase 169:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,8,'.00005755799201399553');

--Testcase 170:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (3,9,'-24926799.735047420');

--Testcase 171:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (3,9,'24926808.355047420');

--Testcase 172:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (3,9,'-107434525.43415438020');

--Testcase 173:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (3,9,'-.00000017290624149855');

--Testcase 174:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,0,'7799461.4119');

--Testcase 175:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,0,'7799461.4119');

--Testcase 176:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,0,'0');

--Testcase 177:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,0,'NaN');

--Testcase 178:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,1,'7799461.4119');

--Testcase 179:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,1,'7799461.4119');

--Testcase 180:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,1,'0');

--Testcase 181:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,1,'NaN');

--Testcase 182:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,2,'-26539030.803497047');

--Testcase 183:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,2,'42137953.627297047');

--Testcase 184:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,2,'-267821744976817.8111137106593');

--Testcase 185:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,2,'-.22713465002993920385');

--Testcase 186:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,3,'7799465.7219');

--Testcase 187:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,3,'7799457.1019');

--Testcase 188:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,3,'33615678.685289');

--Testcase 189:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,3,'1809619.81714617169373549883');

--Testcase 190:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,4,'15598922.8238');

--Testcase 191:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,4,'0');

--Testcase 192:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,4,'60831598315717.14146161');

--Testcase 193:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,4,'1.00000000000000000000');

--Testcase 194:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,5,'7815858.450391');

--Testcase 195:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,5,'7783064.373409');

--Testcase 196:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,5,'127888068979.9935054429');

--Testcase 197:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,5,'475.66281046305802686061');

--Testcase 198:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,6,'7893362.98953026');

--Testcase 199:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,6,'7705559.83426974');

--Testcase 200:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,6,'732381731243.745115764094');

--Testcase 201:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,6,'83.05996138436129499606');

--Testcase 202:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,7,'-75229023.5881');

--Testcase 203:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,7,'90827946.4119');

--Testcase 204:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,7,'-647577464846017.9715');

--Testcase 205:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,7,'-.09393717604145131637');

--Testcase 206:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,8,'7874342.4119');

--Testcase 207:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,8,'7724580.4119');

--Testcase 208:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,8,'584031469984.4839');

--Testcase 209:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,8,'104.15808298366741897143');

--Testcase 210:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (4,9,'-17127342.633147420');

--Testcase 211:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (4,9,'32726265.456947420');

--Testcase 212:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (4,9,'-194415646271340.1815956522980');

--Testcase 213:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (4,9,'-.31289456112403769409');

--Testcase 214:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,0,'16397.038491');

--Testcase 215:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,0,'16397.038491');

--Testcase 216:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,0,'0');

--Testcase 217:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,0,'NaN');

--Testcase 218:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,1,'16397.038491');

--Testcase 219:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,1,'16397.038491');

--Testcase 220:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,1,'0');

--Testcase 221:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,1,'NaN');

--Testcase 222:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,2,'-34322095.176906047');

--Testcase 223:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,2,'34354889.253888047');

--Testcase 224:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,2,'-563049578578.769242506736077');

--Testcase 225:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,2,'-.00047751189505192446');

--Testcase 226:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,3,'16401.348491');

--Testcase 227:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,3,'16392.728491');

--Testcase 228:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,3,'70671.23589621');

--Testcase 229:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,3,'3804.41728329466357308584');

--Testcase 230:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,4,'7815858.450391');

--Testcase 231:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,4,'-7783064.373409');

--Testcase 232:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,4,'127888068979.9935054429');

--Testcase 233:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,4,'.00210232958726897192');

--Testcase 234:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,5,'32794.076982');

--Testcase 235:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,5,'0');

--Testcase 236:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,5,'268862871.275335557081');

--Testcase 237:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,5,'1.00000000000000000000');

--Testcase 238:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,6,'110298.61612126');

--Testcase 239:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,6,'-77504.53913926');

--Testcase 240:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,6,'1539707782.76899778633766');

--Testcase 241:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,6,'.17461941433576102689');

--Testcase 242:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,7,'-83012087.961509');

--Testcase 243:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,7,'83044882.038491');

--Testcase 244:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,7,'-1361421264394.416135');

--Testcase 245:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,7,'-.00019748690453643710');

--Testcase 246:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,8,'91278.038491');

--Testcase 247:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,8,'-58483.961509');

--Testcase 248:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,8,'1227826639.244571');

--Testcase 249:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,8,'.21897461960978085228');

--Testcase 250:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (5,9,'-24910407.006556420');

--Testcase 251:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (5,9,'24943201.083538420');

--Testcase 252:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (5,9,'-408725765384.257043660243220');

--Testcase 253:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (5,9,'-.00065780749354660427');

--Testcase 254:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,0,'93901.57763026');

--Testcase 255:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,0,'93901.57763026');

--Testcase 256:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,0,'0');

--Testcase 257:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,0,'NaN');

--Testcase 258:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,1,'93901.57763026');

--Testcase 259:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,1,'93901.57763026');

--Testcase 260:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,1,'0');

--Testcase 261:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,1,'NaN');

--Testcase 262:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,2,'-34244590.637766787');

--Testcase 263:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,2,'34432393.793027307');

--Testcase 264:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,2,'-3224438592470.18449811926184222');

--Testcase 265:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,2,'-.00273458651128995823');

--Testcase 266:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,3,'93905.88763026');

--Testcase 267:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,3,'93897.26763026');

--Testcase 268:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,3,'404715.7995864206');

--Testcase 269:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,3,'21786.90896293735498839907');

--Testcase 270:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,4,'7893362.98953026');

--Testcase 271:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,4,'-7705559.83426974');

--Testcase 272:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,4,'732381731243.745115764094');

--Testcase 273:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,4,'.01203949512295682469');

--Testcase 274:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,5,'110298.61612126');

--Testcase 275:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,5,'77504.53913926');

--Testcase 276:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,5,'1539707782.76899778633766');

--Testcase 277:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,5,'5.72674008674192359679');

--Testcase 278:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,6,'187803.15526052');

--Testcase 279:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,6,'0');

--Testcase 280:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,6,'8817506281.45174');

--Testcase 281:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,6,'1.00000000000000000000');

--Testcase 282:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,7,'-82934583.42236974');

--Testcase 283:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,7,'83122386.57763026');

--Testcase 284:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,7,'-7796505729750.37795610');

--Testcase 285:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,7,'-.00113095617281538980');

--Testcase 286:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,8,'168782.57763026');

--Testcase 287:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,8,'19020.57763026');

--Testcase 288:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,8,'7031444034.53149906');

--Testcase 289:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,8,'1.25401073209839612184');

--Testcase 290:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (6,9,'-24832902.467417160');

--Testcase 291:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (6,9,'25020705.622677680');

--Testcase 292:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (6,9,'-2340666225110.29929521292692920');

--Testcase 293:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (6,9,'-.00376709254265256789');

--Testcase 294:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,0,'-83028485');

--Testcase 295:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,0,'-83028485');

--Testcase 296:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,0,'0');

--Testcase 297:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,0,'NaN');

--Testcase 298:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,1,'-83028485');

--Testcase 299:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,1,'-83028485');

--Testcase 300:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,1,'0');

--Testcase 301:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,1,'NaN');

--Testcase 302:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,2,'-117366977.215397047');

--Testcase 303:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,2,'-48689992.784602953');

--Testcase 304:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,2,'2851072985828710.485883795');

--Testcase 305:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,2,'2.41794207151503385700');

--Testcase 306:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,3,'-83028480.69');

--Testcase 307:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,3,'-83028489.31');

--Testcase 308:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,3,'-357852770.35');

--Testcase 309:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,3,'-19264149.65197215777262180974');

--Testcase 310:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,4,'-75229023.5881');

--Testcase 311:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,4,'-90827946.4119');

--Testcase 312:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,4,'-647577464846017.9715');

--Testcase 313:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,4,'-10.64541262725136247686');

--Testcase 314:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,5,'-83012087.961509');

--Testcase 315:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,5,'-83044882.038491');

--Testcase 316:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,5,'-1361421264394.416135');

--Testcase 317:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,5,'-5063.62688881730941836574');

--Testcase 318:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,6,'-82934583.42236974');

--Testcase 319:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,6,'-83122386.57763026');

--Testcase 320:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,6,'-7796505729750.37795610');

--Testcase 321:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,6,'-884.20756174009028770294');

--Testcase 322:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,7,'-166056970');

--Testcase 323:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,7,'0');

--Testcase 324:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,7,'6893729321395225');

--Testcase 325:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,7,'1.00000000000000000000');

--Testcase 326:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,8,'-82953604');

--Testcase 327:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,8,'-83103366');

--Testcase 328:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,8,'-6217255985285');

--Testcase 329:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,8,'-1108.80577182462841041118');

--Testcase 330:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (7,9,'-107955289.045047420');

--Testcase 331:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (7,9,'-58101680.954952580');

--Testcase 332:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (7,9,'2069634775752159.035758700');

--Testcase 333:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (7,9,'3.33089171198810413382');

--Testcase 334:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,0,'74881');

--Testcase 335:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,0,'74881');

--Testcase 336:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,0,'0');

--Testcase 337:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,0,'NaN');

--Testcase 338:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,1,'74881');

--Testcase 339:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,1,'74881');

--Testcase 340:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,1,'0');

--Testcase 341:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,1,'NaN');

--Testcase 342:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,2,'-34263611.215397047');

--Testcase 343:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,2,'34413373.215397047');

--Testcase 344:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,2,'-2571300635581.146276407');

--Testcase 345:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,2,'-.00218067233500788615');

--Testcase 346:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,3,'74885.31');

--Testcase 347:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,3,'74876.69');

--Testcase 348:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,3,'322737.11');

--Testcase 349:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,3,'17373.78190255220417633410');

--Testcase 350:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,4,'7874342.4119');

--Testcase 351:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,4,'-7724580.4119');

--Testcase 352:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,4,'584031469984.4839');

--Testcase 353:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,4,'.00960079113741758956');

--Testcase 354:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,5,'91278.038491');

--Testcase 355:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,5,'58483.961509');

--Testcase 356:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,5,'1227826639.244571');

--Testcase 357:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,5,'4.56673929509287019456');

--Testcase 358:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,6,'168782.57763026');

--Testcase 359:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,6,'-19020.57763026');

--Testcase 360:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,6,'7031444034.53149906');

--Testcase 361:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,6,'.79744134113322314424');

--Testcase 362:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,7,'-82953604');

--Testcase 363:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,7,'83103366');

--Testcase 364:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,7,'-6217255985285');

--Testcase 365:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,7,'-.00090187120721280172');

--Testcase 366:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,8,'149762');

--Testcase 367:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,8,'0');

--Testcase 368:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,8,'5607164161');

--Testcase 369:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,8,'1.00000000000000000000');

--Testcase 370:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (8,9,'-24851923.045047420');

--Testcase 371:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (8,9,'25001685.045047420');

--Testcase 372:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (8,9,'-1866544013697.195857020');

--Testcase 373:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (8,9,'-.00300403532938582735');

--Testcase 374:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,0,'-24926804.045047420');

--Testcase 375:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,0,'-24926804.045047420');

--Testcase 376:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,0,'0');

--Testcase 377:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,0,'NaN');

--Testcase 378:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,1,'-24926804.045047420');

--Testcase 379:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,1,'-24926804.045047420');

--Testcase 380:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,1,'0');

--Testcase 381:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,1,'NaN');

--Testcase 382:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,2,'-59265296.260444467');

--Testcase 383:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,2,'9411688.17034962');

--Testcase 384:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,2,'855948866655588.453741509242968740');

--Testcase 385:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,2,'.72591434384152961526');

--Testcase 386:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,3,'-24926799.735047420');

--Testcase 387:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,3,'-24926808.355047420');

--Testcase 388:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,3,'-107434525.43415438020');

--Testcase 389:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,3,'-5783481.21694835730858468677');

--Testcase 390:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,4,'-17127342.633147420');

--Testcase 391:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,4,'-32726265.456947420');

--Testcase 392:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,4,'-194415646271340.1815956522980');

--Testcase 393:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,4,'-3.19596478892958416484');

--Testcase 394:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,5,'-24910407.006556420');

--Testcase 395:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,5,'-24943201.083538420');

--Testcase 396:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,5,'-408725765384.257043660243220');

--Testcase 397:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,5,'-1520.20159364322004505807');

--Testcase 398:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,6,'-24832902.467417160');

--Testcase 399:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,6,'-25020705.622677680');

--Testcase 400:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,6,'-2340666225110.29929521292692920');

--Testcase 401:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,6,'-265.45671195426965751280');

--Testcase 402:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,7,'-107955289.045047420');

--Testcase 403:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,7,'58101680.954952580');

--Testcase 404:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,7,'2069634775752159.035758700');

--Testcase 405:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,7,'.30021990699995814689');

--Testcase 406:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,8,'-24851923.045047420');

--Testcase 407:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,8,'-25001685.045047420');

--Testcase 408:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,8,'-1866544013697.195857020');

--Testcase 409:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,8,'-332.88556569820675471748');

--Testcase 410:
INSERT INTO num_exp_add(id1, id2, expected) VALUES (9,9,'-49853608.090094840');

--Testcase 411:
INSERT INTO num_exp_sub(id1, id2, expected) VALUES (9,9,'0');

--Testcase 412:
INSERT INTO num_exp_mul(id1, id2, expected) VALUES (9,9,'621345559900192.420120630048656400');

--Testcase 413:
INSERT INTO num_exp_div(id1, id2, expected) VALUES (9,9,'1.00000000000000000000');
COMMIT TRANSACTION;
BEGIN TRANSACTION;

--Testcase 414:
INSERT INTO num_exp_sqrt(id, expected) VALUES (0,'0');

--Testcase 415:
INSERT INTO num_exp_sqrt(id, expected) VALUES (1,'0');

--Testcase 416:
INSERT INTO num_exp_sqrt(id, expected) VALUES (2,'5859.90547836712524903505');

--Testcase 417:
INSERT INTO num_exp_sqrt(id, expected) VALUES (3,'2.07605394920266944396');

--Testcase 418:
INSERT INTO num_exp_sqrt(id, expected) VALUES (4,'2792.75158435189147418923');

--Testcase 419:
INSERT INTO num_exp_sqrt(id, expected) VALUES (5,'128.05092147657509145473');

--Testcase 420:
INSERT INTO num_exp_sqrt(id, expected) VALUES (6,'306.43364311096782703406');

--Testcase 421:
INSERT INTO num_exp_sqrt(id, expected) VALUES (7,'9111.99676251039939975230');

--Testcase 422:
INSERT INTO num_exp_sqrt(id, expected) VALUES (8,'273.64392922189960397542');

--Testcase 423:
INSERT INTO num_exp_sqrt(id, expected) VALUES (9,'4992.67503899937593364766');
COMMIT TRANSACTION;
BEGIN TRANSACTION;

--Testcase 424:
INSERT INTO num_exp_ln(id, expected) VALUES (0,'NaN');

--Testcase 425:
INSERT INTO num_exp_ln(id, expected) VALUES (1,'NaN');

--Testcase 426:
INSERT INTO num_exp_ln(id, expected) VALUES (2,'17.35177750493897715514');

--Testcase 427:
INSERT INTO num_exp_ln(id, expected) VALUES (3,'1.46093790411565641971');

--Testcase 428:
INSERT INTO num_exp_ln(id, expected) VALUES (4,'15.86956523951936572464');

--Testcase 429:
INSERT INTO num_exp_ln(id, expected) VALUES (5,'9.70485601768871834038');

--Testcase 430:
INSERT INTO num_exp_ln(id, expected) VALUES (6,'11.45000246622944403127');

--Testcase 431:
INSERT INTO num_exp_ln(id, expected) VALUES (7,'18.23469429965478772991');

--Testcase 432:
INSERT INTO num_exp_ln(id, expected) VALUES (8,'11.22365546576315513668');

--Testcase 433:
INSERT INTO num_exp_ln(id, expected) VALUES (9,'17.03145425013166006962');
COMMIT TRANSACTION;
BEGIN TRANSACTION;

--Testcase 434:
INSERT INTO num_exp_log10(id, expected) VALUES (0,'NaN');

--Testcase 435:
INSERT INTO num_exp_log10(id, expected) VALUES (1,'NaN');

--Testcase 436:
INSERT INTO num_exp_log10(id, expected) VALUES (2,'7.53578122160797276459');

--Testcase 437:
INSERT INTO num_exp_log10(id, expected) VALUES (3,'.63447727016073160075');

--Testcase 438:
INSERT INTO num_exp_log10(id, expected) VALUES (4,'6.89206461372691743345');

--Testcase 439:
INSERT INTO num_exp_log10(id, expected) VALUES (5,'4.21476541614777768626');

--Testcase 440:
INSERT INTO num_exp_log10(id, expected) VALUES (6,'4.97267288886207207671');

--Testcase 441:
INSERT INTO num_exp_log10(id, expected) VALUES (7,'7.91922711353275546914');

--Testcase 442:
INSERT INTO num_exp_log10(id, expected) VALUES (8,'4.87437163556421004138');

--Testcase 443:
INSERT INTO num_exp_log10(id, expected) VALUES (9,'7.39666659961986567059');
COMMIT TRANSACTION;
BEGIN TRANSACTION;

--Testcase 444:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (0,'NaN');

--Testcase 445:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (1,'NaN');

--Testcase 446:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (2,'224790267919917955.13261618583642653184');

--Testcase 447:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (3,'28.90266599445155957393');

--Testcase 448:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (4,'7405685069594999.07733999469386277636');

--Testcase 449:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (5,'5068226527.32127265408584640098');

--Testcase 450:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (6,'281839893606.99372343357047819067');

--Testcase 451:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (7,'1716699575118597095.42330819910640247627');

--Testcase 452:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (8,'167361463828.07491320069016125952');

--Testcase 453:
INSERT INTO num_exp_power_10_ln(id, expected) VALUES (9,'107511333880052007.04141124673540337457');
COMMIT TRANSACTION;
BEGIN TRANSACTION;

--Testcase 454:
INSERT INTO num_data(id, val) VALUES (0, '0');

--Testcase 455:
INSERT INTO num_data(id, val) VALUES (1, '0');

--Testcase 456:
INSERT INTO num_data(id, val) VALUES (2, '-34338492.215397047');

--Testcase 457:
INSERT INTO num_data(id, val) VALUES (3, '4.31');

--Testcase 458:
INSERT INTO num_data(id, val) VALUES (4, '7799461.4119');

--Testcase 459:
INSERT INTO num_data(id, val) VALUES (5, '16397.038491');

--Testcase 460:
INSERT INTO num_data(id, val) VALUES (6, '93901.57763026');

--Testcase 461:
INSERT INTO num_data(id, val) VALUES (7, '-83028485');

--Testcase 462:
INSERT INTO num_data(id, val) VALUES (8, '74881');

--Testcase 463:
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

--Testcase 464:
DELETE FROM num_result;

--Testcase 465:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, t1.val + t2.val
    FROM num_data t1, num_data t2;

--Testcase 466:
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_add t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

--Testcase 467:
DELETE FROM num_result;

--Testcase 468:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, round((t1.val + t2.val)::numeric, 10)
    FROM num_data t1, num_data t2;

--Testcase 469:
SELECT t1.id1, t1.id2, t1.result, round(t2.expected::numeric, 10) as expected
    FROM num_result t1, num_exp_add t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != round(t2.expected::numeric, 10);

-- ******************************
-- * Subtraction check
-- ******************************

--Testcase 470:
DELETE FROM num_result;

--Testcase 471:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, t1.val - t2.val
    FROM num_data t1, num_data t2;

--Testcase 472:
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_sub t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

--Testcase 473:
DELETE FROM num_result;

--Testcase 474:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, round((t1.val - t2.val)::numeric, 40)
    FROM num_data t1, num_data t2;

--Testcase 475:
SELECT t1.id1, t1.id2, t1.result, round(t2.expected::numeric, 40)
    FROM num_result t1, num_exp_sub t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != round(t2.expected::numeric, 40);

-- ******************************
-- * Multiply check
-- ******************************

--Testcase 476:
DELETE FROM num_result;

--Testcase 477:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, t1.val * t2.val
    FROM num_data t1, num_data t2;

--Testcase 478:
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_mul t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

--Testcase 479:
DELETE FROM num_result;

--Testcase 480:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, round((t1.val * t2.val)::numeric, 30)
    FROM num_data t1, num_data t2;

--Testcase 481:
SELECT t1.id1, t1.id2, t1.result, round(t2.expected::numeric, 30) as expected
    FROM num_result t1, num_exp_mul t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != round(t2.expected::numeric, 30);

-- ******************************
-- * Division check
-- ******************************

--Testcase 482:
DELETE FROM num_result;

--Testcase 483:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, t1.val / t2.val
    FROM num_data t1, num_data t2
    WHERE t2.val != '0.0';

--Testcase 484:
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_div t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

--Testcase 485:
DELETE FROM num_result;

--Testcase 486:
INSERT INTO num_result(id1, id2, result) SELECT t1.id, t2.id, round((t1.val / t2.val)::numeric, 80)
    FROM num_data t1, num_data t2
    WHERE t2.val != '0.0';

--Testcase 487:
SELECT t1.id1, t1.id2, t1.result::numeric(210,80), round(t2.expected::numeric, 80) as expected
    FROM num_result t1, num_exp_div t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected::numeric, 80);

-- ******************************
-- * Square root check
-- ******************************

--Testcase 488:
DELETE FROM num_result;

--Testcase 489:
INSERT INTO num_result(id1, id2, result) SELECT id, 0, SQRT(ABS(val))
    FROM num_data;

--Testcase 490:
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_sqrt t2
    WHERE t1.id1 = t2.id
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

-- ******************************
-- * Natural logarithm check
-- ******************************

--Testcase 491:
DELETE FROM num_result;

--Testcase 492:
INSERT INTO num_result(id1, id2, result) SELECT id, 0, LN(ABS(val))
    FROM num_data
    WHERE val != '0.0';

--Testcase 493:
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_ln t2
    WHERE t1.id1 = t2.id
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

-- ******************************
-- * Logarithm base 10 check
-- ******************************

--Testcase 494:
DELETE FROM num_result;

--Testcase 495:
INSERT INTO num_result(id1, id2, result) SELECT id, 0, LOG(numeric '10', ABS(val::numeric))
    FROM num_data
    WHERE val != '0.0';

--Testcase 496:
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_log10 t2
    WHERE t1.id1 = t2.id
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

-- ******************************
-- * POWER(10, LN(value)) check
-- ******************************

--Testcase 497:
DELETE FROM num_result;

--Testcase 498:
INSERT INTO num_result(id1, id2, result) SELECT id, 0, POWER(numeric '10', LN(ABS(round(val::numeric,200))))
    FROM num_data
    WHERE val != '0.0';

--Testcase 499:
SELECT t1.id1, t1.result, t2.expected
    FROM num_result t1, num_exp_power_10_ln t2
    WHERE t1.id1 = t2.id
    AND t1.result::numeric(210,10) != t2.expected::numeric(210,10);

-- ******************************
-- * Check behavior with Inf and NaN inputs.  It's easiest to handle these
-- * separately from the num_data framework used above, because some input
-- * combinations will throw errors.
-- ******************************

--Testcase 500:
create foreign table v (id serial options (rowkey 'true'), x float8) server griddb_svr;

BEGIN;

--Testcase 501:
DELETE FROM v;

--Testcase 502:
INSERT INTO v(x) VALUES ('0'::float8),('1'),('-1'),('4.2'),('inf'),('-inf'),('nan');

--Testcase 503:
SELECT x1::numeric, x2::numeric,
  x1::numeric + x2::numeric AS sum,
  x1::numeric - x2::numeric AS diff,
  x1::numeric * x2::numeric AS prod
FROM v AS v1(id, x1), v AS v2(id, x2);

--Testcase 504:
SELECT x1, x2,
  x1::numeric / x2::numeric AS quot,
  x1::numeric % x2::numeric AS mod,
  div(x1::numeric, x2::numeric) AS div
FROM v AS v1(id, x1), v AS v2(id, x2) WHERE x2 != 0;
ROLLBACK;

BEGIN;

--Testcase 505:
DELETE FROM v;

--Testcase 506:
INSERT INTO v(x) VALUES ('inf':: float8);

--Testcase 507:
SELECT x::numeric / '0' FROM v;
ROLLBACK;

BEGIN;

--Testcase 508:
DELETE FROM v;

--Testcase 509:
INSERT INTO v(x) VALUES ('-inf':: float8);

--Testcase 510:
SELECT x::numeric / '0' FROM v;
ROLLBACK;

BEGIN;

--Testcase 511:
DELETE FROM v;

--Testcase 512:
INSERT INTO v(x) VALUES ('nan':: float8);

--Testcase 513:
SELECT x::numeric / '0' FROM v;
ROLLBACK;

BEGIN;

--Testcase 514:
DELETE FROM v;

--Testcase 515:
INSERT INTO v(x) VALUES ('0':: float8);

--Testcase 516:
SELECT x::numeric / '0' FROM v;
ROLLBACK;

BEGIN;

--Testcase 517:
DELETE FROM v;

--Testcase 518:
INSERT INTO v(x) VALUES ('inf':: float8);

--Testcase 519:
SELECT x::numeric % '0' FROM v;
ROLLBACK;

BEGIN;

--Testcase 520:
DELETE FROM v;

--Testcase 521:
INSERT INTO v(x) VALUES ('-inf':: float8);

--Testcase 522:
SELECT x::numeric % '0' FROM v;
ROLLBACK;

BEGIN;

--Testcase 523:
DELETE FROM v;

--Testcase 524:
INSERT INTO v(x) VALUES ('nan':: float8);

--Testcase 525:
SELECT x::numeric % '0' FROM v;
ROLLBACK;

BEGIN;

--Testcase 526:
DELETE FROM v;

--Testcase 527:
INSERT INTO v(x) VALUES ('0':: float8);

--Testcase 528:
SELECT x::numeric % '0' FROM v;
ROLLBACK;

BEGIN;

--Testcase 529:
DELETE FROM v;

--Testcase 530:
INSERT INTO v(x) VALUES ('inf':: float8);

--Testcase 531:
SELECT div(x::numeric, '0') FROM v;
ROLLBACK;

BEGIN;

--Testcase 532:
DELETE FROM v;

--Testcase 533:
INSERT INTO v(x) VALUES ('-inf':: float8);

--Testcase 534:
SELECT div(x::numeric, '0') FROM v;
ROLLBACK;

BEGIN;

--Testcase 535:
DELETE FROM v;

--Testcase 536:
INSERT INTO v(x) VALUES ('nan':: float8);

--Testcase 537:
SELECT div(x::numeric, '0') FROM v;
ROLLBACK;

BEGIN;

--Testcase 538:
DELETE FROM v;

--Testcase 539:
INSERT INTO v(x) VALUES ('0':: float8);

--Testcase 540:
SELECT div(x::numeric, '0') FROM v;
ROLLBACK;

BEGIN;

--Testcase 541:
DELETE FROM v;

--Testcase 542:
INSERT INTO v(x) VALUES('0'::numeric),('1'),('-1'),('4.2'),('-7.777'),('inf'),('-inf'),('nan');

--Testcase 543:
SELECT x, -x as minusx, abs(x::numeric), floor(x::numeric), ceil(x::numeric), sign(x::numeric), numeric_inc(x::numeric) as inc
FROM v;
ROLLBACK;

BEGIN;

--Testcase 544:
DELETE FROM v;

--Testcase 545:
INSERT INTO v(x) VALUES('0'::numeric),('1'),('-1'),('4.2'),('-7.777'),('inf'),('-inf'),('nan');

--Testcase 546:
SELECT x, round(x::numeric), round(x::numeric,1) as round1, trunc(x::numeric), trunc(x::numeric,1) as trunc1
FROM v;
ROLLBACK;

-- the large values fall into the numeric abbreviation code's maximal classes
-- ('1e340'),('-1e340') is out of range double => can not insert
BEGIN;

--Testcase 547:
DELETE FROM v;

--Testcase 548:
INSERT INTO v(x) VALUES('0'::numeric),('1'),('-1'),('4.2'),('-7.777'),
         ('inf'),('-inf'),('nan'),
         ('inf'),('-inf'),('nan');

--Testcase 549:
SELECT substring(x::text, 1, 32)
FROM v ORDER BY x;
ROLLBACK;

BEGIN;

--Testcase 550:
DELETE FROM v;

--Testcase 551:
INSERT INTO v(x) VALUES('0'::numeric),('1'),('4.2'),('inf'),('nan');

--Testcase 552:
SELECT x::numeric, sqrt(x::numeric)
FROM v;
ROLLBACK;

BEGIN;

--Testcase 553:
DELETE FROM v;

--Testcase 554:
INSERT INTO v(x) VALUES ('-1'::float8);

--Testcase 555:
SELECT sqrt(x::numeric) FROM v;
ROLLBACK;

BEGIN;

--Testcase 556:
DELETE FROM v;

--Testcase 557:
INSERT INTO v(x) VALUES ('-inf'::float8);

--Testcase 558:
SELECT sqrt(x::numeric) FROM v;
ROLLBACK;

BEGIN;

--Testcase 559:
DELETE FROM v;

--Testcase 560:
INSERT INTO v(x) VALUES ('1'::numeric),('4.2'),('inf'),('nan');

--Testcase 561:
SELECT x,
  log(x::numeric),
  log10(x::numeric),
  ln(x::numeric)
FROM v;
ROLLBACK;

BEGIN;

--Testcase 562:
DELETE FROM v;

--Testcase 563:
INSERT INTO v(x) VALUES ('0'::float8);

--Testcase 564:
SELECT ln(x::numeric) FROM v;
ROLLBACK;

BEGIN;

--Testcase 565:
DELETE FROM v;

--Testcase 566:
INSERT INTO v(x) VALUES ('-1'::float8);

--Testcase 567:
SELECT ln(x::numeric) FROM v;
ROLLBACK;

BEGIN;

--Testcase 568:
DELETE FROM v;

--Testcase 569:
INSERT INTO v(x) VALUES ('-inf'::float8);

--Testcase 570:
SELECT ln(x::numeric) FROM v;
ROLLBACK;

BEGIN;

--Testcase 571:
DELETE FROM v;

--Testcase 572:
INSERT INTO v(x) VALUES ('2'::numeric),('4.2'),('inf'),('nan');

--Testcase 573:
SELECT x1, x2,
  log(x1::numeric, x2::numeric)
FROM v AS v1(id, x1), v AS v2(id, x2);
ROLLBACK;

BEGIN;

--Testcase 574:
DELETE FROM v;

--Testcase 575:
INSERT INTO v(x) VALUES ('0'::float8);

--Testcase 576:
SELECT log(x::numeric, '10') FROM v;
ROLLBACK;

BEGIN;

--Testcase 577:
DELETE FROM v;

--Testcase 578:
INSERT INTO v(x) VALUES ('10'::float8);

--Testcase 579:
SELECT log(x::numeric, '0') FROM v;
ROLLBACK;

BEGIN;

--Testcase 580:
DELETE FROM v;

--Testcase 581:
INSERT INTO v(x) VALUES ('-inf'::float8);

--Testcase 582:
SELECT log(x::numeric, '10') FROM v;
ROLLBACK;

BEGIN;

--Testcase 583:
DELETE FROM v;

--Testcase 584:
INSERT INTO v(x) VALUES ('10'::float8);

--Testcase 585:
SELECT log(x::numeric, '-inf') FROM v;
ROLLBACK;

BEGIN;

--Testcase 586:
DELETE FROM v;

--Testcase 587:
INSERT INTO v(x) VALUES ('inf'::float8);

--Testcase 588:
SELECT log(x::numeric, '0') FROM v;
ROLLBACK;

BEGIN;

--Testcase 589:
DELETE FROM v;

--Testcase 590:
INSERT INTO v(x) VALUES ('inf'::float8);

--Testcase 591:
SELECT log(x::numeric, '-inf') FROM v;
ROLLBACK;

BEGIN;

--Testcase 592:
DELETE FROM v;

--Testcase 593:
INSERT INTO v(x) VALUES ('-inf'::float8);

--Testcase 594:
SELECT log(x::numeric, 'inf') FROM v;
ROLLBACK;

BEGIN;

--Testcase 595:
DELETE FROM v;

--Testcase 596:
INSERT INTO v(x) VALUES ('0'::numeric),('1'),('2'),('4.2'),('inf'),('nan');

--Testcase 597:
SELECT x1, x2,
  power(x1::numeric, x2::numeric)
FROM v AS v1(id, x1), v AS v2(id, x2) WHERE x1 != 0 OR x2 >= 0;
ROLLBACK;

BEGIN;

--Testcase 598:
DELETE FROM v;

--Testcase 599:
INSERT INTO v(x) VALUES ('0'::float8);

--Testcase 600:
SELECT power(x::numeric, '-1') FROM v;
ROLLBACK;

BEGIN;

--Testcase 601:
DELETE FROM v;

--Testcase 602:
INSERT INTO v(x) VALUES ('0'::float8);

--Testcase 603:
SELECT power(x::numeric, '-inf') FROM v;
ROLLBACK;

BEGIN;

--Testcase 604:
DELETE FROM v;

--Testcase 605:
INSERT INTO v(x) VALUES ('-1'::float8);

--Testcase 606:
SELECT power(x::numeric, 'inf') FROM v;
ROLLBACK;

BEGIN;

--Testcase 607:
DELETE FROM v;

--Testcase 608:
INSERT INTO v(x) VALUES ('-2'::float8);

--Testcase 609:
SELECT power(x::numeric, '3') FROM v;
ROLLBACK;

BEGIN;

--Testcase 610:
DELETE FROM v;

--Testcase 611:
INSERT INTO v(x) VALUES ('-2'::float8);

--Testcase 612:
SELECT power(x::numeric, '3.3') FROM v;
ROLLBACK;

BEGIN;

--Testcase 613:
DELETE FROM v;

--Testcase 614:
INSERT INTO v(x) VALUES ('-2'::float8);

--Testcase 615:
SELECT power(x::numeric, '-1') FROM v;
ROLLBACK;

BEGIN;

--Testcase 616:
DELETE FROM v;

--Testcase 617:
INSERT INTO v(x) VALUES ('-2'::float8);

--Testcase 618:
SELECT power(x::numeric, '-1.5') FROM v;
ROLLBACK;

BEGIN;

--Testcase 619:
DELETE FROM v;

--Testcase 620:
INSERT INTO v(x) VALUES ('-2'::float8);

--Testcase 621:
SELECT power(x::numeric, 'inf') FROM v;
ROLLBACK;

BEGIN;

--Testcase 622:
DELETE FROM v;

--Testcase 623:
INSERT INTO v(x) VALUES ('-2'::float8);

--Testcase 624:
SELECT power(x::numeric, '-inf') FROM v;
ROLLBACK;

BEGIN;

--Testcase 625:
DELETE FROM v;

--Testcase 626:
INSERT INTO v(x) VALUES ('inf'::float8);

--Testcase 627:
SELECT power(x::numeric, '-2') FROM v;
ROLLBACK;

BEGIN;

--Testcase 628:
DELETE FROM v;

--Testcase 629:
INSERT INTO v(x) VALUES ('inf'::float8);

--Testcase 630:
SELECT power(x::numeric, '-inf') FROM v;
ROLLBACK;

BEGIN;

--Testcase 631:
DELETE FROM v;

--Testcase 632:
INSERT INTO v(x) VALUES ('-inf'::float8);

--Testcase 633:
SELECT power(x::numeric, '2') FROM v;
ROLLBACK;

BEGIN;

--Testcase 634:
DELETE FROM v;

--Testcase 635:
INSERT INTO v(x) VALUES ('-inf'::float8);

--Testcase 636:
SELECT power(x::numeric, '3') FROM v;
ROLLBACK;

BEGIN;

--Testcase 637:
DELETE FROM v;

--Testcase 638:
INSERT INTO v(x) VALUES ('-inf'::float8);

--Testcase 639:
SELECT power(x::numeric, '4.5') FROM v;
ROLLBACK;

BEGIN;

--Testcase 640:
DELETE FROM v;

--Testcase 641:
INSERT INTO v(x) VALUES ('-inf'::float8);

--Testcase 642:
SELECT power(x::numeric, '-2') FROM v;
ROLLBACK;

BEGIN;

--Testcase 643:
DELETE FROM v;

--Testcase 644:
INSERT INTO v(x) VALUES ('-inf'::float8);

--Testcase 645:
SELECT power(x::numeric, '-3') FROM v;
ROLLBACK;

BEGIN;

--Testcase 646:
DELETE FROM v;

--Testcase 647:
INSERT INTO v(x) VALUES ('-inf'::float8);

--Testcase 648:
SELECT power(x::numeric, '0') FROM v;
ROLLBACK;

BEGIN;

--Testcase 649:
DELETE FROM v;

--Testcase 650:
INSERT INTO v(x) VALUES ('-inf'::float8);

--Testcase 651:
SELECT power(x::numeric, 'inf') FROM v;
ROLLBACK;

BEGIN;

--Testcase 652:
DELETE FROM v;

--Testcase 653:
INSERT INTO v(x) VALUES ('-inf'::float8);

--Testcase 654:
SELECT power(x::numeric, '-inf') FROM v;
ROLLBACK;

-- ******************************
-- * miscellaneous checks for things that have been broken in the past...
-- ******************************
-- numeric AVG used to fail on some platforms

--Testcase 655:
SELECT AVG(val) FROM num_data;

--Testcase 656:
SELECT STDDEV(val) FROM num_data;

--Testcase 657:
SELECT MAX(val) FROM num_data;

--Testcase 658:
SELECT MIN(val) FROM num_data;

--Testcase 659:
SELECT VARIANCE(val) FROM num_data;

-- Check for appropriate rounding and overflow

--Testcase 660:
CREATE FOREIGN TABLE fract_only (id serial OPTIONS (rowkey 'true'), val float8) server griddb_svr;

--Testcase 661:
INSERT INTO fract_only VALUES (1, '0.0'::numeric(4,4));

--Testcase 662:
INSERT INTO fract_only VALUES (2, '0.1'::numeric(4,4));

--Testcase 663:
INSERT INTO fract_only VALUES (3, '1.0'::numeric(4,4));	-- should fail

--Testcase 664:
INSERT INTO fract_only VALUES (4, '-0.9999'::numeric(4,4));

--Testcase 665:
INSERT INTO fract_only VALUES (5, '0.99994'::numeric(4,4));

--Testcase 666:
INSERT INTO fract_only VALUES (6, '0.99995'::numeric(4,4));  -- should fail

--Testcase 667:
INSERT INTO fract_only VALUES (7, '0.00001'::numeric(4,4));

--Testcase 668:
INSERT INTO fract_only VALUES (8, '0.00017'::numeric(4,4));

--Testcase 669:
INSERT INTO fract_only VALUES (9, 'NaN'::numeric(4,4));

--Testcase 670:
INSERT INTO fract_only VALUES (10, 'Inf'::numeric(4,4));	-- should fail

--Testcase 671:
INSERT INTO fract_only VALUES (11, '-Inf'::numeric(4,4));	-- should fail

--Testcase 672:
SELECT id, val::numeric(4,4) FROM fract_only;

-- Check conversion to integers
--Testcase 1309:
CREATE FOREIGN TABLE INT8_TMP(id serial OPTIONS (rowkey 'true'), q1 int8, q2 int8) SERVER griddb_svr;
--Testcase 1310:
CREATE FOREIGN TABLE INT4_TMP(id serial OPTIONS (rowkey 'true'), a int4, b int4) SERVER griddb_svr;
--Testcase 1311:
CREATE FOREIGN TABLE INT2_TMP(id serial OPTIONS (rowkey 'true'), f1 int2) SERVER griddb_svr;

--Testcase 1312:
DELETE FROM INT8_TMP;
--Testcase 1271:
INSERT INTO INT8_TMP(q1) VALUES (-9223372036854775808.5); -- should fail
--Testcase 1313:
INSERT INTO INT8_TMP(q1) VALUES (-9223372036854775808.4);
--Testcase 1272:
SELECT q1 FROM INT8_TMP; -- ok
--Testcase 1314:
DELETE FROM INT8_TMP;
--Testcase 1315:
INSERT INTO INT8_TMP(q1) VALUES (9223372036854775807.4);
--Testcase 1273:
SELECT q1 FROM INT8_TMP; -- ok
--Testcase 1316:
DELETE FROM INT8_TMP;
--Testcase 1274:
INSERT INTO INT8_TMP(q1) VALUES (9223372036854775807.5); -- should fail
--Testcase 1275:
INSERT INTO INT4_TMP(a) VALUES (-2147483648.5); -- should fail
--Testcase 1317:
INSERT INTO INT4_TMP(a) VALUES (-2147483648.4);
--Testcase 1276:
SELECT a FROM INT4_TMP; -- ok
--Testcase 1318:
DELETE FROM INT4_TMP;
--Testcase 1319:
INSERT INTO INT4_TMP(a) VALUES (2147483647.4);
--Testcase 1277:
SELECT a FROM INT4_TMP; -- ok
--Testcase 1278:
INSERT INTO INT4_TMP(a) VALUES (2147483647.5); -- should fail
--Testcase 1279:
INSERT INTO INT2_TMP(f1) VALUES (-32768.5); -- should fail
--Testcase 1320:
DELETE FROM INT2_TMP;
--Testcase 1321:
INSERT INTO INT2_TMP(f1) VALUES (-32768.4);
--Testcase 1280:
SELECT f1 FROM INT2_TMP; -- ok
--Testcase 1322:
DELETE FROM INT2_TMP;
--Testcase 1322:
INSERT INTO INT2_TMP(f1) VALUES (32767.4);
--Testcase 1281:
SELECT f1 FROM INT2_TMP; -- ok
--Testcase 1282:
INSERT INTO INT2_TMP(f1) VALUES (32767.5); -- should fail

-- Check inf/nan conversion behavior

--Testcase 673:
DELETE FROM fract_only;

--Testcase 674:
INSERT INTO fract_only(val) VALUES ('NaN'::float8);

--Testcase 675:
SELECT val::numeric AS numeric FROM fract_only;

--Testcase 676:
DELETE FROM fract_only;

--Testcase 677:
INSERT INTO fract_only(val) VALUES ('Infinity'::float8);

--Testcase 678:
SELECT val::numeric FROM fract_only;

--Testcase 679:
DELETE FROM fract_only;

--Testcase 680:
INSERT INTO fract_only(val) VALUES ('-Infinity'::float8);

--Testcase 681:
SELECT val::numeric FROM fract_only;

--Testcase 682:
DELETE FROM fract_only;

-- not work
-- griddb not support numeric type, if we user float8 in stead of numeric,
-- these case is redundant
-- DELETE FROM fract_only;
-- INSERT INTO fract_only(val) VALUES ('NaN'::numeric);
-- SELECT val::float8 AS numeric FROM fract_only;
-- DELETE FROM fract_only;
-- INSERT INTO fract_only(val) VALUES ('Infinity'::numeric);
-- SELECT val::float8 FROM fract_only;
-- DELETE FROM fract_only;
-- INSERT INTO fract_only(val) VALUES ('-Infinity'::numeric);
-- SELECT val::float8 FROM fract_only;
-- DELETE FROM fract_only;

--Testcase 683:
INSERT INTO fract_only(val) VALUES ('NaN'::float8);

--Testcase 684:
SELECT val::numeric AS numeric FROM fract_only;

--Testcase 685:
DELETE FROM fract_only;

--Testcase 686:
INSERT INTO fract_only(val) VALUES ('Infinity'::float4);

--Testcase 687:
SELECT val::numeric FROM fract_only;

--Testcase 688:
DELETE FROM fract_only;

--Testcase 689:
INSERT INTO fract_only(val) VALUES ('-Infinity'::float4);

--Testcase 690:
SELECT val::numeric FROM fract_only;

-- griddb not support numeric type
-- SELECT 'NaN'::numeric::float4;
-- SELECT 'Infinity'::numeric::float4;
-- SELECT '-Infinity'::numeric::float4;
-- SELECT '42'::int2::numeric;
-- SELECT 'NaN'::numeric::int2;
-- SELECT 'Infinity'::numeric::int2;
-- SELECT '-Infinity'::numeric::int2;
-- SELECT 'NaN'::numeric::int4;
-- SELECT 'Infinity'::numeric::int4;
-- SELECT '-Infinity'::numeric::int4;
-- SELECT 'NaN'::numeric::int8;
-- SELECT 'Infinity'::numeric::int8;
-- SELECT '-Infinity'::numeric::int8;

--Testcase 691:
DELETE FROM fract_only;

--Testcase 692:
DROP FOREIGN TABLE fract_only;

-- Simple check that ceil(), floor(), and round() work correctly

--Testcase 693:
CREATE FOREIGN TABLE ceil_floor_round (id serial options (rowkey 'true'), a float8) SERVER griddb_svr;

--Testcase 694:
INSERT INTO ceil_floor_round(a) VALUES ('-5.5');

--Testcase 695:
INSERT INTO ceil_floor_round(a) VALUES ('-5.499999');

--Testcase 696:
INSERT INTO ceil_floor_round(a) VALUES ('9.5');

--Testcase 697:
INSERT INTO ceil_floor_round(a) VALUES ('9.4999999');

--Testcase 698:
INSERT INTO ceil_floor_round(a) VALUES ('0.0');

--Testcase 699:
INSERT INTO ceil_floor_round(a) VALUES ('0.0000001');

--Testcase 700:
INSERT INTO ceil_floor_round(a) VALUES ('-0.000001');

--Testcase 701:
SELECT a::numeric, ceil(a::numeric), ceiling(a::numeric), floor(a::numeric), round(a::numeric) FROM ceil_floor_round;

-- Check rounding, it should round ties away from zero.

--Testcase 702:
DELETE FROM ceil_floor_round;

--Testcase 703:
INSERT INTO ceil_floor_round(a) SELECT * FROM generate_series(-5,5);

--Testcase 704:
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

--Testcase 705:
CREATE FOREIGN TABLE width_bucket_tbl (
	id serial OPTIONS (rowkey 'true'),
	id1 float8,
	id2 float8,
	id3 float8,
	id4 int
) SERVER griddb_svr;

--Testcase 706:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (5.0, 3.0, 4.0, 0);

--Testcase 707:
SELECT width_bucket(id1, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 708:
DELETE FROM width_bucket_tbl;

--Testcase 709:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (5.0, 3.0, 4.0, -5);

--Testcase 710:
SELECT width_bucket(id1, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 711:
DELETE FROM width_bucket_tbl;

--Testcase 712:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (3.5, 3.0, 3.0, 888);

--Testcase 713:
SELECT width_bucket(id1, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 714:
DELETE FROM width_bucket_tbl;

--Testcase 715:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (5.0, 3.0, 4.0, 0);

--Testcase 716:
SELECT width_bucket(id1::float8, id2::float8, id3::float8, id4) FROM width_bucket_tbl;

--Testcase 717:
DELETE FROM width_bucket_tbl;

--Testcase 718:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (5.0, 3.0, 4.0, -5);

--Testcase 719:
SELECT width_bucket(id1::float8, id2::float8, id3::float8, id4) FROM width_bucket_tbl;

--Testcase 720:
DELETE FROM width_bucket_tbl;

--Testcase 721:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (3.5, 3.0, 3.0, 888);

--Testcase 722:
SELECT width_bucket(id1::float8, id2::float8, id3::float8, id4) FROM width_bucket_tbl;

--Testcase 723:
DELETE FROM width_bucket_tbl;

--Testcase 724:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES ('NaN'::numeric, 3.0, 4.0, 888);

--Testcase 725:
SELECT width_bucket(id1, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 726:
DELETE FROM width_bucket_tbl;

--Testcase 727:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (0, 'NaN'::numeric, 4.0, 888);

--Testcase 728:
SELECT width_bucket(id1::float8, id2, id3::float8, id4) FROM width_bucket_tbl;

--Testcase 729:
DELETE FROM width_bucket_tbl;

--Testcase 730:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (2.0, 3.0, '-inf'::float8, 888);

--Testcase 731:
SELECT width_bucket(id1::float8, id2, id3::float8, id4) FROM width_bucket_tbl;

--Testcase 732:
DELETE FROM width_bucket_tbl;

--Testcase 733:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (0, '-inf'::float8, 4.0, 888);

--Testcase 734:
SELECT width_bucket(id1::float8, id2, id3::float8, id4) FROM width_bucket_tbl;

-- normal operation

--Testcase 735:
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

--Testcase 736:
UPDATE width_bucket_test SET operand_f8 = operand_num::float8;

--Testcase 737:
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

-- Check positive and negative infinity: we require
-- finite bucket bounds, but allow an infinite operand

--Testcase 738:
DELETE FROM width_bucket_tbl;

--Testcase 739:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (0.0, 'Infinity', 5, 10);

--Testcase 740:
SELECT width_bucket(id1::numeric, id2::numeric, id3, id4) FROM width_bucket_tbl;  -- error

--Testcase 741:
DELETE FROM width_bucket_tbl;

--Testcase 742:
DELETE FROM width_bucket_tbl;

--Testcase 743:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (0.0, 5, 'Infinity', 10);

--Testcase 744:
SELECT width_bucket(id1::numeric, id2::numeric, id3, id4) FROM width_bucket_tbl;  -- error

--Testcase 745:
DELETE FROM width_bucket_tbl;

--Testcase 746:
DELETE FROM width_bucket_tbl;

--Testcase 747:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES ('Infinity', 1, 10, 10);

--Testcase 748:
SELECT width_bucket(id1::numeric, id2::numeric, id3, id4) FROM width_bucket_tbl;

--Testcase 749:
DELETE FROM width_bucket_tbl;

--Testcase 750:
DELETE FROM width_bucket_tbl;

--Testcase 751:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES ('-Infinity', 1, 10, 10);

--Testcase 752:
SELECT width_bucket(id1::numeric, id2::numeric, id3, id4) FROM width_bucket_tbl;

--Testcase 753:
DELETE FROM width_bucket_tbl;

--Testcase 754:
DELETE FROM width_bucket_tbl;

--Testcase 755:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (0.0, 'Infinity', 5, 10);

--Testcase 756:
SELECT width_bucket(id1::float8, id2::float8, id3, id4) FROM width_bucket_tbl;  -- error

--Testcase 757:
DELETE FROM width_bucket_tbl;

--Testcase 758:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES (0.0, 5, '-Infinity', 20);

--Testcase 759:
SELECT width_bucket(id1::float8, id2, id3::float8, id4) FROM width_bucket_tbl; -- error

--Testcase 760:
DELETE FROM width_bucket_tbl;

--Testcase 761:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES ('Infinity', 1, 10, 10);

--Testcase 762:
SELECT width_bucket(id1::float8, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 763:
DELETE FROM width_bucket_tbl;

--Testcase 764:
INSERT INTO width_bucket_tbl(id1, id2, id3, id4) VALUES ('-Infinity', 1, 10, 10);

--Testcase 765:
SELECT width_bucket(id1::float8, id2, id3, id4) FROM width_bucket_tbl;

--Testcase 766:
DROP FOREIGN TABLE width_bucket_test;

-- Simple test for roundoff error when results should be exact

--Testcase 767:
CREATE FOREIGN TABLE width_bucket_roundoff_tbl (id serial options (rowkey 'true'), x int) SERVER griddb_svr;

--Testcase 768:
INSERT INTO width_bucket_roundoff_tbl(x) SELECT * FROM generate_series(0, 110, 10) x;

--Testcase 769:
SELECT x, width_bucket(x::float8, 10, 100, 9) as flt,
       width_bucket(x::numeric, 10, 100, 9) as num
FROM width_bucket_roundoff_tbl;

--Testcase 770:
SELECT x, width_bucket(x::float8, 100, 10, 9) as flt,
       width_bucket(x::numeric, 100, 10, 9) as num
FROM width_bucket_roundoff_tbl;

--Testcase 771:
DROP FOREIGN TABLE width_bucket_roundoff_tbl;

-- TO_CHAR()
--

--Testcase 772:
SELECT to_char(val::numeric(210,10), '9G999G999G999G999G999')
	FROM num_data;

--Testcase 773:
SELECT to_char(val::numeric(210,10), '9G999G999G999G999G999D999G999G999G999G999')
	FROM num_data;

--Testcase 774:
SELECT to_char(val::numeric(210,10), '9999999999999999.999999999999999PR')
	FROM num_data;

--Testcase 775:
SELECT to_char(val::numeric(210,10), '9999999999999999.999999999999999S')
	FROM num_data;

--Testcase 776:
SELECT to_char(val::numeric(210,10), 'MI9999999999999999.999999999999999')     FROM num_data;

--Testcase 777:
SELECT  to_char(val::numeric(210,10), 'FMS9999999999999999.999999999999999')    FROM num_data;

--Testcase 778:
SELECT to_char(val::numeric(210,10), 'FM9999999999999999.999999999999999THPR') FROM num_data;

--Testcase 779:
SELECT to_char(val::numeric(210,10), 'SG9999999999999999.999999999999999th')   FROM num_data;

--Testcase 780:
SELECT to_char(val::numeric(210,10), '0999999999999999.999999999999999')       FROM num_data;

--Testcase 781:
SELECT to_char(val::numeric(210,10), 'S0999999999999999.999999999999999')      FROM num_data;

--Testcase 782:
SELECT to_char(val::numeric(210,10), 'FM0999999999999999.999999999999999')     FROM num_data;

--Testcase 783:
SELECT to_char(val::numeric(210,10), 'FM9999999999999999.099999999999999') 	FROM num_data;

--Testcase 784:
SELECT to_char(val::numeric(210,10), 'FM9999999999990999.990999999999999') 	FROM num_data;

--Testcase 785:
SELECT to_char(val::numeric(210,10), 'FM0999999999999999.999909999999999') 	FROM num_data;

--Testcase 786:
SELECT to_char(val::numeric(210,10), 'FM9999999990999999.099999999999999') 	FROM num_data;

--Testcase 787:
SELECT to_char(val::numeric(210,10), 'L9999999999999999.099999999999999')	FROM num_data;

--Testcase 788:
SELECT to_char(val::numeric(210,10), 'FM9999999999999999.99999999999999')	FROM num_data;

--Testcase 789:
SELECT to_char(val::numeric(210,10), 'S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9') FROM num_data;

--Testcase 790:
SELECT to_char(val::numeric(210,10), 'FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9') FROM num_data;

--Testcase 791:
SELECT to_char(val::numeric(210,10), E'99999 "text" 9999 "9999" 999 "\\"text between quote marks\\"" 9999') FROM num_data;

--Testcase 792:
SELECT to_char(val::numeric(210,10), '999999SG9999999999')			FROM num_data;

--Testcase 793:
SELECT to_char(val::numeric(210,10), 'FM9999999999999999.999999999999999')	FROM num_data;

--Testcase 794:
SELECT to_char(val::numeric(210,10), '9.999EEEE')				FROM num_data;

--Testcase 795:
DELETE FROM v;

--Testcase 796:
INSERT INTO v(x) VALUES ('0'::numeric),('-4.2'),('4.2e9'),('1.2e-5'),('inf'),('-inf'),('nan');

--Testcase 797:
SELECT x,
  to_char(x, '9.999EEEE') as numeric,
  to_char(x::float8, '9.999EEEE') as float8,
  to_char(x::float4, '9.999EEEE') as float4
FROM v;

--Testcase 1283:
DELETE FROM v;
--Testcase 1284:
INSERT INTO v(x) VALUES (-16379),(-16378),(-1234),(-789),(-45),(-5),(-4),(-3),(-2),(-1),(0),
         (1),(2),(3),(4),(5),(38),(275),(2345),(45678),(131070),(131071);

--Testcase 1285:
SELECT x,
  to_char(('1.2345e'||x)::numeric, '9.999EEEE') as numeric
FROM v;

--Testcase 798:
DELETE FROM v;

--Testcase 799:
INSERT INTO v(x) VALUES ('0'::numeric),('-4.2'),('4.2e9'),('1.2e-5'),('inf'),('-inf'),('nan');

--Testcase 800:
SELECT x,
  to_char(x, 'MI9999999999.99') as numeric,
  to_char(x::float8, 'MI9999999999.99') as float8,
  to_char(x::float4, 'MI9999999999.99') as float4
FROM v;

--Testcase 801:
DELETE FROM v;

--Testcase 802:
INSERT INTO v(x) VALUES ('0'::numeric),('-4.2'),('4.2e9'),('1.2e-5'),('inf'),('-inf'),('nan');

--Testcase 803:
SELECT x,
  to_char(x, 'MI99.99') as numeric,
  to_char(x::float8, 'MI99.99') as float8,
  to_char(x::float4, 'MI99.99') as float4
FROM v;
--Testcase 1286:
DELETE FROM v;

--Testcase 804:
DELETE FROM ceil_floor_round;

--Testcase 805:
INSERT INTO ceil_floor_round(a) VALUES ('100'::numeric);

--Testcase 806:
SELECT to_char(a::numeric, 'FM999.9') FROM ceil_floor_round;

--Testcase 807:
SELECT to_char(a::numeric, 'FM999.') FROM ceil_floor_round;

--Testcase 808:
SELECT to_char(a::numeric, 'FM999') FROM ceil_floor_round;

-- Check parsing of literal text in a format string

--Testcase 809:
SELECT to_char(a::numeric, 'foo999') FROM ceil_floor_round;

--Testcase 810:
SELECT to_char(a::numeric, 'f\oo999') FROM ceil_floor_round;

--Testcase 811:
SELECT to_char(a::numeric, 'f\\oo999') FROM ceil_floor_round;

--Testcase 812:
SELECT to_char(a::numeric, 'f\"oo999') FROM ceil_floor_round;

--Testcase 813:
SELECT to_char(a::numeric, 'f\\"oo999') FROM ceil_floor_round;

--Testcase 814:
SELECT to_char(a::numeric, 'f"ool"999') FROM ceil_floor_round;

--Testcase 815:
SELECT to_char(a::numeric, 'f"\ool"999') FROM ceil_floor_round;

--Testcase 816:
SELECT to_char(a::numeric, 'f"\\ool"999') FROM ceil_floor_round;

--Testcase 817:
SELECT to_char(a::numeric, 'f"ool\"999') FROM ceil_floor_round;

--Testcase 818:
SELECT to_char(a::numeric, 'f"ool\\"999') FROM ceil_floor_round;

-- TO_NUMBER()
--

--Testcase 819:
SET lc_numeric = 'C';

--Testcase 820:
CREATE FOREIGN TABLE to_number_test (
	id serial OPTIONS (rowkey 'true'),
	val text,
	fmt text
) SERVER griddb_svr;

--Testcase 821:
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

--Testcase 822:
SELECT id AS to_number,  to_number(val, fmt) from to_number_test;

--Testcase 823:
RESET lc_numeric;

--Testcase 824:
DROP FOREIGN TABLE to_number_test;

--
-- Input syntax
--

--Testcase 825:
CREATE FOREIGN TABLE num_input_test (id serial options (rowkey 'true'), n1 float8) SERVER griddb_svr;

-- good inputs

--Testcase 826:
INSERT INTO num_input_test(n1) VALUES (' 123');

--Testcase 827:
INSERT INTO num_input_test(n1) VALUES ('   3245874    ');

--Testcase 828:
INSERT INTO num_input_test(n1) VALUES ('  -93853');

--Testcase 829:
INSERT INTO num_input_test(n1) VALUES ('555.50');

--Testcase 830:
INSERT INTO num_input_test(n1) VALUES ('-555.50');

--Testcase 831:
INSERT INTO num_input_test(n1) VALUES ('NaN ');

--Testcase 832:
INSERT INTO num_input_test(n1) VALUES ('        nan');

--Testcase 833:
INSERT INTO num_input_test(n1) VALUES (' inf ');

--Testcase 834:
INSERT INTO num_input_test(n1) VALUES (' +inf ');

--Testcase 835:
INSERT INTO num_input_test(n1) VALUES (' -inf ');

--Testcase 836:
INSERT INTO num_input_test(n1) VALUES (' Infinity ');

--Testcase 837:
INSERT INTO num_input_test(n1) VALUES (' +inFinity ');

--Testcase 838:
INSERT INTO num_input_test(n1) VALUES (' -INFINITY ');

-- bad inputs

--Testcase 839:
INSERT INTO num_input_test(n1) VALUES ('     ');

--Testcase 840:
INSERT INTO num_input_test(n1) VALUES ('   1234   %');

--Testcase 841:
INSERT INTO num_input_test(n1) VALUES ('xyz');

--Testcase 842:
INSERT INTO num_input_test(n1) VALUES ('- 1234');

--Testcase 843:
INSERT INTO num_input_test(n1) VALUES ('5 . 0');

--Testcase 844:
INSERT INTO num_input_test(n1) VALUES ('5. 0   ');

--Testcase 845:
INSERT INTO num_input_test(n1) VALUES ('');

--Testcase 846:
INSERT INTO num_input_test(n1) VALUES (' N aN ');

--Testcase 847:
INSERT INTO num_input_test(n1) VALUES ('+ infinity');

--Testcase 848:
SELECT n1 FROM num_input_test;

--
-- Test some corner cases for multiplication
--

--Testcase 849:
CREATE FOREIGN TABLE num_test_calc (
    id serial options (rowkey 'true'),
    n1 float8,
    n2 float8
) SERVER griddb_svr;

--Testcase 850:
DELETE FROM num_test_calc;

--Testcase 851:
INSERT INTO num_test_calc(n1, n2) VALUES (4790999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);

--Testcase 852:
INSERT INTO num_test_calc(n1, n2) VALUES (4790999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);

--Testcase 853:
INSERT INTO num_test_calc(n1, n2) VALUES (4789999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);

--Testcase 854:
INSERT INTO num_test_calc(n1, n2) VALUES (4770999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);

--Testcase 855:
INSERT INTO num_test_calc(n1, n2) VALUES (4769999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);

--Testcase 856:
SELECT n1::numeric * n2::numeric FROM num_test_calc;

--Testcase 1287:
DELETE FROM num_test_calc;
--Testcase 1288:
INSERT INTO num_test_calc(n1, n2) VALUES ((0.1 - 2e-16383), (0.1 - 3e-16383));
--Testcase 1289:
select trim_scale(n1::numeric * n2::numeric) from num_test_calc;

--
-- Test some corner cases for division
--

--Testcase 857:
DELETE FROM num_test_calc;

--Testcase 858:
INSERT INTO num_test_calc(n1, n2) VALUES (999999999999999999999, 1000000000000000000000);

--Testcase 859:
SELECT n1::numeric/n2::numeric FROM num_test_calc;

--Testcase 860:
SELECT div(n1::numeric,n2::numeric) FROM num_test_calc;

--Testcase 861:
SELECT mod(n1::numeric,n2::numeric) FROM num_test_calc;

--Testcase 862:
SELECT div(-n1::numeric,n2::numeric) FROM num_test_calc;

--Testcase 863:
SELECT mod(-n1::numeric,n2::numeric) FROM num_test_calc;

--Testcase 864:
SELECT div(-n1::numeric,n2::numeric)*n2 + 
	mod(-n1::numeric,n2::numeric) FROM num_test_calc;

--Testcase 865:
DELETE FROM num_test_calc;

--Testcase 866:
INSERT INTO num_test_calc(n1, n2) VALUES (70.0, 70);

--Testcase 867:
SELECT mod (n1::numeric,n2::numeric) FROM num_test_calc;

--Testcase 868:
SELECT div (n1::numeric,n2::numeric) FROM num_test_calc;

--Testcase 869:
SELECT n1::numeric / n2::numeric FROM num_test_calc;

--Testcase 870:
DELETE FROM num_test_calc;

--Testcase 871:
INSERT INTO num_test_calc(n1, n2) VALUES (12345678901234567890, 123);

--Testcase 872:
SELECT n1::numeric % n2::numeric FROM num_test_calc;

--Testcase 873:
SELECT n1::numeric / n2::numeric FROM num_test_calc;

--Testcase 874:
SELECT div(n1::numeric, n2::numeric) FROM num_test_calc;

--Testcase 875:
SELECT div(n1::numeric, n2::numeric) * n2::numeric + (n1::numeric % n2::numeric) FROM num_test_calc;

--
-- Test some corner cases for square root
--

--Testcase 876:
DELETE FROM num_test_calc;

--Testcase 877:
INSERT INTO num_test_calc(n1, n2) VALUES (1.000000000000003, 0);

--Testcase 878:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 879:
DELETE FROM num_test_calc;

--Testcase 880:
INSERT INTO num_test_calc(n1, n2) VALUES (1.000000000000004, 0);

--Testcase 881:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 882:
DELETE FROM num_test_calc;

--Testcase 883:
INSERT INTO num_test_calc(n1, n2) VALUES (96627521408608.56340355805, 0);

--Testcase 884:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 885:
DELETE FROM num_test_calc;

--Testcase 886:
INSERT INTO num_test_calc(n1, n2) VALUES (96627521408608.56340355806, 0);

--Testcase 887:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 888:
DELETE FROM num_test_calc;

--Testcase 889:
INSERT INTO num_test_calc(n1, n2) VALUES (515549506212297735.073688290367, 0);

--Testcase 890:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 891:
DELETE FROM num_test_calc;

--Testcase 892:
INSERT INTO num_test_calc(n1, n2) VALUES (515549506212297735.073688290368, 0);

--Testcase 893:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 894:
DELETE FROM num_test_calc;

--Testcase 895:
INSERT INTO num_test_calc(n1, n2) VALUES (8015491789940783531003294973900306, 0);

--Testcase 896:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--Testcase 897:
DELETE FROM num_test_calc;

--Testcase 898:
INSERT INTO num_test_calc(n1, n2) VALUES (8015491789940783531003294973900307, 0);

--Testcase 899:
SELECT sqrt(n1::numeric) FROM num_test_calc;

--
-- Test code path for raising to integer powers
--

--Testcase 900:
DELETE FROM num_test_calc;

--Testcase 901:
INSERT INTO num_test_calc(n1, n2) VALUES (10.0, -2147483648);

--Testcase 902:
SELECT n1::numeric ^ n2::numeric as rounds_to_zero FROM num_test_calc;

--Testcase 903:
DELETE FROM num_test_calc;

--Testcase 904:
INSERT INTO num_test_calc(n1, n2) VALUES (10.0, -2147483647);

--Testcase 905:
SELECT n1::numeric ^ n2::numeric as rounds_to_zero FROM num_test_calc;

--Testcase 906:
DELETE FROM num_test_calc;

--Testcase 907:
INSERT INTO num_test_calc(n1, n2) VALUES (10.0, 2147483647);

--Testcase 908:
SELECT n1::numeric ^ n2::numeric as overflows FROM num_test_calc;

--Testcase 909:
DELETE FROM num_test_calc;

--Testcase 910:
INSERT INTO num_test_calc(n1, n2) VALUES (117743296169.0, 1000000000);

--Testcase 911:
SELECT n1::numeric ^ n2::numeric as overflows FROM num_test_calc;

-- cases that used to return inaccurate results

--Testcase 912:
DELETE FROM num_test_calc;

--Testcase 913:
INSERT INTO num_test_calc(n1, n2) VALUES (3.789, 21);

--Testcase 914:
INSERT INTO num_test_calc(n1, n2) VALUES (3.789, 35);

--Testcase 915:
INSERT INTO num_test_calc(n1, n2) VALUES (1.2, 345);

--Testcase 916:
INSERT INTO num_test_calc(n1, n2) VALUES (0.12, (-20));

--Testcase 917:
INSERT INTO num_test_calc(n1, n2) VALUES (1.000000000123, (-2147483648));

--Testcase 918:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

--Testcase 1290:
DELETE FROM num_test_calc;
--Testcase 1291:
INSERT INTO num_test_calc(n1, n2) VALUES (0.9999999999, (23300000000000));
--Testcase 1292:
SELECT coalesce(nullif(n1::numeric ^ n2::numeric, 0), 0) AS rounds_to_zero FROM num_test_calc;

-- cases that used to error out

--Testcase 919:
DELETE FROM num_test_calc;

--Testcase 920:
INSERT INTO num_test_calc(n1, n2) VALUES (0.12, -25);

--Testcase 921:
INSERT INTO num_test_calc(n1, n2) VALUES (0.5678, -85);

--Testcase 922:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

--Testcase 1293:
DELETE FROM num_test_calc;
--Testcase 1294:
INSERT INTO num_test_calc(n1, n2) VALUES (0.9999999999, 70000000000000);
--Testcase 1295:
SELECT coalesce(nullif(n1::numeric ^ n2::numeric, 0), 0) AS underflows FROM num_test_calc;

-- negative base to integer powers
--Testcase 1296:
DELETE FROM num_test_calc;
--Testcase 1297:
INSERT INTO num_test_calc(n1, n2) VALUES (-1.0, 2147483646);
--Testcase 1298:
INSERT INTO num_test_calc(n1, n2) VALUES (-1.0, 2147483647);
--Testcase 1299:
INSERT INTO num_test_calc(n1, n2) VALUES (-1.0, 2147483648);
--Testcase 1300:
INSERT INTO num_test_calc(n1, n2) VALUES (-1.0, 1000000000000000);
--Testcase 1301:
INSERT INTO num_test_calc(n1, n2) VALUES (-1.0, 1000000000000001);
--Testcase 1302:
SELECT (n1 ^ n2)::numeric(17,16) FROM num_test_calc;
--
-- Tests for raising to non-integer powers
--

-- special cases

--Testcase 923:
DELETE FROM num_test_calc;

--Testcase 924:
INSERT INTO num_test_calc(n1, n2) VALUES (0.0, 0.0);

--Testcase 925:
INSERT INTO num_test_calc(n1, n2) VALUES (-12.34, 0.0);

--Testcase 926:
INSERT INTO num_test_calc(n1, n2) VALUES (12.34, 0.0);

--Testcase 927:
INSERT INTO num_test_calc(n1, n2) VALUES (0.0, 12.34);

--Testcase 928:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

-- NaNs

--Testcase 929:
DELETE FROM num_test_calc;

--Testcase 930:
INSERT INTO num_test_calc(n1, n2) VALUES ('NaN'::numeric, 'NaN'::numeric);

--Testcase 931:
INSERT INTO num_test_calc(n1, n2) VALUES ('NaN'::numeric, 0);

--Testcase 932:
INSERT INTO num_test_calc(n1, n2) VALUES ('NaN'::numeric, 1);

--Testcase 933:
INSERT INTO num_test_calc(n1, n2) VALUES (0, 'NaN'::numeric);

--Testcase 934:
INSERT INTO num_test_calc(n1, n2) VALUES (1, 'NaN'::numeric);

--Testcase 935:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

-- invalid inputs

--Testcase 936:
DELETE FROM num_test_calc;

--Testcase 937:
INSERT INTO num_test_calc(n1, n2) VALUES (0.0, -12.34);

--Testcase 938:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

--Testcase 939:
DELETE FROM num_test_calc;

--Testcase 940:
INSERT INTO num_test_calc(n1, n2) VALUES (-12.34, 1.2);

--Testcase 941:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

-- cases that used to generate inaccurate results

BEGIN;

--Testcase 942:
DELETE FROM num_test_calc;

--Testcase 943:
INSERT INTO num_test_calc(n1, n2) VALUES (32.1, 9.8);

--Testcase 944:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

--Testcase 945:
DELETE FROM num_test_calc;

--Testcase 946:
INSERT INTO num_test_calc(n1, n2) VALUES (32.1, -9.8);

--Testcase 947:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

--Testcase 948:
DELETE FROM num_test_calc;

--Testcase 949:
INSERT INTO num_test_calc(n1, n2) VALUES (12.3, 45.6);

--Testcase 950:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;

--Testcase 951:
DELETE FROM num_test_calc;

--Testcase 952:
INSERT INTO num_test_calc(n1, n2) VALUES (12.3, (-45.6));

--Testcase 953:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;
ROLLBACK;
-- big test
-- out of range

BEGIN;

--Testcase 954:
DELETE FROM num_input_test;

--Testcase 955:
INSERT INTO num_test_calc(n1, n2) VALUES (1.234, 5678);

--Testcase 956:
SELECT n1::numeric ^ n2::numeric FROM num_test_calc;
ROLLBACK;
--
-- Tests for EXP()
--

-- special cases

--Testcase 957:
DELETE FROM num_input_test;

--Testcase 958:
INSERT INTO num_input_test(n1) VALUES ('0.0');

--Testcase 959:
SELECT exp(n1::numeric) from num_input_test;

--Testcase 960:
DELETE FROM num_input_test;

--Testcase 961:
INSERT INTO num_input_test(n1) VALUES ('1.0');

--Testcase 962:
SELECT exp(n1::numeric) from num_input_test;

--Testcase 963:
DELETE FROM num_input_test;

--Testcase 964:
INSERT INTO num_input_test(n1) VALUES ('1.0');

--Testcase 965:
SELECT exp(n1::numeric(71, 70)) from num_input_test;

--Testcase 966:
DELETE FROM num_input_test;

--Testcase 967:
INSERT INTO num_input_test(n1) VALUES ('nan');

--Testcase 968:
SELECT exp(n1::numeric(71, 70)) from num_input_test;

--Testcase 969:
DELETE FROM num_input_test;

--Testcase 970:
INSERT INTO num_input_test(n1) VALUES ('inf');

--Testcase 971:
SELECT exp(n1::numeric(71, 70)) from num_input_test;

--Testcase 972:
DELETE FROM num_input_test;

--Testcase 973:
INSERT INTO num_input_test(n1) VALUES ('-inf');

--Testcase 974:
SELECT exp(n1::numeric(71, 70)) from num_input_test;

--Testcase 1303:
DELETE FROM num_input_test;
--Testcase 1304:
INSERT INTO num_input_test(n1) VALUES (-5000::numeric);
--Testcase 1305:
SELECT coalesce(nullif(exp(n1::numeric), 0), 0) AS rounds_to_zero FROM num_input_test;

--Testcase 1306:
DELETE FROM num_input_test;
--Testcase 1307:
INSERT INTO num_input_test(n1) VALUES (-10000::numeric);
--Testcase 1308:
SELECT coalesce(nullif(exp(n1::numeric), 0), 0) AS underflows FROM num_input_test;

-- cases that used to generate inaccurate results

--Testcase 975:
DELETE FROM num_input_test;

--Testcase 976:
INSERT INTO num_input_test(n1) VALUES ('32.999');

--Testcase 977:
SELECT exp(n1::numeric) from num_input_test;

--Testcase 978:
SELECT exp(-n1::numeric) from num_input_test;

--Testcase 979:
DELETE FROM num_input_test;

--Testcase 980:
INSERT INTO num_input_test(n1) VALUES ('123.456');

--Testcase 981:
SELECT exp(n1::numeric) from num_input_test;

--Testcase 982:
SELECT exp(-n1::numeric) from num_input_test;

-- big test

--Testcase 983:
DELETE FROM num_input_test;

--Testcase 984:
INSERT INTO num_input_test(n1) VALUES ('1234.5678');

--Testcase 985:
SELECT exp(n1::numeric) from num_input_test;

--
-- Tests for generate_series
--

--Testcase 986:
DELETE FROM num_input_test;

--Testcase 987:
INSERT INTO num_input_test(n1) select * from generate_series(0.0, 4.0);

--Testcase 988:
SELECT n1::numeric(2,1) FROM num_input_test;

--Testcase 989:
DELETE FROM num_input_test;

--Testcase 990:
INSERT INTO num_input_test(n1) select * from generate_series(0.1, 4.0, 1.3);

--Testcase 991:
SELECT n1::numeric(2,1) FROM num_input_test;

--Testcase 992:
DELETE FROM num_input_test;

--Testcase 993:
INSERT INTO num_input_test(n1) select * from generate_series(4.0, -1.5, -2.2);

--Testcase 994:
SELECT n1::numeric(2,1) FROM num_input_test;

-- Trigger errors

--Testcase 995:
DELETE FROM num_input_test;

--Testcase 996:
INSERT INTO num_input_test(n1) select * from generate_series(-100::numeric, 100::numeric, 0::numeric);

--Testcase 997:
SELECT n1 FROM num_input_test;

--Testcase 998:
DELETE FROM num_input_test;

--Testcase 999:
INSERT INTO num_input_test(n1) select * from generate_series(-100::numeric, 100::numeric, 'nan'::numeric);

--Testcase 1000:
SELECT n1 FROM num_input_test;

--Testcase 1001:
DELETE FROM num_input_test;

--Testcase 1002:
INSERT INTO num_input_test(n1) select * from generate_series('nan'::numeric, 100::numeric, 10::numeric);

--Testcase 1003:
SELECT n1 FROM num_input_test;

--Testcase 1004:
DELETE FROM num_input_test;

--Testcase 1005:
INSERT INTO num_input_test(n1) select * from generate_series(0::numeric, 'nan'::numeric, 10::numeric);

--Testcase 1006:
SELECT n1 FROM num_input_test;

--Testcase 1007:
DELETE FROM num_input_test;

--Testcase 1008:
INSERT INTO num_input_test(n1) select * from generate_series('inf'::numeric, 'inf'::numeric, 10::numeric);

--Testcase 1009:
SELECT n1 FROM num_input_test;

--Testcase 1010:
DELETE FROM num_input_test;

--Testcase 1011:
INSERT INTO num_input_test(n1) select * from generate_series(0::numeric, 'inf'::numeric, 10::numeric);

--Testcase 1012:
SELECT n1 FROM num_input_test;

--Testcase 1013:
DELETE FROM num_input_test;

--Testcase 1014:
INSERT INTO num_input_test(n1) select * from generate_series(0::numeric, 42::numeric, '-inf'::numeric);

--Testcase 1015:
SELECT n1 FROM num_input_test; 

-- Checks maximum, output is truncated

--Testcase 1016:
DELETE FROM num_input_test;

--Testcase 1017:
INSERT INTO num_input_test(n1) select (i / (10::numeric ^ 131071))::numeric(1,0)
	from generate_series(6 * (10::numeric ^ 131071),
			     9 * (10::numeric ^ 131071),
			     10::numeric ^ 131071) as a(i);

--Testcase 1018:
SELECT n1 AS numeric FROM num_input_test;

-- Check usage with variables

--Testcase 1019:
DELETE FROM num_test_calc;

--Testcase 1020:
INSERT INTO num_test_calc(n1, n2) select * from generate_series(1::numeric, 3::numeric) i, generate_series(i,3) j;

--Testcase 1021:
SELECT n1 as i, n2 as j FROM num_test_calc;

--Testcase 1022:
DELETE FROM num_test_calc;

--Testcase 1023:
INSERT INTO num_test_calc(n1, n2) select * from generate_series(1::numeric, 3::numeric) i, generate_series(1,i) j;

--Testcase 1024:
SELECT n1 as i, n2 as j FROM num_test_calc;

--Testcase 1025:
DELETE FROM num_test_calc;

--Testcase 1026:
INSERT INTO num_test_calc(n1, n2) select * from generate_series(1::numeric, 3::numeric) i, generate_series(1,5,i) j;

--Testcase 1027:
SELECT n1 as i, n2 as j FROM num_test_calc;

--
-- Tests for LN()
--

-- Invalid inputs

--Testcase 1028:
DELETE FROM num_input_test;

--Testcase 1029:
INSERT INTO num_input_test(n1) values('-12.34');

--Testcase 1030:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 1031:
DELETE FROM num_input_test;

--Testcase 1032:
INSERT INTO num_input_test(n1) values('0.0');

--Testcase 1033:
SELECT ln(n1::numeric) FROM num_input_test;

-- Some random tests

--Testcase 1034:
DELETE FROM num_input_test;

--Testcase 1035:
INSERT INTO num_input_test(n1) values(1.2345678e-28);

--Testcase 1036:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 1037:
DELETE FROM num_input_test;

--Testcase 1038:
INSERT INTO num_input_test(n1) values(0.0456789);

--Testcase 1039:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 1040:
DELETE FROM num_input_test;

--Testcase 1041:
INSERT INTO num_input_test(n1) values(0.349873948359354029493948309745709580730482050975);

--Testcase 1042:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 1043:
DELETE FROM num_input_test;

--Testcase 1044:
INSERT INTO num_input_test(n1) values(0.99949452);

--Testcase 1045:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 1046:
DELETE FROM num_input_test;

--Testcase 1047:
INSERT INTO num_input_test(n1) values(1.00049687395);

--Testcase 1048:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 1049:
DELETE FROM num_input_test;

--Testcase 1050:
INSERT INTO num_input_test(n1) values(1234.567890123456789);

--Testcase 1051:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 1052:
DELETE FROM num_input_test;

--Testcase 1053:
INSERT INTO num_input_test(n1) values(5.80397490724e5);

--Testcase 1054:
SELECT ln(n1::numeric) FROM num_input_test;

--Testcase 1055:
DELETE FROM num_input_test;

--Testcase 1056:
INSERT INTO num_input_test(n1) values(9.342536355e34);

--Testcase 1057:
SELECT ln(n1::numeric) FROM num_input_test;

-- 
-- Tests for LOG() (base 10)
--

-- invalid inputs

--Testcase 1058:
DELETE FROM num_input_test;

--Testcase 1059:
INSERT INTO num_input_test(n1) values('-12.34');

--Testcase 1060:
SELECT log(n1::numeric) FROM num_input_test;

--Testcase 1061:
DELETE FROM num_input_test;

--Testcase 1062:
INSERT INTO num_input_test(n1) values('0.0');

--Testcase 1063:
SELECT log(n1::numeric) FROM num_input_test;

-- some random tests

--Testcase 1064:
DELETE FROM num_input_test;

--Testcase 1065:
INSERT INTO num_input_test(n1) values(1.234567e-89);

--Testcase 1066:
SELECT log(n1::numeric) FROM num_input_test;

--Testcase 1067:
DELETE FROM num_input_test;

--Testcase 1068:
INSERT INTO num_input_test(n1) values(3.4634998359873254962349856073435545);

--Testcase 1069:
SELECT log(n1::numeric) FROM num_input_test;

--Testcase 1070:
DELETE FROM num_input_test;

--Testcase 1071:
INSERT INTO num_input_test(n1) values(9.999999999999999999);

--Testcase 1072:
SELECT log(n1::numeric) FROM num_input_test;

--Testcase 1073:
DELETE FROM num_input_test;

--Testcase 1074:
INSERT INTO num_input_test(n1) values(10.00000000000000000);

--Testcase 1075:
SELECT log(n1::numeric) FROM num_input_test;

--Testcase 1076:
DELETE FROM num_input_test;

--Testcase 1077:
INSERT INTO num_input_test(n1) values(10.00000000000000001);

--Testcase 1078:
SELECT log(n1::numeric) FROM num_input_test;

--Testcase 1079:
DELETE FROM num_input_test;

--Testcase 1080:
INSERT INTO num_input_test(n1) values(590489.45235237);

--Testcase 1081:
SELECT log(n1::numeric) FROM num_input_test;

-- similar as above test. Basically, we can get float8 value and 
-- convert to numeric
-- Tests for LOG() (arbitrary base)
--

-- invalid inputs

--Testcase 1082:
DELETE FROM num_test_calc;

--Testcase 1083:
INSERT INTO num_test_calc(n1, n2) VALUES(-12.34, 56.78);

--Testcase 1084:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;

--Testcase 1085:
DELETE FROM num_test_calc;

--Testcase 1086:
INSERT INTO num_test_calc(n1, n2) VALUES(-12.34, -56.78);

--Testcase 1087:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;

--Testcase 1088:
DELETE FROM num_test_calc;

--Testcase 1089:
INSERT INTO num_test_calc(n1, n2) VALUES(12.34, -56.78);

--Testcase 1090:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;

--Testcase 1091:
DELETE FROM num_test_calc;

--Testcase 1092:
INSERT INTO num_test_calc(n1, n2) VALUES(0.0, 12.34);

--Testcase 1093:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;

--Testcase 1094:
DELETE FROM num_test_calc;

--Testcase 1095:
INSERT INTO num_test_calc(n1, n2) VALUES(12.34, 0.0);

--Testcase 1096:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;

--Testcase 1097:
DELETE FROM num_test_calc;

--Testcase 1098:
INSERT INTO num_test_calc(n1, n2) VALUES(1.0, 12.34);

--Testcase 1099:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;

-- some random tests

--Testcase 1100:
DELETE FROM num_test_calc;

--Testcase 1101:
INSERT INTO num_test_calc(n1, n2) VALUES(1.23e-89, 6.4689e45);

--Testcase 1102:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;

--Testcase 1103:
DELETE FROM num_test_calc;

--Testcase 1104:
INSERT INTO num_test_calc(n1, n2) VALUES(0.99923, 4.58934e34);

--Testcase 1105:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;

--Testcase 1106:
DELETE FROM num_test_calc;

--Testcase 1107:
INSERT INTO num_test_calc(n1, n2) VALUES(1.000016, 8.452010e18);

--Testcase 1108:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;

--Testcase 1109:
DELETE FROM num_test_calc;

--Testcase 1110:
INSERT INTO num_test_calc(n1, n2) VALUES(3.1954752e47, 9.4792021e-73);

--Testcase 1111:
SELECT log(n1::numeric, n2::numeric) FROM num_test_calc;

--
-- Tests for scale()
--

--Testcase 1112:
DELETE FROM num_input_test;

--Testcase 1113:
INSERT INTO num_input_test(n1) values(numeric 'NaN');

--Testcase 1114:
SELECT scale(n1::numeric) FROM num_input_test;

--Testcase 1115:
DELETE FROM num_input_test;

--Testcase 1116:
INSERT INTO num_input_test(n1) values(NULL::numeric);

--Testcase 1117:
SELECT scale(n1::numeric) FROM num_input_test;

--Testcase 1118:
DELETE FROM num_input_test;

--Testcase 1119:
INSERT INTO num_input_test(n1) values(1.12);

--Testcase 1120:
SELECT scale(n1::numeric) FROM num_input_test;

--Testcase 1121:
DELETE FROM num_input_test;

--Testcase 1122:
INSERT INTO num_input_test(n1) values(0);

--Testcase 1123:
SELECT scale(n1::numeric) FROM num_input_test;

--Testcase 1124:
DELETE FROM num_input_test;

--Testcase 1125:
INSERT INTO num_input_test(n1) values(0.00);

--Testcase 1126:
SELECT scale(n1::numeric) FROM num_input_test;

--Testcase 1127:
DELETE FROM num_input_test;

--Testcase 1128:
INSERT INTO num_input_test(n1) values(1.12345);

--Testcase 1129:
SELECT scale(n1::numeric) FROM num_input_test;

--Testcase 1130:
DELETE FROM num_input_test;

--Testcase 1131:
INSERT INTO num_input_test(n1) values(110123.12475871856128);

--Testcase 1132:
SELECT scale(n1::numeric) FROM num_input_test;

--Testcase 1133:
DELETE FROM num_input_test;

--Testcase 1134:
INSERT INTO num_input_test(n1) values(-1123.12471856128);

--Testcase 1135:
SELECT scale(n1::numeric) FROM num_input_test;

--Testcase 1136:
DELETE FROM num_input_test;

--Testcase 1137:
INSERT INTO num_input_test(n1) values(-13.000000000000000);

--Testcase 1138:
SELECT scale(n1::numeric) FROM num_input_test;

--
-- Tests for min_scale()
--

--Testcase 1139:
DELETE FROM num_input_test;

--Testcase 1140:
INSERT INTO num_input_test(n1) values(numeric 'NaN');

--Testcase 1141:
SELECT min_scale(n1::numeric) is NULL FROM num_input_test; -- should be true

--Testcase 1142:
DELETE FROM num_input_test;

--Testcase 1143:
INSERT INTO num_input_test(n1) values(numeric 'inf');

--Testcase 1144:
SELECT min_scale(n1::numeric) is NULL FROM num_input_test; -- should be true

--Testcase 1145:
DELETE FROM num_input_test;

--Testcase 1146:
INSERT INTO num_input_test(n1) values(0);

--Testcase 1147:
SELECT min_scale(n1::numeric) FROM num_input_test; -- no digits

--Testcase 1148:
DELETE FROM num_input_test;

--Testcase 1149:
INSERT INTO num_input_test(n1) values(0.00);

--Testcase 1150:
SELECT min_scale(n1::numeric) FROM num_input_test; -- no digits again

--Testcase 1151:
DELETE FROM num_input_test;

--Testcase 1152:
INSERT INTO num_input_test(n1) values(1.0);

--Testcase 1153:
SELECT min_scale(n1::numeric) FROM num_input_test; -- no scale

--Testcase 1154:
DELETE FROM num_input_test;

--Testcase 1155:
INSERT INTO num_input_test(n1) values(1.1);

--Testcase 1156:
SELECT min_scale(n1::numeric) FROM num_input_test; -- scale 1

--Testcase 1157:
DELETE FROM num_input_test;

--Testcase 1158:
INSERT INTO num_input_test(n1) values(1.12);

--Testcase 1159:
SELECT min_scale(n1::numeric) FROM num_input_test; -- scale 2

--Testcase 1160:
DELETE FROM num_input_test;

--Testcase 1161:
INSERT INTO num_input_test(n1) values(1.123);

--Testcase 1162:
SELECT min_scale(n1::numeric) FROM num_input_test; -- scale 3

--Testcase 1163:
DELETE FROM num_input_test;

--Testcase 1164:
INSERT INTO num_input_test(n1) values(1.1234);

--Testcase 1165:
SELECT min_scale(n1::numeric) FROM num_input_test; -- scale 4, filled digit

--Testcase 1166:
DELETE FROM num_input_test;

--Testcase 1167:
INSERT INTO num_input_test(n1) values(1.12345);

--Testcase 1168:
SELECT min_scale(n1::numeric) FROM num_input_test; -- scale 5, 2 NDIGITS

--Testcase 1169:
DELETE FROM num_input_test;

--Testcase 1170:
INSERT INTO num_input_test(n1) values(1.1000);

--Testcase 1171:
SELECT min_scale(n1::numeric) FROM num_input_test; -- 1 pos in NDIGITS

--Testcase 1172:
DELETE FROM num_input_test;

--Testcase 1173:
INSERT INTO num_input_test(n1) values(1e100);

--Testcase 1174:
SELECT min_scale(n1::numeric) FROM num_input_test; -- very big number

--
-- Tests for trim_scale()
--

--Testcase 1175:
DELETE FROM num_input_test;

--Testcase 1176:
INSERT INTO num_input_test(n1) values(numeric 'NaN');

--Testcase 1177:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 1178:
DELETE FROM num_input_test;

--Testcase 1179:
INSERT INTO num_input_test(n1) values(numeric 'inf');

--Testcase 1180:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 1181:
DELETE FROM num_input_test;

--Testcase 1182:
INSERT INTO num_input_test(n1) values(1.120);

--Testcase 1183:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 1184:
DELETE FROM num_input_test;

--Testcase 1185:
INSERT INTO num_input_test(n1) values(0);

--Testcase 1186:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 1187:
DELETE FROM num_input_test;

--Testcase 1188:
INSERT INTO num_input_test(n1) values(0.00);

--Testcase 1189:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 1190:
DELETE FROM num_input_test;

--Testcase 1191:
INSERT INTO num_input_test(n1) values(1.1234500);

--Testcase 1192:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 1193:
DELETE FROM num_input_test;

--Testcase 1194:
INSERT INTO num_input_test(n1) values(110123.12475871856128000);

--Testcase 1195:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 1196:
DELETE FROM num_input_test;

--Testcase 1197:
INSERT INTO num_input_test(n1) values(-1123.124718561280000000);

--Testcase 1198:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 1199:
DELETE FROM num_input_test;

--Testcase 1200:
INSERT INTO num_input_test(n1) values(-13.00000000000000000000);

--Testcase 1201:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--Testcase 1202:
DELETE FROM num_input_test;

--Testcase 1203:
INSERT INTO num_input_test(n1) values(1e100);

--Testcase 1204:
SELECT trim_scale(n1::numeric) FROM num_input_test;

--
-- Tests for SUM()
--

-- cases that need carry propagation

--Testcase 1205:
DELETE FROM num_input_test;

--Testcase 1206:
INSERT INTO num_input_test(n1) values(generate_series(1, 100000));

--Testcase 1207:
SELECT SUM(9999::numeric) FROM num_input_test;

--Testcase 1208:
SELECT SUM((-9999)::numeric) FROM num_input_test;

--
-- Tests for GCD()
--

--Testcase 1209:
DELETE FROM num_test_calc;

--Testcase 1210:
INSERT INTO num_test_calc(n1, n2) VALUES
             (0::numeric, 0::numeric),
             (0::numeric, numeric 'NaN'),
             (0::numeric, 46375::numeric),
             (433125::numeric, 46375::numeric),
             (43312.5::numeric, 4637.5::numeric),
             (4331.250::numeric, 463.75000::numeric),
             ('inf', '0'),
             ('inf', '42'),
             ('inf', 'inf');

--Testcase 1211:
SELECT n1 as a, n2 as b, gcd(n1::numeric, n2::numeric), gcd(n1::numeric, -n2::numeric), gcd(-n2::numeric, n1::numeric), gcd(-n2::numeric, -n1::numeric) FROM num_test_calc;
--
-- Tests for LCM()
--

--Testcase 1212:
DELETE FROM num_test_calc;

--Testcase 1213:
INSERT INTO num_test_calc(n1, n2) VALUES
             (0::numeric, 0::numeric),
             (0::numeric, numeric 'NaN'),
             (0::numeric, 13272::numeric),
             (13272::numeric, 13272::numeric),
             (423282::numeric, 13272::numeric),
             (42328.2::numeric, 1327.2::numeric),
             (4232.820::numeric, 132.72000::numeric),
             ('inf', '0'),
             ('inf', '42'),
             ('inf', 'inf');

--Testcase 1214:
SELECT n1 as a, n2 as b, lcm(n1::numeric, n2::numeric), lcm(n1::numeric, -n2::numeric), lcm(-n2::numeric, n1::numeric), lcm(-n2::numeric, -n1::numeric) FROM num_test_calc;

--Testcase 1215:
DELETE FROM num_test_calc;

--Testcase 1216:
INSERT INTO num_test_calc(n1, n2) VALUES (10::numeric, 131068);

--Testcase 1217:
SELECT lcm((9999 * (n1::numeric)^n2::numeric + (n1::numeric^n2::numeric - 1)), 2) FROM num_test_calc; -- overflow

--
-- Tests for factorial
--

--Testcase 1218:
CREATE FOREIGN TABLE num_test_int (
    id serial options (rowkey 'true'),
    x bigint
) SERVER griddb_svr;

--Testcase 1219:
DELETE FROM num_test_int;

--Testcase 1220:
INSERT INTO num_test_int(x) VALUES (4::numeric);

--Testcase 1221:
SELECT factorial(x::int) FROM num_test_int;

--Testcase 1222:
DELETE FROM num_test_int;

--Testcase 1223:
INSERT INTO num_test_int(x) VALUES (15::numeric);

--Testcase 1224:
SELECT factorial(x::int) FROM num_test_int;

--Testcase 1225:
DELETE FROM num_test_int;

--Testcase 1226:
INSERT INTO num_test_int(x) VALUES (100000::numeric);

--Testcase 1227:
SELECT factorial(x::int) FROM num_test_int;

--Testcase 1228:
DELETE FROM num_test_int;

--Testcase 1229:
INSERT INTO num_test_int(x) VALUES (0::numeric);

--Testcase 1230:
SELECT factorial(x::int) FROM num_test_int;

--Testcase 1231:
DELETE FROM num_test_int;

--Testcase 1232:
INSERT INTO num_test_int(x) VALUES (-4::numeric);

--Testcase 1233:
SELECT factorial(x::int) FROM num_test_int;

--
-- Tests for pg_lsn()
--

--Testcase 1234:
DELETE FROM num_test_int;

--Testcase 1235:
INSERT INTO num_test_int(x) VALUES (23783416::numeric);

--Testcase 1236:
SELECT pg_lsn(x::numeric) FROM num_test_int;

--Testcase 1237:
DELETE FROM num_test_int;

--Testcase 1238:
INSERT INTO num_test_int(x) VALUES (0::numeric);

--Testcase 1239:
SELECT pg_lsn(x::numeric) FROM num_test_int;

--Testcase 1240:
DELETE FROM num_test_int;

--Testcase 1241:
INSERT INTO num_test_int(x) VALUES (18446744073709551615::numeric);

--Testcase 1242:
SELECT pg_lsn(x::numeric) FROM num_test_int;

--Testcase 1243:
DELETE FROM num_test_int;

--Testcase 1244:
INSERT INTO num_test_int(x) VALUES (-1::numeric);

--Testcase 1245:
SELECT pg_lsn(x::numeric) FROM num_test_int;

--Testcase 1246:
DELETE FROM num_test_int;

--Testcase 1247:
INSERT INTO num_test_int(x) VALUES (18446744073709551616::numeric);

--Testcase 1248:
SELECT pg_lsn(x::numeric) FROM num_test_int;

--Testcase 1249:
DELETE FROM num_test_int;

--Testcase 1250:
INSERT INTO num_test_int(x) VALUES ('NaN'::numeric);

--Testcase 1251:
SELECT pg_lsn(x::numeric) FROM num_test_int;

--Testcase 1252:
DROP FOREIGN TABLE width_bucket_tbl;

--Testcase 1253:
DROP FOREIGN TABLE num_test_calc;

--Testcase 1254:
DROP FOREIGN TABLE num_test_int;

--Testcase 1255:
DROP FOREIGN TABLE num_data;

--Testcase 1256:
DROP FOREIGN TABLE num_exp_add;

--Testcase 1257:
DROP FOREIGN TABLE num_exp_sub;

--Testcase 1258:
DROP FOREIGN TABLE num_exp_div;

--Testcase 1259:
DROP FOREIGN TABLE num_exp_mul;

--Testcase 1260:
DROP FOREIGN TABLE num_exp_sqrt;

--Testcase 1261:
DROP FOREIGN TABLE num_exp_ln;

--Testcase 1262:
DROP FOREIGN TABLE num_exp_log10;

--Testcase 1263:
DROP FOREIGN TABLE num_exp_power_10_ln;

--Testcase 1264:
DROP FOREIGN TABLE num_result;

--Testcase 1265:
DROP FOREIGN TABLE num_input_test;

--Testcase 1266:
DROP FOREIGN TABLE v;

--Testcase 1267:
DROP FOREIGN TABLE ceil_floor_round;

--Testcase 1268:
DROP USER MAPPING FOR public SERVER griddb_svr;

--Testcase 1269:
DROP SERVER griddb_svr CASCADE;

--Testcase 1270:
DROP EXTENSION griddb_fdw CASCADE;
