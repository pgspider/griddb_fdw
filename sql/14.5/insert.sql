\set ECHO none
\ir sql/parameters.conf
\set ECHO all

--
-- insert with DEFAULT in the target_list
--

--Testcase 1:
CREATE EXTENSION griddb_fdw;

--Testcase 2:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');

--Testcase 3:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

--Testcase 4:
CREATE FOREIGN TABLE inserttest01 (id serial OPTIONS (rowkey 'true'), col1 int4, col2 int4 NOT NULL, col3 text default 'testing') SERVER griddb_svr;

--Testcase 5:
insert into inserttest01 (col1, col2, col3) values (DEFAULT, DEFAULT, DEFAULT);

--Testcase 6:
insert into inserttest01 (col2, col3) values (3, DEFAULT);

--Testcase 7:
insert into inserttest01 (col1, col2, col3) values (DEFAULT, 5, DEFAULT);

--Testcase 8:
insert into inserttest01 (col1, col2, col3) values (DEFAULT, 5, 'test');

--Testcase 9:
insert into inserttest01 (col1, col2) values (DEFAULT, 7);

--Testcase 10:
select col1, col2, col3 from inserttest01;

--
-- insert with similar expression / target_list values (all fail)
--

--Testcase 11:
insert into inserttest01 (col1, col2, col3) values (DEFAULT, DEFAULT);

--Testcase 12:
insert into inserttest01 (col1, col2, col3) values (1, 2);

--Testcase 13:
insert into inserttest01 (col1) values (1, 2);

--Testcase 14:
insert into inserttest01 (col1) values (DEFAULT, DEFAULT);

--Testcase 15:
select col1, col2, col3 from inserttest01;

--
-- VALUES test
--

--Testcase 16:
insert into inserttest01 (col1, col2, col3) values(10, 20, '40'), (-1, 2, DEFAULT),
    ((select 2), (select i from (values(3)) as foo (i)), 'values are fun!');

--Testcase 17:
select col1, col2, col3 from inserttest01;

--
-- TOASTed value test
--

--Testcase 18:
insert into inserttest01 (col1, col2, col3) values(30, 50, repeat('x', 10000));

--Testcase 19:
select col1, col2, char_length(col3) from inserttest01;

--Testcase 20:
drop foreign table inserttest01;

--
-- tuple larger than fillfactor - foreign table not support fillfactor
--
-- CREATE TABLE large_tuple_test (a int, b text) WITH (fillfactor = 10);
-- ALTER TABLE large_tuple_test ALTER COLUMN b SET STORAGE plain;

-- -- create page w/ free space in range [nearlyEmptyFreeSpace, MaxHeapTupleSize)
-- INSERT INTO large_tuple_test (select 1, NULL);

-- -- should still fit on the page
-- INSERT INTO large_tuple_test (select 2, repeat('a', 1000));
-- SELECT pg_size_pretty(pg_relation_size('large_tuple_test'::regclass, 'main'));

-- -- add small record to the second page
-- INSERT INTO large_tuple_test (select 3, NULL);

-- -- now this tuple won't fit on the second page, but the insert should
-- -- still succeed by extending the relation
-- INSERT INTO large_tuple_test (select 4, repeat('a', 8126));

-- DROP TABLE large_tuple_test;

--
-- check indirection (field/array assignment), cf bug #14265
--
-- these tests are aware that transformInsertStmt has 3 separate code paths
--

--Testcase 21:
create foreign table inserttest (f1 serial options (rowkey 'true'), f2 int[], if1 int, if2 text[]) server griddb_svr;

--Testcase 22:
insert into inserttest (f2[1], f2[2]) values (1,2);

--Testcase 23:
insert into inserttest (f2[1], f2[2]) values (3,4), (5,6);

--Testcase 24:
insert into inserttest (f2[1], f2[2]) select 7,8;

--Testcase 25:
insert into inserttest (f2[1], f2[2]) values (1,default);  -- not supported

--Testcase 26:
insert into inserttest (if1, if2) values (1,array['foo']);

--Testcase 27:
insert into inserttest (if1, if2) values (1,'{foo}'), (2,'{bar}');

--Testcase 28:
insert into inserttest (if1, if2) select 3, '{baz,quux}';

--Testcase 29:
insert into inserttest (if1, if2) values (1,default);

--Testcase 30:
insert into inserttest (if2[1], if2[2]) values ('foo', 'bar');

--Testcase 31:
insert into inserttest (if2[1], if2[2]) values ('foo', 'bar'), ('baz', 'quux');

--Testcase 32:
insert into inserttest (if2[1], if2[2]) select 'bear', 'beer';

--Testcase 33:
select f1, f2, (if1, if2) as f3 from inserttest;

-- also check reverse-listing

--Testcase 34:
create table inserttest2 (f1 bigint, f2 text);

--Testcase 35:
create rule irule1 as on insert to inserttest2 do also
  insert into inserttest (if2[1], if2[2])
  values (new.f1,new.f2);

--Testcase 36:
create rule irule2 as on insert to inserttest2 do also
  insert into inserttest (if1, if2[2])
  values (1,'fool'),(new.f1,new.f2);

--Testcase 37:
create rule irule3 as on insert to inserttest2 do also
  insert into inserttest (if1, if2[2])
  select new.f1, new.f2;

--Testcase 38:
\d+ inserttest2

--Testcase 39:
drop table inserttest2;

--Testcase 40:
drop foreign table inserttest;

-- direct partition inserts should check partition bound constraint

--Testcase 41:
create table range_parted (
  id serial,
  a text,
  b int
) partition by range (a, (b+0));

-- no partitions, so fail

--Testcase 42:
insert into range_parted(a,b) values ('a', 11);

--Testcase 43:
create foreign table part1 partition of range_parted for values from ('a', 1) to ('a', 10) server griddb_svr;

--Testcase 44:
alter foreign table part1 alter column id options (rowkey 'true');

--Testcase 45:
create foreign table part2 partition of range_parted for values from ('a', 10) to ('a', 20) server griddb_svr;

--Testcase 46:
alter foreign table part2 alter column id options (rowkey 'true');

--Testcase 47:
create foreign table part3 partition of range_parted for values from ('b', 1) to ('b', 10) server griddb_svr;

--Testcase 48:
alter foreign table part3 alter column id options (rowkey 'true');

--Testcase 49:
create foreign table part4 partition of range_parted for values from ('b', 10) to ('b', 20) server griddb_svr;

--Testcase 50:
alter foreign table part4 alter column id options (rowkey 'true');

-- fail, skip because partition bound constraint does not work on GridDB FDW 
--insert into part1(a, b) values ('a', 11);
--insert into part1(a, b) values ('b', 1);
-- ok

--Testcase 51:
insert into part1(a, b) values ('a', 1);
--insert into range_parted(a,b) values ('a', 1);
-- fail, skip because partition bound constraint does not work on GridDB FDW  
--insert into part4(a, b) values ('b', 21);
--insert into part4(a, b) values ('a', 10);
-- ok

--Testcase 52:
insert into part4(a,b) values ('b', 10);

-- fail (partition key a has a NOT NULL constraint)
--insert into part1 values (null);

--Testcase 53:
insert into range_parted values (null);
-- fail (expression key (b+0) cannot be null either)
-- skip because partition bound constraint does not work on GridDB FDW  
--insert into part1 values (1);

--Testcase 54:
create table list_parted (
 id serial,
 a text,
 b int
) partition by list (lower(a));

--Testcase 55:
create foreign table part_aa_bb partition of list_parted FOR VALUES IN ('aa', 'bb') server griddb_svr;

--Testcase 56:
alter foreign table part_aa_bb alter column id options (rowkey 'true');

--Testcase 57:
create foreign table part_cc_dd partition of list_parted FOR VALUES IN ('cc', 'dd') server griddb_svr;

--Testcase 58:
alter foreign table part_cc_dd alter column id options (rowkey 'true');

--Testcase 59:
create foreign table part_null partition of list_parted FOR VALUES IN (null) server griddb_svr;

--Testcase 60:
alter foreign table part_null alter column id options (rowkey 'true');

-- fail
-- skip because partition bound constraint does not work on GridDB FDW  
--insert into part_aa_bb(a,b) values ('cc', 1);
--insert into part_aa_bb(a,b) values ('AAa', 1);
--insert into part_aa_bb(a,b) values (null);
-- ok

--Testcase 61:
insert into part_cc_dd(a,b) values ('cC', 1);

--Testcase 62:
insert into part_null(a,b) values (null, 0);

-- check in case of multi-level partitioned table

--Testcase 63:
create table part_ee_ff partition of list_parted for values in ('ee', 'ff') partition by range (b);

--Testcase 64:
create foreign table part_ee_ff1 partition of part_ee_ff for values from (1) to (10) server griddb_svr;

--Testcase 65:
alter foreign table part_ee_ff1 alter column id options (rowkey 'true');

--Testcase 66:
create foreign table part_ee_ff2 partition of part_ee_ff for values from (10) to (20) server griddb_svr;

--Testcase 67:
alter foreign table part_ee_ff2 alter column id options (rowkey 'true');

-- test default partition

--Testcase 68:
create table part_default partition of list_parted default;
-- Negative test: a row, which would fit in other partition, does not fit
-- default partition, even when inserted directly

--Testcase 69:
insert into part_default(a,b) values ('aa', 2);

--Testcase 70:
insert into part_default(a,b) values (null, 2);
-- ok

--Testcase 71:
insert into part_default(a,b) values ('Zz', 2);
-- test if default partition works as expected for multi-level partitioned
-- table as well as when default partition itself is further partitioned

--Testcase 72:
drop table part_default;

--Testcase 73:
create table part_xx_yy partition of list_parted for values in ('xx', 'yy') partition by list (a);

--Testcase 74:
create foreign table part_xx_yy_p1 partition of part_xx_yy for values in ('xx') server griddb_svr;

--Testcase 75:
alter foreign table part_xx_yy_p1 alter column id options (rowkey 'true');

--Testcase 76:
create foreign table part_xx_yy_defpart partition of part_xx_yy default server griddb_svr;

--Testcase 77:
alter foreign table part_xx_yy_defpart alter column id options (rowkey 'true');

--Testcase 78:
create table part_default partition of list_parted default partition by range(b);

--Testcase 79:
create foreign table part_default_p1 partition of part_default for values from (20) to (30) server griddb_svr;

--Testcase 80:
alter foreign table part_default_p1 alter column id options (rowkey 'true');

--Testcase 81:
create foreign table part_default_p2 partition of part_default for values from (30) to (40) server griddb_svr;

--Testcase 82:
alter foreign table part_default_p2 alter column id options (rowkey 'true');

-- fail
-- skip because partition bound constraint does not work on GridDB FDW  
--insert into part_ee_ff1(a,b) values ('EE', 11);
--insert into part_default_p2(a,b) values ('gg', 43);
-- fail (even the parent's, ie, part_ee_ff's partition constraint applies)
-- skip because partition bound constraint does not work on GridDB FDW  
--insert into part_ee_ff1(a,b)  values ('cc', 1);
--insert into part_default(a,b) values ('gg', 43);
-- ok

--Testcase 83:
insert into part_ee_ff1(a,b)  values ('ff', 1);

--Testcase 84:
insert into part_ee_ff2(a,b)  values ('ff', 11);

--Testcase 85:
insert into part_default_p1(a,b)  values ('cd', 25);

--Testcase 86:
insert into part_default_p2(a,b)  values ('de', 35);

--Testcase 87:
insert into list_parted(a,b) values ('ab', 21);

--Testcase 88:
insert into list_parted(a,b) values ('xx', 1);

--Testcase 89:
insert into list_parted(a,b) values ('yy', 2);

--Testcase 90:
select tableoid::regclass, a, b from list_parted;

-- Check tuple routing for partitioned tables

-- fail

--Testcase 91:
insert into range_parted(a,b) values ('a', 0);
-- ok

--Testcase 92:
insert into range_parted(a,b) values ('a', 1);

--Testcase 93:
insert into range_parted(a,b) values ('a', 10);
-- fail

--Testcase 94:
insert into range_parted(a,b) values ('a', 20);
-- ok

--Testcase 95:
insert into range_parted(a,b) values ('b', 1);

--Testcase 96:
insert into range_parted(a,b) values ('b', 10);
-- fail (partition key (b+0) is null)

--Testcase 97:
insert into range_parted(a) values ('a');

-- Check default partition

--Testcase 98:
create foreign table part_def partition of range_parted default server griddb_svr;

--Testcase 99:
alter foreign table part_def alter column id options (rowkey 'true');
-- fail
-- skip because partition bound constraint does not work on GridDB FDW  
-- insert into part_def(a,b) values ('b', 10);
-- ok

--Testcase 100:
insert into part_def(a,b) values ('c', 10);

--Testcase 101:
insert into range_parted(a,b) values (null, null);

--Testcase 102:
insert into range_parted(a,b) values ('a', null);

--Testcase 103:
insert into range_parted(a,b) values (null, 19);

--Testcase 104:
insert into range_parted(a,b) values ('b', 20);

--Testcase 105:
select tableoid::regclass, a, b from range_parted;
-- ok

--Testcase 106:
insert into list_parted(a,b) values (null, 1);

--Testcase 107:
insert into list_parted (a) values ('aA');
-- fail (partition of part_ee_ff not found in both cases)
-- skip because partition bound constraint does not work on GridDB FDW  

--Testcase 108:
insert into list_parted(a,b) values ('EE', 0);
--insert into part_ee_ff(a,b) values ('EE', 0);
-- ok

--Testcase 109:
insert into list_parted(a,b) values ('EE', 1);

--Testcase 110:
insert into part_ee_ff(a,b) values ('EE', 10);

--Testcase 111:
select tableoid::regclass, a, b from list_parted;

-- some more tests to exercise tuple-routing with multi-level partitioning

--Testcase 112:
create table part_gg partition of list_parted for values in ('gg') partition by range (b);

--Testcase 113:
create table part_gg1 partition of part_gg for values from (minvalue) to (1);

--Testcase 114:
create table part_gg2 partition of part_gg for values from (1) to (10) partition by range (b);

--Testcase 115:
create table part_gg2_1 partition of part_gg2 for values from (1) to (5);

--Testcase 116:
create table part_gg2_2 partition of part_gg2 for values from (5) to (10);

--Testcase 117:
create table part_ee_ff3 partition of part_ee_ff for values from (20) to (30) partition by range (b);

--Testcase 118:
create table part_ee_ff3_1 partition of part_ee_ff3 for values from (20) to (25);

--Testcase 119:
create table part_ee_ff3_2 partition of part_ee_ff3 for values from (25) to (30);

--Testcase 120:
delete from list_parted;

--Testcase 121:
insert into list_parted (a) values ('aa'), ('cc');

--Testcase 122:
insert into list_parted (a,b) select 'Ff', s.a from generate_series(1, 29) s(a);

--Testcase 123:
insert into list_parted (a,b) select 'gg', s.a from generate_series(1, 9) s(a);

--Testcase 124:
insert into list_parted (b) values (1);

--Testcase 125:
select tableoid::regclass::text, a, min(b) as min_b, max(b) as max_b from list_parted group by 1, 2 order by 1;

-- direct partition inserts should check hash partition bound constraint

-- Use hand-rolled hash functions and operator classes to get predictable
-- result on different machines.  The hash function for int4 simply returns
-- the sum of the values passed to it and the one for text returns the length
-- of the non-empty string value passed to it or 0.

--Testcase 126:
create or replace function part_hashint4_noop(value int4, seed int8)
returns int8 as $$
select value + seed;
$$ language sql immutable;

--Testcase 127:
create operator class part_test_int4_ops
for type int4
using hash as
operator 1 =,
function 2 part_hashint4_noop(int4, int8);

--Testcase 128:
create or replace function part_hashtext_length(value text, seed int8)
RETURNS int8 AS $$
select length(coalesce(value, ''))::int8
$$ language sql immutable;

--Testcase 129:
create operator class part_test_text_ops
for type text
using hash as
operator 1 =,
function 2 part_hashtext_length(text, int8);

--Testcase 130:
create table hash_parted (
 id serial,
 a int
) partition by hash (a part_test_int4_ops);

--Testcase 131:
create foreign table hpart10 partition of hash_parted for values with (modulus 4, remainder 0) server griddb_svr;

--Testcase 132:
alter foreign table hpart10 alter column id options (rowkey 'true');

--Testcase 133:
create foreign table hpart11 partition of hash_parted for values with (modulus 4, remainder 1) server griddb_svr;

--Testcase 134:
alter foreign table hpart11 alter column id options (rowkey 'true');

--Testcase 135:
create foreign table hpart12 partition of hash_parted for values with (modulus 4, remainder 2) server griddb_svr;

--Testcase 136:
alter foreign table hpart12 alter column id options (rowkey 'true');

--Testcase 137:
create foreign table hpart13 partition of hash_parted for values with (modulus 4, remainder 3) server griddb_svr;

--Testcase 138:
alter foreign table hpart13 alter column id options (rowkey 'true');

--Testcase 139:
insert into hash_parted(a) values(generate_series(1,10));

-- direct insert of values divisible by 4 - ok;

--Testcase 140:
insert into hpart10(a) values(12),(16);
-- fail;
--insert into hpart10 values(11);
-- 11 % 4 -> 3 remainder i.e. valid data for hpart13 partition
--insert into hash_parted(a) values(11);

--Testcase 141:
insert into hpart13(a) values(11);

-- view data

--Testcase 142:
select tableoid::regclass as part, a, a%4 as "remainder = a % 4"
from hash_parted order by part;

-- test \d+ output on a table which has both partitioned and unpartitioned
-- partitions

--Testcase 143:
\d+ list_parted

-- cleanup

--Testcase 144:
drop table range_parted, list_parted;

--Testcase 145:
drop table hash_parted;

-- test that a default partition added as the first partition accepts any value
-- including null

--Testcase 146:
create table list_parted (id serial, a int) partition by list (a);

--Testcase 147:
create foreign table part_default partition of list_parted default server griddb_svr;

--Testcase 148:
alter foreign table part_default alter column id options (rowkey 'true');

--Testcase 149:
\d+ part_default

--Testcase 150:
insert into part_default(a) values (null);

--Testcase 151:
insert into part_default(a) values (1);

--Testcase 152:
insert into part_default(a) values (-1);

--Testcase 153:
select tableoid::regclass, a from list_parted;
-- cleanup

--Testcase 154:
drop table list_parted;

-- more tests for certain multi-level partitioning scenarios

--Testcase 155:
create table mlparted (a int, b int) partition by range (a, b);

--Testcase 156:
create table mlparted1 (b int not null, a int not null) partition by range ((b+0));

--Testcase 157:
create table mlparted11 (like mlparted1);

--Testcase 158:
alter table mlparted11 drop a;

--Testcase 159:
alter table mlparted11 add a int;

--Testcase 160:
alter table mlparted11 drop a;

--Testcase 161:
alter table mlparted11 add a int not null;
-- attnum for key attribute 'a' is different in mlparted, mlparted1, and mlparted11

--Testcase 162:
select attrelid::regclass, attname, attnum
from pg_attribute
where attname = 'a'
 and (attrelid = 'mlparted'::regclass
   or attrelid = 'mlparted1'::regclass
   or attrelid = 'mlparted11'::regclass)
order by attrelid::regclass::text;

--Testcase 163:
alter table mlparted1 attach partition mlparted11 for values from (2) to (5);

--Testcase 164:
alter table mlparted attach partition mlparted1 for values from (1, 2) to (1, 10);

-- check that "(1, 2)" is correctly routed to mlparted11.

--Testcase 165:
insert into mlparted(a, b) values (1, 2);

--Testcase 166:
select tableoid::regclass, * from mlparted;

-- check that proper message is shown after failure to route through mlparted1

--Testcase 167:
insert into mlparted (a, b) values (1, 5);

truncate mlparted;

--Testcase 168:
alter table mlparted add constraint check_b check (b = 3);

-- have a BR trigger modify the row such that the check_b is violated

--Testcase 169:
create function mlparted11_trig_fn()
returns trigger AS
$$
begin
  NEW.b := 4;
  return NEW;
end;
$$
language plpgsql;

--Testcase 170:
create trigger mlparted11_trig before insert ON mlparted11
  for each row execute procedure mlparted11_trig_fn();

-- check that the correct row is shown when constraint check_b fails after
-- "(1, 2)" is routed to mlparted11 (actually "(1, 4)" would be shown due
-- to the BR trigger mlparted11_trig_fn)

--Testcase 171:
insert into mlparted (a, b) values (1, 2);

--Testcase 172:
drop trigger mlparted11_trig on mlparted11;

--Testcase 173:
drop function mlparted11_trig_fn();

-- check that inserting into an internal partition successfully results in
-- checking its partition constraint before inserting into the leaf partition
-- selected by tuple-routing

--Testcase 174:
insert into mlparted1 (a, b) values (2, 3);

-- check routing error through a list partitioned table when the key is null

--Testcase 175:
create table lparted_nonullpart (a int, b char) partition by list (b);

--Testcase 176:
create foreign table lparted_nonullpart_a partition of lparted_nonullpart for values in ('a') server griddb_svr;

--Testcase 177:
insert into lparted_nonullpart values (1);

--Testcase 178:
drop table lparted_nonullpart;

-- check that RETURNING works correctly with tuple-routing

--Testcase 179:
alter table mlparted drop constraint check_b;

--Testcase 180:
create foreign table mlparted12 partition of mlparted1 for values from (5) to (10) server griddb_svr;

--Testcase 181:
create table mlparted2 (b int not null, a int not null);

--Testcase 182:
alter table mlparted attach partition mlparted2 for values from (1, 10) to (1, 20);

--Testcase 183:
create foreign table mlparted3 partition of mlparted for values from (1, 20) to (1, 30) server griddb_svr;

--Testcase 184:
create table mlparted4 (like mlparted);

--Testcase 185:
alter table mlparted4 drop a;

--Testcase 186:
alter table mlparted4 add a int not null;

--Testcase 187:
alter table mlparted attach partition mlparted4 for values from (1, 30) to (1, 40);

--Testcase 188:
with ins (a, b, c) as
  (insert into mlparted (b, a) select s.a, 1 from generate_series(2, 39) s(a) returning tableoid::regclass, *)
  select a, b, min(c), max(c) from ins group by a, b order by 1;

--Testcase 189:
alter table mlparted add c text;

--Testcase 190:
create table mlparted5 (a int not null, b int not null, c text) partition by list (c);

--Testcase 191:
create table mlparted5a (a int not null, c text, b int not null);

--Testcase 192:
alter table mlparted5 attach partition mlparted5a for values in ('a');

--Testcase 193:
alter table mlparted attach partition mlparted5 for values from (1, 40) to (1, 50);

--Testcase 194:
alter table mlparted add constraint check_b check (a = 1 and b < 45);

--Testcase 195:
insert into mlparted(a, b, c) values (1, 45, 'a');

--Testcase 196:
create function mlparted5abrtrig_func() returns trigger as $$ begin new.c = 'b'; return new; end; $$ language plpgsql;

--Testcase 197:
create trigger mlparted5abrtrig before insert on mlparted5a for each row execute procedure mlparted5abrtrig_func();

--Testcase 198:
insert into mlparted5 (a, b, c) values (1, 40, 'a');

--Testcase 199:
drop table mlparted5;

--Testcase 200:
alter table mlparted drop constraint check_b;

-- Check multi-level default partition

--Testcase 201:
create table mlparted_def partition of mlparted default partition by range(a);

--Testcase 202:
create foreign table mlparted_def1 partition of mlparted_def for values from (40) to (50) server griddb_svr;

--Testcase 203:
create foreign table mlparted_def2 partition of mlparted_def for values from (50) to (60) server griddb_svr;

--Testcase 204:
insert into mlparted(a, b) values (40, 100);

--Testcase 205:
insert into mlparted_def1(a, b) values (42, 100);

--Testcase 206:
insert into mlparted_def2(a, b) values (54, 50);
-- fail

--Testcase 207:
insert into mlparted(a,b) values (70, 100);
--skip because does not support partition bound constraint
--insert into mlparted_def1 values (52, 50);
--insert into mlparted_def2 values (34, 50);
-- ok

--Testcase 208:
create foreign table mlparted_defd partition of mlparted_def default server griddb_svr;

--Testcase 209:
insert into mlparted(a, b) values (70, 100);

--Testcase 210:
select tableoid::regclass, * from mlparted_def;

-- Check multi-level tuple routing with attributes dropped from the
-- top-most parent.  First remove the last attribute.

--Testcase 211:
alter table mlparted add d int, add e int;

--Testcase 212:
alter table mlparted drop e;

--Testcase 213:
create table mlparted5 partition of mlparted
  for values from (1, 40) to (1, 50) partition by range (c);

--Testcase 214:
create table mlparted5_ab partition of mlparted5
  for values from ('a') to ('c') partition by list (c);
-- This partitioned table should remain with no partitions.

--Testcase 215:
create table mlparted5_cd partition of mlparted5
  for values from ('c') to ('e') partition by list (c);

--Testcase 216:
create foreign table mlparted5_a partition of mlparted5_ab for values in ('a') server griddb_svr;

--Testcase 217:
create table mlparted5_b (a int, b int, c text, d int);

--Testcase 218:
alter table mlparted5_ab attach partition mlparted5_b for values in ('b');
-- cannot truncate foreign table, drop foreign table.

--Testcase 219:
drop foreign table mlparted_defd;

--Testcase 220:
drop foreign table mlparted3;

--Testcase 221:
drop foreign table mlparted12;

--Testcase 222:
drop foreign table mlparted_def1;

--Testcase 223:
drop foreign table mlparted_def2;

--Testcase 224:
drop foreign table mlparted5_a;
truncate mlparted;

--Testcase 225:
create foreign table mlparted5_a partition of mlparted5_ab for values in ('a') server griddb_svr;

--Testcase 226:
insert into mlparted(a, b, c, d) values (1, 2, 'a', 1);

--Testcase 227:
insert into mlparted(a, b, c, d) values (1, 40, 'a', 1);  -- goes to mlparted5_a

--Testcase 228:
insert into mlparted(a, b, c, d) values (1, 45, 'b', 1);  -- goes to mlparted5_b

--Testcase 229:
insert into mlparted(a, b, c, d) values (1, 45, 'c', 1);  -- goes to mlparted5_cd, fails

--Testcase 230:
insert into mlparted(a, b, c, d) values (1, 45, 'f', 1);  -- goes to mlparted5, fails

--Testcase 231:
select tableoid::regclass, * from mlparted order by a, b, c, d;

--Testcase 232:
alter table mlparted drop d;

--Testcase 233:
drop foreign table mlparted5_a;
truncate mlparted;
-- Remove the before last attribute.

--Testcase 234:
alter table mlparted add e int, add d int;

--Testcase 235:
alter table mlparted drop e;

--Testcase 236:
create foreign table mlparted5_a partition of mlparted5_ab for values in ('a') server griddb_svr;

--Testcase 237:
insert into mlparted(a, b, c, d) values (1, 2, 'a', 1);

--Testcase 238:
insert into mlparted(a, b, c, d) values (1, 40, 'a', 1);  -- goes to mlparted5_a

--Testcase 239:
insert into mlparted(a, b, c, d) values (1, 45, 'b', 1);  -- goes to mlparted5_b

--Testcase 240:
insert into mlparted(a, b, c, d) values (1, 45, 'c', 1);  -- goes to mlparted5_cd, fails

--Testcase 241:
insert into mlparted(a, b, c, d) values (1, 45, 'f', 1);  -- goes to mlparted5, fails

--Testcase 242:
select tableoid::regclass, * from mlparted order by a, b, c, d;

--Testcase 243:
alter table mlparted drop d;

--Testcase 244:
drop table mlparted5;

-- check that message shown after failure to find a partition shows the
-- appropriate key description (or none) in various situations

--Testcase 245:
create table key_desc (a int, b int) partition by list ((a+0));

--Testcase 246:
create table key_desc_1 partition of key_desc for values in (1) partition by range (b);

--Testcase 247:
create user regress_insert_other_user;
grant select (a) on key_desc_1 to regress_insert_other_user;
grant insert on key_desc to regress_insert_other_user;

--Testcase 248:
set role regress_insert_other_user;
-- no key description is shown

--Testcase 249:
insert into key_desc(a, b) values (1, 1);

--Testcase 250:
reset role;
grant select (b) on key_desc_1 to regress_insert_other_user;

--Testcase 251:
set role regress_insert_other_user;
-- key description (b)=(1) is now shown

--Testcase 252:
insert into key_desc(a, b) values (1, 1);

-- key description is not shown if key contains expression

--Testcase 253:
insert into key_desc(a, b) values (2, 1);

--Testcase 254:
reset role;
revoke all on key_desc from regress_insert_other_user;
revoke all on key_desc_1 from regress_insert_other_user;

--Testcase 255:
drop role regress_insert_other_user;

--Testcase 256:
drop table key_desc, key_desc_1;

-- test minvalue/maxvalue restrictions

--Testcase 257:
create table mcrparted (id serial, a int, b int, c int) partition by range (a, abs(b), c);

--Testcase 258:
create foreign table mcrparted0 partition of mcrparted for values from (minvalue, 0, 0) to (1, maxvalue, maxvalue) server griddb_svr;

--Testcase 259:
create foreign table mcrparted2 partition of mcrparted for values from (10, 6, minvalue) to (10, maxvalue, minvalue) server griddb_svr;

--Testcase 260:
create foreign table mcrparted4 partition of mcrparted for values from (21, minvalue, 0) to (30, 20, minvalue) server griddb_svr;

-- check multi-column range partitioning expression enforces the same
-- constraint as what tuple-routing would determine it to be

--Testcase 261:
create foreign table mcrparted0 partition of mcrparted for values from (minvalue, minvalue, minvalue) to (1, maxvalue, maxvalue) server griddb_svr;

--Testcase 262:
create foreign table mcrparted1 partition of mcrparted for values from (2, 1, minvalue) to (10, 5, 10) server griddb_svr;

--Testcase 263:
create foreign table mcrparted2 partition of mcrparted for values from (10, 6, minvalue) to (10, maxvalue, maxvalue) server griddb_svr;

--Testcase 264:
create foreign table mcrparted3 partition of mcrparted for values from (11, 1, 1) to (20, 10, 10) server griddb_svr;

--Testcase 265:
create foreign table mcrparted4 partition of mcrparted for values from (21, minvalue, minvalue) to (30, 20, maxvalue) server griddb_svr;

--Testcase 266:
create foreign table mcrparted5 partition of mcrparted for values from (30, 21, 20) to (maxvalue, maxvalue, maxvalue) server griddb_svr;

--Testcase 267:
alter foreign table mcrparted0 alter column id options (rowkey 'true');

--Testcase 268:
alter foreign table mcrparted1 alter column id options (rowkey 'true');

--Testcase 269:
alter foreign table mcrparted2 alter column id options (rowkey 'true');

--Testcase 270:
alter foreign table mcrparted3 alter column id options (rowkey 'true');

--Testcase 271:
alter foreign table mcrparted4 alter column id options (rowkey 'true');

--Testcase 272:
alter foreign table mcrparted5 alter column id options (rowkey 'true');

-- null not allowed in range partition

--Testcase 273:
insert into mcrparted (a,b,c) values (null, null, null);

-- routed to mcrparted0

--Testcase 274:
insert into mcrparted (a,b,c) values (0, 1, 1);

--Testcase 275:
insert into mcrparted0 (a,b,c) values (0, 1, 1);

-- routed to mcparted1

--Testcase 276:
insert into mcrparted (a,b,c) values (9, 1000, 1);

--Testcase 277:
insert into mcrparted1(a,b,c) values (9, 1000, 1);

--Testcase 278:
insert into mcrparted (a,b,c) values (10, 5, -1);

--Testcase 279:
insert into mcrparted1(a,b,c) values (10, 5, -1);

--Testcase 280:
insert into mcrparted (a,b,c) values (2, 1, 0);

--Testcase 281:
insert into mcrparted1(a,b,c) values (2, 1, 0);

-- routed to mcparted2

--Testcase 282:
insert into mcrparted (a,b,c) values (10, 6, 1000);

--Testcase 283:
insert into mcrparted2(a,b,c) values (10, 6, 1000);

--Testcase 284:
insert into mcrparted (a,b,c) values (10, 1000, 1000);

--Testcase 285:
insert into mcrparted2(a,b,c) values (10, 1000, 1000);

-- no partition exists, nor does mcrparted3 accept it

--Testcase 286:
insert into mcrparted (a,b,c) values (11, 1, -1);
-- skip
--insert into mcrparted3(a,b,c) values (11, 1, -1);

-- routed to mcrparted5

--Testcase 287:
insert into mcrparted (a,b,c) values (30, 21, 20);

--Testcase 288:
insert into mcrparted5(a,b,c) values(30, 21, 20);
-- skip
--insert into mcrparted4(a,b,c) values (30, 21, 20);	-- error

-- check rows

--Testcase 289:
select tableoid::regclass::text, a, b, c from mcrparted order by 1;

-- cleanup

--Testcase 290:
drop table mcrparted;

-- check that a BR constraint can't make partition contain violating rows

--Testcase 291:
create table brtrigpartcon (a int, b text) partition by list (a);

--Testcase 292:
create foreign table brtrigpartcon1 partition of brtrigpartcon for values in (1) server griddb_svr;

--Testcase 293:
create or replace function brtrigpartcon1trigf() returns trigger as $$begin new.a := 2; return new; end$$ language plpgsql;

--Testcase 294:
create trigger brtrigpartcon1trig before insert on brtrigpartcon1 for each row execute procedure brtrigpartcon1trigf();
-- ignore, no partition bound constraint check

--Testcase 295:
insert into brtrigpartcon values (1, 'hi there');

--Testcase 296:
insert into brtrigpartcon1 values (1, 'hi there');

-- check that the message shows the appropriate column description in a
-- situation where the partitioned table is not the primary ModifyTable node

--Testcase 297:
create foreign table inserttest3 (f1 text default 'foo', f2 text default 'bar', f3 int) server griddb_svr;

--Testcase 298:
create role regress_coldesc_role;
grant insert on inserttest3 to regress_coldesc_role;
grant insert on brtrigpartcon to regress_coldesc_role;
revoke select on brtrigpartcon from regress_coldesc_role;

--Testcase 299:
set role regress_coldesc_role;
--ignore, no partition bound constraint check

--Testcase 300:
with result as (insert into brtrigpartcon values (1, 'hi there') returning 1)
  insert into inserttest3 (f3) select * from result;

--Testcase 301:
reset role;

-- cleanup
revoke all on inserttest3 from regress_coldesc_role;
revoke all on brtrigpartcon from regress_coldesc_role;

--Testcase 302:
drop role regress_coldesc_role;

--Testcase 303:
drop foreign table inserttest3;

--Testcase 304:
drop table brtrigpartcon;

--Testcase 305:
drop function brtrigpartcon1trigf();

-- check that "do nothing" BR triggers work with tuple-routing

--Testcase 306:
create table donothingbrtrig_test (a int, b text) partition by list (a);

--Testcase 307:
create foreign table donothingbrtrig_test1 (a int, b text) server griddb_svr;

--Testcase 308:
create foreign table donothingbrtrig_test2 (a int, b text, c text) server griddb_svr;

--Testcase 309:
alter table donothingbrtrig_test2 drop column c;

--Testcase 310:
create or replace function donothingbrtrig_func() returns trigger as $$begin raise notice 'b: %', new.b; return NULL; end$$ language plpgsql;

--Testcase 311:
create trigger donothingbrtrig1 before insert on donothingbrtrig_test1 for each row execute procedure donothingbrtrig_func();

--Testcase 312:
create trigger donothingbrtrig2 before insert on donothingbrtrig_test2 for each row execute procedure donothingbrtrig_func();

--Testcase 313:
alter table donothingbrtrig_test attach partition donothingbrtrig_test1 for values in (1);

--Testcase 314:
alter table donothingbrtrig_test attach partition donothingbrtrig_test2 for values in (2);

--Testcase 315:
insert into donothingbrtrig_test values (1, 'foo'), (2, 'bar');

copy donothingbrtrig_test from stdin;
1	baz
2	qux
\.

--Testcase 316:
select tableoid::regclass, * from donothingbrtrig_test;

-- cleanup

--Testcase 317:
drop table donothingbrtrig_test;

--Testcase 318:
drop function donothingbrtrig_func();

-- check multi-column range partitioning with minvalue/maxvalue constraints

--Testcase 319:
create table mcrparted (a text, b int) partition by range(a, b);

--Testcase 320:
create foreign table mcrparted1_lt_b partition of mcrparted for values from (minvalue, minvalue) to ('b', minvalue) server griddb_svr;

--Testcase 321:
create foreign table mcrparted2_b partition of mcrparted for values from ('b', minvalue) to ('c', minvalue) server griddb_svr;

--Testcase 322:
create foreign table mcrparted3_c_to_common partition of mcrparted for values from ('c', minvalue) to ('common', minvalue) server griddb_svr;

--Testcase 323:
create foreign table mcrparted4_common_lt_0 partition of mcrparted for values from ('common', minvalue) to ('common', 0) server griddb_svr;

--Testcase 324:
create foreign table mcrparted5_common_0_to_10 partition of mcrparted for values from ('common', 0) to ('common', 10) server griddb_svr;

--Testcase 325:
create foreign table mcrparted6_common_ge_10 partition of mcrparted for values from ('common', 10) to ('common', maxvalue) server griddb_svr;

--Testcase 326:
create foreign table mcrparted7_gt_common_lt_d partition of mcrparted for values from ('common', maxvalue) to ('d', minvalue) server griddb_svr;

--Testcase 327:
create foreign table mcrparted8_ge_d partition of mcrparted for values from ('d', minvalue) to (maxvalue, maxvalue) server griddb_svr;

--Testcase 328:
\d+ mcrparted

--Testcase 329:
\d+ mcrparted1_lt_b

--Testcase 330:
\d+ mcrparted2_b

--Testcase 331:
\d+ mcrparted3_c_to_common

--Testcase 332:
\d+ mcrparted4_common_lt_0

--Testcase 333:
\d+ mcrparted5_common_0_to_10

--Testcase 334:
\d+ mcrparted6_common_ge_10

--Testcase 335:
\d+ mcrparted7_gt_common_lt_d

--Testcase 336:
\d+ mcrparted8_ge_d

--Testcase 337:
insert into mcrparted values ('aaa', 0), ('b', 0), ('bz', 10), ('c', -10),
    ('comm', -10), ('common', -10), ('common', 0), ('common', 10),
    ('commons', 0), ('d', -10), ('e', 0);

--Testcase 338:
select tableoid::regclass, * from mcrparted order by a, b;

--Testcase 339:
drop table mcrparted;

-- check that wholerow vars in the RETURNING list work with partitioned tables

--Testcase 340:
create table returningwrtest (a int) partition by list (a);

--Testcase 341:
create foreign table returningwrtest1 partition of returningwrtest for values in (1) server griddb_svr;

--Testcase 342:
insert into returningwrtest values (1) returning returningwrtest;

-- check also that the wholerow vars in RETURNING list are converted as needed

--Testcase 343:
alter table returningwrtest add b text;

--Testcase 344:
create foreign table returningwrtest2 (a int, b text, c int) server griddb_svr;

--Testcase 345:
alter foreign table returningwrtest2 drop c;

--Testcase 346:
alter table returningwrtest attach partition returningwrtest2 for values in (2);

--Testcase 347:
insert into returningwrtest(a, b) values (2, 'foo') returning returningwrtest;

--Testcase 348:
drop table returningwrtest;

-- drop all foreign tables
DO $d$
declare
  l_rec record;
begin
  for l_rec in (select foreign_table_schema, foreign_table_name 
                from information_schema.foreign_tables) loop
     execute format('drop foreign table %I.%I cascade;', l_rec.foreign_table_schema, l_rec.foreign_table_name);
  end loop;
end;
$d$;

--Testcase 349:
DROP USER MAPPING FOR public SERVER griddb_svr;

--Testcase 350:
DROP SERVER griddb_svr;

--Testcase 351:
DROP EXTENSION griddb_fdw CASCADE;
