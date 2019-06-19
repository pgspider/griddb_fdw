--
-- insert with DEFAULT in the target_list
--
CREATE EXTENSION griddb_fdw;
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS(host '239.0.0.1', port '31999', clustername 'ktymCluster');
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS(username 'admin', password 'testadmin');
CREATE FOREIGN TABLE inserttest01 (id serial, col1 int4, col2 int4 NOT NULL, col3 text default 'testing') SERVER griddb_svr;
insert into inserttest01 (col1, col2, col3) values (DEFAULT, DEFAULT, DEFAULT);
insert into inserttest01 (col2, col3) values (3, DEFAULT);
insert into inserttest01 (col1, col2, col3) values (DEFAULT, 5, DEFAULT);
insert into inserttest01 (col1, col2, col3) values (DEFAULT, 5, 'test');
insert into inserttest01 (col1, col2) values (DEFAULT, 7);

select * from inserttest01;

--
-- insert with similar expression / target_list values (all fail)
--
insert into inserttest01 (col1, col2, col3) values (DEFAULT, DEFAULT);
insert into inserttest01 (col1, col2, col3) values (1, 2);
insert into inserttest01 (col1) values (1, 2);
insert into inserttest01 (col1) values (DEFAULT, DEFAULT);

select * from inserttest01;

--
-- VALUES test
--
insert into inserttest01 (col1, col2, col3) values(10, 20, '40'), (-1, 2, DEFAULT),
    ((select 2), (select i from (values(3)) as foo (i)), 'values are fun!');

select * from inserttest01;

--
-- TOASTed value test
--
insert into inserttest01 (col1, col2, col3) values(30, 50, repeat('x', 10000));

select col1, col2, char_length(col3) from inserttest01;

--
-- check indirection (field/array assignment), cf bug #14265
--
-- these tests are aware that transformInsertStmt has 3 separate code paths
--

create foreign table inserttest (f1 serial options (rowkey 'true'), f2 int[], if1 int, if2 text[]) server griddb_svr;

insert into inserttest (f2[1], f2[2]) values (1,2);
insert into inserttest (f2[1], f2[2]) values (3,4), (5,6);
insert into inserttest (f2[1], f2[2]) select 7,8;
insert into inserttest (f2[1], f2[2]) values (1,default);  -- not supported

insert into inserttest (if1, if2) values (1,array['foo']);
insert into inserttest (if1, if2) values (1,'{foo}'), (2,'{bar}');
insert into inserttest (if1, if2) select 3, '{baz,quux}';
insert into inserttest (if1, if2) values (1,default);

insert into inserttest (if2[1], if2[2]) values ('foo', 'bar');
insert into inserttest (if2[1], if2[2]) values ('foo', 'bar'), ('baz', 'quux');
insert into inserttest (if2[1], if2[2]) select 'bear', 'beer';

select f1, f2, (if1, if2) as f3 from inserttest;

-- also check reverse-listing
create table inserttest2 (f1 bigint, f2 text);
create rule irule1 as on insert to inserttest2 do also
  insert into inserttest (if2[1], if2[2])
  values (new.f1,new.f2);
create rule irule2 as on insert to inserttest2 do also
  insert into inserttest (if1, if2[2])
  values (1,'fool'),(new.f1,new.f2);
create rule irule3 as on insert to inserttest2 do also
  insert into inserttest (if1, if2[2])
  select new.f1, new.f2;
\d+ inserttest2

create table range_parted (
  id serial,
  a text,
  b int
) partition by range (a, (b+0));

-- no partitions, so fail
insert into range_parted(a,b) values ('a', 11);

create foreign table part1 partition of range_parted for values from ('a', 1) to ('a', 10) server griddb_svr;
alter foreign table part1 alter column id options (rowkey 'true');
create foreign table part2 partition of range_parted for values from ('a', 10) to ('a', 20) server griddb_svr;
alter foreign table part2 alter column id options (rowkey 'true');
create foreign table part3 partition of range_parted for values from ('b', 1) to ('b', 10) server griddb_svr;
alter foreign table part3 alter column id options (rowkey 'true');
create foreign table part4 partition of range_parted for values from ('b', 10) to ('b', 20) server griddb_svr;
alter foreign table part4 alter column id options (rowkey 'true');

-- ok
insert into range_parted(a,b) values ('a', 1);
insert into range_parted(a,b) values ('b', 10);

-- fail (no partition found)
insert into range_parted(a,b) values ('c', 10);

create table list_parted (
 id serial,
 a text,
 b int
) partition by list (lower(a));
create foreign table part_aa_bb partition of list_parted FOR VALUES IN ('aa', 'bb') server griddb_svr;
alter foreign table part_aa_bb alter column id options (rowkey 'true');
create foreign table part_cc_dd partition of list_parted FOR VALUES IN ('cc', 'dd') server griddb_svr;
alter foreign table part_cc_dd alter column id options (rowkey 'true');
create foreign table part_null partition of list_parted FOR VALUES IN (null) server griddb_svr;
alter foreign table part_null alter column id options (rowkey 'true');

-- fail
insert into list_parted(a,b) values ('AAa', 1);
-- ok
insert into list_parted(a,b) values ('cC', 1);
insert into list_parted(a,b) values (null, 0);

-- check in case of multi-level partitioned table
create table part_ee_ff partition of list_parted for values in ('ee', 'ff') partition by range (b);
create foreign table part_ee_ff1 partition of part_ee_ff for values from (1) to (10) server griddb_svr;
alter foreign table part_ee_ff1 alter column id options (rowkey 'true');
create foreign table part_ee_ff2 partition of part_ee_ff for values from (10) to (20) server griddb_svr;
alter foreign table part_ee_ff2 alter column id options (rowkey 'true');

-- test default partition
create table part_default partition of list_parted default;
-- Negative test: a row, which would fit in other partition, does not fit
-- default partition, even when inserted directly
insert into part_default(a,b) values ('aa', 2);
insert into part_default(a,b) values (null, 2);
-- ok
insert into part_default(a,b) values ('Zz', 2);
-- test if default partition works as expected for multi-level partitioned
-- table as well as when default partition itself is further partitioned
drop table part_default;
create table part_xx_yy partition of list_parted for values in ('xx', 'yy') partition by list (a);
create foreign table part_xx_yy_p1 partition of part_xx_yy for values in ('xx') server griddb_svr;
alter foreign table part_xx_yy_p1 alter column id options (rowkey 'true');
create foreign table part_xx_yy_defpart partition of part_xx_yy default server griddb_svr;
alter foreign table part_xx_yy_defpart alter column id options (rowkey 'true');
create table part_default partition of list_parted default partition by range(b);
create foreign table part_default_p1 partition of part_default for values from (20) to (30) server griddb_svr;
alter foreign table part_default_p1 alter column id options (rowkey 'true');
create foreign table part_default_p2 partition of part_default for values from (30) to (40) server griddb_svr;
alter foreign table part_default_p2 alter column id options (rowkey 'true');

-- fail (even the parent's, ie, part_ee_ff's partition constraint applies)
insert into part_default(a,b) values ('gg', 43);
-- ok
insert into list_parted(a,b) values ('ff', 1);
insert into list_parted(a,b) values ('ff', 11);
insert into list_parted(a,b) values ('cd', 25);
insert into list_parted(a,b) values ('de', 35);
insert into list_parted(a,b) values ('ab', 21);
insert into list_parted(a,b) values ('xx', 1);
insert into list_parted(a,b) values ('yy', 2);
select tableoid::regclass, * from list_parted;

-- Check tuple routing for partitioned tables

-- fail
insert into range_parted(a,b) values ('a', 0);
-- ok
insert into range_parted(a,b) values ('a', 1);
insert into range_parted(a,b) values ('a', 10);
-- fail
insert into range_parted(a,b) values ('a', 20);
-- ok
insert into range_parted(a,b) values ('b', 1);
insert into range_parted(a,b) values ('b', 10);
-- fail (partition key (b+0) is null)
insert into range_parted(a) values ('a');

-- Check default partition
create foreign table part_def partition of range_parted default server griddb_svr;
alter foreign table part_def alter column id options (rowkey 'true');
-- ok
insert into range_parted(a,b) values ('c', 10);
insert into range_parted(a,b) values (null, null);
insert into range_parted(a,b) values ('a', null);
insert into range_parted(a,b) values (null, 19);
insert into range_parted(a,b) values ('b', 20);

select tableoid::regclass, * from range_parted;
-- ok
insert into list_parted(a,b) values (null, 1);
insert into list_parted (a) values ('aA');
-- fail (partition of part_ee_ff not found in both cases)
insert into list_parted(a,b) values ('EE', 0);
insert into part_ee_ff(a,b) values ('EE', 0);
-- ok
insert into list_parted(a,b) values ('EE', 1);
insert into part_ee_ff(a,b) values ('EE', 10);
select tableoid::regclass, * from list_parted;

-- some more tests to exercise tuple-routing with multi-level partitioning
create table part_gg partition of list_parted for values in ('gg') partition by range (b);
create table part_gg1 partition of part_gg for values from (minvalue) to (1);
create table part_gg2 partition of part_gg for values from (1) to (10) partition by range (b);
create table part_gg2_1 partition of part_gg2 for values from (1) to (5);
create table part_gg2_2 partition of part_gg2 for values from (5) to (10);

create table part_ee_ff3 partition of part_ee_ff for values from (20) to (30) partition by range (b);
create table part_ee_ff3_1 partition of part_ee_ff3 for values from (20) to (25);
create table part_ee_ff3_2 partition of part_ee_ff3 for values from (25) to (30);

delete from list_parted;

insert into list_parted (a) values ('aa'), ('cc');
insert into list_parted (a,b) select 'Ff', s.a from generate_series(1, 29) s(a);
insert into list_parted (a,b) select 'gg', s.a from generate_series(1, 9) s(a);
insert into list_parted (b) values (1);
select tableoid::regclass::text, a, min(b) as min_b, max(b) as max_b from list_parted group by 1, 2 order by 1;

-- direct partition inserts should check hash partition bound constraint

-- Use hand-rolled hash functions and operator classes to get predictable
-- result on different matchines.  The hash function for int4 simply returns
-- the sum of the values passed to it and the one for text returns the length
-- of the non-empty string value passed to it or 0.

create or replace function part_hashint4_noop(value int4, seed int8)
returns int8 as $$
select value + seed;
$$ language sql immutable;

create operator class part_test_int4_ops
for type int4
using hash as
operator 1 =,
function 2 part_hashint4_noop(int4, int8);

create or replace function part_hashtext_length(value text, seed int8)
RETURNS int8 AS $$
select length(coalesce(value, ''))::int8
$$ language sql immutable;

create operator class part_test_text_ops
for type text
using hash as
operator 1 =,
function 2 part_hashtext_length(text, int8);

create table hash_parted (
 id serial,
 a int
) partition by hash (a part_test_int4_ops);
create foreign table hpart10 partition of hash_parted for values with (modulus 4, remainder 0) server griddb_svr;
alter foreign table hpart10 alter column id options (rowkey 'true');
create foreign table hpart11 partition of hash_parted for values with (modulus 4, remainder 1) server griddb_svr;
alter foreign table hpart11 alter column id options (rowkey 'true');
create foreign table hpart12 partition of hash_parted for values with (modulus 4, remainder 2) server griddb_svr;
alter foreign table hpart12 alter column id options (rowkey 'true');
create foreign table hpart13 partition of hash_parted for values with (modulus 4, remainder 3) server griddb_svr;
alter foreign table hpart13 alter column id options (rowkey 'true');

insert into hash_parted(a) values(generate_series(1,10));

-- ok;
insert into hash_parted(a) values(12),(16);
-- 11 % 4 -> 3 remainder i.e. valid data for hpart3 partition
insert into hash_parted(a) values(11);

-- view data
select tableoid::regclass as part, a, a%4 as "remainder = a % 4"
from hash_parted order by part;

-- test \d+ output on a table which has both partitioned and unpartitioned
-- partitions
\d+ list_parted

-- cleanup
drop table range_parted, list_parted;
drop table hash_parted;

-- test that a default partition added as the first partition accepts any value
-- including null
create table list_parted (id serial, a int) partition by list (a);
create foreign table part_default partition of list_parted default server griddb_svr;
\d+ part_default
insert into part_default(a) values (null);
insert into part_default(a) values (1);
insert into part_default(a) values (-1);
select tableoid::regclass, a from list_parted;
-- cleanup
drop table list_parted;

-- test minvalue/maxvalue restrictions
create table mcrparted (id serial, a int, b int, c int) partition by range (a, abs(b), c);
create foreign table mcrparted0 partition of mcrparted for values from (minvalue, 0, 0) to (1, maxvalue, maxvalue) server griddb_svr;
create foreign table mcrparted2 partition of mcrparted for values from (10, 6, minvalue) to (10, maxvalue, minvalue) server griddb_svr;
create foreign table mcrparted4 partition of mcrparted for values from (21, minvalue, 0) to (30, 20, minvalue) server griddb_svr;

-- check multi-column range partitioning expression enforces the same
-- constraint as what tuple-routing would determine it to be
create foreign table mcrparted0 partition of mcrparted for values from (minvalue, minvalue, minvalue) to (1, maxvalue, maxvalue) server griddb_svr;
create foreign table mcrparted1 partition of mcrparted for values from (2, 1, minvalue) to (10, 5, 10) server griddb_svr;
create foreign table mcrparted2 partition of mcrparted for values from (10, 6, minvalue) to (10, maxvalue, maxvalue) server griddb_svr;
create foreign table mcrparted3 partition of mcrparted for values from (11, 1, 1) to (20, 10, 10) server griddb_svr;
create foreign table mcrparted4 partition of mcrparted for values from (21, minvalue, minvalue) to (30, 20, maxvalue) server griddb_svr;
create foreign table mcrparted5 partition of mcrparted for values from (30, 21, 20) to (maxvalue, maxvalue, maxvalue) server griddb_svr;
alter foreign table mcrparted0 alter column id options (rowkey 'true');
alter foreign table mcrparted1 alter column id options (rowkey 'true');
alter foreign table mcrparted2 alter column id options (rowkey 'true');
alter foreign table mcrparted3 alter column id options (rowkey 'true');
alter foreign table mcrparted4 alter column id options (rowkey 'true');
alter foreign table mcrparted5 alter column id options (rowkey 'true');

-- null not allowed in range partition
insert into mcrparted (a,b,c) values (null, null, null);

-- routed to mcrparted0
insert into mcrparted (a,b,c) values (0, 1, 1);

-- routed to mcparted1
insert into mcrparted (a,b,c) values (9, 1000, 1);
insert into mcrparted (a,b,c) values (10, 5, -1);
insert into mcrparted (a,b,c) values (2, 1, 0);

-- routed to mcparted2
insert into mcrparted (a,b,c) values (10, 6, 1000);
insert into mcrparted (a,b,c) values (10, 1000, 1000);

-- no partition exists, nor does mcrparted3 accept it
insert into mcrparted (a,b,c) values (11, 1, -1);

-- routed to mcrparted5
insert into mcrparted (a,b,c) values (30, 21, 20);

-- check rows
select tableoid::regclass::text, * from mcrparted order by 1;

-- cleanup
drop table mcrparted;

-- check that "do nothing" BR triggers work with tuple-routing (this checks
-- that estate->es_result_relation_info is appropriately set/reset for each
-- routed tuple)
create table donothingbrtrig_test (a int, b text) partition by list (a);
create foreign table donothingbrtrig_test1 (a int, b text) server griddb_svr;
create foreign table donothingbrtrig_test2 (a int, b text, c text) server griddb_svr;
alter table donothingbrtrig_test2 drop column c;
create or replace function donothingbrtrig_func() returns trigger as $$begin raise notice 'b: %', new.b; return NULL; end$$ language plpgsql;
create trigger donothingbrtrig1 before insert on donothingbrtrig_test1 for each row execute procedure donothingbrtrig_func();
create trigger donothingbrtrig2 before insert on donothingbrtrig_test2 for each row execute procedure donothingbrtrig_func();
alter table donothingbrtrig_test attach partition donothingbrtrig_test1 for values in (1);
alter table donothingbrtrig_test attach partition donothingbrtrig_test2 for values in (2);
insert into donothingbrtrig_test values (1, 'foo'), (2, 'bar');

copy donothingbrtrig_test from stdin;
1	baz
2	qux
\.
select tableoid::regclass, * from donothingbrtrig_test;

-- cleanup
drop table donothingbrtrig_test;
drop function donothingbrtrig_func();

-- check multi-column range partitioning with minvalue/maxvalue constraints
create table mcrparted (a text, b int) partition by range(a, b);
create foreign table mcrparted1_lt_b partition of mcrparted for values from (minvalue, minvalue) to ('b', minvalue) server griddb_svr;
create foreign table mcrparted2_b partition of mcrparted for values from ('b', minvalue) to ('c', minvalue) server griddb_svr;
create foreign table mcrparted3_c_to_common partition of mcrparted for values from ('c', minvalue) to ('common', minvalue) server griddb_svr;
create foreign table mcrparted4_common_lt_0 partition of mcrparted for values from ('common', minvalue) to ('common', 0) server griddb_svr;
create foreign table mcrparted5_common_0_to_10 partition of mcrparted for values from ('common', 0) to ('common', 10) server griddb_svr;
create foreign table mcrparted6_common_ge_10 partition of mcrparted for values from ('common', 10) to ('common', maxvalue) server griddb_svr;
create foreign table mcrparted7_gt_common_lt_d partition of mcrparted for values from ('common', maxvalue) to ('d', minvalue) server griddb_svr;
create foreign table mcrparted8_ge_d partition of mcrparted for values from ('d', minvalue) to (maxvalue, maxvalue) server griddb_svr;

\d+ mcrparted
\d+ mcrparted1_lt_b
\d+ mcrparted2_b
\d+ mcrparted3_c_to_common
\d+ mcrparted4_common_lt_0
\d+ mcrparted5_common_0_to_10
\d+ mcrparted6_common_ge_10
\d+ mcrparted7_gt_common_lt_d
\d+ mcrparted8_ge_d

insert into mcrparted values ('aaa', 0), ('b', 0), ('bz', 10), ('c', -10),
    ('comm', -10), ('common', -10), ('common', 0), ('common', 10),
    ('commons', 0), ('d', -10), ('e', 0);
select tableoid::regclass, * from mcrparted order by a, b;
drop table mcrparted;

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
DROP USER MAPPING FOR public SERVER griddb_svr;
DROP SERVER griddb_svr;
DROP EXTENSION griddb_fdw CASCADE;
