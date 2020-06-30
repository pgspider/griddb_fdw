--
-- UPDATE syntax tests
--
CREATE EXTENSION griddb_fdw;
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw OPTIONS(host '239.0.0.1', port '31999', clustername 'griddbfdwTestCluster');
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS(username 'admin', password 'testadmin');

CREATE FOREIGN TABLE update_test (
    id  serial options (rowkey 'true'),
    a   INT DEFAULT 10,
    b   INT,
    c   TEXT
) SERVER griddb_svr;

CREATE FOREIGN TABLE upsert_test (
    a   INT OPTIONS (rowkey 'true'),
    b   TEXT
) SERVER griddb_svr;

--Testcase 1:
DELETE FROM update_test;
--Testcase 2:
DELETE FROM upsert_test;
--Testcase 3:
INSERT INTO update_test(a, b, c) VALUES (5, 10, 'foo');
--Testcase 4:
INSERT INTO update_test(b, a) VALUES (15, 10);

--Testcase 5:
SELECT * FROM update_test;

--Testcase 6:
UPDATE update_test SET a = DEFAULT, b = DEFAULT;

--Testcase 7:
SELECT * FROM update_test;

-- aliases for the UPDATE target table
--Testcase 8:
UPDATE update_test AS t SET b = 10 WHERE t.a = 10;

--Testcase 9:
SELECT * FROM update_test;

--Testcase 10:
UPDATE update_test t SET b = t.b + 10 WHERE t.a = 10;

--Testcase 11:
SELECT * FROM update_test;

--
-- Test VALUES in FROM
--

--Testcase 12:
UPDATE update_test SET a=v.i FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

--Testcase 13:
SELECT * FROM update_test;

-- fail, wrong data type:
--Testcase 14:
UPDATE update_test SET a = v.* FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

--
-- Test multiple-set-clause syntax
--

--Testcase 15:
INSERT INTO update_test(a,b,c) SELECT a,b+1,c FROM update_test;
--Testcase 16:
SELECT * FROM update_test;

--Testcase 17:
UPDATE update_test SET (c,b,a) = ('bugle', b+11, DEFAULT) WHERE c = 'foo';
--Testcase 18:
SELECT * FROM update_test;
--Testcase 19:
UPDATE update_test SET (c,b) = ('car', a+b), a = a + 1 WHERE a = 10;
--Testcase 20:
SELECT * FROM update_test;
-- fail, multi assignment to same column:
--Testcase 21:
UPDATE update_test SET (c,b) = ('car', a+b), b = a + 1 WHERE a = 10;

-- uncorrelated sub-select:
--Testcase 22:
UPDATE update_test
  SET (b,a) = (select a,b from update_test where b = 41 and c = 'car')
  WHERE a = 100 AND b = 20;
--Testcase 23:
SELECT * FROM update_test;
-- correlated sub-select:
--Testcase 24:
UPDATE update_test o
  SET (b,a) = (select a+1,b from update_test i
               where i.a=o.a and i.b=o.b and i.c is not distinct from o.c);
--Testcase 25:
SELECT * FROM update_test;
-- fail, multiple rows supplied:
--Testcase 26:
UPDATE update_test SET (b,a) = (select a+1,b from update_test);
-- set to null if no rows supplied:
--Testcase 27:
UPDATE update_test SET (b,a) = (select a+1,b from update_test where a = 1000)
  WHERE a = 11;
--Testcase 28:
SELECT * FROM update_test;
-- *-expansion should work in this context:
--Testcase 29:
UPDATE update_test SET (a,b) = ROW(v.*) FROM (VALUES(21, 100)) AS v(i, j)
  WHERE update_test.a = v.i;
-- you might expect this to work, but syntactically it's not a RowExpr:
--Testcase 30:
UPDATE update_test SET (a,b) = (v.*) FROM (VALUES(21, 101)) AS v(i, j)
  WHERE update_test.a = v.i;

-- if an alias for the target table is specified, don't allow references
-- to the original table name
--Testcase 31:
UPDATE update_test AS t SET b = update_test.b + 10 WHERE t.a = 10;

-- Make sure that we can update to a TOASTed value.
--Testcase 32:
UPDATE update_test SET c = repeat('x', 10000) WHERE c = 'car';
--Testcase 33:
SELECT a, b, char_length(c) FROM update_test;

-- Check multi-assignment with a Result node to handle a one-time filter.
--Testcase 34:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE update_test t
  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
  WHERE CURRENT_USER = SESSION_USER;
--Testcase 35:
UPDATE update_test t
  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
  WHERE CURRENT_USER = SESSION_USER;
--Testcase 36:
SELECT a, b, char_length(c) FROM update_test;

-- ON CONFLICT is not supported.
-- Test ON CONFLICT DO UPDATE
--Testcase 37:
INSERT INTO upsert_test VALUES(1, 'Boo');
-- uncorrelated  sub-select:
--Testcase 38:
WITH aaa AS (SELECT 1 AS a, 'Foo' AS b) INSERT INTO upsert_test
  VALUES (1, 'Bar') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b, a FROM aaa) RETURNING *;
-- correlated sub-select:
--Testcase 39:
INSERT INTO upsert_test VALUES (1, 'Baz') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Correlated', a from upsert_test i WHERE i.a = upsert_test.a)
  RETURNING *;
-- correlated sub-select (EXCLUDED.* alias):
--Testcase 40:
INSERT INTO upsert_test VALUES (1, 'Bat') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING *;

-- ON CONFLICT using system attributes in RETURNING, testing both the
-- inserting and updating paths. See bug report at:
-- https://www.postgresql.org/message-id/73436355-6432-49B1-92ED-1FE4F7E7E100%40finefun.com.au
CREATE FUNCTION xid_current() RETURNS xid LANGUAGE SQL AS $$SELECT (txid_current() % ((1::int8<<32)))::text::xid;$$;
--Testcase 41:
INSERT INTO upsert_test VALUES (2, 'Beeble') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING tableoid::regclass, xmin = xid_current() AS xmin_correct, xmax = 0 AS xmax_correct;
-- currently xmax is set after a conflict - that's probably not good,
-- but it seems worthwhile to have to be explicit if that changes.
--Testcase 42:
INSERT INTO upsert_test VALUES (2, 'Brox') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING tableoid::regclass, xmin = xid_current() AS xmin_correct, xmax = xid_current() AS xmax_correct;

DROP FUNCTION xid_current();
DROP FOREIGN TABLE update_test;
DROP FOREIGN TABLE upsert_test;


---------------------------
-- UPDATE with row movement
---------------------------

-- When a partitioned table receives an UPDATE to the partitioned key and the
-- new values no longer meet the partition's bound, the row must be moved to
-- the correct partition for the new partition key (if one exists). We must
-- also ensure that updatable views on partitioned tables properly enforce any
-- WITH CHECK OPTION that is defined. The situation with triggers in this case
-- also requires thorough testing as partition key updates causing row
-- movement convert UPDATEs into DELETE+INSERT.

-- TODO: Does not support bigint, numeric, varchar
CREATE TABLE range_parted (
	id serial,
	a text,
	b int,
	c float8,
	d int,
	e text
) PARTITION BY RANGE (a, b);

-- Create partitions intentionally in descending bound order, so as to test
-- that update-row-movement works with the leaf partitions not in bound order.
CREATE TABLE part_b_20_b_30 (id serial NOT NULL, a text, b int, c float8, d int, e text);
ALTER TABLE range_parted ATTACH PARTITION part_b_20_b_30 FOR VALUES FROM ('b', 20) TO ('b', 30);
CREATE TABLE part_b_10_b_20 (id serial NOT NULL, a text, b int, c float8, d int, e text) PARTITION BY RANGE (c);
CREATE FOREIGN TABLE part_b_1_b_10 PARTITION OF range_parted FOR VALUES FROM ('b', 1) TO ('b', 10) server griddb_svr;
alter foreign table part_b_1_b_10 alter column id options (rowkey 'true');

ALTER TABLE range_parted ATTACH PARTITION part_b_10_b_20 FOR VALUES FROM ('b', 10) TO ('b', 20);
CREATE FOREIGN TABLE part_a_10_a_20 PARTITION OF range_parted FOR VALUES FROM ('a', 10) TO ('a', 20) server griddb_svr;
CREATE FOREIGN TABLE part_a_1_a_10 PARTITION OF range_parted FOR VALUES FROM ('a', 1) TO ('a', 10) server griddb_svr;
alter foreign table part_a_10_a_20 alter column id options (rowkey 'true');
alter foreign table part_a_1_a_10 alter column id options (rowkey 'true');

-- Check that partition-key UPDATE works sanely on a partitioned table that
-- does not have any child partitions.
--Testcase 43:
UPDATE part_b_10_b_20 set b = b - 6;

-- Create some more partitions following the above pattern of descending bound
-- order, but let's make the situation a bit more complex by having the
-- attribute numbers of the columns vary from their parent partition.
CREATE TABLE part_c_100_200 (id serial NOT NULL, a text, b int, c float8, d int, e text) PARTITION BY range (abs(d));
--ALTER TABLE part_c_100_200 DROP COLUMN e, DROP COLUMN c, DROP COLUMN a;
--ALTER TABLE part_c_100_200 ADD COLUMN c numeric, ADD COLUMN e varchar, ADD COLUMN a text;
--ALTER TABLE part_c_100_200 DROP COLUMN b;
--ALTER TABLE part_c_100_200 ADD COLUMN b bigint;
CREATE TABLE part_d_1_15 PARTITION OF part_c_100_200 FOR VALUES FROM (1) TO (15);
CREATE TABLE part_d_15_20 PARTITION OF part_c_100_200 FOR VALUES FROM (15) TO (20);
ALTER TABLE part_b_10_b_20 ATTACH PARTITION part_c_100_200 FOR VALUES FROM (100) TO (200);

CREATE TABLE part_c_1_100 (id serial NOT NULL, a text, b int, c float8, d int, e text);
ALTER TABLE part_b_10_b_20 ATTACH PARTITION part_c_1_100 FOR VALUES FROM (1) TO (100);

\set init_range_parted 'delete from range_parted; insert into range_parted(a, b, c, d) VALUES (''a'', 1, 1, 1), (''a'', 10, 200, 1), (''b'', 12, 96, 1), (''b'', 13, 97, 2), (''b'', 15, 105, 16), (''b'', 17, 105, 19)'
\set show_data 'select tableoid::regclass::text COLLATE "C" partname, * from range_parted ORDER BY 1, 2, 3, 4, 5, 6'
:init_range_parted;
:show_data;

-- The order of subplans should be in bound order
EXPLAIN (costs off) UPDATE range_parted set c = c - 50 WHERE c > 97;

-- fail, row movement happens only within the partition subtree.
-- skip, no bound check is not applied
-- UPDATE part_c_100_200 set c = c - 20, d = c WHERE c = 105;
-- fail, no partition key update, so no attempt to move tuple,
-- but "a = 'a'" violates partition constraint enforced by root partition)
--Testcase 44:
UPDATE part_b_10_b_20 set a = 'a';
-- ok, partition key update, no constraint violation
--Testcase 45:
UPDATE range_parted set d = d - 10 WHERE d > 10;
-- ok, no partition key update, no constraint violation
--Testcase 46:
UPDATE range_parted set e = d;
-- No row found
--Testcase 47:
UPDATE part_c_1_100 set c = c + 20 WHERE c = 98;
-- ok, row movement
--Testcase 48:
UPDATE part_b_10_b_20 set c = c + 20;
:show_data;

-- fail, row movement happens only within the partition subtree.
-- skip, bound check is not applied.
-- UPDATE part_b_10_b_20 set b = b - 6 WHERE c > 116 returning *;
-- ok, row movement, with subset of rows moved into different partition.
--Testcase 49:
UPDATE range_parted set b = b - 6 WHERE c > 116;

:show_data;

-- Common table needed for multiple test scenarios.
CREATE TABLE mintab(c1 int);
--Testcase 50:
INSERT into mintab VALUES (120);

-- update partition key using updatable view.
CREATE VIEW upview AS SELECT * FROM range_parted WHERE (select c > c1 FROM mintab) WITH CHECK OPTION;
-- ok
--Testcase 51:
UPDATE upview set c = 199 WHERE b = 4;
-- fail, check option violation
-- UPDATE upview set c = 120 WHERE b = 4;
-- fail, row movement with check option violation
-- UPDATE upview set a = 'b', b = 15, c = 120 WHERE b = 4;
-- ok, row movement, check option passes
--Testcase 52:
UPDATE upview set a = 'b', b = 15 WHERE b = 4;

:show_data;

-- cleanup
DROP VIEW upview;

-- RETURNING having whole-row vars.
:init_range_parted;
--Testcase 53:
UPDATE range_parted set c = 95 WHERE a = 'b' and b > 10 and c > 100;
:show_data;


-- Transition tables with update row movement
:init_range_parted;

CREATE FUNCTION trans_updatetrigfunc() RETURNS trigger LANGUAGE plpgsql AS
$$
  begin
    raise notice 'trigger = %, old table = %, new table = %',
                 TG_NAME,
                 (select string_agg(old_table::text, ', ' ORDER BY a) FROM old_table),
                 (select string_agg(new_table::text, ', ' ORDER BY a) FROM new_table);
    return null;
  end;
$$;

CREATE TRIGGER trans_updatetrig
  AFTER UPDATE ON range_parted REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
  FOR EACH STATEMENT EXECUTE PROCEDURE trans_updatetrigfunc();

--Testcase 54:
UPDATE range_parted set c = (case when c = 96 then 110 else c + 1 end ) WHERE a = 'b' and b > 10 and c >= 96;
:show_data;
:init_range_parted;

-- Enabling OLD TABLE capture for both DELETE as well as UPDATE stmt triggers
-- should not cause DELETEd rows to be captured twice. Similar thing for
-- INSERT triggers and inserted rows.
CREATE TRIGGER trans_deletetrig
  AFTER DELETE ON range_parted REFERENCING OLD TABLE AS old_table
  FOR EACH STATEMENT EXECUTE PROCEDURE trans_updatetrigfunc();
CREATE TRIGGER trans_inserttrig
  AFTER INSERT ON range_parted REFERENCING NEW TABLE AS new_table
  FOR EACH STATEMENT EXECUTE PROCEDURE trans_updatetrigfunc();
--Testcase 55:
UPDATE range_parted set c = c + 50 WHERE a = 'b' and b > 10 and c >= 96;
:show_data;
DROP TRIGGER trans_deletetrig ON range_parted;
DROP TRIGGER trans_inserttrig ON range_parted;
-- Don't drop trans_updatetrig yet. It is required below.

-- Test with transition tuple conversion happening for rows moved into the
-- new partition. This requires a trigger that references transition table
-- (we already have trans_updatetrig). For inserted rows, the conversion
-- is not usually needed, because the original tuple is already compatible with
-- the desired transition tuple format. But conversion happens when there is a
-- BR trigger because the trigger can change the inserted row. So install a
-- BR triggers on those child partitions where the rows will be moved.
CREATE FUNCTION func_parted_mod_b() RETURNS trigger AS $$
BEGIN
   NEW.b = NEW.b + 1;
   return NEW;
END $$ language plpgsql;
CREATE TRIGGER trig_c1_100 BEFORE UPDATE OR INSERT ON part_c_1_100
   FOR EACH ROW EXECUTE PROCEDURE func_parted_mod_b();
CREATE TRIGGER trig_d1_15 BEFORE UPDATE OR INSERT ON part_d_1_15
   FOR EACH ROW EXECUTE PROCEDURE func_parted_mod_b();
CREATE TRIGGER trig_d15_20 BEFORE UPDATE OR INSERT ON part_d_15_20
   FOR EACH ROW EXECUTE PROCEDURE func_parted_mod_b();
:init_range_parted;
--Testcase 56:
UPDATE range_parted set c = (case when c = 96 then 110 else c + 1 end) WHERE a = 'b' and b > 10 and c >= 96;
:show_data;
:init_range_parted;
--Testcase 57:
UPDATE range_parted set c = c + 50 WHERE a = 'b' and b > 10 and c >= 96;
:show_data;

-- Case where per-partition tuple conversion map array is allocated, but the
-- map is not required for the particular tuple that is routed, thanks to
-- matching table attributes of the partition and the target table.
:init_range_parted;
--Testcase 58:
UPDATE range_parted set b = 15 WHERE b = 1;
:show_data;

DROP TRIGGER trans_updatetrig ON range_parted;
DROP TRIGGER trig_c1_100 ON part_c_1_100;
DROP TRIGGER trig_d1_15 ON part_d_1_15;
DROP TRIGGER trig_d15_20 ON part_d_15_20;
DROP FUNCTION func_parted_mod_b();

-- RLS policies with update-row-movement
-----------------------------------------

ALTER TABLE range_parted ENABLE ROW LEVEL SECURITY;
CREATE USER regress_range_parted_user;
GRANT ALL ON range_parted, mintab TO regress_range_parted_user;
CREATE POLICY seeall ON range_parted AS PERMISSIVE FOR SELECT USING (true);
CREATE POLICY policy_range_parted ON range_parted for UPDATE USING (true) WITH CHECK (c::numeric % 2 = 0);

:init_range_parted;
SET SESSION AUTHORIZATION regress_range_parted_user;
-- This should fail with RLS violation error while moving row from
-- part_a_10_a_20 to part_d_1_15, because we are setting 'c' to an odd number.
-- UPDATE range_parted set a = 'b', c = 151 WHERE a = 'a' and c = 200;

RESET SESSION AUTHORIZATION;
-- Create a trigger on part_d_1_15
CREATE FUNCTION func_d_1_15() RETURNS trigger AS $$
BEGIN
   NEW.c = NEW.c + 1; -- Make even numbers odd, or vice versa
   return NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER trig_d_1_15 BEFORE INSERT ON part_d_1_15
   FOR EACH ROW EXECUTE PROCEDURE func_d_1_15();

:init_range_parted;
SET SESSION AUTHORIZATION regress_range_parted_user;

-- Here, RLS checks should succeed while moving row from part_a_10_a_20 to
-- part_d_1_15. Even though the UPDATE is setting 'c' to an odd number, the
-- trigger at the destination partition again makes it an even number.
--Testcase 59:
UPDATE range_parted set a = 'b', c = 151 WHERE a = 'a' and c = 200;

RESET SESSION AUTHORIZATION;
:init_range_parted;
SET SESSION AUTHORIZATION regress_range_parted_user;
-- This should fail with RLS violation error. Even though the UPDATE is setting
-- 'c' to an even number, the trigger at the destination partition again makes
-- it an odd number.
--UPDATE range_parted set a = 'b', c = 150 WHERE a = 'a' and c = 200;

-- Cleanup
RESET SESSION AUTHORIZATION;
DROP TRIGGER trig_d_1_15 ON part_d_1_15;
DROP FUNCTION func_d_1_15();

-- Policy expression contains SubPlan
RESET SESSION AUTHORIZATION;
:init_range_parted;
CREATE POLICY policy_range_parted_subplan on range_parted
    AS RESTRICTIVE for UPDATE USING (true)
    WITH CHECK ((SELECT range_parted.c <= c1 FROM mintab));
SET SESSION AUTHORIZATION regress_range_parted_user;
-- fail, mintab has row with c1 = 120
-- UPDATE range_parted set a = 'b', c = 122 WHERE a = 'a' and c = 200;
-- ok
--Testcase 60:
UPDATE range_parted set a = 'b', c = 120 WHERE a = 'a' and c = 200;

-- RLS policy expression contains whole row.

RESET SESSION AUTHORIZATION;
:init_range_parted;
CREATE POLICY policy_range_parted_wholerow on range_parted AS RESTRICTIVE for UPDATE USING (true)
   WITH CHECK (range_parted= row(1000, 'b', 10, 112, 1, NULL)::range_parted);
SET SESSION AUTHORIZATION regress_range_parted_user;
-- ok, should pass the RLS check
--Testcase 61:
UPDATE range_parted set a = 'b', c = 112 WHERE a = 'a' and c = 200;
RESET SESSION AUTHORIZATION;
:init_range_parted;
SET SESSION AUTHORIZATION regress_range_parted_user;
-- fail, the whole row RLS check should fail
--UPDATE range_parted set a = 'b', c = 116 WHERE a = 'a' and c = 200;

-- Cleanup
RESET SESSION AUTHORIZATION;
DROP POLICY policy_range_parted ON range_parted;
DROP POLICY policy_range_parted_subplan ON range_parted;
DROP POLICY policy_range_parted_wholerow ON range_parted;
REVOKE ALL ON range_parted, mintab FROM regress_range_parted_user;
DROP USER regress_range_parted_user;
DROP TABLE mintab;


-- statement triggers with update row movement
---------------------------------------------------

:init_range_parted;

CREATE FUNCTION trigfunc() returns trigger language plpgsql as
$$
  begin
    raise notice 'trigger = % fired on table % during %',
                 TG_NAME, TG_TABLE_NAME, TG_OP;
    return null;
  end;
$$;
-- Triggers on root partition
CREATE TRIGGER parent_delete_trig
  AFTER DELETE ON range_parted for each statement execute procedure trigfunc();
CREATE TRIGGER parent_update_trig
  AFTER UPDATE ON range_parted for each statement execute procedure trigfunc();
CREATE TRIGGER parent_insert_trig
  AFTER INSERT ON range_parted for each statement execute procedure trigfunc();

-- Triggers on leaf partition part_c_1_100
CREATE TRIGGER c1_delete_trig
  AFTER DELETE ON part_c_1_100 for each statement execute procedure trigfunc();
CREATE TRIGGER c1_update_trig
  AFTER UPDATE ON part_c_1_100 for each statement execute procedure trigfunc();
CREATE TRIGGER c1_insert_trig
  AFTER INSERT ON part_c_1_100 for each statement execute procedure trigfunc();

-- Triggers on leaf partition part_d_1_15
CREATE TRIGGER d1_delete_trig
  AFTER DELETE ON part_d_1_15 for each statement execute procedure trigfunc();
CREATE TRIGGER d1_update_trig
  AFTER UPDATE ON part_d_1_15 for each statement execute procedure trigfunc();
CREATE TRIGGER d1_insert_trig
  AFTER INSERT ON part_d_1_15 for each statement execute procedure trigfunc();
-- Triggers on leaf partition part_d_15_20
CREATE TRIGGER d15_delete_trig
  AFTER DELETE ON part_d_15_20 for each statement execute procedure trigfunc();
CREATE TRIGGER d15_update_trig
  AFTER UPDATE ON part_d_15_20 for each statement execute procedure trigfunc();
CREATE TRIGGER d15_insert_trig
  AFTER INSERT ON part_d_15_20 for each statement execute procedure trigfunc();

-- Move all rows from part_c_100_200 to part_c_1_100. None of the delete or
-- insert statement triggers should be fired.
--Testcase 62:
UPDATE range_parted set c = c - 50 WHERE c > 97;
:show_data;

DROP TRIGGER parent_delete_trig ON range_parted;
DROP TRIGGER parent_update_trig ON range_parted;
DROP TRIGGER parent_insert_trig ON range_parted;
DROP TRIGGER c1_delete_trig ON part_c_1_100;
DROP TRIGGER c1_update_trig ON part_c_1_100;
DROP TRIGGER c1_insert_trig ON part_c_1_100;
DROP TRIGGER d1_delete_trig ON part_d_1_15;
DROP TRIGGER d1_update_trig ON part_d_1_15;
DROP TRIGGER d1_insert_trig ON part_d_1_15;
DROP TRIGGER d15_delete_trig ON part_d_15_20;
DROP TRIGGER d15_update_trig ON part_d_15_20;
DROP TRIGGER d15_insert_trig ON part_d_15_20;


-- Creating default partition for range
:init_range_parted;
create foreign table part_def1 partition of range_parted default server griddb_svr;
alter foreign table part_def1 alter column id options (rowkey 'true');
\d+ part_def1
--Testcase 63:
insert into range_parted(a, b) values ('c', 9);
-- ok
--Testcase 64:
update range_parted set a = 'd' where a = 'c';
-- fail
--update range_parted set a = 'a' where a = 'd';

:show_data;

-- Update row movement from non-default to default partition.
-- fail, default partition is not under part_a_10_a_20;
-- UPDATE part_a_10_a_20 set a = 'ad' WHERE a = 'a';
-- ok
--Testcase 65:
UPDATE range_parted set a = 'ad' WHERE a = 'a';
--Testcase 66:
UPDATE range_parted set a = 'bd' WHERE a = 'b';
:show_data;
-- Update row movement from default to non-default partitions.
-- ok
--Testcase 67:
UPDATE range_parted set a = 'a' WHERE a = 'ad';
--Testcase 68:
UPDATE range_parted set a = 'b' WHERE a = 'bd';
:show_data;

-- Cleanup: range_parted no longer needed.
DROP TABLE range_parted;

CREATE TABLE list_parted (
	id serial NOT NULL,
	a text,
	b int
) PARTITION BY list (a);
CREATE FOREIGN TABLE list_part1  PARTITION OF list_parted for VALUES in ('a', 'b') server griddb_svr;
CREATE FOREIGN TABLE list_default PARTITION OF list_parted default server griddb_svr;
alter foreign table list_part1 alter column id options (rowkey 'true');
alter foreign table list_default alter column id options (rowkey 'true');
--Testcase 69:
DELETE FROM list_parted;
--Testcase 70:
INSERT into list_part1(a, b) VALUES ('a', 1);
--Testcase 71:
INSERT into list_default(a, b) VALUES ('d', 10);

-- fail
-- UPDATE list_default set a = 'a' WHERE a = 'd';
-- ok
--Testcase 72:
UPDATE list_default set a = 'x' WHERE a = 'd';

DROP TABLE list_parted;

--------------
-- Some more update-partition-key test scenarios below. This time use list
-- partitions.
--------------

-- Setup for list partitions
CREATE TABLE list_parted (a numeric, b int, c int8) PARTITION BY list (a);
CREATE TABLE sub_parted PARTITION OF list_parted for VALUES in (1) PARTITION BY list (b);

CREATE TABLE sub_part1(b int, c int8, a numeric);
ALTER TABLE sub_parted ATTACH PARTITION sub_part1 for VALUES in (1);
CREATE TABLE sub_part2(b int, c int8, a numeric);
ALTER TABLE sub_parted ATTACH PARTITION sub_part2 for VALUES in (2);

CREATE TABLE list_part1(a numeric, b int, c int8);
ALTER TABLE list_parted ATTACH PARTITION list_part1 for VALUES in (2,3);

--Testcase 73:
INSERT into list_parted VALUES (2,5,50);
--Testcase 74:
INSERT into list_parted VALUES (3,6,60);
--Testcase 75:
INSERT into sub_parted VALUES (1,1,60);
--Testcase 76:
INSERT into sub_parted VALUES (1,2,10);

-- Test partition constraint violation when intermediate ancestor is used and
-- constraint is inherited from upper root.
--Testcase 77:
UPDATE sub_parted set a = 2 WHERE c = 10;

-- Test update-partition-key, where the unpruned partitions do not have their
-- partition keys updated.
--Testcase 78:
SELECT tableoid::regclass::text, * FROM list_parted WHERE a = 2 ORDER BY 1;
--Testcase 79:
UPDATE list_parted set b = c + a WHERE a = 2;
--Testcase 80:
SELECT tableoid::regclass::text, * FROM list_parted WHERE a = 2 ORDER BY 1;


-- Test the case where BR UPDATE triggers change the partition key.
CREATE FUNCTION func_parted_mod_b() returns trigger as $$
BEGIN
   NEW.b = 2; -- This is changing partition key column.
   return NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER parted_mod_b before update on sub_part1
   for each row execute procedure func_parted_mod_b();

--Testcase 81:
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;

-- This should do the tuple routing even though there is no explicit
-- partition-key update, because there is a trigger on sub_part1.
--Testcase 82:
UPDATE list_parted set c = 70 WHERE b  = 1;
--Testcase 83:
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;

DROP TRIGGER parted_mod_b ON sub_part1;

-- If BR DELETE trigger prevented DELETE from happening, we should also skip
-- the INSERT if that delete is part of UPDATE=>DELETE+INSERT.
CREATE OR REPLACE FUNCTION func_parted_mod_b() returns trigger as $$
BEGIN
   raise notice 'Trigger: Got OLD row %, but returning NULL', OLD;
   return NULL;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER trig_skip_delete before delete on sub_part2
   for each row execute procedure func_parted_mod_b();
--Testcase 84:
UPDATE list_parted set b = 1 WHERE c = 70;
--Testcase 85:
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;
-- Drop the trigger. Now the row should be moved.
DROP TRIGGER trig_skip_delete ON sub_part2;
--Testcase 86:
UPDATE list_parted set b = 1 WHERE c = 70;
--Testcase 87:
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;
DROP FUNCTION func_parted_mod_b();

-- UPDATE partition-key with FROM clause. If join produces multiple output
-- rows for the same row to be modified, we should tuple-route the row only
-- once. There should not be any rows inserted.
CREATE TABLE non_parted (id int);
--Testcase 88:
INSERT into non_parted VALUES (1), (1), (1), (2), (2), (2), (3), (3), (3);
--Testcase 89:
UPDATE list_parted t1 set a = 2 FROM non_parted t2 WHERE t1.a = t2.id and a = 1;
--Testcase 90:
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;
DROP TABLE non_parted;

-- Cleanup: list_parted no longer needed.
DROP TABLE list_parted;


-- create custom operator class and hash function, for the same reason
-- explained in alter_table.sql
create or replace function dummy_hashint4(a int4, seed int8) returns int8 as
$$ begin return (a + seed); end; $$ language 'plpgsql' immutable;
create operator class custom_opclass for type int4 using hash as
operator 1 = , function 2 dummy_hashint4(int4, int8);

create table hash_parted (
	id serial,
	a int,
	b int
) partition by hash (a custom_opclass, b custom_opclass);
create foreign table hpart1 partition of hash_parted for values with (modulus 2, remainder 1) server griddb_svr;
create foreign table hpart2 partition of hash_parted for values with (modulus 4, remainder 2) server griddb_svr;
create foreign table hpart3 partition of hash_parted for values with (modulus 8, remainder 0) server griddb_svr;
create foreign table hpart4 partition of hash_parted for values with (modulus 8, remainder 4) server griddb_svr;
alter foreign table hpart1 alter column id options (rowkey 'true');
alter foreign table hpart2 alter column id options (rowkey 'true');
alter foreign table hpart3 alter column id options (rowkey 'true');
alter foreign table hpart4 alter column id options (rowkey 'true');
--Testcase 91:
delete from hash_parted;
--Testcase 92:
insert into hpart1(a, b) values (1, 1);
--Testcase 93:
insert into hpart2(a, b) values (2, 5);
--Testcase 94:
insert into hpart4(a, b) values (3, 4);

-- fail
-- skip
-- update hpart1 set a = 3, b=4 where a = 1;
-- ok, row movement
--Testcase 95:
update hash_parted set b = b - 1 where b = 1;
-- ok
--Testcase 96:
update hash_parted set b = b + 8 where b = 1;

--drop all foreign tables
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

