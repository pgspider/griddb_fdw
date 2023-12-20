--
-- UPDATE syntax tests
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
CREATE FOREIGN TABLE update_test (
    id  serial options (rowkey 'true'),
    a   INT DEFAULT 10,
    b   INT,
    c   TEXT
) SERVER griddb_svr;

--Testcase 5:
CREATE FOREIGN TABLE upsert_test (
    a   INT OPTIONS (rowkey 'true'),
    b   TEXT
) SERVER griddb_svr;

--Testcase 6:
DELETE FROM update_test;

--Testcase 7:
DELETE FROM upsert_test;

--Testcase 8:
INSERT INTO update_test(a, b, c) VALUES (5, 10, 'foo');

--Testcase 9:
INSERT INTO update_test(b, a) VALUES (15, 10);

--Testcase 10:
SELECT a, b, c FROM update_test;

--Testcase 11:
UPDATE update_test SET a = DEFAULT, b = DEFAULT;

--Testcase 12:
SELECT a, b, c FROM update_test;

-- aliases for the UPDATE target table

--Testcase 13:
UPDATE update_test AS t SET b = 10 WHERE t.a = 10;

--Testcase 14:
SELECT a, b, c FROM update_test;

--Testcase 15:
UPDATE update_test t SET b = t.b + 10 WHERE t.a = 10;

--Testcase 16:
SELECT a, b, c FROM update_test;

--
-- Test VALUES in FROM
--

--Testcase 17:
UPDATE update_test SET a=v.i FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

--Testcase 18:
SELECT a, b, c FROM update_test;

-- fail, wrong data type:

--Testcase 19:
UPDATE update_test SET a = v.* FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

--
-- Test multiple-set-clause syntax
--

--Testcase 20:
INSERT INTO update_test(a,b,c) SELECT a,b+1,c FROM update_test;

--Testcase 21:
SELECT a, b, c FROM update_test;

--Testcase 22:
UPDATE update_test SET (c,b,a) = ('bugle', b+11, DEFAULT) WHERE c = 'foo';

--Testcase 23:
SELECT a, b, c FROM update_test;

--Testcase 24:
UPDATE update_test SET (c,b) = ('car', a+b), a = a + 1 WHERE a = 10;

--Testcase 25:
SELECT a, b, c FROM update_test;
-- fail, multi assignment to same column:

--Testcase 26:
UPDATE update_test SET (c,b) = ('car', a+b), b = a + 1 WHERE a = 10;

-- uncorrelated sub-select:

--Testcase 27:
UPDATE update_test
  SET (b,a) = (select a,b from update_test where b = 41 and c = 'car')
  WHERE a = 100 AND b = 20;

--Testcase 28:
SELECT a, b, c FROM update_test;
-- correlated sub-select:

--Testcase 29:
UPDATE update_test o
  SET (b,a) = (select a+1,b from update_test i
               where i.a=o.a and i.b=o.b and i.c is not distinct from o.c);

--Testcase 30:
SELECT a, b, c FROM update_test;
-- fail, multiple rows supplied:

--Testcase 31:
UPDATE update_test SET (b,a) = (select a+1,b from update_test);
-- set to null if no rows supplied:

--Testcase 32:
UPDATE update_test SET (b,a) = (select a+1,b from update_test where a = 1000)
  WHERE a = 11;

--Testcase 33:
SELECT a, b, c FROM update_test;
-- *-expansion should work in this context:

--Testcase 34:
UPDATE update_test SET (a,b) = ROW(v.*) FROM (VALUES(21, 100)) AS v(i, j)
  WHERE update_test.a = v.i;
-- you might expect this to work, but syntactically it's not a RowExpr:

--Testcase 35:
UPDATE update_test SET (a,b) = (v.*) FROM (VALUES(21, 101)) AS v(i, j)
  WHERE update_test.a = v.i;

-- if an alias for the target table is specified, don't allow references
-- to the original table name

--Testcase 36:
UPDATE update_test AS t SET b = update_test.b + 10 WHERE t.a = 10;

-- Make sure that we can update to a TOASTed value.

--Testcase 37:
UPDATE update_test SET c = repeat('x', 10000) WHERE c = 'car';

--Testcase 38:
SELECT a, b, char_length(c) FROM update_test;

-- Check multi-assignment with a Result node to handle a one-time filter.

--Testcase 39:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE update_test t
  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
  WHERE CURRENT_USER = SESSION_USER;

--Testcase 40:
UPDATE update_test t
  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
  WHERE CURRENT_USER = SESSION_USER;

--Testcase 41:
SELECT a, b, char_length(c) FROM update_test;

-- ON CONFLICT is not supported.
-- Test ON CONFLICT DO UPDATE
/*
INSERT INTO upsert_test VALUES(1, 'Boo');
-- uncorrelated  sub-select:

WITH aaa AS (SELECT 1 AS a, 'Foo' AS b) INSERT INTO upsert_test
  VALUES (1, 'Bar') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b, a FROM aaa) RETURNING *;
-- correlated sub-select:

INSERT INTO upsert_test VALUES (1, 'Baz') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Correlated', a from upsert_test i WHERE i.a = upsert_test.a)
  RETURNING *;
-- correlated sub-select (EXCLUDED.* alias):

INSERT INTO upsert_test VALUES (1, 'Bat') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING *;

-- ON CONFLICT using system attributes in RETURNING, testing both the
-- inserting and updating paths. See bug report at:
-- https://www.postgresql.org/message-id/73436355-6432-49B1-92ED-1FE4F7E7E100%40finefun.com.au
CREATE FUNCTION xid_current() RETURNS xid LANGUAGE SQL AS $$SELECT (txid_current() % ((1::int8<<32)))::text::xid;$$;

INSERT INTO upsert_test VALUES (2, 'Beeble') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING tableoid::regclass, xmin = xid_current() AS xmin_correct, xmax = 0 AS xmax_correct;
-- currently xmax is set after a conflict - that's probably not good,
-- but it seems worthwhile to have to be explicit if that changes.

INSERT INTO upsert_test VALUES (2, 'Brox') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING tableoid::regclass, xmin = xid_current() AS xmin_correct, xmax = xid_current() AS xmax_correct;

DROP FUNCTION xid_current();
DROP FOREIGN TABLE update_test;
DROP FOREIGN TABLE upsert_test;
*/

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

--Testcase 42:
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

--Testcase 43:
CREATE TABLE part_b_20_b_30 (id serial NOT NULL, a text, b int, c float8, d int, e text);

--Testcase 44:
ALTER TABLE range_parted ATTACH PARTITION part_b_20_b_30 FOR VALUES FROM ('b', 20) TO ('b', 30);

--Testcase 45:
CREATE TABLE part_b_10_b_20 (id serial NOT NULL, a text, b int, c float8, d int, e text) PARTITION BY RANGE (c);

--Testcase 46:
CREATE FOREIGN TABLE part_b_1_b_10 PARTITION OF range_parted FOR VALUES FROM ('b', 1) TO ('b', 10) server griddb_svr;

--Testcase 47:
alter foreign table part_b_1_b_10 alter column id options (rowkey 'true');

--Testcase 48:
ALTER TABLE range_parted ATTACH PARTITION part_b_10_b_20 FOR VALUES FROM ('b', 10) TO ('b', 20);

--Testcase 49:
CREATE FOREIGN TABLE part_a_10_a_20 PARTITION OF range_parted FOR VALUES FROM ('a', 10) TO ('a', 20) server griddb_svr;

--Testcase 50:
CREATE FOREIGN TABLE part_a_1_a_10 PARTITION OF range_parted FOR VALUES FROM ('a', 1) TO ('a', 10) server griddb_svr;

--Testcase 51:
alter foreign table part_a_10_a_20 alter column id options (rowkey 'true');

--Testcase 52:
alter foreign table part_a_1_a_10 alter column id options (rowkey 'true');

-- Check that partition-key UPDATE works sanely on a partitioned table that
-- does not have any child partitions.

--Testcase 53:
UPDATE part_b_10_b_20 set b = b - 6;

-- Create some more partitions following the above pattern of descending bound
-- order, but let's make the situation a bit more complex by having the
-- attribute numbers of the columns vary from their parent partition.

--Testcase 54:
CREATE TABLE part_c_100_200 (id serial NOT NULL, a text, b int, c float8, d int, e text) PARTITION BY range (abs(d));
--ALTER TABLE part_c_100_200 DROP COLUMN e, DROP COLUMN c, DROP COLUMN a;
--ALTER TABLE part_c_100_200 ADD COLUMN c numeric, ADD COLUMN e varchar, ADD COLUMN a text;
--ALTER TABLE part_c_100_200 DROP COLUMN b;
--ALTER TABLE part_c_100_200 ADD COLUMN b bigint;

--Testcase 55:
CREATE TABLE part_d_1_15 PARTITION OF part_c_100_200 FOR VALUES FROM (1) TO (15);

--Testcase 56:
CREATE TABLE part_d_15_20 PARTITION OF part_c_100_200 FOR VALUES FROM (15) TO (20);

--Testcase 57:
ALTER TABLE part_b_10_b_20 ATTACH PARTITION part_c_100_200 FOR VALUES FROM (100) TO (200);

--Testcase 58:
CREATE TABLE part_c_1_100 (id serial NOT NULL, a text, b int, c float8, d int, e text);

--Testcase 59:
ALTER TABLE part_b_10_b_20 ATTACH PARTITION part_c_1_100 FOR VALUES FROM (1) TO (100);

\set init_range_parted 'delete from range_parted; insert into range_parted(a, b, c, d) VALUES (''a'', 1, 1, 1), (''a'', 10, 200, 1), (''b'', 12, 96, 1), (''b'', 13, 97, 2), (''b'', 15, 105, 16), (''b'', 17, 105, 19)'
\set show_data 'select tableoid::regclass::text COLLATE "C" partname, a, b, c, d, e from range_parted ORDER BY 1, 2, 3, 4, 5, 6'
:init_range_parted;
:show_data;

-- The order of subplans should be in bound order

--Testcase 60:
EXPLAIN (costs off) UPDATE range_parted set c = c - 50 WHERE c > 97;

-- fail, row movement happens only within the partition subtree.
-- skip, no bound check is not applied
-- UPDATE part_c_100_200 set c = c - 20, d = c WHERE c = 105;
-- fail, no partition key update, so no attempt to move tuple,
-- but "a = 'a'" violates partition constraint enforced by root partition)

--Testcase 61:
UPDATE part_b_10_b_20 set a = 'a';
-- ok, partition key update, no constraint violation

--Testcase 62:
UPDATE range_parted set d = d - 10 WHERE d > 10;
-- ok, no partition key update, no constraint violation

--Testcase 63:
UPDATE range_parted set e = d;
-- No row found

--Testcase 64:
UPDATE part_c_1_100 set c = c + 20 WHERE c = 98;
-- ok, row movement

--Testcase 65:
UPDATE part_b_10_b_20 set c = c + 20;
:show_data;

-- fail, row movement happens only within the partition subtree.
-- skip, bound check is not applied.
-- UPDATE part_b_10_b_20 set b = b - 6 WHERE c > 116 returning *;
-- ok, row movement, with subset of rows moved into different partition.

--Testcase 66:
UPDATE range_parted set b = b - 6 WHERE c > 116;

:show_data;

-- Common table needed for multiple test scenarios.

--Testcase 67:
CREATE TABLE mintab(c1 int);

--Testcase 68:
INSERT into mintab VALUES (120);

-- update partition key using updatable view.

--Testcase 69:
CREATE VIEW upview AS SELECT * FROM range_parted WHERE (select c > c1 FROM mintab) WITH CHECK OPTION;
-- ok

--Testcase 70:
UPDATE upview set c = 199 WHERE b = 4;
-- fail, check option violation
-- UPDATE upview set c = 120 WHERE b = 4;
-- fail, row movement with check option violation
-- UPDATE upview set a = 'b', b = 15, c = 120 WHERE b = 4;
-- ok, row movement, check option passes

--Testcase 71:
UPDATE upview set a = 'b', b = 15 WHERE b = 4;

:show_data;

-- cleanup

--Testcase 72:
DROP VIEW upview;

-- RETURNING having whole-row vars.
:init_range_parted;

--Testcase 73:
UPDATE range_parted set c = 95 WHERE a = 'b' and b > 10 and c > 100;
:show_data;

-- Transition tables with update row movement
:init_range_parted;

--Testcase 74:
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

--Testcase 75:
CREATE TRIGGER trans_updatetrig
  AFTER UPDATE ON range_parted REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
  FOR EACH STATEMENT EXECUTE PROCEDURE trans_updatetrigfunc();

--Testcase 76:
UPDATE range_parted set c = (case when c = 96 then 110 else c + 1 end ) WHERE a = 'b' and b > 10 and c >= 96;
:show_data;
:init_range_parted;

-- Enabling OLD TABLE capture for both DELETE as well as UPDATE stmt triggers
-- should not cause DELETEd rows to be captured twice. Similar thing for
-- INSERT triggers and inserted rows.

--Testcase 77:
CREATE TRIGGER trans_deletetrig
  AFTER DELETE ON range_parted REFERENCING OLD TABLE AS old_table
  FOR EACH STATEMENT EXECUTE PROCEDURE trans_updatetrigfunc();

--Testcase 78:
CREATE TRIGGER trans_inserttrig
  AFTER INSERT ON range_parted REFERENCING NEW TABLE AS new_table
  FOR EACH STATEMENT EXECUTE PROCEDURE trans_updatetrigfunc();

--Testcase 79:
UPDATE range_parted set c = c + 50 WHERE a = 'b' and b > 10 and c >= 96;
:show_data;

--Testcase 80:
DROP TRIGGER trans_deletetrig ON range_parted;

--Testcase 81:
DROP TRIGGER trans_inserttrig ON range_parted;
-- Don't drop trans_updatetrig yet. It is required below.

-- Test with transition tuple conversion happening for rows moved into the
-- new partition. This requires a trigger that references transition table
-- (we already have trans_updatetrig). For inserted rows, the conversion
-- is not usually needed, because the original tuple is already compatible with
-- the desired transition tuple format. But conversion happens when there is a
-- BR trigger because the trigger can change the inserted row. So install a
-- BR triggers on those child partitions where the rows will be moved.

--Testcase 82:
CREATE FUNCTION func_parted_mod_b() RETURNS trigger AS $$
BEGIN
   NEW.b = NEW.b + 1;
   return NEW;
END $$ language plpgsql;

--Testcase 83:
CREATE TRIGGER trig_c1_100 BEFORE UPDATE OR INSERT ON part_c_1_100
   FOR EACH ROW EXECUTE PROCEDURE func_parted_mod_b();

--Testcase 84:
CREATE TRIGGER trig_d1_15 BEFORE UPDATE OR INSERT ON part_d_1_15
   FOR EACH ROW EXECUTE PROCEDURE func_parted_mod_b();

--Testcase 85:
CREATE TRIGGER trig_d15_20 BEFORE UPDATE OR INSERT ON part_d_15_20
   FOR EACH ROW EXECUTE PROCEDURE func_parted_mod_b();
:init_range_parted;

--Testcase 86:
UPDATE range_parted set c = (case when c = 96 then 110 else c + 1 end) WHERE a = 'b' and b > 10 and c >= 96;
:show_data;
:init_range_parted;

--Testcase 87:
UPDATE range_parted set c = c + 50 WHERE a = 'b' and b > 10 and c >= 96;
:show_data;

-- Case where per-partition tuple conversion map array is allocated, but the
-- map is not required for the particular tuple that is routed, thanks to
-- matching table attributes of the partition and the target table.
:init_range_parted;

--Testcase 88:
UPDATE range_parted set b = 15 WHERE b = 1;
:show_data;

--Testcase 89:
DROP TRIGGER trans_updatetrig ON range_parted;

--Testcase 90:
DROP TRIGGER trig_c1_100 ON part_c_1_100;

--Testcase 91:
DROP TRIGGER trig_d1_15 ON part_d_1_15;

--Testcase 92:
DROP TRIGGER trig_d15_20 ON part_d_15_20;

--Testcase 93:
DROP FUNCTION func_parted_mod_b();

-- RLS policies with update-row-movement
-----------------------------------------

--Testcase 94:
ALTER TABLE range_parted ENABLE ROW LEVEL SECURITY;

--Testcase 95:
CREATE USER regress_range_parted_user;
GRANT ALL ON range_parted, mintab TO regress_range_parted_user;

--Testcase 96:
CREATE POLICY seeall ON range_parted AS PERMISSIVE FOR SELECT USING (true);

--Testcase 97:
CREATE POLICY policy_range_parted ON range_parted for UPDATE USING (true) WITH CHECK (c::numeric % 2 = 0);

:init_range_parted;

--Testcase 98:
SET SESSION AUTHORIZATION regress_range_parted_user;
-- This should fail with RLS violation error while moving row from
-- part_a_10_a_20 to part_d_1_15, because we are setting 'c' to an odd number.
-- UPDATE range_parted set a = 'b', c = 151 WHERE a = 'a' and c = 200;

--Testcase 99:
RESET SESSION AUTHORIZATION;
-- Create a trigger on part_d_1_15

--Testcase 100:
CREATE FUNCTION func_d_1_15() RETURNS trigger AS $$
BEGIN
   NEW.c = NEW.c + 1; -- Make even numbers odd, or vice versa
   return NEW;
END $$ LANGUAGE plpgsql;

--Testcase 101:
CREATE TRIGGER trig_d_1_15 BEFORE INSERT ON part_d_1_15
   FOR EACH ROW EXECUTE PROCEDURE func_d_1_15();

:init_range_parted;

--Testcase 102:
SET SESSION AUTHORIZATION regress_range_parted_user;

-- Here, RLS checks should succeed while moving row from part_a_10_a_20 to
-- part_d_1_15. Even though the UPDATE is setting 'c' to an odd number, the
-- trigger at the destination partition again makes it an even number.

--Testcase 103:
UPDATE range_parted set a = 'b', c = 151 WHERE a = 'a' and c = 200;

--Testcase 104:
RESET SESSION AUTHORIZATION;
:init_range_parted;

--Testcase 105:
SET SESSION AUTHORIZATION regress_range_parted_user;
-- This should fail with RLS violation error. Even though the UPDATE is setting
-- 'c' to an even number, the trigger at the destination partition again makes
-- it an odd number.
--UPDATE range_parted set a = 'b', c = 150 WHERE a = 'a' and c = 200;

-- Cleanup

--Testcase 106:
RESET SESSION AUTHORIZATION;

--Testcase 107:
DROP TRIGGER trig_d_1_15 ON part_d_1_15;

--Testcase 108:
DROP FUNCTION func_d_1_15();

-- Policy expression contains SubPlan

--Testcase 109:
RESET SESSION AUTHORIZATION;
:init_range_parted;

--Testcase 110:
CREATE POLICY policy_range_parted_subplan on range_parted
    AS RESTRICTIVE for UPDATE USING (true)
    WITH CHECK ((SELECT range_parted.c <= c1 FROM mintab));

--Testcase 111:
SET SESSION AUTHORIZATION regress_range_parted_user;
-- fail, mintab has row with c1 = 120
-- UPDATE range_parted set a = 'b', c = 122 WHERE a = 'a' and c = 200;
-- ok

--Testcase 112:
UPDATE range_parted set a = 'b', c = 120 WHERE a = 'a' and c = 200;

-- RLS policy expression contains whole row.

--Testcase 113:
RESET SESSION AUTHORIZATION;
:init_range_parted;

--Testcase 114:
CREATE POLICY policy_range_parted_wholerow on range_parted AS RESTRICTIVE for UPDATE USING (true)
   WITH CHECK (range_parted = row(1000, 'b', 10, 112, 1, NULL)::range_parted);

--Testcase 115:
SET SESSION AUTHORIZATION regress_range_parted_user;
-- ok, should pass the RLS check

--Testcase 116:
UPDATE range_parted set a = 'b', c = 112 WHERE a = 'a' and c = 200;

--Testcase 117:
RESET SESSION AUTHORIZATION;
:init_range_parted;

--Testcase 118:
SET SESSION AUTHORIZATION regress_range_parted_user;
-- fail, the whole row RLS check should fail
--UPDATE range_parted set a = 'b', c = 116 WHERE a = 'a' and c = 200;

-- Cleanup

--Testcase 119:
RESET SESSION AUTHORIZATION;

--Testcase 120:
DROP POLICY policy_range_parted ON range_parted;

--Testcase 121:
DROP POLICY policy_range_parted_subplan ON range_parted;

--Testcase 122:
DROP POLICY policy_range_parted_wholerow ON range_parted;
REVOKE ALL ON range_parted, mintab FROM regress_range_parted_user;

--Testcase 123:
DROP USER regress_range_parted_user;

--Testcase 124:
DROP TABLE mintab;

-- statement triggers with update row movement
---------------------------------------------------

:init_range_parted;

--Testcase 125:
CREATE FUNCTION trigfunc() returns trigger language plpgsql as
$$
  begin
    raise notice 'trigger = % fired on table % during %',
                 TG_NAME, TG_TABLE_NAME, TG_OP;
    return null;
  end;
$$;
-- Triggers on root partition

--Testcase 126:
CREATE TRIGGER parent_delete_trig
  AFTER DELETE ON range_parted for each statement execute procedure trigfunc();

--Testcase 127:
CREATE TRIGGER parent_update_trig
  AFTER UPDATE ON range_parted for each statement execute procedure trigfunc();

--Testcase 128:
CREATE TRIGGER parent_insert_trig
  AFTER INSERT ON range_parted for each statement execute procedure trigfunc();

-- Triggers on leaf partition part_c_1_100

--Testcase 129:
CREATE TRIGGER c1_delete_trig
  AFTER DELETE ON part_c_1_100 for each statement execute procedure trigfunc();

--Testcase 130:
CREATE TRIGGER c1_update_trig
  AFTER UPDATE ON part_c_1_100 for each statement execute procedure trigfunc();

--Testcase 131:
CREATE TRIGGER c1_insert_trig
  AFTER INSERT ON part_c_1_100 for each statement execute procedure trigfunc();

-- Triggers on leaf partition part_d_1_15

--Testcase 132:
CREATE TRIGGER d1_delete_trig
  AFTER DELETE ON part_d_1_15 for each statement execute procedure trigfunc();

--Testcase 133:
CREATE TRIGGER d1_update_trig
  AFTER UPDATE ON part_d_1_15 for each statement execute procedure trigfunc();

--Testcase 134:
CREATE TRIGGER d1_insert_trig
  AFTER INSERT ON part_d_1_15 for each statement execute procedure trigfunc();
-- Triggers on leaf partition part_d_15_20

--Testcase 135:
CREATE TRIGGER d15_delete_trig
  AFTER DELETE ON part_d_15_20 for each statement execute procedure trigfunc();

--Testcase 136:
CREATE TRIGGER d15_update_trig
  AFTER UPDATE ON part_d_15_20 for each statement execute procedure trigfunc();

--Testcase 137:
CREATE TRIGGER d15_insert_trig
  AFTER INSERT ON part_d_15_20 for each statement execute procedure trigfunc();

-- Move all rows from part_c_100_200 to part_c_1_100. None of the delete or
-- insert statement triggers should be fired.

--Testcase 138:
UPDATE range_parted set c = c - 50 WHERE c > 97;
:show_data;

--Testcase 139:
DROP TRIGGER parent_delete_trig ON range_parted;

--Testcase 140:
DROP TRIGGER parent_update_trig ON range_parted;

--Testcase 141:
DROP TRIGGER parent_insert_trig ON range_parted;

--Testcase 142:
DROP TRIGGER c1_delete_trig ON part_c_1_100;

--Testcase 143:
DROP TRIGGER c1_update_trig ON part_c_1_100;

--Testcase 144:
DROP TRIGGER c1_insert_trig ON part_c_1_100;

--Testcase 145:
DROP TRIGGER d1_delete_trig ON part_d_1_15;

--Testcase 146:
DROP TRIGGER d1_update_trig ON part_d_1_15;

--Testcase 147:
DROP TRIGGER d1_insert_trig ON part_d_1_15;

--Testcase 148:
DROP TRIGGER d15_delete_trig ON part_d_15_20;

--Testcase 149:
DROP TRIGGER d15_update_trig ON part_d_15_20;

--Testcase 150:
DROP TRIGGER d15_insert_trig ON part_d_15_20;

-- Creating default partition for range
:init_range_parted;

--Testcase 151:
create foreign table part_def1 partition of range_parted default server griddb_svr;

--Testcase 152:
alter foreign table part_def1 alter column id options (rowkey 'true');

--Testcase 153:
\d+ part_def1

--Testcase 154:
insert into range_parted(a, b) values ('c', 9);
-- ok

--Testcase 155:
update range_parted set a = 'd' where a = 'c';
-- fail
--update range_parted set a = 'a' where a = 'd';

:show_data;

-- Update row movement from non-default to default partition.
-- fail, default partition is not under part_a_10_a_20;
-- UPDATE part_a_10_a_20 set a = 'ad' WHERE a = 'a';
-- ok

--Testcase 156:
UPDATE range_parted set a = 'ad' WHERE a = 'a';

--Testcase 157:
UPDATE range_parted set a = 'bd' WHERE a = 'b';
:show_data;
-- Update row movement from default to non-default partitions.
-- ok

--Testcase 158:
UPDATE range_parted set a = 'a' WHERE a = 'ad';

--Testcase 159:
UPDATE range_parted set a = 'b' WHERE a = 'bd';
:show_data;

-- Cleanup: range_parted no longer needed.

--Testcase 160:
DROP TABLE range_parted;

--Testcase 161:
CREATE TABLE list_parted (
	id serial NOT NULL,
	a text,
	b int
) PARTITION BY list (a);

--Testcase 162:
CREATE FOREIGN TABLE list_part1  PARTITION OF list_parted for VALUES in ('a', 'b') server griddb_svr;

--Testcase 163:
CREATE FOREIGN TABLE list_default PARTITION OF list_parted default server griddb_svr;

--Testcase 164:
alter foreign table list_part1 alter column id options (rowkey 'true');

--Testcase 165:
alter foreign table list_default alter column id options (rowkey 'true');

--Testcase 166:
DELETE FROM list_parted;

--Testcase 167:
INSERT into list_part1(a, b) VALUES ('a', 1);

--Testcase 168:
INSERT into list_default(a, b) VALUES ('d', 10);

-- fail
-- UPDATE list_default set a = 'a' WHERE a = 'd';
-- ok

--Testcase 169:
UPDATE list_default set a = 'x' WHERE a = 'd';

--Testcase 170:
DROP TABLE list_parted;

--------------
-- Some more update-partition-key test scenarios below. This time use list
-- partitions.
--------------

-- Setup for list partitions

--Testcase 171:
CREATE TABLE list_parted (a numeric, b int, c int8) PARTITION BY list (a);

--Testcase 172:
CREATE TABLE sub_parted PARTITION OF list_parted for VALUES in (1) PARTITION BY list (b);

--Testcase 173:
CREATE TABLE sub_part1(b int, c int8, a numeric);

--Testcase 174:
ALTER TABLE sub_parted ATTACH PARTITION sub_part1 for VALUES in (1);

--Testcase 175:
CREATE TABLE sub_part2(b int, c int8, a numeric);

--Testcase 176:
ALTER TABLE sub_parted ATTACH PARTITION sub_part2 for VALUES in (2);

--Testcase 177:
CREATE TABLE list_part1(a numeric, b int, c int8);

--Testcase 178:
ALTER TABLE list_parted ATTACH PARTITION list_part1 for VALUES in (2,3);

--Testcase 179:
INSERT into list_parted VALUES (2,5,50);

--Testcase 180:
INSERT into list_parted VALUES (3,6,60);

--Testcase 181:
INSERT into sub_parted VALUES (1,1,60);

--Testcase 182:
INSERT into sub_parted VALUES (1,2,10);

-- Test partition constraint violation when intermediate ancestor is used and
-- constraint is inherited from upper root.

--Testcase 183:
UPDATE sub_parted set a = 2 WHERE c = 10;

-- Test update-partition-key, where the unpruned partitions do not have their
-- partition keys updated.

--Testcase 184:
SELECT tableoid::regclass::text, * FROM list_parted WHERE a = 2 ORDER BY 1;

--Testcase 185:
UPDATE list_parted set b = c + a WHERE a = 2;

--Testcase 186:
SELECT tableoid::regclass::text, * FROM list_parted WHERE a = 2 ORDER BY 1;

-- Test the case where BR UPDATE triggers change the partition key.

--Testcase 187:
CREATE FUNCTION func_parted_mod_b() returns trigger as $$
BEGIN
   NEW.b = 2; -- This is changing partition key column.
   return NEW;
END $$ LANGUAGE plpgsql;

--Testcase 188:
CREATE TRIGGER parted_mod_b before update on sub_part1
   for each row execute procedure func_parted_mod_b();

--Testcase 189:
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;

-- This should do the tuple routing even though there is no explicit
-- partition-key update, because there is a trigger on sub_part1.

--Testcase 190:
UPDATE list_parted set c = 70 WHERE b  = 1;

--Testcase 191:
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;

--Testcase 192:
DROP TRIGGER parted_mod_b ON sub_part1;

-- If BR DELETE trigger prevented DELETE from happening, we should also skip
-- the INSERT if that delete is part of UPDATE=>DELETE+INSERT.

--Testcase 193:
CREATE OR REPLACE FUNCTION func_parted_mod_b() returns trigger as $$
BEGIN
   raise notice 'Trigger: Got OLD row %, but returning NULL', OLD;
   return NULL;
END $$ LANGUAGE plpgsql;

--Testcase 194:
CREATE TRIGGER trig_skip_delete before delete on sub_part2
   for each row execute procedure func_parted_mod_b();

--Testcase 195:
UPDATE list_parted set b = 1 WHERE c = 70;

--Testcase 196:
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;
-- Drop the trigger. Now the row should be moved.

--Testcase 197:
DROP TRIGGER trig_skip_delete ON sub_part2;

--Testcase 198:
UPDATE list_parted set b = 1 WHERE c = 70;

--Testcase 199:
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;

--Testcase 200:
DROP FUNCTION func_parted_mod_b();

-- UPDATE partition-key with FROM clause. If join produces multiple output
-- rows for the same row to be modified, we should tuple-route the row only
-- once. There should not be any rows inserted.

--Testcase 201:
CREATE TABLE non_parted (id int);

--Testcase 202:
INSERT into non_parted VALUES (1), (1), (1), (2), (2), (2), (3), (3), (3);

--Testcase 203:
UPDATE list_parted t1 set a = 2 FROM non_parted t2 WHERE t1.a = t2.id and a = 1;

--Testcase 204:
SELECT tableoid::regclass::text, * FROM list_parted ORDER BY 1, 2, 3, 4;

--Testcase 205:
DROP TABLE non_parted;

-- Cleanup: list_parted no longer needed.

--Testcase 206:
DROP TABLE list_parted;

-- create custom operator class and hash function, for the same reason
-- explained in alter_table.sql

--Testcase 207:
create or replace function dummy_hashint4(a int4, seed int8) returns int8 as
$$ begin return (a + seed); end; $$ language 'plpgsql' immutable;

--Testcase 208:
create operator class custom_opclass for type int4 using hash as
operator 1 = , function 2 dummy_hashint4(int4, int8);

--Testcase 209:
create table hash_parted (
	id serial,
	a int,
	b int
) partition by hash (a custom_opclass, b custom_opclass);

--Testcase 210:
create foreign table hpart1 partition of hash_parted for values with (modulus 2, remainder 1) server griddb_svr;

--Testcase 211:
create foreign table hpart2 partition of hash_parted for values with (modulus 4, remainder 2) server griddb_svr;

--Testcase 212:
create foreign table hpart3 partition of hash_parted for values with (modulus 8, remainder 0) server griddb_svr;

--Testcase 213:
create foreign table hpart4 partition of hash_parted for values with (modulus 8, remainder 4) server griddb_svr;

--Testcase 214:
alter foreign table hpart1 alter column id options (rowkey 'true');

--Testcase 215:
alter foreign table hpart2 alter column id options (rowkey 'true');

--Testcase 216:
alter foreign table hpart3 alter column id options (rowkey 'true');

--Testcase 217:
alter foreign table hpart4 alter column id options (rowkey 'true');

--Testcase 218:
delete from hash_parted;

--Testcase 219:
insert into hpart1(a, b) values (1, 1);

--Testcase 220:
insert into hpart2(a, b) values (2, 5);

--Testcase 221:
insert into hpart4(a, b) values (3, 4);

-- fail
-- skip
-- update hpart1 set a = 3, b=4 where a = 1;
-- ok, row movement

--Testcase 222:
update hash_parted set b = b - 1 where b = 1;
-- ok

--Testcase 223:
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

--Testcase 224:
DROP USER MAPPING FOR public SERVER griddb_svr;

--Testcase 225:
DROP SERVER griddb_svr;

--Testcase 226:
DROP EXTENSION griddb_fdw CASCADE;

