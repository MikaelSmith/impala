-- Example with unique primary key. Non-unique primary key is the same with explicit
-- inclusion of auto_incrementing_id in the log and merge conditions.
drop table foo;
drop table foo_pit;
drop table foo_iceberg;
-- Can drop and recreate these two tables to simulate aging out old data.
drop table foo_dels;
drop table foo_kudu;

-- Create the iceberg table, and a kudu table for write caching.
-- Create ancilary point-in-time and delete log tables for migration.
create table foo_kudu (i int primary key, comm string, ts timestamp) stored as kudu;
create table foo_iceberg(i int, comm string, ts timestamp) stored as iceberg;
-- Use non-unique primary key to log deletes; auto_incrementing_id is the logical timestamp.
-- Needs to be a log for the same reasons the main table does: we need to include this in
-- migrating data, and be able to clear them later; new queries can run in-between.
create table foo_dels (i int non unique primary key) stored as kudu;
-- Could also use timestamp for migration_ts. Using bigint simplified kudu_lookup.
create table foo_pit (
    id int primary key, migration_ts bigint, snapshot_id bigint) stored as kudu;
-- Initialize baseline timestamp to simplify migration code.
create table foo(i int, comm string, ts timestamp) stored as iceberg
    tblproperties('impala.streaming.kudu'='foo_kudu', 'impala.streaming.iceberg'='foo_iceberg',
                  'impala.streaming.pit'='foo_pit', 'impala.streaming.dels'='foo_dels');

-- Insert initial data to Kudu.
upsert into foo values (1, 'a', now()), (2, 'b', now()), (3, 'c', now()), (4, 'd', now()), (5, 'e', now());

-- Query the main table, which merges Kudu and Iceberg.
select * from foo order by i;

-- Merge Kudu to Iceberg.
merge foo;
-- drop/recreate to simulate aging out old data; in real world this would be done by TTL.

-- Add new and modify existing data after migrate.
upsert into foo values (6, 'f', now()), (7, 'g', null);
upsert into foo select 1, 'aa', ts from foo where i=1;
insert into foo_dels values (3);
insert into foo_dels values (3);
upsert into foo values (6, 'ff', now());
select * from foo;
merge foo;

-- TODO: Impala conditional DMLs collect primary keys from Kudu and call Delete on them,
-- and collect primary keys from Iceberg and insert deletes in foo_dels. Initially support
-- conditional delete, but hybrid clients can support conditional update by adding to
-- foo_dels then inserting a new row to Kudu with the modified old data.

-- TODO: Think about partitioning.

-- On startup, check that foo_pit is consistent. If next_migration_id exists, it means the
-- last migration did not complete. If last_migration_id = next_migration_id, delete
-- next_migration_id. If not, check whether newer Iceberg snapshot exists. If so, write
-- last_migration_id and clear next_migration_id. Otherwise start a new migration.

-- How to handle multiple potential migrations? Insert into foo_pit; if write succeeds
-- then proceed with migration. If write fails due to primary key violation, another
-- migration is in progress so abort. Need to write coordinator id to tell who's
-- responsible for restarting migrations later.
