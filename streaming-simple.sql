-- Example with unique primary key.
drop table foo;
drop table foo_pit;
drop table foo_iceberg;
drop table foo_dels;
drop table foo_kudu;

-- Create the iceberg table for long-term storage, kudu table for write caching,
-- ancilary point-in-time and delete log tables for migration tracking.
create table foo_iceberg (`group` int, `user` int, `hired` DATE, `name` string, `salary` int, `updated` timestamp)
    partitioned by spec (`group`, bucket(5, `user`), month(`hired`), truncate(3, `name`), hour(`updated`))
    stored as iceberg;
create table foo_kudu (`group` int, `user` int, `hired` date, `name` string, `salary` int, `updated` timestamp,
    primary key (`group`, `user`)) partition by hash partitions 4 stored as kudu;
create table foo_dels (`group` int, `user` int, non unique primary key (`group`, `user`)) stored as kudu;
create table foo_pit (id int primary key, migration_ts bigint, snapshot_id bigint) stored as kudu;
create table foo (`group` int, `user` int, `hired` DATE, `name` string, `salary` int, `updated` timestamp) stored as iceberg
    tblproperties('impala.streaming.kudu'='foo_kudu', 'impala.streaming.iceberg'='foo_iceberg',
                  'impala.streaming.pit'='foo_pit', 'impala.streaming.dels'='foo_dels');

-- Insert initial data to Kudu and query the main table.
upsert into foo values
    (1, 101, DATE '2021-01-15', 'Alice', 95000, now()),
    (1, 102, DATE '2020-06-10', 'Bob', 88000, now()),
    (2, 201, DATE '2022-03-21', 'Carol', 99000, now()),
    (2, 202, DATE '2019-11-05', 'David', 105000, now()),
    (3, 301, DATE '2023-08-01', 'Eve', 91000, now());
select * from foo order by `group`, `user`;

-- Merge Kudu to Iceberg.
merge foo;
select * from foo order by `group`, `user`;

-- Add new and modify existing data after migrate.
upsert into foo values
    (4, 401, DATE '2024-02-14', 'Frank', 97000, now()),
    (5, 501, DATE '2021-12-01', 'Grace', 93000, now());
delete from foo where `group`=3;
delete from foo where `name`='Carol';
upsert into foo values (2, 201, DATE '2022-03-21', 'Carol', 99500, now());
select * from foo order by hired;

merge foo;
select * from foo order by hired;

-- WARNING: has a race condition where if another update runs between the select and data
-- sink, the other update will be lost. i.e. in one session:
--   set debug_action=FIS_KUDU_TABLE_SINK_CREATE_SESSION:sleep@3000;
--   update foo set comm='d' where i=4;
-- and another: "update foo set ts=now() where comm='d'". Setting ts=now() will be lost.
update foo set updated=now() where `group`=1;
select * from foo order by updated;

-- On startup, check that foo_pit is consistent. If next_migration_id exists, it means the
-- last migration did not complete. If last_migration_id = next_migration_id, delete
-- next_migration_id. If not, check whether newer Iceberg snapshot exists. If so, write
-- last_migration_id and clear next_migration_id. Otherwise start a new migration.

-- How to handle multiple potential migrations? Insert into foo_pit; if write succeeds
-- then proceed with migration. If write fails due to primary key violation, another
-- migration is in progress so abort. Need to write coordinator id to tell who's
-- responsible for restarting migrations later.
