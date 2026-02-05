-- Example with unique primary key. Non-unique primary key is the same with explicit
-- inclusion of auto_incrementing_id in the log and merge conditions.
set var:last_migration_id=1;
set var:next_migration_id=2;
drop table foo_iceberg;
drop table foo_kudu;
drop table foo_dels;
drop table foo_pit;

create table foo_iceberg (i int, comm string, ts timestamp) stored as iceberg;
create table foo_kudu (i int primary key, comm string, ts timestamp) stored as kudu;
-- Use same table with non-unique primary key and is_delete flag to log deletes.
-- auto_incrementing_id becomes the logical timestamp for the log.
-- Needs to be a log for the same reasons the main table does. We need to include this in
-- migrating data, and be able to clear them later; new queries can run in-between.
create table foo_dels (i int non unique primary key) stored as kudu;
-- Could also use timestamp for migration_ts. Using bigint simplified kudu_lookup.
create table foo_pit (
    id int primary key, migration_ts bigint, iceberg_snapshot bigint) stored as kudu;
-- Initialize baseline timestamp to simplify migration code.
insert into foo_pit values (${var:last_migration_id}, utc_to_unix_micros(utc_timestamp()), 0);
-- Insert initial data to Kudu.
upsert into foo_kudu values (1, 'a', now()), (2, 'b', now()), (3, 'c', now()), (4, 'd', now()), (5, 'e', now());

insert into foo_pit values (${var:next_migration_id}, utc_to_unix_micros(utc_timestamp()), 0);
--    utc_to_unix_micros(utc_timestamp() - interval 1 minute)
-- Merge delete log and Kudu table to Iceberg.
merge into foo_iceberg as src
using (
    -- Collect Kudu updates since last migration. If a row is in foo_kudu, use is_deleted
    -- from DiffScan; otherwise set is_delete=true for rows in the delete log.
    select coalesce(foo_kudu.i, dels.i) as i, comm, ts, coalesce(is_deleted, dels.is_delete) as is_delete
    from foo_kudu for system_time
        from coalesce(kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id}), -1)
        AS OF kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:next_migration_id})
    full outer join (
        select distinct i, true as is_delete from foo_dels for system_time
            from coalesce(kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id}), -1)
            as of kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:next_migration_id})) dels
    on foo_kudu.i = dels.i
) as updates
on src.i = updates.i
when matched and updates.is_delete then delete
when matched and not updates.is_delete then
    update set comm = coalesce(updates.comm, src.comm), ts = coalesce(updates.ts, src.ts)
when not matched and not updates.is_delete then
    insert (i, comm, ts) values (updates.i, updates.comm, updates.ts);
-- This is not precise enough to always get the right snapshot id. Currently gets the
-- latest snapshot, but other snapshots could be created during migration.
upsert into foo_pit select ${var:last_migration_id}, migration_ts, snapshot_id from
    (select migration_ts from default.foo_pit where id=${var:next_migration_id} limit 1) t1
    join (select snapshot_id from default.foo_iceberg.snapshots order by committed_at desc limit 1) t2;
delete from foo_pit where id=${var:next_migration_id};
-- TODO: Kudu command to move ancient history timestame to migration_ts of last migration,
-- so that we can clean up old history after migration completes.

-- Add new data after migrate.
upsert into foo_kudu values (6, 'f', now()), (7, 'g', null);

-- Upsert of new values; log a delete first because Iceberg doesn't have upsert.
insert into foo_dels values (1); -- log delete before upsert
upsert into foo_kudu (i, comm) values (1, 'aa');
insert into foo_dels values (3);
insert into foo_dels values (3);
-- TODO: Impala conditional DMLs collect primary keys from Kudu and call Delete on them,
-- and collect primary keys from Iceberg and insert deletes in foo_dels. Initially support
-- conditional delete, but hybrid clients can support conditional update by adding to
-- foo_dels then inserting a new row to Kudu with the modified old data.

-- Combined query.
select coalesce(kudu_new.i, foo_iceberg.i) as i,
    coalesce(kudu_new.comm, foo_iceberg.comm) as comm,
    coalesce(kudu_new.ts, foo_iceberg.ts) as ts
from foo_iceberg for system_version
    AS OF kudu_lookup('impala::default.foo_pit', 'iceberg_snapshot', ${var:last_migration_id})
left anti join (
    select i from foo_dels for system_time
        from kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id})
        as of now()
    union distinct
    select i from foo_kudu for system_time
        from coalesce(kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id}), -1)
        as of now() where is_deleted
) deleted
on foo_iceberg.i = deleted.i
full outer join (
    select i, comm, ts from foo_kudu for system_time
        from coalesce(kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id}), -1)
        as of now() where not is_deleted
) kudu_new
on foo_iceberg.i = kudu_new.i;

-- TODO: Think about partitioning.

-- On startup, check that foo_pit is consistent. If next_migration_id exists, it means the
-- last migration did not complete. If last_migration_id = next_migration_id, delete
-- next_migration_id. If not, check whether newer Iceberg snapshot exists. If so, write
-- last_migration_id and clear next_migration_id. Otherwise start a new migration.

-- How to handle multiple potential migrations? Insert into foo_pit; if write succeeds
-- then proceed with migration. If write fails due to primary key violation, another
-- migration is in progress so abort. Need to write coordinator id to tell who's
-- responsible for restarting migrations later.
