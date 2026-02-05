create table foo_iceberg (i int, comm string, ts timestamp) stored as iceberg;
create table foo_kudu (i int primary key, comm string, ts timestamp) stored as kudu;
upsert into foo_kudu values (1, 'a', now()), (2, 'b', now()), (3, 'c', now()), (4, 'd', now()), (5, 'e', now());
-- Could also use timestamp for migration_ts. Using bigint simplified kudu_lookup.
create table foo_pit (
    id int primary key, migration_ts bigint, iceberg_snapshot bigint) stored as kudu;
-- Use same table with non-unique primary key and is_delete flag to log updates and deletes.
-- auto_incrementing_id becomes the logical timestamp for the log.
create table foo_dmls (i int non unique primary key, comm string, ts timestamp, is_delete boolean) stored as kudu;
-- create view foo tblproperties ('streaming'='true') as
--     select * from foo_iceberg union all select * from foo_kudu;

set var:last_migration_id=1;
set var:next_migration_id=2;

-- Should we store timestamps or UTC epoch microseconds? FOR SYSTEM_TIME AS OF now() does
-- utc_to_unix_micros(to_utc_timestamp(now(), "<tz>")) to get UTC epoch in microseconds.
insert into foo_pit values (${var:next_migration_id}, utc_to_unix_micros(utc_timestamp()), 0);
--    utc_to_unix_micros(utc_timestamp() - interval 1 minute)
-- last_migration_id returns BIGINT Kudu timestamp of last migrated record
-- or NULL if no records have been migrated yet. If 0/NULL, need to do a snapshot read with
-- SetSnapshotMicros to migrate all existing records.
-- Created kudu_lookup to fetch timestamps; it's a lot more complicated to initialize the
-- Kudu scan from the results of a subquery.
insert into foo_iceberg SELECT i, comm, ts FROM foo_kudu FOR SYSTEM_TIME
    FROM coalesce(kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id}), -1)
    AS OF kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:next_migration_id});
-- This is not precise enough to always get the right snapshot id. Currently gets the
-- latest snapshot, but other snapshots could be created during migration.
upsert into foo_pit select ${var:last_migration_id}, migration_ts, snapshot_id from
    (select migration_ts from default.foo_pit where id=${var:next_migration_id} limit 1) t1
    join (select snapshot_id from default.foo_iceberg.snapshots order by committed_at desc limit 1) t2;
delete from foo_pit where id=${var:next_migration_id};
-- TODO: Kudu command to move ancient history timestame to migration_ts of last migration,
-- so that we can clean up old history after migration completes.

-- Queries
select * from foo_iceberg for system_version
    AS OF kudu_lookup('impala::default.foo_pit', 'iceberg_snapshot', ${var:last_migration_id})
union all
select i, comm, ts from foo_kudu for system_time
    FROM kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id})
    AS OF now();
upsert into foo_kudu values (6, 'f', now()), (7, 'g', null);

-- Work on DELETE/UPDATE list.
-- Needs to be a log for the same reasons the main table does. We need to include this in
-- migrating data, and be able to clear them later; new queries can run in-between.

-- Upsert of new values. Need to log a delete first because Iceberg doesn't have upsert.
insert into foo_dmls (i, is_delete) values (1, true); -- log delete before upsert
upsert into foo_kudu (i, comm) values (1, 'aa');
-- Migrate values. Next operations assume row is missing from Kudu.
insert into foo_dmls (i, comm) values (1, 'aaa'); -- update
insert into foo_dmls (i, comm) values (1, 'aaaa'); -- update
insert into foo_dmls (i, is_delete) values (1, true); -- delete
insert into foo_dmls (i, comm, ts) values (1, 'aaaaa', now()); -- update
insert into foo_dmls (i, is_delete) values (1, true); -- delete before upsert
upsert into foo_kudu values (1, 'aaaaa', now());
insert into foo_dmls (i, ts) values (2, now());
insert into foo_dmls (i, is_delete) values (3, true);

-- Merge log and Kudu table to Iceberg.
merge into foo_iceberg as t
using (
    select distinct * from (
        select i,
            last_value(comm ignore nulls) over (partition by i order by auto_incrementing_id
                rows between unbounded preceding and unbounded following) as comm,
            last_value(ts ignore nulls) over (partition by i order by auto_incrementing_id
                rows between unbounded preceding and unbounded following) as ts,
            max(is_delete) over (partition by i order by auto_incrementing_id
                rows between unbounded preceding and unbounded following) as is_delete
            from foo_dmls for system_time
                from kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id})
                as of now()
    ) last_updates
) as s
on t.i = s.i
when matched and s.is_delete then delete
when matched and not s.is_delete then
    update set comm = coalesce(s.comm, t.comm), ts = coalesce(s.ts, t.ts);
-- TODO: combine into one merge statement. For non-unique primary keys, we want a union of new entries.
-- May need to preserve auto_incrementing_id in iceberg to merge correctly.
merge into foo_iceberg as t
using (
    select i, comm, ts, is_deleted from foo_kudu FOR SYSTEM_TIME
        FROM coalesce(kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id}), -1)
        AS OF kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:next_migration_id})
) as s
on t.i = s.i
when matched and s.is_deleted then delete
when matched and not s.is_deleted then
    update set comm = coalesce(s.comm, t.comm), ts = coalesce(s.ts, t.ts)
when not matched then insert (i, comm, ts) values (s.i, s.comm, s.ts);

-- Combined query.
select coalesce(kudu_new.i, after_updates.i) as i,
    coalesce(kudu_new.comm, after_updates.comm) as comm,
    coalesce(kudu_new.ts, after_updates.ts) as ts from (
    select after_delete.i as i,
        coalesce(kudu_updates.comm, after_delete.comm) as comm,
        coalesce(kudu_updates.ts, after_delete.ts) as ts
    from (
        select * from foo_iceberg for system_version
            AS OF kudu_lookup('impala::default.foo_pit', 'iceberg_snapshot', ${var:last_migration_id})
        left anti join (select i from foo_dmls for system_time
                        from kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id})
                        as of now() where is_delete) deleted
        on foo_iceberg.i = deleted.i
    ) after_delete
    left outer join (
        select distinct * from (
            select i,
                last_value(comm ignore nulls) over (partition by i order by auto_incrementing_id
                    rows between unbounded preceding and unbounded following) as comm,
                last_value(ts ignore nulls) over (partition by i order by auto_incrementing_id
                    rows between unbounded preceding and unbounded following) as ts
                from foo_dmls for system_time
                    from kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id})
                    as of now()
        ) last_updates
    ) as kudu_updates
    on after_delete.i = kudu_updates.i
) after_updates
left anti join (
    select i from foo_kudu FOR SYSTEM_TIME
        FROM coalesce(kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id}), -1)
        AS OF now() where is_deleted) deleted
on after_updates.i = deleted.i
-- TODO: Non-unique keys could be a union, except we can't distinguish between a new row and an update
-- to an existing row without auto_incrementing_id.
full outer join (
    select i, comm, ts from foo_kudu FOR SYSTEM_TIME
        FROM coalesce(kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id}), -1)
        AS OF now() where not is_deleted
) kudu_new
on after_updates.i = kudu_new.i;

-- What about "update ... where ..." modifiers? Kudu may get these in the future, where
-- updates won't specify a condition instead of matching the primary key.
WAL
TYPE	COND		SET
UPDATE  i=1			comm='aa'
DELETE	comm='a'
DELETE	comm='b'
DELETE	comm='c' AND ts>now()-interval 1 day
UPDATE	i=3			comm='cc'
UPDATE	i=4			comm='dd', ts=now()
UPDATE	comm='e'	ts=now()

Compacted WAL
TYPE	COND		SET
UPDATE  i=1			comm='aa'
DELETE  comm IN('a', 'b') OR (comm='c' AND ts>now()-interval 1 day)	 -- Adjacent deletes can always be compacted
UPDATE  i=3; i=4	comm='cc'; comm='dd', ts=now()  -- Can be compacted since i was not set
UPDATE	comm='e'	ts=now()  -- Not compacted, condition columns differ. Could be if case is prepended.

drop table foo_dmls if exists;
-- UPDATE if operation has a value, otherwise DELETE.
create table foo_dmls (
    micros bigint non unique primary key, cond string not null, operation string) stored as kudu;
insert into foo_dmls values
    (utc_to_unix_micros(utc_timestamp()), 'i=1', 'comm=aa'),
    (utc_to_unix_micros(utc_timestamp()), 'comm=a', null),
    (utc_to_unix_micros(utc_timestamp()), 'comm=b', null),
    (utc_to_unix_micros(utc_timestamp()), 'comm=c AND ts>now()-interval 1 day', null),
    (utc_to_unix_micros(utc_timestamp()), 'i=3', 'comm=cc'),
    (utc_to_unix_micros(utc_timestamp()), 'i=4', 'comm=dd, ts=now()'),
    (utc_to_unix_micros(utc_timestamp()), 'comm=e', 'ts=now()');
select cond, operation from foo_dmls for system_time
    from kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id})
    as of now() order by micros, auto_incrementing_id;

-- Note that now() should be evaluated when adding to WAL, not during compaction. Just being lazy.
-- TODO: statement that constructs another statement. Do we construct it in AST
-- (and parse cond/operation) or as a subquery?
WITH update1 AS (
    SELECT i, CASE WHEN i=1 THEN 'aa' ELSE comm END AS comm, ts
    FROM foo_iceberg FOR SYSTEM_VERSION
    AS OF kudu_lookup('impala::default.foo_pit', 'iceberg_snapshot', ${var:last_migration_id})
),
-- Update after delete can share a query block
delete1 AS (
    SELECT i,
        CASE WHEN i=3 THEN 'cc' WHEN i=4 THEN 'dd' ELSE comm END AS comm,
        CASE WHEN i=4 THEN now() ELSE ts END AS ts
    FROM update1 WHERE NOT (comm IN('a', 'b') OR (comm='c' AND ts>now()-interval 1 day))
),
update2 AS (
    SELECT i, comm, CASE WHEN comm='e' THEN now() ELSE ts END AS ts
    FROM delete1
)
SELECT * FROM update2
left anti join (
    select i from foo_kudu FOR SYSTEM_TIME
        FROM coalesce(kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id}), -1)
        AS OF now() where is_deleted) deleted
on update2.i = deleted.i
FULL OUTER JOIN (
    SELECT i, comm, ts FROM foo_kudu FOR SYSTEM_TIME
        FROM coalesce(kudu_lookup('impala::default.foo_pit', 'migration_ts', ${var:last_migration_id}), -1)
        AS OF now() WHERE NOT is_deleted
) kudu_new
on update2.i = kudu_new.i;

-- TODO: Think about partitioning.

-- Use a compactor to combine operations efficiently. Apply during query/migration, or run
-- regularly on data as written? Look at how Iceberg merge is implemented.
-- Delete would replace any prior update/delete.
-- Update needs to merge with prior updates. Updates after a delete should be ignored; if
-- delete exists because of an upsert, then the update should have been applied directly.

-- On startup, check that foo_pit is consistent. If next_migration_id exists, it means the
-- last migration did not complete. If last_migration_id = next_migration_id, delete
-- next_migration_id. If not, check whether newer Iceberg snapshot exists. If so, write
-- last_migration_id and clear next_migration_id. Otherwise start a new migration.

-- How to handle multiple potential migrations? Insert into foo_pit; if write succeeds
-- then proceed with migration. If write fails due to primary key violation, another
-- migration is in progress so abort. Need to write coordinator id to tell who's
-- responsible for restarting migrations later.
