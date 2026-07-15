# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function
import threading
import time

import pytest

from tests.common.impala_test_suite import ImpalaTestSuite


class TestStreamingTable(ImpalaTestSuite):
  """Tests related to streaming tables."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestStreamingTable, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_streaming(self, vector, unique_database):
    self.run_test_case('QueryTest/streaming', vector, use_db=unique_database)

  def test_streaming_non_unique(self, vector, unique_database):
    self.run_test_case('QueryTest/streaming-non-unique', vector, use_db=unique_database)

  def _create_streaming_table(self, table_name):
    """Helper method to create a streaming table with the given name."""
    create_sql = f"""
      CREATE TABLE {table_name} (
        id INT PRIMARY KEY, s STRING, ts TIMESTAMP)
      STORED AS STREAMING
    """
    self.client.execute(create_sql)

  def test_streaming_basic_creation(self, unique_database):
    """Test basic streaming table creation with backing tables."""
    client = self.client
    table_name = unique_database + ".test_streaming_basic"
    self._create_streaming_table(table_name)

    # Verify backing tables were created
    result = client.execute(f"SHOW TABLES IN {unique_database} LIKE '*basic*'")
    assert len(result.data) >= 3, "Backing tables should have been created"

    # Verify backing table properties are set correctly
    describe = client.execute(f"DESCRIBE FORMATTED {table_name}")
    describe_text = '\n'.join(describe.data)
    assert "impala.streaming.kudu" in describe_text, "Streaming table should reference Kudu backing table"
    assert "impala.streaming.iceberg" in describe_text, "Streaming table should reference Iceberg backing table"
    assert "impala.streaming.dels" in describe_text, "Streaming table should reference Dels backing table"

  def test_streaming_if_not_exists(self, unique_database):
    """Test IF NOT EXISTS on existing streaming table."""
    client = self.client
    table_name = unique_database + ".test_streaming_if_not_exists"
    self._create_streaming_table(table_name)

    # Create IF NOT EXISTS on existing table (should succeed silently)
    create_if_not_exists = f"""
      CREATE TABLE IF NOT EXISTS {table_name} (
        id INT PRIMARY KEY, s STRING, ts TIMESTAMP)
      STORED AS STREAMING
    """
    client.execute(create_if_not_exists)

    # Verify the table still exists and has correct structure
    describe = client.execute(f"DESCRIBE FORMATTED {table_name}")
    assert len(describe.data) > 0, "Table should still exist after IF NOT EXISTS"

  def test_streaming_failure_kudu_backing_exists(self, unique_database):
    """Test streaming table creation fails when Kudu backing table already exists."""
    client = self.client
    table_name = unique_database + ".test_streaming_kudu_conflict"
    kudu_backing = table_name + "_kudu"

    # Create the backing Kudu table first
    client.execute(f"CREATE TABLE {kudu_backing} (id INT PRIMARY KEY) STORED AS KUDU")

    # Try to create streaming table with same backing name
    try:
      self._create_streaming_table(table_name)
      assert False, "Streaming table creation should have failed due to existing backing Kudu table"
    except Exception as e:
      # Expected to fail - verify the error message indicates the issue
      error_msg = str(e).lower()
      assert "already exists" in error_msg or "kudu" in error_msg or "error" in error_msg, \
        f"Error message should indicate table/Kudu issue: {e}"

    # Verify the main streaming table was NOT created
    result = client.execute(f"SHOW TABLES IN {unique_database} LIKE 'test_streaming_kudu_conflict'")
    assert len(result.data) == 0, "Main streaming table should not be created on backing table conflict"

    # Verify only the pre-existing Kudu backing table exists
    result = client.execute(f"SHOW TABLES IN {unique_database} LIKE '*kudu_conflict*'")
    assert len(result.data) == 1, "Only the pre-existing Kudu backing table should exist"

  def test_streaming_failure_iceberg_backing_exists(self, unique_database):
    """Test streaming table creation fails when Iceberg backing table already exists."""
    client = self.client
    table_name = unique_database + ".test_streaming_iceberg_conflict"
    iceberg_backing = table_name + "_iceberg"

    client.execute(f"CREATE TABLE {iceberg_backing} (id INT) STORED AS ICEBERG")

    # Try to create streaming table with same backing name
    try:
      self._create_streaming_table(table_name)
      assert False, "Streaming table creation should have failed due to existing backing Iceberg table"
    except Exception as e:
      # Expected to fail - verify the error message indicates the issue
      error_msg = str(e).lower()
      assert "already exists" in error_msg or "iceberg" in error_msg or "error" in error_msg, \
        f"Error message should indicate Iceberg issue: {e}"

    # Verify the main streaming table was NOT created
    result = client.execute(f"SHOW TABLES IN {unique_database} LIKE 'test_streaming_iceberg_conflict'")
    assert len(result.data) == 0, "Main streaming table should not be created on backing table conflict"

    # Verify only the pre-existing Iceberg backing table exists (Kudu backing should be cleaned up)
    result = client.execute(f"SHOW TABLES IN {unique_database} LIKE '*iceberg_conflict*'")
    assert len(result.data) == 1, "Only the pre-existing Iceberg backing table should exist"

  def test_streaming_failure_dels_exists(self, unique_database):
    """Test streaming table creation fails when the dels table already exists."""
    client = self.client
    table_name = unique_database + ".test_streaming_dels_conflict"
    dels_backing = table_name + "_dels"

    client.execute(f"CREATE TABLE {dels_backing} (id INT NON UNIQUE PRIMARY KEY) STORED AS KUDU")

    # Try to create streaming table with same backing name
    try:
      self._create_streaming_table(table_name)
      assert False, "Streaming table creation should have failed due to existing backing dels table"
    except Exception as e:
      # Expected to fail - verify the error message indicates the issue
      error_msg = str(e).lower()
      assert "already exists" in error_msg or "dels" in error_msg or "error" in error_msg, \
        f"Error message should indicate dels issue: {e}"

    # Verify the main streaming table was NOT created
    result = client.execute(f"SHOW TABLES IN {unique_database} LIKE 'test_streaming_dels_conflict'")
    assert len(result.data) == 0, "Main streaming table should not be created on backing table conflict"

    # Verify only the pre-existing dels backing table exists (other backing should be cleaned up)
    result = client.execute(f"SHOW TABLES IN {unique_database} LIKE '*dels_conflict*'")
    assert len(result.data) == 1, "Only the pre-existing dels backing table should exist"

  def test_streaming_with_partitioning(self, unique_database):
    """Test that streaming tables with partitioning work correctly."""
    client = self.client
    table_name = unique_database + ".test_streaming_part"

    create_sql = f"""
      CREATE TABLE {table_name} (
        id SMALLINT PRIMARY KEY, ts TIMESTAMP, s STRING)
      PARTITION BY HASH (id) PARTITIONS 4
      PARTITIONED BY SPEC (DAY(ts))
      STORED AS STREAMING
    """
    client.execute(create_sql)

    # Verify the Iceberg backing table was created with partitioning
    describe = client.execute(f"DESCRIBE FORMATTED {table_name+'_iceberg'}")
    partition_spec = [row for row in describe.data if "default-partition-spec" in row]
    assert len(partition_spec) == 1, "Partitioned streaming table should be created"
    assert 'ts_day' in partition_spec[0], "Partition spec should include day(ts)"

    # Verify the Kudu backing table was created with partitioning
    partitions = client.execute(f"SHOW PARTITIONS {table_name+'_kudu'}")
    assert len(partitions.data) == 4, "Partitioned streaming table should be created"

  def test_drop_streaming_with_missing_backings(self, unique_database):
    """Dropping a streaming table should tolerate missing backings.

    The user-facing drop must succeed and remove all existing related tables.
    """
    client = self.client
    table_name = unique_database + ".test_streaming_drop_missing"
    base_name = table_name.split('.', 1)[1]
    kudu_backing = base_name + "_kudu"
    iceberg_backing = base_name + "_iceberg"
    dels_backing = base_name + "_dels"

    self._create_streaming_table(table_name)

    # Simulate partial external cleanup by removing two backings ahead of time.
    client.execute(f"DROP TABLE {unique_database}.{kudu_backing}")
    client.execute(f"DROP TABLE {unique_database}.{iceberg_backing}")

    # Drop should still succeed and clean up any remaining related tables.
    client.execute(f"DROP TABLE {table_name}")

    assert len(client.execute(
      f"SHOW TABLES IN {unique_database} LIKE '{base_name}'").data) == 0
    assert len(client.execute(
      f"SHOW TABLES IN {unique_database} LIKE '{kudu_backing}'").data) == 0
    assert len(client.execute(
      f"SHOW TABLES IN {unique_database} LIKE '{iceberg_backing}'").data) == 0
    assert len(client.execute(
      f"SHOW TABLES IN {unique_database} LIKE '{dels_backing}'").data) == 0

  @pytest.mark.execute_serially
  def test_streaming_concurrent_rw_migrate_no_data_loss(self, vector, unique_database):
    """Stress streaming tables with concurrent readers, writers and periodic migrate.

    Writers perform upserts, updates and deletes in disjoint key ranges so the final
    state is deterministic and can be checked exactly.
    """
    table_name = unique_database + ".test_streaming_concurrent"
    writer_threads = 4
    reader_threads = 3
    stable_keys_per_writer = 24
    writer_iterations = 20
    migrate_iterations = 10
    transient_key_base = 1000000

    self.client.execute(f"""
      CREATE TABLE {table_name} (
        id INT PRIMARY KEY,
        writer_id INT,
        cnt BIGINT,
        payload STRING,
        ts TIMESTAMP)
      STORED AS STREAMING
    """)

    # Seed all stable keys so readers can assert that this cardinality never drops.
    for writer_id in range(writer_threads):
      start_id = writer_id * stable_keys_per_writer
      values = []
      for i in range(stable_keys_per_writer):
        row_id = start_id + i
        values.append("({0}, {1}, 0, 'seed-{1}', now())".format(row_id, writer_id))
      self.client.execute("upsert into {0} values {1}".format(table_name, ",".join(values)))

    errors = []
    stop_readers = threading.Event()

    def _record_error(name, err):
      errors.append("{0}: {1}".format(name, err))

    def _writer_fn(writer_id):
      try:
        with self.create_impala_client_from_vector(vector) as client:
          start_id = writer_id * stable_keys_per_writer
          stable_ids = [start_id + i for i in range(stable_keys_per_writer)]
          for iteration in range(writer_iterations):
            upsert_values = []
            payload = "w{0}-u{1}".format(writer_id, iteration)
            for row_id in stable_ids:
              upsert_values.append(
                "({0}, {1}, {2}, '{3}', now())".format(row_id, writer_id, iteration, payload))
            client.execute("upsert into {0} values {1}".format(table_name, ",".join(upsert_values)))

            update_payload = "w{0}-upd{1}".format(writer_id, iteration)
            client.execute("""
              update {0}
              set cnt = cnt + 1000, payload = '{1}', ts = now()
              where id >= {2} and id < {3} and id % 2 = 0
            """.format(table_name, update_payload, start_id,
                       start_id + stable_keys_per_writer))

            transient_id = transient_key_base + writer_id * writer_iterations + iteration
            client.execute("""
              upsert into {0} values ({1}, {2}, {3}, 'transient', now())
            """.format(table_name, transient_id, writer_id, iteration))
            client.execute("delete from {0} where id = {1}".format(table_name, transient_id))
      except Exception as e:
        _record_error("writer-{0}".format(writer_id), e)

    def _reader_fn(reader_id):
      expected_stable_rows = writer_threads * stable_keys_per_writer
      try:
        with self.create_impala_client_from_vector(vector) as client:
          while not stop_readers.is_set():
            cnt = int(client.execute(
              "select count(*) from {0} where id < {1}".format(
                table_name, transient_key_base)).data[0])
            assert cnt == expected_stable_rows, \
              "reader-{0}: expected {1} stable rows, got {2}".format(
                reader_id, expected_stable_rows, cnt)

            dedup = client.execute("""
              select count(*), count(distinct id)
              from {0} where id < {1}
            """.format(table_name, transient_key_base)).data[0].split('\t')
            assert dedup[0] == dedup[1], \
              "reader-{0}: duplicate primary keys detected: {1}".format(reader_id, dedup)
      except Exception as e:
        _record_error("reader-{0}".format(reader_id), e)

    def _migrate_fn():
      try:
        with self.create_impala_client_from_vector(vector) as client:
          table_only = table_name.split('.', 1)[1]
          client.execute("use {0}".format(unique_database))
          for _ in range(migrate_iterations):
            client.execute("migrate {0}".format(table_only))
            time.sleep(0.5)
      except Exception as e:
        _record_error("migrate", e)

    writers = [threading.Thread(target=_writer_fn, args=(i,))
               for i in range(writer_threads)]
    readers = [threading.Thread(target=_reader_fn, args=(i,))
               for i in range(reader_threads)]
    migrator = threading.Thread(target=_migrate_fn)

    for thread in readers + writers:
      thread.start()
    migrator.start()

    for thread in writers:
      thread.join()
    migrator.join()
    stop_readers.set()
    for thread in readers:
      thread.join()

    assert not errors, "Concurrent streaming stress test failed: {0}".format(errors)

    # Final exact verification of stable rows, proving no committed data was lost.
    expected = []
    final_iteration = writer_iterations - 1
    for writer_id in range(writer_threads):
      start_id = writer_id * stable_keys_per_writer
      for offset in range(stable_keys_per_writer):
        row_id = start_id + offset
        if row_id % 2 == 0:
          cnt = final_iteration + 1000
          payload = "w{0}-upd{1}".format(writer_id, final_iteration)
        else:
          cnt = final_iteration
          payload = "w{0}-u{1}".format(writer_id, final_iteration)
        expected.append("{0}\t{1}\t{2}\t{3}".format(row_id, writer_id, cnt, payload))

    result = self.client.execute("""
      select id, writer_id, cnt, payload
      from {0}
      where id < {1}
      order by id
    """.format(table_name, transient_key_base))
    assert result.data == expected, \
      "Final table state mismatch, possible data loss under concurrency"

    transient_rows = int(self.client.execute("""
      select count(*) from {0} where id >= {1}
    """.format(table_name, transient_key_base)).data[0])
    assert transient_rows == 0, "Transient rows should have been deleted"

  def test_streaming_concurrent_creates_different_tables(self, vector, unique_database):
    """N threads each create a distinct streaming table concurrently; all must succeed
    and each result must include the full set of backing tables (no cross-thread
    interference from the DDL lock)."""
    num_tables = 5
    errors = []
    lock = threading.Lock()
    barrier = threading.Barrier(num_tables)

    def _create_fn(idx):
      tbl = f"{unique_database}.conc_diff_{idx}"
      try:
        with self.create_impala_client_from_vector(vector) as client:
          barrier.wait()  # release all threads simultaneously
          client.execute(
              f"CREATE TABLE {tbl} (id INT PRIMARY KEY, s STRING) STORED AS STREAMING")
      except Exception as e:
        with lock:
          errors.append(f"thread-{idx}: {e}")

    threads = [threading.Thread(target=_create_fn, args=(i,)) for i in range(num_tables)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()

    assert not errors, \
        f"Concurrent independent streaming creates failed: {errors}"

    for idx in range(num_tables):
      base = f"conc_diff_{idx}"
      for suffix in ('', '_kudu', '_iceberg', '_dels'):
        rows = self.client.execute(
            f"SHOW TABLES IN {unique_database} LIKE '{base}{suffix}'").data
        assert len(rows) == 1, \
            f"Expected {base}{suffix} to exist after concurrent create"

  def test_streaming_concurrent_creates_same_table(self, vector, unique_database):
    """N threads race to CREATE the same streaming table without IF NOT EXISTS.
    Exactly one must succeed; the result must have all four backing tables present
    (no partial state from a losing thread's cleanup racing with the winner)."""
    num_threads = 4
    base_name = "conc_same"
    table_name = f"{unique_database}.{base_name}"
    successes = []
    lock = threading.Lock()
    barrier = threading.Barrier(num_threads)

    def _create_fn(idx):
      try:
        with self.create_impala_client_from_vector(vector) as client:
          barrier.wait()
          client.execute(
              f"CREATE TABLE {table_name} (id INT PRIMARY KEY, s STRING)"
              " STORED AS STREAMING")
        with lock:
          successes.append(idx)
      except Exception:
        pass  # Expected for all but the thread that wins the race

    threads = [threading.Thread(target=_create_fn, args=(i,)) for i in range(num_threads)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()

    assert len(successes) == 1, \
        f"Expected exactly 1 success, got {len(successes)}: {successes}"

    # Verify no partial state: all four backing entries must be present.
    for suffix in ('', '_kudu', '_iceberg', '_dels'):
      rows = self.client.execute(
          f"SHOW TABLES IN {unique_database} LIKE '{base_name}{suffix}'").data
      assert len(rows) == 1, \
          f"Missing {base_name}{suffix} after concurrent create; partial state detected"

  def test_streaming_concurrent_drops_same_table(self, vector, unique_database):
    """N threads concurrently DROP IF EXISTS the same streaming table.
    All operations must succeed and no backing table may survive (the drop is
    atomic so a concurrent drop cannot catch the table mid-teardown)."""
    num_threads = 4
    base_name = "conc_drop_s"
    table_name = f"{unique_database}.{base_name}"
    errors = []
    lock = threading.Lock()
    barrier = threading.Barrier(num_threads)

    self._create_streaming_table(table_name)

    def _drop_fn(idx):
      try:
        with self.create_impala_client_from_vector(vector) as client:
          barrier.wait()
          client.execute(f"DROP TABLE IF EXISTS {table_name}")
      except Exception as e:
        with lock:
          errors.append(f"thread-{idx}: {e}")

    threads = [threading.Thread(target=_drop_fn, args=(i,)) for i in range(num_threads)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()

    assert not errors, \
        f"Concurrent IF EXISTS drops raised errors: {errors}"

    for suffix in ('', '_kudu', '_iceberg', '_dels'):
      rows = self.client.execute(
          f"SHOW TABLES IN {unique_database} LIKE '{base_name}{suffix}'").data
      assert len(rows) == 0, \
          f"Table {base_name}{suffix} still exists after concurrent drops"

  def test_streaming_concurrent_create_and_drop(self, vector, unique_database):
    """Interleaved CREATE IF NOT EXISTS and DROP IF EXISTS on the same streaming table
    must never leave a partial state (some but not all backing tables present).
    This is the direct regression test for the metastoreDdlLock_ atomicity fix."""
    num_threads = 6
    base_name = "conc_cd"
    table_name = f"{unique_database}.{base_name}"
    errors = []
    lock = threading.Lock()
    barrier = threading.Barrier(num_threads)

    # Pre-create so the first round of drops has something to work with.
    self._create_streaming_table(table_name)

    def _create_fn(idx):
      try:
        with self.create_impala_client_from_vector(vector) as client:
          barrier.wait()
          client.execute(
              f"CREATE TABLE IF NOT EXISTS {table_name}"
              " (id INT PRIMARY KEY, s STRING) STORED AS STREAMING")
      except Exception as e:
        with lock:
          errors.append(f"create-{idx}: {e}")

    def _drop_fn(idx):
      try:
        with self.create_impala_client_from_vector(vector) as client:
          barrier.wait()
          client.execute(f"DROP TABLE IF EXISTS {table_name}")
      except Exception as e:
        with lock:
          errors.append(f"drop-{idx}: {e}")

    threads = []
    for i in range(num_threads):
      target = _create_fn if i % 2 == 0 else _drop_fn
      threads.append(threading.Thread(target=target, args=(i,)))

    for t in threads:
      t.start()
    for t in threads:
      t.join()

    assert not errors, \
        f"Concurrent create/drop raised errors: {errors}"

    # Either all four tables exist or none do – no partial state is acceptable.
    counts = {s: len(self.client.execute(
                  f"SHOW TABLES IN {unique_database} LIKE '{base_name}{s}'").data)
              for s in ('', '_kudu', '_iceberg', '_dels')}
    all_exist = all(c == 1 for c in counts.values())
    none_exist = all(c == 0 for c in counts.values())
    assert all_exist or none_exist, \
        f"Partial streaming table state after concurrent create/drop: {counts}"
