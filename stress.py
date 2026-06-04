#!/bin/env impala-python3

import argparse
import os
import signal
import subprocess
import time
from impala import dbapi
from multiprocessing import Pool, Process

TARGET_DB = "stream_stress"
# Table name, schema (without PRIMARY KEY), primary key columns, partition columns
# (empty string for none).
TPCH_SCHEMA = [
  ("lineitem", """
  L_ORDERKEY BIGINT,
  L_PARTKEY BIGINT,
  L_SUPPKEY BIGINT,
  L_LINENUMBER INT,
  L_QUANTITY DECIMAL(12,2),
  L_EXTENDEDPRICE DECIMAL(12,2),
  L_DISCOUNT DECIMAL(12,2),
  L_TAX DECIMAL(12,2),
  L_RETURNFLAG STRING,
  L_LINESTATUS STRING,
  L_SHIPDATE STRING,
  L_COMMITDATE STRING,
  L_RECEIPTDATE STRING,
  L_SHIPINSTRUCT STRING,
  L_SHIPMODE STRING,
  L_COMMENT STRING
  """, "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER", "L_ORDERKEY"),
  ("part", """
  P_PARTKEY BIGINT,
  P_NAME STRING,
  P_MFGR STRING,
  P_BRAND STRING,
  P_TYPE STRING,
  P_SIZE INT,
  P_CONTAINER STRING,
  P_RETAILPRICE DECIMAL(12,2),
  P_COMMENT STRING
  """, "P_PARTKEY", "P_PARTKEY"),
  ("partsupp", """
  PS_PARTKEY BIGINT,
  PS_SUPPKEY BIGINT,
  PS_AVAILQTY BIGINT,
  PS_SUPPLYCOST DECIMAL(12,2),
  PS_COMMENT STRING
  """, "PS_PARTKEY, PS_SUPPKEY", "PS_PARTKEY, PS_SUPPKEY"),
  ("supplier", """
  S_SUPPKEY BIGINT,
  S_NAME STRING,
  S_ADDRESS STRING,
  S_NATIONKEY SMALLINT,
  S_PHONE STRING,
  S_ACCTBAL DECIMAL(12,2),
  S_COMMENT STRING
  """, "S_SUPPKEY", "S_SUPPKEY"),
  ("nation", """
  N_NATIONKEY SMALLINT,
  N_NAME STRING,
  N_REGIONKEY SMALLINT,
  N_COMMENT STRING
  """, "N_NATIONKEY", ""),
  ("region", """
  R_REGIONKEY SMALLINT,
  R_NAME STRING,
  R_COMMENT STRING
  """, "R_REGIONKEY", ""),
  ("orders", """
  O_ORDERKEY BIGINT,
  O_CUSTKEY BIGINT,
  O_ORDERSTATUS STRING,
  O_TOTALPRICE DECIMAL(12,2),
  O_ORDERDATE STRING,
  O_ORDERPRIORITY STRING,
  O_CLERK STRING,
  O_SHIPPRIORITY INT,
  O_COMMENT STRING
  """, "O_ORDERKEY", "O_ORDERKEY"),
  ("customer", """
  C_CUSTKEY BIGINT,
  C_NAME STRING,
  C_ADDRESS STRING,
  C_NATIONKEY SMALLINT,
  C_PHONE STRING,
  C_ACCTBAL DECIMAL(12,2),
  C_MKTSEGMENT STRING,
  C_COMMENT STRING
  """, "C_CUSTKEY", "C_CUSTKEY")]

CONNECTION_OPTIONS = {
  "host": "localhost",
  "port": 21050,
  "user": None,
}

def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "--host",
      default=os.environ.get("IMPALA_HOST", "localhost"),
      help="Impala coordinator hostname (default: %(default)s)")
  parser.add_argument(
      "--port",
      type=int,
      default=int(os.environ.get("IMPALA_PORT", "21050")),
      help="Impala coordinator port (default: %(default)s)")
  parser.add_argument(
      "--user",
      default=os.environ.get("IMPALA_USER"),
      help="Username for Impala connections (default: current auth context)")
  parser.add_argument(
      "--source-db",
      default="tpch",
      help="Source TPC-H database to load from (default: %(default)s)")
  parser.add_argument(
      "--parallel-loads",
      type=int,
      default=8,
      help="Number of parallel load workers (default: %(default)s)")
  parser.add_argument(
      "--parallel-queries",
      type=int,
      default=2,
      help="Number of parallel query workers (default: %(default)s)")
  parser.add_argument(
      "--table",
      choices=["kudu", "iceberg", "hybrid"],
      default="hybrid",
      help=("Create Kudu, Iceberg, or hybrid streaming table"))
  parser.add_argument(
      "--updates",
      type=int,
      default=3,
      help=("Periodically update data from stress/table_name.tbl.u* files into the "
            "specified table while queries run"))
  parser.add_argument(
      "--batch-size",
      type=int,
      default=100,
      help=("Number of rows to update in each batch when applying updates from "
            "stress/table_name.tbl.u* files (default: %(default)s)"))
  parser.add_argument(
      "--merge-interval",
      type=int,
      default=15,
      help=("Interval in seconds between merge operations for a hybrid table "
            "(default: %(default)s)"))

  options = parser.parse_args()
  if options.parallel_loads < 1:
    parser.error("--parallel-loads must be >= 1")
  if options.parallel_queries < 1:
    parser.error("--parallel-queries must be >= 1")
  return options

def _connect(database):
  kwargs = {
    "host": CONNECTION_OPTIONS["host"],
    "port": CONNECTION_OPTIONS["port"],
  }
  if CONNECTION_OPTIONS["user"]:
    kwargs["user"] = CONNECTION_OPTIONS["user"]
  if database:
    kwargs["database"] = database
  return dbapi.connect(**kwargs)

def _query_result_to_stdout(rows):
  if not rows:
    return ""
  lines = ["\t".join("" if value is None else str(value) for value in row) for row in rows]
  return ("\n".join(lines) + "\n")

def run(queries, use_target_db=True):
  stdout_chunks = []
  with _connect(TARGET_DB if use_target_db else None) as conn:
    with conn.cursor() as cursor:
      for statement in queries if isinstance(queries, list) else [queries]:
        cursor.execute(statement)
        if cursor.description:
          stdout_chunks.append(_query_result_to_stdout(cursor.fetchall()))
  return "".join(stdout_chunks)

def _format_kudu_partition_spec(partitions):
  return f"PARTITION BY HASH ({partitions}) PARTITIONS 9" if partitions else ""

def _format_iceberg_partition_spec(partitions):
  if not partitions:
    return ""
  partition_columns = [col.strip() for col in partitions.split(',') if col.strip()]
  return f"PARTITIONED BY SPEC ({', '.join(f'BUCKET(9,{col})' for col in partition_columns)})"

def create(table, schema, primary_key, partitions, table_type):
  match table_type:
    case "kudu":
      return run(f"CREATE TABLE {table} ({schema}, PRIMARY KEY({primary_key})) "
                 f"{_format_kudu_partition_spec(partitions)} STORED AS KUDU")
    case "iceberg":
      iceberg_schema = schema.replace("SMALLINT", "INT")  # Iceberg doesn't support SMALLINT
      part = _format_iceberg_partition_spec(partitions)
      return run(f"CREATE TABLE {table} ({iceberg_schema}) {part} STORED AS ICEBERG "
                 f"TBLPROPERTIES('format-version'='3')")
    case "hybrid":
      return run(f"CREATE TABLE {table} ({schema}, PRIMARY KEY({primary_key})) "
                f"{_format_kudu_partition_spec(partitions)} "
                f"{_format_iceberg_partition_spec(partitions)} STORED AS STREAMING")

def load(table, source_db, table_type):
  op = "UPSERT" if table_type == "hybrid" else "INSERT"
  return run([f"{op} INTO TABLE {table} SELECT * FROM {source_db}.{table}",
              f"COMPUTE STATS {table}"])

def update_from_file(table, iter, table_type, batch_size):
  """Update data from stress/table_name.tbl.u* files into the specified table."""
  string_column_indexes = {
    "lineitem": {8, 9, 10, 11, 12, 13, 14, 15},
    "orders": {2, 4, 5, 6, 8},
  }
  orderkey = table[0] + "_orderkey"  # e.g. L_ORDERKEY or O_ORDERKEY
  string_columns = string_column_indexes.get(table, set())
  file_path = f"stress/{table}.tbl.u{iter}"
  del_path = f"stress/delete.{iter}"

  start = time.perf_counter()
  with _connect(TARGET_DB) as conn:
    with conn.cursor() as cursor:
      dml_op = "INSERT" if table_type == "iceberg" else "UPSERT"
      # Insert new data (RF1) in batches (to control file size)
      # then delete by key (RF2) to simulate updates.
      def run_dml(values_list):
        query = f"{dml_op} INTO {table} VALUES {','.join(values_list)}"
        cursor.execute(query)
        if cursor.description:
          print(f"Updated {cursor.fetchall()}")

      # Parse the rows from the file and build an INSERT/UPSERT statement
      # TPC-H format uses pipe-delimited values
      values_list = []
      with open(file_path, 'r') as f:
        for line in f:
          line = line.rstrip('\n')
          if not line:
            continue
          # Quote values for SQL
          quoted_values = [f"'{v}'" if i in string_columns else v
                          for i, v in enumerate(line.split('|'))]
          values_list.append(f"({', '.join(quoted_values)})")

          if len(values_list) > batch_size:
            run_dml(values_list)
            values_list = []

      if values_list:
        run_dml(values_list)

      with open(del_path, 'r') as f:
        del_ids = [line.strip() for line in f if line.strip()]
        cursor.execute(f"DELETE FROM {table} WHERE {orderkey} IN ({','.join(del_ids)})")
        if cursor.description:
          print(f"Deleted {cursor.fetchall()}")

      return time.perf_counter() - start

def run_query(query_file):
  with open(query_file, "r") as file_handle:
    query = file_handle.read()
  return run(query)

def do_updates(num_updates, table_type, batch_size):
  with Pool(processes=4) as pool:
    for update_iter in range(1, num_updates + 1):
      # Update data from stress/table_name.tbl.u* files into the specified table.
      start_update = time.perf_counter()
      pool.starmap(update_from_file, [
          (table, update_iter, table_type, batch_size) for table in ["lineitem", "orders"]])
      update_time = time.perf_counter() - start_update
      print(f"Completed update iteration {update_iter} in {update_time:.2f} seconds.")

      start_counts = time.perf_counter()
      table_counts = pool.map(run, [f"SELECT COUNT(*) FROM {table}" for table, _, _, _ in TPCH_SCHEMA])
      counts_time = time.perf_counter() - start_counts
      print(f"Table counts after iteration {update_iter} ({counts_time:.2f} seconds): "
            f"{[count.strip() for count in table_counts]}")

def do_merges(merge_interval):
  continue_running = True
  def _handle_sigterm(signum, frame):
    del signum, frame
    nonlocal continue_running
    continue_running = False
  signal.signal(signal.SIGTERM, _handle_sigterm)

  with Pool(processes=4) as pool:
    last_merge = 0
    while continue_running:
      seconds_until_merge = merge_interval - (time.perf_counter() - last_merge)
      if seconds_until_merge > 0:
        print(f"Waiting {seconds_until_merge:.2f} seconds until next merge...")
        time.sleep(seconds_until_merge)
        if not continue_running:
          break

      last_merge = time.perf_counter()
      pool.map(run, [f"MIGRATE {table}" for table, _, _, _ in TPCH_SCHEMA])
      print(f"Completed merges in {time.perf_counter() - last_merge:.2f} seconds.")

def main():
  options = parse_args()
  do_merge = options.table == "hybrid"

  global CONNECTION_OPTIONS
  CONNECTION_OPTIONS = {
    "host": options.host,
    "port": options.port,
    "user": options.user,
  }

  print(f"Generating {options.updates} TPC-H incremental updates...")
  subprocess.run([os.getenv("IMPALA_TOOLCHAIN_PACKAGES_HOME") + "/tpc-h-2.17.0/bin/dbgen",
                  "-U", str(options.updates)],
                  cwd=os.getenv("IMPALA_HOME") + "/stress",
                  check=True)

  run([f"drop database if exists {TARGET_DB} cascade", f"create database {TARGET_DB}"],
      use_target_db=False)

  with Pool(processes=options.parallel_loads) as load_pool:
    print(f"Creating TPC-H {options.table} tables in {TARGET_DB}...")
    start_create = time.perf_counter()
    load_pool.starmap(create, [(table, schema, primary_key, partitions, options.table)
                               for table, schema, primary_key, partitions in TPCH_SCHEMA])
    print(f"Created in {time.perf_counter() - start_create:.2f} seconds.")

    print(f"Loading {options.source_db} data into {options.table} tables...")
    start_load = time.perf_counter()
    load_pool.starmap(load, [(table, options.source_db, options.table)
                             for table, _, _, _ in TPCH_SCHEMA])
    print(f"Loaded in {time.perf_counter() - start_load:.2f} seconds.")

  with Pool(processes=options.parallel_queries) as pool:
    # Run options.updates incremental updates in parallel with options.parallel_queries test query runs.
    # Stop new query runs after the last update has loaded. Do merges in parallel with updates.
    start_run = time.perf_counter()
    if do_merge:
      merges = Process(target=do_merges, args=(options.merge_interval,))
      merges.start()
    updates = Process(target=do_updates, args=(options.updates, options.table, options.batch_size,))
    updates.start()
    print("Starting test queries while loading...")
    query_runs = 0
    while updates.is_alive():
      start_queries = time.perf_counter()
      pool.map(run_query, [f"stress/q{i}.sql" for i in range(1, 23)])
      end_queries = time.perf_counter()
      query_runs += 1
      print(f"Ran test queries in {end_queries - start_queries:.2f} seconds.")

    run_time = time.perf_counter() - start_run
    if do_merge:
      merges.terminate()
      merges.join()
    updates.join()
    print(f"Completed {query_runs} query iterations in {run_time:.2f} seconds.")

if __name__ == "__main__":
  main()
