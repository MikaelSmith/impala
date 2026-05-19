#!/bin/env python3

import subprocess
import time
import argparse
from multiprocessing import Pool

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

def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "--source-db",
      default="tpch",
      help="Source TPC-H database to load from (default: %(default)s)")
  parser.add_argument(
      "--parallel-loads",
      type=int,
      default=2,
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
      "--with-deletes",
      action="store_true",
      help=("Periodically delete 1% of rows from a few tables while queries run"))

  options = parser.parse_args()
  if options.parallel_loads < 1:
    parser.error("--parallel-loads must be >= 1")
  if options.parallel_queries < 1:
    parser.error("--parallel-queries must be >= 1")
  return options


def _parse_columns(schema):
  columns = []
  for raw_line in schema.splitlines():
    line = raw_line.strip().rstrip(',')
    if not line:
      continue
    col_name, col_type = line.split(None, 1)
    columns.append((col_name, col_type.strip()))
  return columns


def _split_columns(csv_columns):
  return [col.strip() for col in csv_columns.split(',') if col.strip()]


def _format_schema(columns):
  return ",\n  ".join(f"{col} {col_type}" for col, col_type in columns)


def _format_kudu_partition_spec(partitions):
  return f"PARTITION BY HASH ({partitions}) PARTITIONS 9" if partitions else ""


def _format_iceberg_partition_spec(partitions):
  if not partitions:
    return ""
  partition_columns = [col.strip() for col in partitions.split(',') if col.strip()]
  return f"PARTITIONED BY SPEC ({', '.join(f'BUCKET(9,{col})' for col in partition_columns)})"


def run(query):
  return subprocess.run(["impala-shell.sh", "--quiet", "-B", "-d", TARGET_DB, "-q", query], capture_output=True)


def _create_hybrid(table, schema, primary_key, partitions):
  columns = _parse_columns(schema)
  pk_columns = _split_columns(primary_key)

  iceberg_schema = _format_schema(columns).replace("SMALLINT", "INT")  # Iceberg doesn't support SMALLINT
  partition_spec = _format_iceberg_partition_spec(partitions)
  kudu_partition = _format_kudu_partition_spec(partitions)
  primary_key_clause = f"PRIMARY KEY({', '.join(pk_columns)})"

  type_by_column = {col_name: col_type for col_name, col_type in columns}
  dels_columns = ",\n  ".join(f"{col} {type_by_column[col]}" for col in pk_columns)
  dels_pk = ", ".join(pk_columns)

  statements = [
      f"CREATE TABLE {table}_iceberg ({iceberg_schema}) {partition_spec} STORED AS ICEBERG",
      (f"CREATE TABLE {table}_kudu ({iceberg_schema},\n"
       f"  {primary_key_clause}) {kudu_partition} STORED AS KUDU"),
      f"CREATE TABLE {table}_dels ({dels_columns},\n"
      f"  NON UNIQUE PRIMARY KEY({dels_pk})) STORED AS KUDU",
      (f"CREATE TABLE {table}_pit (id INT PRIMARY KEY, migration_ts BIGINT, "
       f"snapshot_id BIGINT) STORED AS KUDU"),
      (f"CREATE TABLE {table} ({iceberg_schema}) STORED AS ICEBERG "
       f"TBLPROPERTIES('impala.streaming.kudu'='{table}_kudu', "
       f"'impala.streaming.iceberg'='{table}_iceberg', "
       f"'impala.streaming.pit'='{table}_pit', "
       f"'impala.streaming.dels'='{table}_dels')")
  ]

  return run("; ".join(statements))

def create(table, schema, primary_key, partitions, table_type):
  match table_type:
    case "kudu":
      pk_clause = f"PRIMARY KEY({primary_key})"
      part = f"PARTITION BY HASH ({partitions}) PARTITIONS 9" if partitions else ""
      return run(f"CREATE TABLE {table} ({schema}, {pk_clause}) {part} STORED AS KUDU")
    case "iceberg":
      columns = _parse_columns(schema)
      iceberg_schema = _format_schema(columns).replace("SMALLINT", "INT")  # Iceberg doesn't support SMALLINT
      part = _format_iceberg_partition_spec(partitions)
      return run(f"CREATE TABLE {table} ({iceberg_schema}) {part} STORED AS ICEBERG")
    case "hybrid":
      return _create_hybrid(table, schema, primary_key, partitions)

def load(table, source_db, table_type):
  op = "UPSERT" if table_type == "hybrid" else "INSERT"
  return run(f"{op} INTO TABLE {table} SELECT * FROM {source_db}.{table}; COMPUTE STATS {table}")

def merge(table):
  start = time.perf_counter()
  run(f"MIGRATE {table}")
  return time.perf_counter() - start

def delete(table, key, iter):
  start = time.perf_counter()
  run(f"DELETE {table} WHERE {key} % 100 = {iter}")
  return time.perf_counter() - start

STEP_CREATE = ["creating", "created"]
STEP_LOAD = ["loading", "loaded"]
STEP_QUERY = ["running", "ran"]

def print_ready(step, results):
  finished = set()
  for label, future in results.items():
    if future.ready():
      result = future.get()
      if result.returncode != 0:
        print(f"Error {step[0]} {label}: {result}")
      else:
        print(f"{label} successfully {step[1]}")
      finished.add(label)
  for label in finished:
    del results[label]

def run_query(query_file):
  return subprocess.run(["impala-shell.sh", "--quiet", "-d", TARGET_DB,
      "-f", query_file], capture_output=True)

def run_queries(pool):
    results = {f"q{i}": pool.apply_async(run_query, (f"stress/q{i}.sql",)) for i in range(1, 23)}
    while results:
      time.sleep(0.01)
      print_ready(STEP_QUERY, results)

def main():
  options = parse_args()
  do_merge = options.table == "hybrid"

  subprocess.run(["impala-shell.sh",
      "-q", f"drop database if exists {TARGET_DB} cascade; create database {TARGET_DB}"])

  with Pool(processes=options.parallel_loads) as load_pool, \
       Pool(processes=options.parallel_queries) as query_pool, Pool(processes=4) as merge_pool:
    print(f"Creating TPC-H {options.table} table in {TARGET_DB}...")
    results = {table: merge_pool.apply_async(
        create, (table, schema, primary_key, partitions, options.table))
               for table, schema, primary_key, partitions in TPCH_SCHEMA}
    while results:
      time.sleep(0.01)
      print_ready(STEP_CREATE, results)

    print(f"Loading {options.source_db} data into {options.table} table...")
    start_load = time.perf_counter()
    results = {table: load_pool.apply_async(load, (table, options.source_db, options.table))
               for table, _, _, _ in TPCH_SCHEMA}

    query_runs = 0
    while results:
      if options.with_deletes:
        # Delete 1% of rows from a few tables
        customer_del = merge_pool.apply_async(delete, ("customer", "C_CUSTKEY", query_runs))
        supplier_del = merge_pool.apply_async(delete, ("supplier", "S_SUPPKEY", query_runs))
      # Start table migration while queries run.
      if do_merge:
        merge_result = [merge_pool.apply_async(merge, (table,)) for table, _, _, _ in TPCH_SCHEMA]
      # Optional; doesn't work on hybrid table yet.
      # subprocess.run(["impala-shell.sh", "--quiet", "-d", TARGET_DB, "-q", f"COMPUTE STATS lineitem"])
      print("Starting test queries while loading...")
      start_queries = time.perf_counter()
      run_queries(query_pool)
      end_queries = time.perf_counter()
      query_runs += 1
      print(f"Ran test queries in {end_queries - start_queries:.2f} seconds.")
      # TODO: move before queries?
      print_ready(STEP_LOAD, results)
      if options.with_deletes:
        print(f"Deleted 1% of rows from customer with iter={query_runs} in {customer_del.get():.2f} seconds.")
        print(f"Deleted 1% of rows from supplier with iter={query_runs} in {supplier_del.get():.2f} seconds.")
      if do_merge:
        print(f"Merge completed in {sum([merge_result.get() for merge_result in merge_result]):.2f} seconds.")
      start_counts = time.perf_counter()
      table_counts = merge_pool.map(run, [f"SELECT COUNT(*) FROM {table}" for table, _, _, _ in TPCH_SCHEMA])
      end_counts = time.perf_counter()
      print(f"Table counts after iteration {query_runs} ({end_counts - start_counts:.2f} seconds): "
            f"{[count.stdout.decode().strip() for count in table_counts]}")
    end_load = time.perf_counter()
    print(f"Loaded in {end_load - start_load:.2f} seconds with {query_runs} runs of the test queries.")


if __name__ == "__main__":
  main()
