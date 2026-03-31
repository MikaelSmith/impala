#!/bin/env python3

import subprocess
import time
import argparse
from multiprocessing import Pool

TARGET_DB = "stream_stress"
# Table name, schema, partition columns (empty string for none).
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
  L_COMMENT STRING,
  PRIMARY KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
  """, "L_ORDERKEY"),
  ("part", """
  P_PARTKEY BIGINT PRIMARY KEY,
  P_NAME STRING,
  P_MFGR STRING,
  P_BRAND STRING,
  P_TYPE STRING,
  P_SIZE INT,
  P_CONTAINER STRING,
  P_RETAILPRICE DECIMAL(12,2),
  P_COMMENT STRING
  """, "P_PARTKEY"),
  ("partsupp", """
  PS_PARTKEY BIGINT,
  PS_SUPPKEY BIGINT,
  PS_AVAILQTY BIGINT,
  PS_SUPPLYCOST DECIMAL(12,2),
  PS_COMMENT STRING,
  PRIMARY KEY(PS_PARTKEY, PS_SUPPKEY)
  """, "PS_PARTKEY, PS_SUPPKEY"),
  ("supplier", """
  S_SUPPKEY BIGINT PRIMARY KEY,
  S_NAME STRING,
  S_ADDRESS STRING,
  S_NATIONKEY SMALLINT,
  S_PHONE STRING,
  S_ACCTBAL DECIMAL(12,2),
  S_COMMENT STRING
  """, "S_SUPPKEY"),
  ("nation", """
  N_NATIONKEY SMALLINT PRIMARY KEY,
  N_NAME STRING,
  N_REGIONKEY SMALLINT,
  N_COMMENT STRING
  """, ""),
  ("region", """
  R_REGIONKEY SMALLINT PRIMARY KEY,
  R_NAME STRING,
  R_COMMENT STRING
  """, ""),
  ("orders", """
  O_ORDERKEY BIGINT PRIMARY KEY,
  O_CUSTKEY BIGINT,
  O_ORDERSTATUS STRING,
  O_TOTALPRICE DECIMAL(12,2),
  O_ORDERDATE STRING,
  O_ORDERPRIORITY STRING,
  O_CLERK STRING,
  O_SHIPPRIORITY INT,
  O_COMMENT STRING
  """, "O_ORDERKEY"),
  ("customer", """
  C_CUSTKEY BIGINT PRIMARY KEY,
  C_NAME STRING,
  C_ADDRESS STRING,
  C_NATIONKEY SMALLINT,
  C_PHONE STRING,
  C_ACCTBAL DECIMAL(12,2),
  C_MKTSEGMENT STRING,
  C_COMMENT STRING
  """, "C_CUSTKEY")]

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

  options = parser.parse_args()
  if options.parallel_loads < 1:
    parser.error("--parallel-loads must be >= 1")
  if options.parallel_queries < 1:
    parser.error("--parallel-queries must be >= 1")
  return options

def create(table, schema, partitions):
  part = f"PARTITION BY HASH ({partitions}) PARTITIONS 9" if partitions else ""
  return subprocess.run(["impala-shell.sh", "--quiet", "-d", TARGET_DB,
      "-q", f"CREATE TABLE {table} ({schema}) {part} STORED AS KUDU"], capture_output=True)

def load(table, source_db):
  return subprocess.run(["impala-shell.sh", "--quiet", "-d", TARGET_DB, "-q",
  f"INSERT INTO TABLE {table} SELECT * FROM {source_db}.{table}; COMPUTE STATS {table}"],
      capture_output=True)

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

  # Create Kudu tables so we can start querying them.
  subprocess.run(["impala-shell.sh",
      "-q", f"drop database if exists {TARGET_DB} cascade; create database {TARGET_DB}"])

  with Pool(processes=options.parallel_loads) as load_pool:
    print(f"Creating TPC-H Kudu tables at {TARGET_DB}...")
    results = {table: load_pool.apply_async(create, (table, schema, partitions))
               for table, schema, partitions in TPCH_SCHEMA}
    while results:
      time.sleep(0.01)
      print_ready(STEP_CREATE, results)

    print(f"Loading {options.source_db} data into Kudu tables...")
    start_load = time.perf_counter()
    results = {table: load_pool.apply_async(load, (table, options.source_db))
               for table, _, _ in TPCH_SCHEMA}

    query_runs = 0
    with Pool(processes=options.parallel_queries) as query_pool:
      while results:
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
    end_load = time.perf_counter()
    print(f"Loaded in {end_load - start_load:.2f} seconds with {query_runs} runs of the test queries.")


if __name__ == "__main__":
  main()
