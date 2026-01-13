# Project Overview

Apache Impala is an open-source, distributed SQL query engine for large-scale data analytics. It provides low-latency, high-concurrency queries on Hadoop ecosystems, supporting Iceberg, Parquet, ORC, and various cloud storage options.

## Architecture

Impala has a distributed, multi-language architecture with clear separation between planning (Java) and execution (C++):

**Query Execution Flow:**
1. Client connects to impalad coordinator via Beeswax, HiveServer2, or HTTP protocols
2. C++ coordinator calls Java frontend via JNI (`be/src/service/frontend.cc`)
3. Java frontend (`fe/src/main/java/org/apache/impala/service/Frontend.java`) performs parsing, analysis, planning
4. TExecRequest (Thrift) returned to C++ containing distributed execution plan
5. C++ coordinator (`be/src/runtime/coordinator.cc`) sends ExecQueryFInstances RPCs to executors
6. Each executor runs query fragments, returns results to coordinator

**Key Components:**
- **impalad** (daemon): Coordinator and/or executor. Written in C++ for execution engine, calls Java for query planning. See `be/src/service/impala-server.cc` and `be/src/runtime/coordinator.cc`
- **catalogd** (daemon): Metadata caching layer (Java with C++ networking). Fetches from Hive Metastore/Iceberg REST catalogs, distributes to impalads
- **statestored** (daemon): Cluster membership and health tracking (C++)
- **impala-shell**: Python CLI client using Beeswax/HS2 protocol

**Cross-Language Communication:**
- Java ↔ C++ via JNI: See `be/src/util/jni-util.h` and `fe/src/main/java/org/apache/impala/service/JniFrontend.java`
- Thrift serialization for all inter-component RPCs: `common/thrift/` defines shared structs
- Key Thrift files: `Frontend.thrift` (C++→Java), `ImpalaInternalService.thrift` (coordinator↔executor)

## Folder Structure

- `/be`: C++ backend (execution engine, runtime, storage). Build: CMake + Ninja/Make. Tests: GoogleTest via CTest
- `/fe`: Java frontend (query planner). Build: Maven (`java/pom.xml` is parent POM). Tests: JUnit. Java 17 required
- `/common`: Thrift/Protobuf/Flatbuffer definitions shared across components
- `/java`: Maven sub-projects (utilities, test frameworks, Calcite planner alternative)
- `/bin`: Build and development scripts. **Source `bin/impala-config.sh` first for all dev work**
- `/tests`: Python integration tests using pytest. Base class: `tests/common/impala_test_suite.py`
- `/testdata`: Test datasets and data loading scripts
- `/toolchain`: Pre-compiled dependencies (LLVM, Boost, etc.) downloaded during build

## Developer Workflows

**Initial Setup:**
```bash
source bin/impala-config.sh  # REQUIRED - sets IMPALA_HOME, JAVA_HOME, classpaths, etc.
./buildall.sh                 # Full build (backend + frontend + tests)
```

**Build Commands:**
```bash
./buildall.sh -notests       # Skip tests (faster)
./buildall.sh -noclean       # Incremental build
./buildall.sh -release       # Release build (default: Debug)
./buildall.sh -so            # Build shared libraries

# Backend only (from be/)
ninja                         # or: make -j${IMPALA_BUILD_THREADS}

# Frontend only
cd java && mvn clean package -DskipTests
```

**Running Tests:**
```bash
# Backend C++ tests
bin/run-backend-tests.sh     # Runs CTest (GoogleTest-based tests)

# Frontend Java tests
cd java && mvn test

# Python integration tests (requires running cluster)
bin/run-all-tests.sh          # Full suite
pytest tests/query_test/test_queries.py  # Specific test file
pytest -k "test_name" -x      # Stop on first failure
```

**Key Environment Variables:** (see `README-build.md` and `bin/impala-config.sh`)
- `IMPALA_HOME`: Project root (auto-set by impala-config.sh)
- `IMPALA_BUILD_THREADS`: Parallel build jobs (default: nproc)
- `CMAKE_BUILD_TYPE`: Debug (default), Release, ASAN, TSAN, UBSAN
- `IMPALA_MAKE_CMD`: "make" or "ninja" (default: make)

## Coding Standards

**Line Length & Formatting:**
- 90 character limit (strictly enforced except for exclusions in `bin/jenkins/critique-gerrit-review.py`)
- Break long lines at high-level constructs (after commas, before operators), indent continuation 4 spaces
- 2 space indentation (never tabs). No trailing whitespace

**C++ Standards** (`.clang-format` for auto-formatting):
- Based on Google C++ Style with exceptions:
  - `#pragma once` for include guards (not `#ifdef` macros)
  - `UPPER_CASE` constant names (not `kConstantName`)
  - `.inline.h` suffix for inline function definitions (separate from headers)
  - `using namespace` allowed in `.cc` files only
  - Single-line `if` statements only when entire statement fits in 90 chars; otherwise always use braces
- GoogleTest for unit tests: `TEST_F(ClassName, TestName)` pattern. See `be/src/testutil/gtest-util.h`

**Java Standards** (`.clang-format` applies to Java too):
- Maven build: `java/pom.xml` is parent, `fe/pom.xml` is main module
- JUnit for tests: `@Test`, `@Before`, `@After` annotations. Base class: `fe/src/test/java/org/apache/impala/common/FrontendTestBase.java`

**Python Standards:**
- Integration tests inherit from `ImpalaTestSuite` in `tests/common/impala_test_suite.py`
- Use pytest fixtures for test data/dimensions

**Licensing:**
- Include Apache 2.0 header in all new source files (copy from existing files)

## Critical Patterns

**JNI Integration Pattern:**
When C++ needs Java services (query planning, metadata operations):
```cpp
// be/src/service/frontend.cc
JNIEnv* env = JniUtil::GetJNIEnv();
JniLocalFrame frame; frame.push(env);
jobject result = env->CallObjectMethod(frontend_instance_, method_id_, params);
// Deserialize Thrift result
```

**Thrift Communication:**
All component communication uses Thrift. When adding new operations:
1. Define structs/services in `common/thrift/*.thrift`
2. Rebuild to generate code: `make` (backend) or `mvn generate-sources` (frontend)
3. Generated code: `be/generated-sources/` (C++), `fe/generated-sources/` (Java)

**Test Data Dimensions:**
Python integration tests use multi-dimensional test vectors (file format, compression, exec options):
```python
class TestExample(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super().add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension())
```
