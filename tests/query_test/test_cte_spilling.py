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

# Tests for spilling of CTE results buffered between a CTE PRODUCER and its CTE
# CONSUMERs (Calcite planner).

from __future__ import absolute_import, division, print_function
import re

import pytest

from tests.common.environ import ImpalaTestClusterProperties
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
    add_mandatory_exec_option, create_exec_option_dimension_from_dict,
    create_parquet_dimension)

IMPALA_TEST_CLUSTER_PROPERTIES = ImpalaTestClusterProperties.get_instance()

DENY_RESERVATION = '-1:OPEN:SET_DENY_RESERVATION_PROBABILITY@1.0'

# The UNION ALL reads its children one after another, so the second consumer only
# starts once the first is done: the whole CTE result must be buffered.
UNION_QUERY = """
WITH v AS (
  SELECT l_orderkey, l_partkey, l_comment FROM tpch_parquet.lineitem
  WHERE l_shipmode = 'AIR')
SELECT count(*), sum(l_orderkey), sum(l_partkey), min(l_comment), max(l_comment)
FROM (SELECT * FROM v UNION ALL SELECT * FROM v) u"""

# The probe-side consumer of the join waits for the build-side consumer to finish.
JOIN_QUERY = """
WITH v AS (
  SELECT l_orderkey, l_partkey, l_comment FROM tpch_parquet.lineitem
  WHERE l_shipmode = 'AIR')
SELECT count(*), sum(a.l_partkey), sum(b.l_partkey), max(a.l_comment),
  max(b.l_comment)
FROM v a, v b WHERE a.l_orderkey + 1 = b.l_orderkey"""

# Rows of 120KB-520KB. The producer's pages fit the largest row (see
# LARGE_ROWS_MAX_ROW_SIZE), so only a few rows fit in a page and many rows don't fit in
# the remainder of a page. Segments created while a consumer is waiting have smaller
# pages, so rows added to them need a large page.
LARGE_ROWS_QUERY = """
WITH v AS (
  SELECT l_orderkey, repeat(l_comment, 12000) AS s FROM tpch_parquet.lineitem
  WHERE l_orderkey < 200 AND l_shipmode = 'AIR')
SELECT count(*), sum(l_orderkey), sum(length(s)), max(substr(s, 1, 20))
FROM (SELECT * FROM v UNION ALL SELECT * FROM v) u"""

# Leaves room above the largest row of LARGE_ROWS_QUERY (43 * 12000 bytes plus the fixed
# part), which the default max_row_size of 512KB barely fits.
LARGE_ROWS_MAX_ROW_SIZE = '1m'

# The projected CTE output has NULL, empty, small (inlined) and regular strings, a CHAR
# and two BIGINTs in a different order than in the table.
STRINGS_QUERY = """
WITH v AS (
  SELECT l_suppkey, l_partkey,
    CASE l_linenumber WHEN 1 THEN NULL WHEN 2 THEN '' ELSE l_shipinstruct END AS s,
    CAST(l_returnflag AS CHAR(3)) AS c, l_comment
  FROM tpch_parquet.lineitem WHERE l_shipmode = 'AIR')
SELECT count(*), count(s), sum(length(s)), min(s), max(s), count(c), min(c), max(c),
  sum(l_suppkey), sum(l_partkey), sum(l_partkey - l_suppkey), sum(length(l_comment)),
  min(l_comment)
FROM (SELECT * FROM v UNION ALL SELECT * FROM v) u"""

QUERIES = {'union': UNION_QUERY, 'join': JOIN_QUERY, 'large_rows': LARGE_ROWS_QUERY,
           'strings': STRINGS_QUERY}


@pytest.mark.xfail(IMPALA_TEST_CLUSTER_PROPERTIES.is_remote_cluster(),
                   reason='Queries may not spill on larger clusters')
class TestCteSpilling(ImpalaTestSuite):

  @classmethod
  def add_test_dimensions(cls):
    super(TestCteSpilling, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.clear_constraints()
    cls.ImpalaTestMatrix.add_dimension(create_parquet_dimension('tpch'))
    # num_nodes=1 places the CTE producer and consumers in the same fragment.
    # disable_codegen covers the interpreted LocalExchanger::AddRows().
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension_from_dict({
        'debug_action': [None, DENY_RESERVATION],
        'mt_dop': [0, 2],
        'num_nodes': [0, 1],
        'disable_codegen': [False, True]}))
    if cls.exploration_strategy() != 'exhaustive':
      cls.ImpalaTestMatrix.add_constraint(lambda v:
          (v.get_value('exec_option')['mt_dop'] == 0
           or v.get_value('exec_option')['num_nodes'] == 0)
          and (not v.get_value('exec_option')['disable_codegen']
               or (v.get_value('exec_option')['mt_dop'] == 0
                   and v.get_value('exec_option')['num_nodes'] == 0)))
    add_mandatory_exec_option(cls, 'planner', 'CALCITE')
    add_mandatory_exec_option(cls, 'fallback_planner', 'CALCITE')
    add_mandatory_exec_option(cls, 'cte_threshold', 1)
    # Small buffers so that the queries need many buffers and spill quickly.
    add_mandatory_exec_option(cls, 'default_spillable_buffer_size', '256k')

  def _expected_result(self, query, exec_option):
    """Returns the result of 'query' computed without CTEs and without spilling."""
    options = dict(exec_option)
    options['cte_threshold'] = -1
    options.pop('debug_action', None)
    return self.execute_query_expect_success(self.client, query, options).data

  @pytest.mark.parametrize('query_name', sorted(QUERIES.keys()))
  def test_cte_spilling(self, vector, query_name):
    query = QUERIES[query_name]
    exec_option = vector.get_value('exec_option')
    if query_name == 'large_rows':
      exec_option = dict(exec_option, max_row_size=LARGE_ROWS_MAX_ROW_SIZE)
    explain = self.execute_query_expect_success(
        self.client, 'explain ' + query, exec_option).data
    assert any('CTE PRODUCER' in line for line in explain), '\n'.join(explain)

    expected = self._expected_result(query, exec_option)
    result = self.execute_query_expect_success(self.client, query, exec_option)
    assert result.data == expected

    spilled = [int(n) for n in
        re.findall(r'SegmentsSpilled: (\d+)', result.runtime_profile)]
    assert spilled, "No LocalExchanger in profile:\n" + result.runtime_profile
    if exec_option['debug_action'] == DENY_RESERVATION:
      # The producer can't grow its reservation beyond its minimum, so it must spill.
      assert sum(spilled) > 0, result.runtime_profile
      assert re.search(r'SegmentsRepinned: [1-9]', result.runtime_profile)
    else:
      # Plenty of memory is available, so the exchanger grows instead of spilling.
      assert sum(spilled) == 0, result.runtime_profile

  def test_cte_spilling_scratch_disabled(self, vector):
    """With spilling disabled, a CTE exchanger that can't grow its reservation fails
    with a clear error instead of exceeding its memory."""
    exec_option = dict(vector.get_value('exec_option'))
    if exec_option['debug_action'] != DENY_RESERVATION:
      pytest.skip('Only relevant when the exchanger has to spill')
    exec_option['scratch_limit'] = 0
    error = self.execute_query_expect_failure(self.client, UNION_QUERY, exec_option)
    assert 'Could not free memory by spilling to disk: scratch_limit is 0' in str(error)
