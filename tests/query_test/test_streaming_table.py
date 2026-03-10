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
from copy import deepcopy

from tests.common.impala_test_suite import ImpalaTestSuite


class TestStreamingTable(ImpalaTestSuite):
  """Tests related to streaming tables."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestStreamingTable, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_streaming(self, vector, unique_database):
    # TODO: allow re-analyze rather than fail because migration already started.
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['test_replan'] = 0
    self.run_test_case('QueryTest/streaming', new_vector, use_db=unique_database)
