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
import logging

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import build_flavor_timeout
from time import sleep

LOG = logging.getLogger('catalogd_ha_test')
DEFAULT_STATESTORE_SERVICE_PORT = 24000
DEFAULT_CATALOG_SERVICE_PORT = 26000


class TestCatalogdHA(CustomClusterTestSuite):
  """A simple wrapper class to launch a cluster with catalogd HA enabled.
  The cluster will be launched with two catalogd instances as Active-Passive HA pair.
  statestored and catalogds are started with starting flag FLAGS_enable_catalogd_ha
  as true. """

  def get_workload(self):
    return 'functional-query'

  # Verify port of the active catalogd of statestore is matching with the catalog
  # service port of the given catalogd service.
  def __verify_statestore_active_catalogd_port(self, catalogd_service):
    statestore_service = self.cluster.statestored.service
    active_catalogd_address = \
        statestore_service.get_metric_value("statestore.active-catalogd-address")
    _, catalog_service_port = active_catalogd_address.split(":")
    assert(int(catalog_service_port) == catalogd_service.get_catalog_service_port())

  # Verify port of the active catalogd of impalad is matching with the catalog
  # service port of the given catalogd service.
  def __verify_impalad_active_catalogd_port(self, impalad_index, catalogd_service):
    impalad_service = self.cluster.impalads[impalad_index].service
    active_catalogd_address = \
        impalad_service.get_metric_value("catalog.active-catalogd-address")
    _, catalog_service_port = active_catalogd_address.split(":")
    assert(int(catalog_service_port) == catalogd_service.get_catalog_service_port())

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    start_args="--enable_catalogd_ha")
  def test_catalogd_ha_with_two_catalogd(self):
    """The test case for cluster started with catalogd HA enabled."""
    # Verify two catalogd instances are created with one as active.
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 2)
    catalogd_service_1 = catalogds[0].service
    catalogd_service_2 = catalogds[1].service
    assert(catalogd_service_1.get_metric_value("catalog-server.ha-active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.ha-active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)
    # Verify simple query is ran successfully.
    self.execute_query_expect_success(
        self.client, "select count(*) from functional.alltypes")

    # Restart one coordinator. Verify it get active catalogd address from statestore.
    self.cluster.impalads[0].restart()
    self.cluster.impalads[0].service.wait_for_metric_value('impala-server.ready',
        expected_value=1, timeout=30)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)

  @CustomClusterTestSuite.with_args(
    statestored_args="--enable_catalogd_ha=true "
                     "--use_subscriber_id_as_catalogd_priority=true "
                     "--catalogd_ha_preemption_wait_period_ms=200",
    catalogd_args="--enable_catalogd_ha=true")
  def test_catalogd_ha_with_one_catalogd(self):
    """The test case for cluster with only one catalogd when catalogd HA is enabled."""
    # Verify the catalogd instances is created as active.
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 1)
    catalogd_service_1 = catalogds[0].service
    assert(catalogd_service_1.get_metric_value("catalog-server.ha-active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)
    # Verify simple query is ran successfully.
    self.execute_query_expect_success(
        self.client, "select count(*) from functional.alltypes")

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true "
                     "--statestore_heartbeat_frequency_ms=1000",
    start_args="--enable_catalogd_ha")
  def test_catalogd_auto_failover(self):
    """Stop active catalogd and verify standby catalogd becomes active.
    Restart original active catalogd. Verify that statestore does not resume its
    active role."""
    # Verify two catalogd instances are created with one as active.
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 2)
    catalogd_service_1 = catalogds[0].service
    catalogd_service_2 = catalogds[1].service
    assert(catalogd_service_1.get_metric_value("catalog-server.ha-active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.ha-active-status"))

    statestore_service = self.cluster.statestored.service
    start_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")

    # Kill active catalogd
    catalogds[0].kill()

    # Wait for long enough for the statestore to detect the failure of active catalogd
    # and assign active role to standby catalogd.
    catalogd_service_2.wait_for_metric_value(
        "catalog-server.ha-active-status", expected_value=True, timeout=30)
    assert(catalogd_service_2.get_metric_value(
        "catalog-server.ha-number-active-status-change") > 0)
    assert(catalogd_service_2.get_metric_value("catalog-server.ha-active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_2)
    # Verify simple query is ran successfully.
    self.execute_query_expect_success(
        self.client, "select count(*) from functional.alltypes")

    end_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")
    assert end_count_clear_topic_entries > start_count_clear_topic_entries

    # Restart original active catalogd. Verify that statestore does not resume it as
    # active to avoid flip-flop.
    catalogds[0].start(wait_until_ready=True)
    sleep(1)
    catalogd_service_1 = catalogds[0].service
    assert(not catalogd_service_1.get_metric_value("catalog-server.ha-active-status"))
    assert(catalogd_service_2.get_metric_value("catalog-server.ha-active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_2)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true "
                     "--statestore_heartbeat_frequency_ms=1000",
    start_args="--enable_catalogd_ha")
  def test_catalogd_manual_failover(self):
    """Stop active catalogd and verify standby catalogd becomes active.
    Restart original active catalogd with force_catalogd_active as true. Verify that
    statestore resume it as active.
    """
    # Verify two catalogd instances are created with one as active.
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 2)
    catalogd_service_1 = catalogds[0].service
    catalogd_service_2 = catalogds[1].service
    assert(catalogd_service_1.get_metric_value("catalog-server.ha-active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.ha-active-status"))

    statestore_service = self.cluster.statestored.service
    start_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")

    # Kill active catalogd
    catalogds[0].kill()

    # Wait for long enough for the statestore to detect the failure of active catalogd
    # and assign active role to standby catalogd.
    catalogd_service_2.wait_for_metric_value(
        "catalog-server.ha-active-status", expected_value=True, timeout=30)
    assert(catalogd_service_2.get_metric_value(
        "catalog-server.ha-number-active-status-change") > 0)
    assert(catalogd_service_2.get_metric_value("catalog-server.ha-active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_2)

    # Verify simple query is ran successfully.
    self.execute_query_expect_success(
        self.client, "select count(*) from functional.alltypes")

    end_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")
    assert end_count_clear_topic_entries > start_count_clear_topic_entries
    start_count_clear_topic_entries = end_count_clear_topic_entries

    # Restart original active catalogd with force_catalogd_active as true.
    # Verify that statestore resume it as active.
    catalogds[0].start(wait_until_ready=True,
                       additional_args="--force_catalogd_active=true")
    catalogd_service_1 = catalogds[0].service
    catalogd_service_1.wait_for_metric_value(
        "catalog-server.ha-active-status", expected_value=True, timeout=15)
    assert(catalogd_service_1.get_metric_value("catalog-server.ha-active-status"))
    sleep_time_s = build_flavor_timeout(2, slow_build_timeout=5)
    sleep(sleep_time_s)
    assert(not catalogd_service_2.get_metric_value("catalog-server.ha-active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)

    end_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")
    assert end_count_clear_topic_entries > start_count_clear_topic_entries

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    start_args="--enable_catalogd_ha")
  def test_restart_statestore(self):
    """The test case for restarting statestore after the cluster is created with
    catalogd HA enabled."""
    # Verify two catalogd instances are created with one as active.
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 2)
    catalogd_service_1 = catalogds[0].service
    catalogd_service_2 = catalogds[1].service
    assert(catalogd_service_1.get_metric_value("catalog-server.ha-active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.ha-active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)

    # Restart statestore. Verify one catalogd is assigned as active, the other is
    # assigned as standby.
    self.cluster.statestored.restart()
    wait_time_s = build_flavor_timeout(90, slow_build_timeout=180)
    self.cluster.statestored.service.wait_for_metric_value('statestore.live-backends',
        expected_value=5, timeout=wait_time_s)
    sleep_time_s = build_flavor_timeout(2, slow_build_timeout=5)
    sleep(sleep_time_s)
    assert(catalogd_service_1.get_metric_value("catalog-server.ha-active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.ha-active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)
    # Verify simple query is ran successfully.
    self.execute_query_expect_success(
        self.client, "select count(*) from functional.alltypes")
