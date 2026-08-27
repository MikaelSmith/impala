// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.util;

import static org.apache.impala.util.CatalogBlacklistUtils.getBlacklistedDbsCount;
import static org.apache.impala.util.CatalogBlacklistUtils.getBlacklistedTablesCount;
import static org.apache.impala.util.CatalogBlacklistUtils.getBlacklistedTablesDbs;
import static org.apache.impala.util.CatalogBlacklistUtils.isDbBlacklisted;
import static org.apache.impala.util.CatalogBlacklistUtils.isTableBlacklisted;
import static org.apache.impala.util.CatalogBlacklistUtils.reload;
import static org.apache.impala.util.CatalogBlacklistUtils.verifyDbName;
import static org.apache.impala.util.CatalogBlacklistUtils.verifyTableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

public class CatalogBlacklistUtilsTest {

  private static TBackendGflags origFlags;

  @BeforeAll
  public static void setup() {
    // The original BackendConfig need to be saved so they can be restored and not break
    // other tests.
    if (BackendConfig.INSTANCE == null) {
      BackendConfig.create(new TBackendGflags());
    }
    origFlags = BackendConfig.INSTANCE.getBackendCfg();
  }

  @AfterAll
  public static void teardown() {
    BackendConfig.create(origFlags);
  }

  @Test
  public void testParsingBlacklistedDbsHappyPath() throws AnalysisException {
    setBlacklist("db1,db2", "");

    assertEquals(2, getBlacklistedDbsCount());
    assertTrue(isDbBlacklisted("db1"));
    assertTrue(isDbBlacklisted("db2"));
    assertFalse(isDbBlacklisted("db3"));

    verifyDbName("db3");
    try {
      verifyDbName("db1");
      fail("Expected AnalysisException for blacklisted db");
    } catch (AnalysisException e) {
      assertEquals("Invalid db name: db1. It has been blacklisted "
          + "using --blacklisted_dbs", e.getMessage());
    }
  }

  @Test
  public void testParsingBlacklistedDbsNamesWithSpaces() {
    setBlacklist(" db1 , db2 ", "");

    assertEquals(2, getBlacklistedDbsCount());
    assertTrue(isDbBlacklisted("db1"));
    assertTrue(isDbBlacklisted("db2"));
    assertFalse(isDbBlacklisted("db3"));
  }

  @Test
  public void testParsingBlacklistedDbsCaseInsensitiveNames() {
    setBlacklist("DB1,Db2", "");

    assertEquals(2, getBlacklistedDbsCount());
    assertTrue(isDbBlacklisted("db1"));
    assertTrue(isDbBlacklisted("db2"));
    assertFalse(isDbBlacklisted("db3"));
  }

  @Test
  public void testParsingBlacklistedDbsInvalidNames() {
    setBlacklist("db1,", "");

    assertEquals(1, getBlacklistedDbsCount());
    assertTrue(isDbBlacklisted("db1"));
    assertFalse(isDbBlacklisted("db2"));
    assertFalse(isDbBlacklisted("db3"));
  }

  @Test
  public void testParsingBlacklistedDbsNone() {
    assertEquals(0, getBlacklistedDbsCount());
  }

  @Test
  public void testParsingBlacklistedTablesHappyPath() throws AnalysisException {
    TableName foo = new TableName("db3", "foo");
    TableName baz = new TableName("db3", "baz");
    setBlacklist("", "db3.foo,db3.bar,db4.tbl1");

    assertEquals(3, getBlacklistedTablesCount());
    assertTrue(isTableBlacklisted(foo.getDb(), foo.getTbl()));
    assertTrue(isTableBlacklisted(foo));
    assertTrue(isTableBlacklisted("db3", "bar"));
    assertTrue(isTableBlacklisted(new TableName("db3", "bar")));
    assertFalse(isTableBlacklisted(baz.getDb(), baz.getTbl()));
    assertFalse(isTableBlacklisted(baz));
    assertTrue(isTableBlacklisted("db4", "tbl1"));
    assertBlacklistedTableDbs("db3", "db4");

    verifyTableName(baz);
    try {
      verifyTableName(foo);
      fail("Expected AnalysisException for blacklisted table");
    } catch (AnalysisException e) {
      assertEquals("Invalid table/view name: " + foo
          + ". It has been blacklisted using --blacklisted_tables", e.getMessage());
    }
  }

  @Test
  public void testParsingBlacklistedTablesNamesWithInputSpaces() {
    setBlacklist("", " db3 . foo , db3 . bar  ");

    assertEquals(2, getBlacklistedTablesCount());
    assertTrue(isTableBlacklisted("db3", "foo"));
    assertTrue(isTableBlacklisted("db3", "bar"));
    assertFalse(isTableBlacklisted("db3", "baz"));
    assertBlacklistedTableDbs("db3");
  }

  @Test
  public void testParsingBlacklistedTablesNamesWithoutDb() {
    setBlacklist("", "foo");

    assertEquals(1, getBlacklistedTablesCount());
    assertTrue(isTableBlacklisted(Catalog.DEFAULT_DB, "foo"));
    assertBlacklistedTableDbs("default");
  }

  @Test
  public void testParsingBlacklistedTablesCaseInsensitiveNames() {
    setBlacklist("", "DB3.Foo,db3.Bar");

    assertEquals(2, getBlacklistedTablesCount());
    assertTrue(isTableBlacklisted("db3", "foo"));
    assertTrue(isTableBlacklisted("db3", "bar"));
    assertBlacklistedTableDbs("db3");
  }

  @Test
  public void testParsingBlacklistedTablesInvalidNames() {
    // Test abnormal inputs
    setBlacklist("", "db3.,.bar,,");

    assertEquals(1, getBlacklistedTablesCount());
    assertTrue(isTableBlacklisted(Catalog.DEFAULT_DB, "bar"));
    assertBlacklistedTableDbs("default");
  }

  @Test
  public void testParsingBlacklistedDbsAndTables() {
    setBlacklist("db1,db2", "db3.foo,db3.bar");

    assertEquals(2, getBlacklistedDbsCount());
    assertTrue(isDbBlacklisted("db1"));
    assertTrue(isDbBlacklisted("db2"));
    assertFalse(isDbBlacklisted("db3"));

    assertEquals(2, getBlacklistedTablesCount());
    assertFalse(isTableBlacklisted("db1", "foo"));
    assertFalse(isTableBlacklisted("db2", "bar"));
    assertTrue(isTableBlacklisted("db3", "foo"));
    assertTrue(isTableBlacklisted("db3", "bar"));
    assertFalse(isTableBlacklisted("db3", "baz"));
    assertBlacklistedTableDbs("db3");
  }

  @Test
  public void testWorkloadManagementEnabled() {
    setBlacklist("sys", "", true);

    assertFalse(isDbBlacklisted("sys"));
    assertFalse(isTableBlacklisted("sys", "impala_query_log"));
    assertFalse(isTableBlacklisted("sys", "impala_query_live"));
    assertTrue(isTableBlacklisted("sys", "other_tbl"));
  }

  @Test
  public void testWorkloadManagementDisabled() {
    setBlacklist("sys", "", false);

    assertTrue(isDbBlacklisted("sys"));
  }

  private void assertBlacklistedTableDbs(String... expectedDbs) {
    assertEquals(ImmutableSet.copyOf(expectedDbs),
        ImmutableSet.copyOf(getBlacklistedTablesDbs()));
  }

  public static void setBlacklist(String blacklistedDbs, String blacklistedTables,
      boolean enableWorkloadMgmt) {
    TBackendGflags backendGflags = new TBackendGflags();
    backendGflags.setBlacklisted_dbs(blacklistedDbs);
    backendGflags.setBlacklisted_tables(blacklistedTables);
    backendGflags.setEnable_workload_mgmt(enableWorkloadMgmt);
    backendGflags.setQuery_log_table_name("impala_query_log");
    BackendConfig.create(backendGflags, false);
    reload();
  }

  public static void setBlacklist(String blacklistedDbs, String blacklistedTables) {
    setBlacklist(blacklistedDbs, blacklistedTables, false);
  }

}
