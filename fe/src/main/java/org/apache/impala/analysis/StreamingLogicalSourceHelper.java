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

package org.apache.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.util.KuduUtil;

/**
 * Helper class for building logical streaming sources for UPDATE and DELETE operations.
 * Provides shared utilities used by both FromClause and MigrateStmt.
 */
public class StreamingLogicalSourceHelper {

  public static String sqlStringLiteral(String value) {
    return new StringLiteral(value, Type.STRING, false).toSql(ToSqlOptions.DEFAULT);
  }

  /**
   * Output format for generated SQL.
   * WITH_CLAUSE: Generates WITH clause format (used by FromClause)
   * INLINE_SUBQUERY: Generates inline subquery format (used by MigrateStmt)
   */
  public enum SourceFormat {
    WITH_CLAUSE,
    INLINE_SUBQUERY
  }

  /**
   * Represents a logical streaming source with optional WITH clause and source reference.
   */
  public static class LogicalStreamingSource {
    public final String withClause;  // nullable for INLINE_SUBQUERY format
    public final String source;

    public LogicalStreamingSource(String withClause, String source) {
      this.withClause = withClause;
      this.source = source;
    }

    public LogicalStreamingSource(String source) {
      this(null, source);
    }
  }

  private static final String UPDATE_CTE_PREFIX = "update";
  private static final String DELETE_CTE_PREFIX = "del";

  /**
   * Builds a logical streaming source by processing a list of streaming operations
   * (updates and deletes) and generating either a WITH clause or inline subqueries.
   *
   * @param columnNames list of column names in the base table
   * @param initialInput initial input source (e.g., Iceberg table snapshot reference)
   * @param operations list of streaming logical operations to process
   * @param format output format (WITH_CLAUSE or INLINE_SUBQUERY)
   * @return LogicalStreamingSource with generated SQL
   * @throws AnalysisException if update assignments cannot be decoded
   */
  public static LogicalStreamingSource buildLogicalStreamingSource(
      List<String> columnNames,
      String initialInput,
      List<KuduUtil.StreamingLogicalOperation> operations,
      SourceFormat format)
      throws AnalysisException {
    if (format == SourceFormat.WITH_CLAUSE) {
      return buildWithClauseFormat(columnNames, initialInput, operations);
    } else {
      return buildInlineSubqueryFormat(columnNames, initialInput, operations);
    }
  }

  /**
   * Builds logical streaming source in WITH clause format.
   * Generates: WITH cte1 AS (...), cte2 AS (...) ... final_source
   */
  private static LogicalStreamingSource buildWithClauseFormat(
      List<String> columnNames,
      String initialInput,
      List<KuduUtil.StreamingLogicalOperation> operations)
      throws AnalysisException {
    List<String> ctes = new ArrayList<>();
    String input = initialInput;
    boolean inputHasRowId = false;
    int deleteCount = 0;
    int updateCount = 0;

    for (KuduUtil.StreamingLogicalOperation op : operations) {
      if (op.isUpdate()) {
        String cteName = UPDATE_CTE_PREFIX + (++updateCount);
        ctes.add("%s as (select %s from %s)".formatted(cteName,
            buildLogicalUpdateSelectList(columnNames, op), input));
        input = cteName;
        inputHasRowId = true;
      } else {
        String cteName = DELETE_CTE_PREFIX + (++deleteCount);
        String cteSelectList = inputHasRowId ? "*" : "*, _row_id";
        ctes.add("%s as (select %s from %s where not (%s))".formatted(
            cteName, cteSelectList, input, op.predicateSql));
        input = cteName;
        inputHasRowId = true;
      }
    }

    String withClause = "with " + String.join(",\n", ctes) + "\n";
    return new LogicalStreamingSource(withClause, input);
  }

  /**
   * Builds logical streaming source in inline subquery format.
   * Generates: (select ... from ...) alias1 where ...
   */
  private static LogicalStreamingSource buildInlineSubqueryFormat(
      List<String> columnNames,
      String initialInput,
      List<KuduUtil.StreamingLogicalOperation> operations)
      throws AnalysisException {
    String input = initialInput;
    boolean inputHasRowId = false;
    int deleteCount = 0;
    int updateCount = 0;

    for (KuduUtil.StreamingLogicalOperation op : operations) {
      if (op.isUpdate()) {
        String cteName = UPDATE_CTE_PREFIX + (++updateCount);
        input = "(select %s from %s) %s".formatted(
            buildLogicalUpdateSelectList(columnNames, op), input, cteName);
        inputHasRowId = true;
      } else {
        String cteName = DELETE_CTE_PREFIX + (++deleteCount);
        String cteSelectList = inputHasRowId ? "*" : "*, _row_id";
        input = "(select %s from %s where not (%s)) %s".formatted(
            cteSelectList, input, op.predicateSql, cteName);
        inputHasRowId = true;
      }
    }

    return new LogicalStreamingSource(input);
  }

  /**
   * Builds the SELECT list for an UPDATE operation, including CASE expressions
   * that conditionally apply updates based on the operation's predicate.
   *
   * @param columnNames list of column names to include in select list
   * @param op streaming logical operation containing assignment expressions
   * @return SELECT list with column selections and CASE expressions for updated columns
   * @throws AnalysisException if assignment expressions cannot be decoded
   */
  private static String buildLogicalUpdateSelectList(
      List<String> columnNames,
      KuduUtil.StreamingLogicalOperation op)
      throws AnalysisException {
    List<KuduUtil.StreamingUpdateAssignment> assignments;
    try {
      assignments = KuduUtil.decodeStreamingUpdateAssignments(op.assignmentExprs);
    } catch (TableLoadingException e) {
      throw new AnalysisException(e);
    }

    List<String> selectItems = new ArrayList<>();
    for (String columnName : columnNames) {
      String exprSql = null;
      for (KuduUtil.StreamingUpdateAssignment assignment : assignments) {
        if (assignment.columnName.equalsIgnoreCase(columnName)) {
          exprSql = assignment.exprSql;
          break;
        }
      }
      if (exprSql == null) {
        selectItems.add("`%s`".formatted(columnName));
      } else {
        selectItems.add("case when %s then %s else `%s` end as `%s`".formatted(
            op.predicateSql, exprSql, columnName, columnName));
      }
    }
    selectItems.add("_row_id");
    return String.join(", ", selectItems);
  }
}
