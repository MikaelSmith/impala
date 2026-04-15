package org.apache.impala.calcite.cte;

import com.cloudera.insights.advisor.materializations.AdvisorConf;
import com.cloudera.insights.advisor.materializations.MaterializationsAdvisor;
import com.cloudera.insights.advisor.materializations.rel.metadata.DASMetadataProvider;
import com.cloudera.insights.advisor.materializations.tools.WorkloadInput;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCommonExpressionSuggester;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.impala.calcite.rules.ImpalaCoreRules;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class AdvisorSuggester implements RelCommonExpressionSuggester {
  @Override
  public List<RelNode> suggest(RelNode input, Context context) {
    // Run in a separate thread since Advisor touches Thread local structures
    // and, we don't want to mess up the current state.
    ExecutorService service = Executors.newSingleThreadExecutor();
    Future<List<RelOptMaterialization>> result = service.submit(() -> {
      WorkloadInput i = WorkloadInput.builder().inputName("query").jsonPlan("{}")
          .plan(input).runtime(0.0f).build();
      AdvisorConf conf = new AdvisorConf();
      conf.set(AdvisorConf.Property.REMOVE_IS_NOT_NULL, "true");
      RelMetadataQuery.THREAD_PROVIDERS.set(DASMetadataProvider.DEFAULT);
      final MaterializationsAdvisor advisor = new MaterializationsAdvisor(
          conf, i.plan().getCluster(), Collections.singletonList(i), ImmutableSet.of());
      return advisor.generateRecommendations();
    });
    try {
      return result.get().stream().map(m -> m.queryRel)
          .map(AdvisorSuggester::optimize).collect(Collectors.toList());
    } catch (Exception e) {
      return Collections.emptyList();
    } finally {
      service.shutdown();
    }
  }

  /**
   * Optimizes the CTE recommendation by applying standard transformation rules. The advisor module finds and
   * generates common table expressions but the actual plan (RelNode) may not be optimal for execution. For instance,
   * joins are represented with cartesian products followed by filters which is very bad in terms of execution plan.
   * This method ensures that recommendations will be in optimal form for direct execution by the engine.
   *
   * @param cte the relational expression to optimize
   * @return an optimized relational expression
   */
  private static RelNode optimize(RelNode cte) {
    HepProgram program =
        new HepProgramBuilder().addRuleInstance(ImpalaCoreRules.FILTER_INTO_JOIN).build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(cte);
    return planner.findBestExp();
  }
}
