/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redislabs.redisearch.jdbc;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Elasticsearch.
 */
public class RediSearchFilter extends Filter implements RediSearchRel {
  RediSearchFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() == RediSearchRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override
  public Filter copy(RelTraitSet relTraitSet, RelNode input, RexNode condition) {
    return new RediSearchFilter(getCluster(), relTraitSet, input, condition);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    PredicateAnalyzerTranslator translator = new PredicateAnalyzerTranslator();
    try {
      implementor.add(translator.translateMatch(condition));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (PredicateAnalyzer.ExpressionNotAnalyzableException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * New version of translator which uses visitor pattern
   * and allow to process more complex (boolean) predicates.
   */
  static class PredicateAnalyzerTranslator {

    String translateMatch(RexNode condition) throws IOException,
        PredicateAnalyzer.ExpressionNotAnalyzableException {
      return PredicateAnalyzer.analyze(condition).toString();
    }
  }

}
