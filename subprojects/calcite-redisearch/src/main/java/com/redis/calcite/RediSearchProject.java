package com.redis.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of
 * {@link Project}
 * relational expression in RediSearch.
 */
public class RediSearchProject extends Project implements RediSearchRel {

    RediSearchProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
        assert getConvention() == CONVENTION;
        assert getConvention() == input.getConvention();
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new RediSearchProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }

    @Override
    public void implement(RediSearchImplementContext rediSearchImplementContext) {
        rediSearchImplementContext.visitChild(getInput());

        final RediSearchRules.RexToRediSearchTranslator translator = new RediSearchRules.RexToRediSearchTranslator(RediSearchRules.rediSearchFieldNames(getInput().getRowType()));
        final Map<String, String> fields = new LinkedHashMap<>();
        for (Pair<RexNode, String> pair : getNamedProjects()) {
            final String name = pair.right;
            final String originalName = pair.left.accept(translator);
            fields.put(originalName, name);
        }
        rediSearchImplementContext.addSelectFields(fields);
    }
}
