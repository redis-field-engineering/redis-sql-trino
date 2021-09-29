package com.redis.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Implementation of
 * {@link Sort}
 * relational expression in RediSearch.
 */
public class RediSearchSort extends Sort implements RediSearchRel {

    /**
     * Creates a RediSearchSort.
     */
    RediSearchSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);

        assert getConvention() == CONVENTION;
        assert getConvention() == input.getConvention();
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {

        RelOptCost cost = super.computeSelfCost(planner, mq);

        if (fetch != null) {
            return cost.multiplyBy(0.05);
        } else {
            return cost.multiplyBy(0.9);
        }
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new RediSearchSort(getCluster(), traitSet, input, collation, offset, fetch);
    }

    @Override
    public void implement(RediSearchImplementContext rediSearchImplementContext) {
        rediSearchImplementContext.visitChild(getInput());

        List<RelFieldCollation> sortCollations = collation.getFieldCollations();

        for (RelFieldCollation fieldCollation : sortCollations) {
            final String name = fieldName(fieldCollation.getFieldIndex());
            rediSearchImplementContext.addOrderBy(name, fieldCollation.getDirection());
        }

        if (fetch != null) {
            rediSearchImplementContext.setLimit(((RexLiteral) fetch).getValueAs(Long.class));
        }
        if (offset != null) {
            rediSearchImplementContext.setOffset(((RexLiteral) offset).getValueAs(Long.class));
        }
    }

    private String fieldName(int index) {
        return getRowType().getFieldList().get(index).getName();
    }

}
