package com.redislabs.jdbc.rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of
 * {@link Aggregate} relational expression
 * in RediSearch.
 */
public class RediSearchAggregate extends Aggregate implements RediSearchRel {

    /**
     * Creates a RediSearchAggregate.
     */
    public RediSearchAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);

        assert getConvention() == RediSearchRel.CONVENTION;
        assert getConvention() == this.input.getConvention();
        assert getConvention() == input.getConvention();
        assert this.groupSets.size() == 1 : "Grouping sets not supported";

        for (AggregateCall aggCall : aggCalls) {
            if (aggCall.isDistinct()) {
                System.out.println("DISTINCT based aggregation!");
            }
        }
    }

    @Deprecated // to be removed before 2.0
    public RediSearchAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        this(cluster, traitSet, input, groupSet, groupSets, aggCalls);
        checkIndicator(indicator);
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new RediSearchAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }

    @Override
    public void implement(RediSearchImplementContext rediSearchImplementContext) {
        rediSearchImplementContext.visitChild(getInput());

        List<String> inputFields = fieldNames(getInput().getRowType());

        List<String> groupByFields = new ArrayList<>();

        for (int group : groupSet) {
            groupByFields.add(inputFields.get(group));
        }

        rediSearchImplementContext.addGroupBy(groupByFields);

        // Find the aggregate functions (e.g. MAX, SUM ...)
        ImmutableMap.Builder<String, String> aggregateFunctionMap = ImmutableMap.builder();
        for (AggregateCall aggCall : aggCalls) {

            List<String> aggCallFieldNames = new ArrayList<>();
            for (int i : aggCall.getArgList()) {
                aggCallFieldNames.add(inputFields.get(i));
            }
            String functionName = aggCall.getAggregation().getName();

            // Workaround to handle count(*) case. RediSearch doesn't allow "AS" aliases on
            // 'count(*)' but allows it for count('any column name'). So we are
            // converting the count(*) into count (first input ColumnName).
            if ("COUNT".equalsIgnoreCase(functionName) && aggCallFieldNames.isEmpty()) {
                aggCallFieldNames.add(inputFields.get(0));
            }

            String oqlAggregateCall = Util.toString(aggCallFieldNames, functionName + "(", ", ", ")");

            aggregateFunctionMap.put(aggCall.getName(), oqlAggregateCall);
        }

        rediSearchImplementContext.addAggregateFunctions(aggregateFunctionMap.build());

    }

    private static List<String> fieldNames(RelDataType relDataType) {
        ArrayList<String> names = new ArrayList<>();

        for (RelDataTypeField rdtf : relDataType.getFieldList()) {
            names.add(rdtf.getName());
        }
        return names;
    }
}
