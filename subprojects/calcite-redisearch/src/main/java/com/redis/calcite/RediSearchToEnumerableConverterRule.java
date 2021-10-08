package com.redis.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert a relational expression from
 * {@link RediSearchRel#CONVENTION} to {@link EnumerableConvention}.
 */
public class RediSearchToEnumerableConverterRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new RediSearchToEnumerableConverterRule(RelFactories.LOGICAL_BUILDER);

    private RediSearchToEnumerableConverterRule(RelBuilderFactory relBuilderFactory) {
        super(RelNode.class, (Predicate<RelNode>) r -> true,
                RediSearchRel.CONVENTION, EnumerableConvention.INSTANCE,
                relBuilderFactory, "RediSearchToEnumerableConverterRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
        return new RediSearchToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
    }
}
