package com.redislabs.jdbc.rel;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Rule to convert a relational expression from
 * {@link RediSearchRel#CONVENTION} to {@link EnumerableConvention}.
 */
public class RediSearchToEnumerableConverterRule extends ConverterRule {

    public static final ConverterRule INSTANCE = Config.INSTANCE.withConversion(RelNode.class, RediSearchRel.CONVENTION, EnumerableConvention.INSTANCE, "RediSearchToEnumerableConverterRule").withRuleFactory(RediSearchToEnumerableConverterRule::new).toRule(RediSearchToEnumerableConverterRule.class);

    protected RediSearchToEnumerableConverterRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
        return new RediSearchToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
    }
}
