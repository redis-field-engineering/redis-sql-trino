package com.redis.calcite;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Relational expression representing a scan of a table in a RediSearch data source.
 */
public class RediSearchToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    protected RediSearchToEnumerableConverter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traitSet, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new RediSearchToEnumerableConverter(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }

    /**
     * Reference to the method {@link RediSearchTable.RediSearchQueryable#query},
     * used in the {@link Expression}.
     */
    private static final Method REDISEARCH_QUERY_METHOD = Types.lookupMethod(RediSearchTable.RediSearchQueryable.class, "query", List.class, List.class, List.class, List.class, List.class, List.class, Long.class, Long.class);

    /**
     * {@inheritDoc}
     *
     * @param implementor RediSearchImplementContext
     */
    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {

        // travers all relations form this to the scan leaf
        final RediSearchRel.RediSearchImplementContext rediSearchImplementContext = new RediSearchRel.RediSearchImplementContext();
        ((RediSearchRel) getInput()).implement(rediSearchImplementContext);

        final RelDataType rowType = getRowType();

        // PhysType is Enumerable Adapter class that maps SQL types (getRowType)
        // with physical Java types (getJavaTypes())
        final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));

        final List<Class<?>> physFieldClasses = new AbstractList<Class<?>>() {
            @Override
            public Class<?> get(int index) {
                return physType.fieldClass(index);
            }

            @Override
            public int size() {
                return rowType.getFieldCount();
            }
        };

        // Expression meta-program for calling the RediSearchTable.RediSearchQueryable#query
        // method form the generated code
        final BlockBuilder blockBuilder = new BlockBuilder().append(Expressions.call(rediSearchImplementContext.table.getExpression(RediSearchTable.RediSearchQueryable.class), REDISEARCH_QUERY_METHOD,
                // fields
                constantArrayList(Pair.zip(RediSearchRules.rediSearchFieldNames(rowType), physFieldClasses), Pair.class),
                // selected fields
                constantArrayList(toListMapPairs(rediSearchImplementContext.selectFields), Pair.class),
                // aggregate functions
                constantArrayList(toListMapPairs(rediSearchImplementContext.oqlAggregateFunctions), Pair.class), constantArrayList(rediSearchImplementContext.groupByFields, String.class), constantArrayList(rediSearchImplementContext.whereClause, String.class), constantArrayList(rediSearchImplementContext.orderByFields, Pair.class), Expressions.constant(rediSearchImplementContext.offsetValue), Expressions.constant(rediSearchImplementContext.limitValue)));

        return implementor.result(physType, blockBuilder.toBlock());
    }

    private static List<Map.Entry<String, String>> toListMapPairs(Map<String, String> map) {
        List<Map.Entry<String, String>> selectList = new ArrayList<>();
        for (Map.Entry<String, String> entry : Pair.zip(map.keySet(), map.values())) {
            selectList.add(entry);
        }
        return selectList;
    }

    /**
     * E.g. {@code constantArrayList("x", "y")} returns
     * "Arrays.asList('x', 'y')".
     */
    private static <T> MethodCallExpression constantArrayList(List<T> values, Class<?> clazz) {
        return Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method, Expressions.newArrayInit(clazz, constantList(values)));
    }

    /**
     * E.g. {@code constantList("x", "y")} returns
     * {@code {ConstantExpression("x"), ConstantExpression("y")}}.
     */
    private static <T> List<Expression> constantList(List<T> values) {
        return Util.transform(values, Expressions::constant);
    }
}
