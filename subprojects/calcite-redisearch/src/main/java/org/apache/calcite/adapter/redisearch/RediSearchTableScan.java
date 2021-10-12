package org.apache.calcite.adapter.redisearch;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Relational expression representing a scan of a RediSearch collection.
 */
public class RediSearchTableScan extends TableScan implements RediSearchRel {

    final RediSearchTable rediSearchTable;
    final RelDataType projectRowType;

    /**
     * Creates a RediSearchTableScan.
     *
     * @param cluster         Cluster
     * @param traitSet        Traits
     * @param table           Table
     * @param rediSearchTable RediSearch table
     * @param projectRowType  Fields and types to project; null to project raw row
     */
    public RediSearchTableScan(RelOptCluster cluster, RelTraitSet traitSet,
                               RelOptTable table, RediSearchTable rediSearchTable, RelDataType projectRowType) {
        super(cluster, traitSet, table);
        this.rediSearchTable = rediSearchTable;
        this.projectRowType = projectRowType;

        assert rediSearchTable != null;
        assert getConvention() == CONVENTION;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override
    public RelDataType deriveRowType() {
        return projectRowType != null ? projectRowType : super.deriveRowType();
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(RediSearchToEnumerableConverterRule.INSTANCE);
        for (RelOptRule rule : RediSearchRules.RULES) {
            planner.addRule(rule);
        }
    }

    @Override
    public void implement(RediSearchImplementContext rediSearchImplementContext) {
        // Note: Scan is the leaf and we do NOT visit its inputs
        rediSearchImplementContext.rediSearchTable = rediSearchTable;
        rediSearchImplementContext.table = table;
    }
}
