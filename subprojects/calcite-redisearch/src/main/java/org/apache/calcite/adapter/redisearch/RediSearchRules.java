package org.apache.calcite.adapter.redisearch;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

/**
 * Rules and relational operators for {@link RediSearchRel#CONVENTION}
 * calling convention.
 */
public class RediSearchRules {

    public static final RelOptRule[] RULES = {RediSearchSortLimitRule.INSTANCE, RediSearchFilterRule.INSTANCE, RediSearchProjectRule.INSTANCE, RediSearchAggregateRule.INSTANCE,};


    private RediSearchRules() {
    }

    static List<String> rediSearchFieldNames(final RelDataType rowType) {
        return SqlValidatorUtil.uniquify(rowType.getFieldNames(), true);
    }

    /**
     * Translator from {@link RexNode} to strings in RediSearch's expression language.
     */
    static class RexToRediSearchTranslator extends RexVisitorImpl<String> {

        private final List<String> inFields;

        protected RexToRediSearchTranslator(List<String> inFields) {
            super(true);
            this.inFields = inFields;
        }

        @Override
        public String visitInputRef(RexInputRef inputRef) {
            return inFields.get(inputRef.getIndex());
        }

        @Override
        public String visitCall(RexCall call) {
            final List<String> strings = visitList(call.operands);
            if (call.getOperator() == SqlStdOperatorTable.ITEM) {
                final RexNode op1 = call.getOperands().get(1);
                if (op1 instanceof RexLiteral) {
                    if (op1.getType().getSqlTypeName() == SqlTypeName.INTEGER) {
                        return stripQuotes(strings.get(0)) + "[" + ((RexLiteral) op1).getValue2() + "]";
                    } else if (op1.getType().getSqlTypeName() == SqlTypeName.CHAR) {
                        return stripQuotes(strings.get(0)) + "." + ((RexLiteral) op1).getValue2();
                    }
                }
            }

            return super.visitCall(call);
        }

        List<String> visitList(List<RexNode> list) {
            final List<String> strings = new ArrayList<>();
            for (RexNode node : list) {
                strings.add(node.accept(this));
            }
            return strings;
        }

        private static String stripQuotes(String s) {
            return s.startsWith("'") && s.endsWith("'") ? s.substring(1, s.length() - 1) : s;
        }
    }

    /**
     * Rule to convert a {@link LogicalProject} to a {@link RediSearchProject}.
     */
    private static class RediSearchProjectRule extends RediSearchConverterRule {
        private static final RediSearchProjectRule INSTANCE = new RediSearchProjectRule();

        private RediSearchProjectRule() {
            super(LogicalProject.class, Convention.NONE, RediSearchRel.CONVENTION, "RediSearchProjectRule");
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            LogicalProject project = call.rel(0);
            for (RexNode e : project.getProjects()) {
                if (e.getType().getSqlTypeName() == SqlTypeName.GEOMETRY) {
                    // For spatial Functions Drop to Calcite Enumerable
                    return false;
                }
            }

            return true;
        }

        @Override
        public RelNode convert(RelNode relNode) {
            final LogicalProject project = (LogicalProject) relNode;
            final RelTraitSet traitSet = project.getTraitSet().replace(getOutConvention());
            return new RediSearchProject(project.getCluster(), traitSet, convert(project.getInput(), getOutConvention()), project.getProjects(), project.getRowType());
        }
    }

    /**
     * Rule to convert {@link org.apache.calcite.rel.core.Aggregate} to a
     * {@link RediSearchAggregate}.
     */
    private static class RediSearchAggregateRule extends RediSearchConverterRule {
        private static final RediSearchAggregateRule INSTANCE = new RediSearchAggregateRule();

        protected RediSearchAggregateRule() {
            super(LogicalAggregate.class, Convention.NONE, RediSearchRel.CONVENTION,
                    "RediSearchAggregateRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            final LogicalAggregate aggregate = (LogicalAggregate) rel;
            final RelTraitSet traitSet = aggregate.getTraitSet().replace(getOutConvention());
            return new RediSearchAggregate(aggregate.getCluster(), traitSet, convert(aggregate.getInput(), traitSet.simplify()), aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
        }
    }

    /**
     * Rule to convert the Limit in {@link Sort} to a
     * {@link RediSearchSort}.
     */
    public static class RediSearchSortLimitRule extends RediSearchConverterRule {

        private static final RediSearchSortLimitRule INSTANCE = new RediSearchSortLimitRule();

        /**
         * Creates a RediSearchSortLimitRule.
         */
        protected RediSearchSortLimitRule() {
            super(Sort.class, Convention.NONE, RediSearchRel.CONVENTION,
                    "RediSearchSortRule");
        }

        @Override
        public RelNode convert(RelNode relNode) {
            final Sort sort = (Sort) relNode;

            final RelTraitSet traitSet = sort.getTraitSet().replace(out).replace(sort.getCollation());

            return new RediSearchSort(relNode.getCluster(), traitSet, convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)), sort.getCollation(), sort.offset, sort.fetch);
        }

    }

    /**
     * Rule to convert a {@link LogicalFilter} to a
     * {@link RediSearchFilter}.
     */
    public static class RediSearchFilterRule extends RelOptRule {

        public static final RediSearchFilterRule INSTANCE = new RediSearchFilterRule();

        /**
         * Creates a RediSearchFilterRule.
         */
        protected RediSearchFilterRule() {
            super(operand(LogicalFilter.class, operand(RediSearchTableScan.class, none())),
                    "RediSearchFilterRule");
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            // Get the condition from the filter operation
            LogicalFilter filter = call.rel(0);
            RexNode condition = filter.getCondition();

            List<String> fieldNames = RediSearchRules.rediSearchFieldNames(filter.getInput().getRowType());

            List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
            if (disjunctions.size() != 1) {
                return true;
            } else {
                // Check that all conjunctions are primary field conditions.
                condition = disjunctions.get(0);
                for (RexNode predicate : RelOptUtil.conjunctions(condition)) {
                    if (!isEqualityOnKey(predicate, fieldNames)) {
                        return false;
                    }
                }
            }

            return true;
        }

        /**
         * Check if the node is a supported predicate (primary field condition).
         *
         * @param node       Condition node to check
         * @param fieldNames Names of all columns in the table
         * @return True if the node represents an equality predicate on a primary key
         */
        private static boolean isEqualityOnKey(RexNode node, List<String> fieldNames) {

            if (isBooleanColumnReference(node, fieldNames)) {
                return true;
            }

            if (!SqlKind.COMPARISON.contains(node.getKind())) {
                return false;
            }

            RexCall call = (RexCall) node;
            final RexNode left = call.operands.get(0);
            final RexNode right = call.operands.get(1);

            if (checkConditionContainsInputRefOrLiterals(left, right, fieldNames)) {
                return true;
            }
            return checkConditionContainsInputRefOrLiterals(right, left, fieldNames);

        }

        private static boolean isBooleanColumnReference(RexNode node, List<String> fieldNames) {
            // FIXME Ignore casts for rel and assume they aren't really necessary
            if (node.isA(SqlKind.CAST)) {
                node = ((RexCall) node).getOperands().get(0);
            }
            if (node.isA(SqlKind.NOT)) {
                node = ((RexCall) node).getOperands().get(0);
            }
            if (node.isA(SqlKind.INPUT_REF)) {
                if (node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
                    final RexInputRef left1 = (RexInputRef) node;
                    String name = fieldNames.get(left1.getIndex());
                    return name != null;
                }
            }
            return false;
        }

        /**
         * Checks whether a condition contains input refs of literals.
         *
         * @param left       Left operand of the equality
         * @param right      Right operand of the equality
         * @param fieldNames Names of all columns in the table
         * @return Whether condition is supported
         */
        private static boolean checkConditionContainsInputRefOrLiterals(RexNode left, RexNode right, List<String> fieldNames) {
            // FIXME Ignore casts for rel and assume they aren't really necessary
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).getOperands().get(0);
            }

            if (right.isA(SqlKind.CAST)) {
                right = ((RexCall) right).getOperands().get(0);
            }

            if (left.isA(SqlKind.INPUT_REF) && right.isA(SqlKind.LITERAL)) {
                final RexInputRef left1 = (RexInputRef) left;
                String name = fieldNames.get(left1.getIndex());
                return name != null;
            } else if (left.isA(SqlKind.INPUT_REF) && right.isA(SqlKind.INPUT_REF)) {

                final RexInputRef left1 = (RexInputRef) left;
                String leftName = fieldNames.get(left1.getIndex());

                final RexInputRef right1 = (RexInputRef) right;
                String rightName = fieldNames.get(right1.getIndex());

                return (leftName != null) && (rightName != null);
            }
            if (left.isA(SqlKind.OTHER_FUNCTION) && right.isA(SqlKind.LITERAL)) {
                return ((RexCall) left).getOperator() == SqlStdOperatorTable.ITEM;
            }
            return false;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalFilter filter = call.rel(0);
            RediSearchTableScan scan = call.rel(1);
            if (filter.getTraitSet().contains(Convention.NONE)) {
                final RelNode converted = convert(filter, scan);
                call.transformTo(converted);
            }
        }

        private static RelNode convert(LogicalFilter filter, RediSearchTableScan scan) {
            final RelTraitSet traitSet = filter.getTraitSet().replace(RediSearchRel.CONVENTION);
            return new RediSearchFilter(filter.getCluster(), traitSet, convert(filter.getInput(), RediSearchRel.CONVENTION), filter.getCondition(), scan.rediSearchTable.indexFields());
        }

    }

    /**
     * Base class for planner rules that convert a relational
     * expression to RediSearch calling convention.
     */
    abstract static class RediSearchConverterRule extends ConverterRule {

        final Convention out;

        protected RediSearchConverterRule(Class<? extends RelNode> clazz, RelTrait in, Convention out, String description) {
            super(clazz, in, out, description);
            this.out = out;
        }
    }
}
