package com.redislabs.jdbc.rel;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Rules and relational operators for {@link RediSearchRel#CONVENTION}
 * calling convention.
 */
public class RediSearchRules {

    static final RelOptRule[] RULES = {RediSearchSortLimitRule.INSTANCE, RediSearchFilterRule.INSTANCE, RediSearchProjectRule.INSTANCE, RediSearchAggregateRule.INSTANCE,};


    private RediSearchRules() {
    }

    /**
     * Returns 'string' if it is a call to item['string'], null otherwise.
     */
    static String isItem(RexCall call) {
        if (call.getOperator() != SqlStdOperatorTable.ITEM) {
            return null;
        }
        final RexNode op0 = call.getOperands().get(0);
        final RexNode op1 = call.getOperands().get(1);

        if (op0 instanceof RexInputRef && ((RexInputRef) op0).getIndex() == 0 && op1 instanceof RexLiteral && ((RexLiteral) op1).getValue2() instanceof String) {
            return (String) ((RexLiteral) op1).getValue2();
        }
        return null;
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
            final List<String> strings = new ArrayList<>();
            visitList(call.operands, strings);
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

        private static String stripQuotes(String s) {
            return s.startsWith("'") && s.endsWith("'") ? s.substring(1, s.length() - 1) : s;
        }
    }

    /**
     * Rule to convert a {@link LogicalProject} to a {@link RediSearchProject}.
     */
    private static class RediSearchProjectRule extends RediSearchConverterRule {
        private static final RediSearchProjectRule INSTANCE = Config.INSTANCE.withConversion(LogicalProject.class, Convention.NONE, RediSearchRel.CONVENTION, "RediSearchProjectRule").withRuleFactory(RediSearchProjectRule::new).toRule(RediSearchProjectRule.class);

        protected RediSearchProjectRule(Config config) {
            super(config);
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
        public RelNode convert(RelNode rel) {
            final LogicalProject project = (LogicalProject) rel;
            final RelTraitSet traitSet = project.getTraitSet().replace(getOutConvention());
            return new RediSearchProject(project.getCluster(), traitSet, convert(project.getInput(), getOutConvention()), project.getProjects(), project.getRowType());
        }
    }

    /**
     * Rule to convert {@link org.apache.calcite.rel.core.Aggregate} to a
     * {@link RediSearchAggregate}.
     */
    private static class RediSearchAggregateRule extends RediSearchConverterRule {
        private static final RediSearchAggregateRule INSTANCE = Config.INSTANCE.withConversion(LogicalAggregate.class, Convention.NONE, RediSearchRel.CONVENTION, "RediSearchAggregateRule").withRuleFactory(RediSearchAggregateRule::new).toRule(RediSearchAggregateRule.class);

        protected RediSearchAggregateRule(Config config) {
            super(config);
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
    public static class RediSearchSortLimitRule extends RelRule<RediSearchSortLimitRule.Config> {

        private static final RediSearchSortLimitRule INSTANCE = Config.EMPTY.withOperandSupplier(b -> b.operand(Sort.class)
                // OQL doesn't support offsets (e.g. LIMIT 10 OFFSET 500)
                .predicate(sort -> sort.offset == null).anyInputs()).as(Config.class).toRule();

        /**
         * Creates a RediSearchSortLimitRule.
         */
        protected RediSearchSortLimitRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final Sort sort = call.rel(0);

            final RelTraitSet traitSet = sort.getTraitSet().replace(RediSearchRel.CONVENTION).replace(sort.getCollation());

            RediSearchSort rediSearchSort = new RediSearchSort(sort.getCluster(), traitSet, convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)), sort.getCollation(), sort.fetch, sort.offset);

            call.transformTo(rediSearchSort);
        }

        /**
         * Rule configuration.
         */
        public interface Config extends RelRule.Config {
            @Override
            default RediSearchSortLimitRule toRule() {
                return new RediSearchSortLimitRule(this);
            }
        }
    }

    /**
     * Rule to convert a {@link LogicalFilter} to a
     * {@link RediSearchFilter}.
     */
    public static class RediSearchFilterRule extends RelRule<RediSearchFilterRule.Config> {

        private static final RediSearchFilterRule INSTANCE = Config.EMPTY.withOperandSupplier(b0 -> b0.operand(LogicalFilter.class).oneInput(b1 -> b1.operand(RediSearchTableScan.class).noInputs())).as(Config.class).toRule();

        /**
         * Creates a RediSearchFilterRule.
         */
        protected RediSearchFilterRule(Config config) {
            super(config);
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

            if (!SqlKind.COMPARISON.contains(node.getKind()) && node.getKind() != SqlKind.SEARCH) {
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
            } else if (left.isA(SqlKind.ITEM) && right.isA(SqlKind.LITERAL)) {
                return true;
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

        /**
         * Rule configuration.
         */
        public interface Config extends RelRule.Config {
            @Override
            default RediSearchFilterRule toRule() {
                return new RediSearchFilterRule(this);
            }
        }
    }

    /**
     * Base class for planner rules that convert a relational
     * expression to RediSearch calling convention.
     */
    abstract static class RediSearchConverterRule extends ConverterRule {
        protected RediSearchConverterRule(Config config) {
            super(config);
        }
    }
}
