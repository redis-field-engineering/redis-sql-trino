package com.redis.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.util.*;

/**
 * Relational expression that uses RediSearch calling convention.
 */
public interface RediSearchRel extends RelNode {

  /**
   * Calling convention for relational operations that occur in RediSearch.
   */
  Convention CONVENTION = new Convention.Impl("REDISEARCH", RediSearchRel.class);

  /**
   * Callback for the implementation process that collects the context from the
   * {@link RediSearchRel} required to convert the relational tree into physical such.
   *
   * @param rediSearchImplementContext Context class that collects the feedback from the
   *                              call back method calls
   */
  void implement(RediSearchImplementContext rediSearchImplementContext);

  /**
   * Shared context used by the {@link RediSearchRel} relations.
   *
   * <p>Callback context class for the implementation process that converts a
   * tree of {@code RediSearchRel} nodes into an OQL query.
   */
  class RediSearchImplementContext {

    final Map<String, String> selectFields = new LinkedHashMap<>();
    final List<String> whereClause = new ArrayList<>();
    final List<Map.Entry<String, RelFieldCollation.Direction>> orderByFields = new ArrayList<>();
    final List<String> groupByFields = new ArrayList<>();
    final Map<String, String> oqlAggregateFunctions = new LinkedHashMap<>();
    Long limitValue;
    Long offsetValue;
    RelOptTable table;
    RediSearchTable rediSearchTable;

    /**
     * Adds new projected fields.
     *
     * @param fields New fields to be projected from a query
     */
    public void addSelectFields(Map<String, String> fields) {
      if (fields != null) {
        selectFields.putAll(fields);
      }
    }

    /**
     * Adds new  restricted predicates.
     *
     * @param predicates New predicates to be applied to the query
     */
    public void addPredicates(List<String> predicates) {
      if (predicates != null) {
        whereClause.addAll(predicates);
      }
    }

    public void addOrderBy(String field, RelFieldCollation.Direction direction) {
      Objects.requireNonNull(field, "field");
      orderByFields.add(new Pair<>(field, direction));
    }

    public void setLimit(long limit) {
      limitValue = limit;
    }

    public void setOffset(long offset) {
      offsetValue = offset;
    }

    public void addGroupBy(List<String> groupByFields) {
      this.groupByFields.addAll(groupByFields);
    }

    public void addAggregateFunctions(Map<String, String> oqlAggregateFunctions) {
      this.oqlAggregateFunctions.putAll(oqlAggregateFunctions);
    }

    void visitChild(RelNode input) {
      ((RediSearchRel) input).implement(this);
    }

    @Override public String toString() {
      return "RediSearchImplementContext{"
          + "selectFields=" + selectFields
          + ", whereClause=" + whereClause
          + ", orderByFields=" + orderByFields
          + ", offsetValue='" + offsetValue + '\''
          + ", limitValue='" + limitValue + '\''
          + ", groupByFields=" + groupByFields
          + ", table=" + table
          + ", rediSearchTable=" + rediSearchTable
          + '}';
    }
  }
}
