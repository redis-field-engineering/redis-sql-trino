/*
 * MIT License
 *
 * Copyright (c) 2022, Redis Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.redis.trino;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.Group;
import com.redis.lettucemod.search.Reducer;
import com.redis.lettucemod.search.Reducers.Avg;
import com.redis.lettucemod.search.Reducers.Count;
import com.redis.lettucemod.search.Reducers.Max;
import com.redis.lettucemod.search.Reducers.Min;
import com.redis.lettucemod.search.Reducers.Sum;
import com.redis.lettucemod.search.querybuilder.Node;
import com.redis.lettucemod.search.querybuilder.QueryBuilder;
import com.redis.lettucemod.search.querybuilder.QueryNode;
import com.redis.lettucemod.search.querybuilder.Value;
import com.redis.lettucemod.search.querybuilder.Values;
import com.redis.lettucemod.util.RedisModulesUtils;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

public class RediSearchQueryBuilder {

	private static final Logger log = Logger.get(RediSearchQueryBuilder.class);

	private static final Map<String, BiFunction<String, String, Reducer>> CONVERTERS = Map.of(RediSearchAggregation.MAX,
			(alias, field) -> Max.property(field).as(alias).build(), RediSearchAggregation.MIN,
			(alias, field) -> Min.property(field).as(alias).build(), RediSearchAggregation.SUM,
			(alias, field) -> Sum.property(field).as(alias).build(), RediSearchAggregation.AVG,
			(alias, field) -> Avg.property(field).as(alias).build(), RediSearchAggregation.COUNT,
			(alias, field) -> Count.as(alias));

	public String buildQuery(TupleDomain<ColumnHandle> tupleDomain) {
		return buildQuery(tupleDomain, Map.of());
	}

	public String buildQuery(TupleDomain<ColumnHandle> tupleDomain, Map<String, String> wildcards) {
		List<Node> nodes = new ArrayList<>();
		Optional<Map<ColumnHandle, Domain>> domains = tupleDomain.getDomains();
		if (domains.isPresent()) {
			for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
				RediSearchColumnHandle column = (RediSearchColumnHandle) entry.getKey();
				Domain domain = entry.getValue();
				checkArgument(!domain.isNone(), "Unexpected NONE domain for %s", column.getName());
				if (!domain.isAll()) {
					buildPredicate(column, domain).ifPresent(nodes::add);
				}
			}
		}
		for (Entry<String, String> wildcard : wildcards.entrySet()) {
			nodes.add(QueryBuilder.intersect(wildcard.getKey(), wildcard.getValue()));
		}
		if (nodes.isEmpty()) {
			return "*";
		}
		return QueryBuilder.intersect(nodes.toArray(new Node[0])).toString();
	}

	private Optional<Node> buildPredicate(RediSearchColumnHandle column, Domain domain) {
		String columnName = RedisModulesUtils.escapeTag(column.getName());
		checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");
		if (domain.getValues().isNone()) {
			return Optional.empty();
		}
		if (domain.getValues().isAll()) {
			return Optional.empty();
		}
		Set<Object> singleValues = new HashSet<>();
		List<Node> disjuncts = new ArrayList<>();
		for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
			if (range.isSingleValue()) {
				singleValues.add(translateValue(range.getSingleValue(), column.getType()));
			} else {
				List<Value> rangeConjuncts = new ArrayList<>();
				if (!range.isLowUnbounded()) {
					Object translated = translateValue(range.getLowBoundedValue(), column.getType());
					if (translated instanceof Number) {
						double doubleValue = ((Number) translated).doubleValue();
						rangeConjuncts.add(range.isLowInclusive() ? Values.ge(doubleValue) : Values.gt(doubleValue));
					} else {
						throw new UnsupportedOperationException(
								String.format("Range constraint not supported for type %s (column: '%s')",
										column.getType(), column.getName()));
					}
				}
				if (!range.isHighUnbounded()) {
					Object translated = translateValue(range.getHighBoundedValue(), column.getType());
					if (translated instanceof Number) {
						double doubleValue = ((Number) translated).doubleValue();
						rangeConjuncts.add(range.isHighInclusive() ? Values.le(doubleValue) : Values.lt(doubleValue));
					} else {
						throw new UnsupportedOperationException(
								String.format("Range constraint not supported for type %s (column: '%s')",
										column.getType(), column.getName()));
					}
				}
				// If conjuncts is null, then the range was ALL, which should already have been
				// checked for
				if (!rangeConjuncts.isEmpty()) {
					disjuncts.add(QueryBuilder.intersect(columnName, rangeConjuncts.toArray(Value[]::new)));
				}
			}
		}
		singleValues(column, singleValues).ifPresent(disjuncts::add);
		return Optional.of(QueryBuilder.union(disjuncts.toArray(Node[]::new)));
	}

	private Optional<QueryNode> singleValues(RediSearchColumnHandle column, Set<Object> singleValues) {
		if (singleValues.isEmpty()) {
			return Optional.empty();
		}
		if (singleValues.size() == 1) {
			return Optional.of(
					QueryBuilder.intersect(column.getName(), value(Iterables.getOnlyElement(singleValues), column)));
		}
		if (column.getType() instanceof VarcharType && column.getFieldType() == Field.Type.TAG) {
			// Takes care of IN: col IN ('value1', 'value2', ...)
			return Optional
					.of(QueryBuilder.intersect(column.getName(), Values.tags(singleValues.toArray(String[]::new))));
		}
		Value[] values = singleValues.stream().map(v -> value(v, column)).toArray(Value[]::new);
		return Optional.of(QueryBuilder.union(column.getName(), values));
	}

	private Value value(Object trinoNativeValue, RediSearchColumnHandle column) {
		requireNonNull(trinoNativeValue, "trinoNativeValue is null");
		requireNonNull(column, "column is null");
		Type type = column.getType();
		if (type == DOUBLE) {
			return Values.eq((Double) trinoNativeValue);
		}
		if (type == TINYINT) {
			return Values.eq((long) SignedBytes.checkedCast(((Long) trinoNativeValue)));
		}
		if (type == SMALLINT) {
			return Values.eq((long) Shorts.checkedCast(((Long) trinoNativeValue)));
		}
		if (type == IntegerType.INTEGER) {
			return Values.eq((long) toIntExact(((Long) trinoNativeValue)));
		}
		if (type == BIGINT) {
			return Values.eq((Long) trinoNativeValue);
		}
		if (type instanceof VarcharType) {
			if (column.getFieldType() == Field.Type.TAG) {
				return Values.tags(RedisModulesUtils.escapeTag((String) trinoNativeValue));
			}
			return Values.value((String) trinoNativeValue);
		}
		throw new UnsupportedOperationException("Type " + type + " not supported");
	}

	private Object translateValue(Object trinoNativeValue, Type type) {
		requireNonNull(trinoNativeValue, "trinoNativeValue is null");
		requireNonNull(type, "type is null");
		checkArgument(Primitives.wrap(type.getJavaType()).isInstance(trinoNativeValue),
				"%s (%s) is not a valid representation for %s", trinoNativeValue, trinoNativeValue.getClass(), type);

		if (type == DOUBLE) {
			return trinoNativeValue;
		}
		if (type == TINYINT) {
			return (long) SignedBytes.checkedCast(((Long) trinoNativeValue));
		}

		if (type == SMALLINT) {
			return (long) Shorts.checkedCast(((Long) trinoNativeValue));
		}

		if (type == IntegerType.INTEGER) {
			return (long) toIntExact(((Long) trinoNativeValue));
		}

		if (type == BIGINT) {
			return trinoNativeValue;
		}
		if (type instanceof VarcharType) {
			return ((Slice) trinoNativeValue).toStringUtf8();
		}
		throw new IllegalArgumentException("Unhandled type: " + type);
	}

	private Reducer reducer(RediSearchAggregation aggregation) {
		Optional<RediSearchColumnHandle> column = aggregation.getColumnHandle();
		String field = column.isPresent() ? column.get().getName() : null;
		return CONVERTERS.get(aggregation.getFunctionName()).apply(aggregation.getAlias(), field);
	}

	public Optional<Group> group(RediSearchTableHandle table) {
		List<RediSearchAggregationTerm> terms = table.getTermAggregations();
		List<RediSearchAggregation> aggregates = table.getMetricAggregations();
		List<String> groupFields = new ArrayList<>();
		if (terms != null && !terms.isEmpty()) {
			groupFields = terms.stream().map(RediSearchAggregationTerm::getTerm).collect(Collectors.toUnmodifiableList());
		}
		List<Reducer> reducers = aggregates.stream().map(this::reducer).collect(Collectors.toUnmodifiableList());
		if (reducers.isEmpty()) {
			return Optional.empty();
		}
		log.info("Group fields=%s reducers=%s", groupFields, reducers);
		return Optional
				.of(Group.by(groupFields.toArray(String[]::new)).reducers(reducers.toArray(Reducer[]::new)).build());
	}
}
