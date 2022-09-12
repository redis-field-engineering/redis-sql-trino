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
import static com.google.common.base.Verify.verify;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import com.redis.lettucemod.search.Group;
import com.redis.lettucemod.search.Reducer;
import com.redis.lettucemod.search.Reducers.Avg;
import com.redis.lettucemod.search.Reducers.Count;
import com.redis.lettucemod.search.Reducers.Max;
import com.redis.lettucemod.search.Reducers.Min;
import com.redis.lettucemod.search.Reducers.Sum;
import com.redis.lettucemod.search.querybuilder.Node;
import com.redis.lettucemod.search.querybuilder.QueryBuilder;
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

	private static final Map<String, BiFunction<String, String, Reducer>> CONVERTERS = Map.of(
			MetricAggregation.MAX, (alias, field) -> Max.property(field).as(alias).build(), MetricAggregation.MIN,
			(alias, field) -> Min.property(field).as(alias).build(), MetricAggregation.SUM,
			(alias, field) -> Sum.property(field).as(alias).build(), MetricAggregation.AVG,
			(alias, field) -> Avg.property(field).as(alias).build(), MetricAggregation.COUNT,
			(alias, field) -> Count.as(alias));

	private RediSearchQueryBuilder() {
	}

	public static String buildQuery(TupleDomain<ColumnHandle> tupleDomain) {
		List<Node> nodes = new ArrayList<>();
		Optional<Map<ColumnHandle, Domain>> domains = tupleDomain.getDomains();
		if (domains.isPresent()) {
			for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
				RediSearchColumnHandle column = (RediSearchColumnHandle) entry.getKey();
				Domain domain = entry.getValue();
				checkArgument(!domain.isNone(), "Unexpected NONE domain for %s", column.getName());
				if (domain.isAll()) {
					continue;
				}
				buildPredicate(column.getName(), domain, column.getType()).ifPresent(nodes::add);
			}
		}
		if (nodes.isEmpty()) {
			return "*";
		}
		return QueryBuilder.intersect(nodes.toArray(new Node[0])).toString();
	}

	private static Optional<Node> buildPredicate(String name, Domain domain, Type type) {
		String columnName = RedisModulesUtils.escapeTag(name);
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
				Optional<Object> translated = translateValue(range.getSingleValue(), type);
				if (translated.isEmpty()) {
					return Optional.empty();
				}
				singleValues.add(translated.get());
			} else {
				List<Value> rangeConjuncts = new ArrayList<>();
				if (!range.isLowUnbounded()) {
					Optional<Object> translated = translateValue(range.getLowBoundedValue(), type);
					if (translated.isEmpty()) {
						return Optional.empty();
					}
					double value = ((Number) translated.get()).doubleValue();
					rangeConjuncts.add(range.isLowInclusive() ? Values.ge(value) : Values.gt(value));
				}
				if (!range.isHighUnbounded()) {
					Optional<Object> translated = translateValue(range.getHighBoundedValue(), type);
					if (translated.isEmpty()) {
						return Optional.empty();
					}
					if (type instanceof VarcharType) {
						String value = (String) translated.get();
						return Optional.of(QueryBuilder.disjunct(columnName, Values.tags(value)));
					}
					double value = ((Number) translated.get()).doubleValue();
					rangeConjuncts.add(range.isHighInclusive() ? Values.le(value) : Values.lt(value));
				}
				// If conjuncts is null, then the range was ALL, which should already have been
				// checked for
				verify(!rangeConjuncts.isEmpty());
				disjuncts.add(QueryBuilder.intersect(columnName, rangeConjuncts.toArray(Value[]::new)));
			}
		}
		if (singleValues.size() == 1) {
			disjuncts.add(QueryBuilder.intersect(columnName, value(Iterables.getOnlyElement(singleValues), type)));
		} else if (singleValues.size() > 1) {
			disjuncts.add(QueryBuilder.union(columnName,
					singleValues.stream().map(v -> value(v, type)).toArray(Value[]::new)));
		}
		return Optional.of(QueryBuilder.union(disjuncts.toArray(Node[]::new)));
	}

	private static Value value(Object trinoNativeValue, Type type) {
		requireNonNull(trinoNativeValue, "trinoNativeValue is null");
		requireNonNull(type, "type is null");
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
			return Values.tags(RedisModulesUtils.escapeTag((String) trinoNativeValue));
		}
		throw new UnsupportedOperationException("Type " + type + " not supported");
	}

	private static Optional<Object> translateValue(Object trinoNativeValue, Type type) {
		requireNonNull(trinoNativeValue, "trinoNativeValue is null");
		requireNonNull(type, "type is null");
		checkArgument(Primitives.wrap(type.getJavaType()).isInstance(trinoNativeValue),
				"%s (%s) is not a valid representation for %s", trinoNativeValue, trinoNativeValue.getClass(), type);

		if (type == DOUBLE) {
			return Optional.of(trinoNativeValue);
		}
		if (type == TINYINT) {
			return Optional.of((long) SignedBytes.checkedCast(((Long) trinoNativeValue)));
		}

		if (type == SMALLINT) {
			return Optional.of((long) Shorts.checkedCast(((Long) trinoNativeValue)));
		}

		if (type == IntegerType.INTEGER) {
			return Optional.of((long) toIntExact(((Long) trinoNativeValue)));
		}

		if (type == BIGINT) {
			return Optional.of(trinoNativeValue);
		}
		if (type instanceof VarcharType) {
			return Optional.of(((Slice) trinoNativeValue).toStringUtf8());
		}
		return Optional.empty();
	}

	private static Reducer reducer(MetricAggregation aggregation) {
		Optional<RediSearchColumnHandle> column = aggregation.getColumnHandle();
		String field = column.isPresent() ? column.get().getName() : null;
		return CONVERTERS.get(aggregation.getFunctionName()).apply(aggregation.getAlias(), field);
	}

	public static Optional<Group> group(RediSearchTableHandle table) {
		List<TermAggregation> terms = table.getTermAggregations();
		List<MetricAggregation> aggregates = table.getMetricAggregations();
		List<String> groupFields = new ArrayList<>();
		if (terms != null && !terms.isEmpty()) {
			groupFields = terms.stream().map(TermAggregation::getTerm).toList();
		}
		List<Reducer> reducers = aggregates.stream().map(RediSearchQueryBuilder::reducer).toList();
		if (reducers.isEmpty()) {
			return Optional.empty();
		}
		log.info("Group fields=%s reducers=%s", groupFields, reducers);
		return Optional
				.of(Group.by(groupFields.toArray(String[]::new)).reducers(reducers.toArray(Reducer[]::new)).build());
	}
}
