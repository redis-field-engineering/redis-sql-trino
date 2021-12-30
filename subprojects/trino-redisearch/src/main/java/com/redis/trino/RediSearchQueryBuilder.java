package com.redis.trino;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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

import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import com.redis.lettucemod.RedisModulesUtils;

import io.airlift.slice.Slice;
import io.redisearch.querybuilder.Node;
import io.redisearch.querybuilder.QueryBuilder;
import io.redisearch.querybuilder.Value;
import io.redisearch.querybuilder.Values;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

public class RediSearchQueryBuilder {

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
				if (!domain.isAll()) {
					buildPredicate(column.getName(), domain, column.getType()).ifPresent(nodes::add);
				}
			}
		}
		if (nodes.isEmpty()) {
			return "*";
		}
		return QueryBuilder.intersect(nodes.toArray(new Node[0])).toString();
	}

	private static Optional<Node> buildPredicate(String columnName, Domain domain, Type type) {
		List<Node> nodes = new ArrayList<>();
		checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");
		if (domain.getValues().isNone()) {
			return Optional.empty();
		}

		if (domain.getValues().isAll()) {
			return Optional.empty();
		}

		for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
			Set<Object> valuesToInclude = new HashSet<>();
			List<Value> rangeConjuncts = new ArrayList<>();
			checkState(!range.isAll(), "Invalid range for column: %s", columnName);
			if (range.isSingleValue()) {
				Optional<Object> value = translateValue(range.getSingleValue(), type);
				value.ifPresent(valuesToInclude::add);
			} else {
				if (!range.isLowUnbounded()) {
					Optional<Object> lowBound = translateValue(range.getLowBoundedValue(), type);
					if (lowBound.isPresent()) {
						double value = ((Number) lowBound.get()).doubleValue();
						if (range.isLowInclusive()) {
							rangeConjuncts.add(Values.ge(value));
						} else {
							rangeConjuncts.add(Values.gt(value));
						}
					}
				}
				if (!range.isHighUnbounded()) {
					Optional<Object> highBound = translateValue(range.getHighBoundedValue(), type);
					if (highBound.isPresent()) {
						double value = ((Number) highBound.get()).doubleValue();
						if (range.isLowInclusive()) {
							rangeConjuncts.add(Values.le(value));
						} else {
							rangeConjuncts.add(Values.lt(value));
						}
					}
				}
			}

			if (valuesToInclude.size() == 1) {
				nodes.add(QueryBuilder.intersect(columnName, value(Iterables.getOnlyElement(valuesToInclude), type)));
			}
			if (!rangeConjuncts.isEmpty()) {
				nodes.add(QueryBuilder.intersect(columnName, rangeConjuncts.toArray(new Value[0])));
			}
		}
		return Optional.of(QueryBuilder.union(nodes.toArray(new Node[0])));
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
}
