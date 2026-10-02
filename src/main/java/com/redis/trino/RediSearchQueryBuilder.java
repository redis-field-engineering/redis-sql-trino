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
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.lettuce.core.search.arguments.AggregateArgs.GroupBy;
import io.lettuce.core.search.arguments.AggregateArgs.Reducer;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

public class RediSearchQueryBuilder {

	private static final Logger log = Logger.get(RediSearchQueryBuilder.class);

	private static final Map<String, BiFunction<String, String, Reducer>> CONVERTERS = Map.of(RediSearchAggregation.MAX,
			(alias, field) -> Reducer.max(property(field)).as(alias), RediSearchAggregation.MIN,
			(alias, field) -> Reducer.min(property(field)).as(alias), RediSearchAggregation.SUM,
			(alias, field) -> Reducer.sum(property(field)).as(alias), RediSearchAggregation.AVG,
			(alias, field) -> Reducer.avg(property(field)).as(alias), RediSearchAggregation.COUNT,
			(alias, field) -> Reducer.count().as(alias));

	private static final Set<Type> NUMERIC_TYPES = Set.of(DOUBLE, TINYINT, SMALLINT, IntegerType.INTEGER, BIGINT);

	// Redis splits TEXT values into terms on space, tab and this ASCII punctuation. It keeps other characters, such as
	// '_' and non-ASCII letters, inside terms.
	private static final Pattern TEXT_SEPARATORS = Pattern.compile("[ \\t!\"#$%&'()*+,\\-./:;<=>?@\\[\\]^`{|}~]+");

	// Characters Redis neither splits on nor indexes as they are: the escape character and the other control characters
	private static final Pattern TEXT_UNSUPPORTED_CHARACTERS = Pattern.compile("[\\\\\\x00-\\x08\\x0A-\\x1F\\x7F]");

	// A query term that is a stop word matches no documents
	private static final Set<String> DEFAULT_STOPWORDS = Set.of("a", "is", "the", "an", "and", "are", "as", "at", "be",
			"but", "by", "for", "if", "in", "into", "it", "no", "not", "of", "on", "or", "such", "that", "their", "then",
			"there", "these", "they", "this", "to", "was", "will", "with");

	// Redis trims ASCII whitespace around each tag, so a query for a value that starts or ends with it matches nothing
	private static final Pattern TAG_SURROUNDING_WHITESPACE = Pattern.compile("^\\s|\\s\\z");

	// ASCII control characters that a tag query can't express, even escaped
	private static final Pattern TAG_UNSUPPORTED_CHARACTERS = Pattern.compile("[\\x00-\\x08\\x0E-\\x1F\\x7F]");

	// Control characters, which a FILTER string literal may not take as they are
	private static final Pattern FILTER_UNSUPPORTED_CHARACTERS = Pattern.compile("[\\x00-\\x1F\\x7F]");

	// Field names a FILTER expression can refer to as @name
	private static final Pattern PROPERTY_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

	private static String property(String field) {
		return "@" + field;
	}

	/**
	 * Whether {@link #buildQuery} can translate a column's domain into a query that matches every row in it. Redis
	 * can't match a missing field, so domains that allow nulls are left to Trino, and so is {@code IS NOT NULL}. On
	 * VARCHAR columns only {@code =} and {@code IN} are supported, for values a TAG or TEXT query can match.
	 */
	public static boolean isSupported(RediSearchColumnHandle column, Domain domain) {
		ValueSet values = domain.getValues();
		if (domain.isNullAllowed() || values.isAll() || values.isNone()) {
			return false;
		}
		switch (column.getFieldType()) {
		case NUMERIC:
			return NUMERIC_TYPES.contains(column.getType());
		case TAG:
			return column.getType() instanceof VarcharType && values.isDiscreteSet() && values.getDiscreteSet().stream()
					.allMatch(value -> isTagQueryable(((Slice) value).toStringUtf8(), column.getTagSeparator()));
		case TEXT:
			return column.getType() instanceof VarcharType && values.isDiscreteSet() && values.getDiscreteSet().stream()
					.allMatch(value -> textTerms(((Slice) value).toStringUtf8()).isPresent());
		default:
			return false;
		}
	}

	/**
	 * Whether the scan returns exactly the rows in a supported domain, so Trino doesn't have to filter them. A NUMERIC
	 * query matches exactly. A TEXT query matches the documents containing the value's terms, with stemming. A TAG
	 * query matches the documents with that value among the tags Redis splits a stored value into and trims, ignoring
	 * case unless the field is CASESENSITIVE. For these, {@link #filters} keeps the equal rows, when the field is
	 * {@link RediSearchColumnHandle#isFilterable filterable}, its name can be referred to in an expression, and no
	 * value has a control character.
	 */
	public static boolean isExact(RediSearchColumnHandle column, Domain domain) {
		switch (column.getFieldType()) {
		case NUMERIC:
			return true;
		case TAG:
		case TEXT:
			return column.isFilterable() && PROPERTY_NAME.matcher(column.getName()).matches()
					&& domain.getValues().isDiscreteSet() && domain.getValues().getDiscreteSet().stream()
							.noneMatch(value -> FILTER_UNSUPPORTED_CHARACTERS.matcher(((Slice) value).toStringUtf8()).find());
		default:
			return false;
		}
	}

	/**
	 * FT.AGGREGATE FILTER expressions, by field name, that keep the rows equal to the TAG and TEXT domains that
	 * {@link #isExact} accepts. Each compares the field's value as a string, e.g.
	 * {@code exists(@style) && @style == "Wheat"}, so it must come after the field is loaded: on a SORTABLE field,
	 * FILTER would otherwise compare the normalized sort value. Without {@code exists}, Redis fails the query on a
	 * document without the field instead of leaving it out.
	 */
	public Map<String, String> filters(TupleDomain<ColumnHandle> tupleDomain) {
		Map<String, String> filters = new LinkedHashMap<>();
		tupleDomain.getDomains().ifPresent(domains -> domains.forEach((columnHandle, domain) -> {
			RediSearchColumnHandle column = (RediSearchColumnHandle) columnHandle;
			if (column.getFieldType() != RediSearchFieldType.NUMERIC && !domain.isAll() && isExact(column, domain)) {
				String property = property(column.getName());
				filters.put(column.getName(), "exists(" + property + ") && " + anyOf(domain.getValues().getDiscreteSet()
						.stream().map(value -> property + " == " + stringLiteral(((Slice) value).toStringUtf8())).toList()));
			}
		}));
		return filters;
	}

	// Nested in halves, so that Redis parses and evaluates a long IN list only log2(n) levels deep
	private static String anyOf(List<String> conditions) {
		if (conditions.size() == 1) {
			return conditions.get(0);
		}
		int middle = conditions.size() / 2;
		return "(" + anyOf(conditions.subList(0, middle)) + " || " + anyOf(conditions.subList(middle, conditions.size()))
				+ ")";
	}

	// FILTER doesn't take PARAMS, so values are inlined
	private static String stringLiteral(String value) {
		return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
	}

	/**
	 * Whether a tag query for the value matches every document with that value. It matches nothing if the value is
	 * empty, contains the field's separator, starts or ends with whitespace, or has a control character Redis can't
	 * query.
	 */
	static boolean isTagQueryable(String value, Optional<Character> separator) {
		return !value.isEmpty() && (separator.isEmpty() || value.indexOf(separator.get()) < 0)
				&& !TAG_SURROUNDING_WHITESPACE.matcher(value).find()
				&& !TAG_UNSUPPORTED_CHARACTERS.matcher(value).find();
	}

	/**
	 * The terms Redis indexes a TEXT value as, without default stop words. Empty if no terms are left, or if the value
	 * has characters that a term query can't match.
	 */
	static Optional<List<String>> textTerms(String value) {
		if (TEXT_UNSUPPORTED_CHARACTERS.matcher(value).find()) {
			return Optional.empty();
		}
		List<String> terms = TEXT_SEPARATORS.splitAsStream(value).filter(term -> !term.isEmpty())
				.filter(term -> !DEFAULT_STOPWORDS.contains(term.toLowerCase(ENGLISH))).toList();
		return terms.isEmpty() ? Optional.empty() : Optional.of(terms);
	}

	public String buildQuery(TupleDomain<ColumnHandle> tupleDomain) {
		List<String> nodes = new ArrayList<>();
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
		if (nodes.isEmpty()) {
			return "*";
		}
		return intersect(nodes);
	}

	private Optional<String> buildPredicate(RediSearchColumnHandle column, Domain domain) {
		String columnName = escapeTag(column.getName());
		checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");
		checkArgument(isSupported(column, domain), "Unsupported domain for %s: %s", column.getName(), domain);
		Set<Object> singleValues = new LinkedHashSet<>();
		List<String> disjuncts = new ArrayList<>();
		for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
			if (range.isSingleValue()) {
				singleValues.add(translateValue(range.getSingleValue(), column.getType()));
			} else {
				List<String> rangeConjuncts = new ArrayList<>();
				if (!range.isLowUnbounded()) {
					Object translated = translateValue(range.getLowBoundedValue(), column.getType());
					if (translated instanceof Number) {
						double doubleValue = ((Number) translated).doubleValue();
						rangeConjuncts.add(numericRange(doubleValue, range.isLowInclusive(), Double.POSITIVE_INFINITY, true));
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
						rangeConjuncts.add(numericRange(Double.NEGATIVE_INFINITY, true, doubleValue, range.isHighInclusive()));
					} else {
						throw new UnsupportedOperationException(
								String.format("Range constraint not supported for type %s (column: '%s')",
										column.getType(), column.getName()));
					}
				}
				// If conjuncts is null, then the range was ALL, which should already have been
				// checked for
				if (!rangeConjuncts.isEmpty()) {
					disjuncts.add(intersect(fields(columnName, rangeConjuncts)));
				}
			}
		}
		singleValues(column, singleValues).ifPresent(disjuncts::add);
		return Optional.of(union(disjuncts));
	}

	private Optional<String> singleValues(RediSearchColumnHandle column, Set<Object> singleValues) {
		if (singleValues.isEmpty()) {
			return Optional.empty();
		}
		if (column.getFieldType() == RediSearchFieldType.TEXT) {
			// Documents containing all of a value's terms: @col:(term1 term2), or a union of these for IN
			return Optional.of(union(singleValues.stream()
					.map(value -> field(column.getName(),
							"(" + String.join(" ", textTerms((String) value).orElseThrow()) + ")"))
					.collect(Collectors.toList())));
		}
		if (singleValues.size() == 1) {
			return Optional.of(field(column.getName(), value(Iterables.getOnlyElement(singleValues), column)));
		}
		if (column.getType() instanceof VarcharType) {
			// Takes care of IN: col IN ('value1', 'value2', ...)
			return Optional.of(field(column.getName(),
					tags(singleValues.stream().map(String.class::cast).map(RediSearchQueryBuilder::escapeTag).toList())));
		}
		return Optional.of(union(fields(column.getName(),
				singleValues.stream().map(v -> value(v, column)).collect(Collectors.toList()))));
	}

	private String value(Object trinoNativeValue, RediSearchColumnHandle column) {
		requireNonNull(trinoNativeValue, "trinoNativeValue is null");
		requireNonNull(column, "column is null");
		Type type = column.getType();
		if (type == DOUBLE) {
			return numericEquals((Double) trinoNativeValue);
		}
		if (type == TINYINT) {
			return numericEquals(SignedBytes.checkedCast(((Long) trinoNativeValue)));
		}
		if (type == SMALLINT) {
			return numericEquals(Shorts.checkedCast(((Long) trinoNativeValue)));
		}
		if (type == IntegerType.INTEGER) {
			return numericEquals(toIntExact(((Long) trinoNativeValue)));
		}
		if (type == BIGINT) {
			return numericEquals((Long) trinoNativeValue);
		}
		if (type instanceof VarcharType) {
			return tags(List.of(escapeTag((String) trinoNativeValue)));
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

	public Optional<GroupBy> group(RediSearchTableHandle table) {
		List<RediSearchAggregationTerm> terms = table.getTermAggregations();
		List<RediSearchAggregation> aggregates = table.getMetricAggregations();
		List<String> groupFields = new ArrayList<>();
		if (terms != null && !terms.isEmpty()) {
			groupFields = terms.stream().map(RediSearchAggregationTerm::getTerm).collect(Collectors.toList());
		}
		List<Reducer> reducers = aggregates.stream().map(this::reducer).collect(Collectors.toList());
		if (reducers.isEmpty()) {
			return Optional.empty();
		}
		log.info("Group fields=%s reducers=%s", groupFields, reducers);
		GroupBy groupBy = GroupBy.of(groupFields.stream().map(RediSearchQueryBuilder::property).toArray(String[]::new));
		reducers.forEach(groupBy::reduce);
		return Optional.of(groupBy);
	}

	// Query syntax helpers, formatted the same way as the lettucemod 3.x query builder

	// Escapes ASCII punctuation, whitespace and control characters. Redis matches nothing for a value with a backslash
	// before a non-ASCII character, unless it's the first one.
	public static String escapeTag(String value) {
		return value.replaceAll("([\\p{ASCII}&&[^a-zA-Z0-9]])", "\\\\$1");
	}

	private static String tags(List<String> tags) {
		checkArgument(!tags.isEmpty(), "Must have at least one tag");
		return "{" + String.join(" | ", tags) + "}";
	}

	private static String field(String name, String value) {
		return "@" + name + ":" + value;
	}

	private static List<String> fields(String name, List<String> values) {
		return values.stream().map(value -> field(name, value)).collect(Collectors.toList());
	}

	private static String intersect(List<String> nodes) {
		return join(" ", nodes);
	}

	private static String union(List<String> nodes) {
		return join("|", nodes);
	}

	private static String join(String separator, List<String> nodes) {
		if (nodes.size() == 1) {
			return nodes.get(0);
		}
		return "(" + String.join(separator, nodes) + ")";
	}

	private static String numericEquals(double value) {
		return numericRange(value, true, value, true);
	}

	private static String numericRange(double from, boolean fromInclusive, double to, boolean toInclusive) {
		return "[" + number(from, fromInclusive) + " " + number(to, toInclusive) + "]";
	}

	private static String number(double value, boolean inclusive) {
		String prefix = inclusive ? "" : "(";
		if (value == Double.NEGATIVE_INFINITY) {
			return prefix + "-inf";
		}
		if (value == Double.POSITIVE_INFINITY) {
			return prefix + "inf";
		}
		return prefix + value;
	}
}
