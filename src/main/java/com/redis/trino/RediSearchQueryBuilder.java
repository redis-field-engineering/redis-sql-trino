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
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.lettuce.core.search.arguments.AggregateArgs.GroupBy;
import io.lettuce.core.search.arguments.AggregateArgs.Reducer;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
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

	// Written as the numbers Redis parses: integers, doubles and floats as Java formats them, epoch milliseconds
	private static final Set<Type> NUMERIC_TYPES = Set.of(DOUBLE, REAL, TINYINT, SMALLINT, IntegerType.INTEGER, BIGINT,
			TIMESTAMP_MILLIS, TIMESTAMP_TZ_MILLIS);

	// Decimals of up to 15 digits are distinct doubles, in the same order, so Redis compares them exactly
	private static final int MAX_EXACT_DECIMAL_PRECISION = 15;

	// The most values of a range of dates a tag query lists
	private static final int MAX_TAG_VALUES = 1000;

	// 2^53: integers of smaller magnitude are doubles exactly
	private static final long MAX_EXACT_LONG = 1L << 53;

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

	/**
	 * Whether a field's name can be referred to as {@code @name} in an expression or SORTBY.
	 */
	static boolean isProperty(String field) {
		return PROPERTY_NAME.matcher(field).matches();
	}

	private static String property(String field) {
		return "@" + field;
	}

	/**
	 * Whether {@link #buildQuery} can translate a column's domain into a query that matches every row in it. Redis
	 * can't match a missing field, so domains that allow nulls are left to Trino, and so is {@code IS NOT NULL}. On
	 * TAG and TEXT columns only {@code =} and {@code IN} are supported, for values a TAG or TEXT query can match.
	 * <p>
	 * Values are compared as {@link RediSearchPageSink#value} writes them: other types than VARCHAR and DOUBLE are
	 * only declared by tables created through Trino.
	 */
	public static boolean isSupported(RediSearchColumnHandle column, Domain domain) {
		ValueSet values = domain.getValues();
		if (domain.isNullAllowed() || values.isAll() || values.isNone()) {
			return false;
		}
		Type type = column.getType();
		switch (column.getFieldType()) {
		case NUMERIC:
			return isNumericType(type) && values.getRanges().getOrderedRanges().stream().allMatch(
					range -> (range.isLowUnbounded() || numericValue(type, range.getLowBoundedValue()).isPresent())
							&& (range.isHighUnbounded() || numericValue(type, range.getHighBoundedValue()).isPresent()));
		case TAG:
			return isTagType(type) && tagValues(values).filter(list -> list.stream()
					.allMatch(value -> isTagQueryable(tagValue(type, value), column.getTagSeparator()))).isPresent();
		case TEXT:
			return type instanceof VarcharType && tagValues(values).filter(list -> list.stream()
					.allMatch(value -> textTerms(((Slice) value).toStringUtf8()).isPresent())).isPresent();
		default:
			return false;
		}
	}

	/**
	 * The values of a TAG or TEXT column's domain. Trino merges consecutive dates into a range, whose dates a tag
	 * query also lists, up to {@link #MAX_TAG_VALUES}.
	 */
	static Optional<List<Object>> tagValues(ValueSet values) {
		if (values.isDiscreteSet()) {
			return Optional.of(values.getDiscreteSet());
		}
		if (values.getType() != DATE) {
			return Optional.empty();
		}
		List<Object> days = new ArrayList<>();
		for (Range range : values.getRanges().getOrderedRanges()) {
			if (range.isLowUnbounded() || range.isHighUnbounded()) {
				return Optional.empty();
			}
			long first = (Long) range.getLowBoundedValue() + (range.isLowInclusive() ? 0 : 1);
			long last = (Long) range.getHighBoundedValue() - (range.isHighInclusive() ? 0 : 1);
			if (last - first >= MAX_TAG_VALUES - days.size()) {
				return Optional.empty();
			}
			for (long day = first; day <= last; day++) {
				days.add(day);
			}
		}
		// Exclusive bounds a day apart hold no dates, which a tag query can't list
		return days.isEmpty() ? Optional.empty() : Optional.of(days);
	}

	private static boolean isNumericType(Type type) {
		return NUMERIC_TYPES.contains(type)
				|| (type instanceof DecimalType decimal && decimal.getPrecision() <= MAX_EXACT_DECIMAL_PRECISION);
	}

	/**
	 * The double Redis compares a NUMERIC field's value with, for a value of the column's type: the one it parses
	 * from the text the connector writes. Empty for a BIGINT or timestamp of 2^53 or more, which Redis would compare
	 * after rounding both it and the field's values, so that the query could leave out rows as well as return others.
	 */
	static Optional<Double> numericValue(Type type, Object value) {
		if (type == DOUBLE) {
			return Optional.of((Double) value);
		}
		if (type == REAL) {
			// Written as Float.toString, which Redis parses as a double: 1.1 rather than 1.100000023841858
			return Optional.of(Double.parseDouble(Float.toString(intBitsToFloat(toIntExact((Long) value)))));
		}
		if (type instanceof DecimalType decimal) {
			return Optional.of(Double.parseDouble(BigDecimal.valueOf((Long) value, decimal.getScale()).toString()));
		}
		long number;
		if (type == TIMESTAMP_MILLIS) {
			number = floorDiv((Long) value, MICROSECONDS_PER_MILLISECOND);
		} else if (type == TIMESTAMP_TZ_MILLIS) {
			number = unpackMillisUtc((Long) value);
		} else {
			number = (Long) value;
		}
		return isExactAsDouble(number) ? Optional.of((double) number) : Optional.empty();
	}

	private static boolean isTagType(Type type) {
		return type instanceof VarcharType || type instanceof CharType || type == BOOLEAN || type == DATE
				|| type == UUID;
	}

	/**
	 * The text {@link RediSearchPageSink#value} writes for a value of a TAG or TEXT column. A CHAR value is without
	 * the spaces the connector pads it with, which Redis trims from tags.
	 */
	static String tagValue(Type type, Object value) {
		if (type == BOOLEAN) {
			return value.toString();
		}
		if (type == DATE) {
			return DateTimeFormatter.ISO_DATE.format(LocalDate.ofEpochDay((Long) value));
		}
		if (type == UUID) {
			return trinoUuidToJavaUuid((Slice) value).toString();
		}
		return ((Slice) value).toStringUtf8();
	}

	static boolean isExactAsDouble(long value) {
		return -MAX_EXACT_LONG < value && value < MAX_EXACT_LONG;
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
			// A FILTER would compare a CHAR value with the spaces it's padded with, which other clients may not write
			return column.isFilterable() && !(column.getType() instanceof CharType)
					&& PROPERTY_NAME.matcher(column.getName()).matches()
					&& tagValues(domain.getValues()).filter(values -> values.stream().noneMatch(
							value -> FILTER_UNSUPPORTED_CHARACTERS.matcher(tagValue(column.getType(), value)).find()))
							.isPresent();
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
				filters.put(column.getName(), "exists(" + property + ") && " + anyOf(tagValues(domain.getValues())
						.orElseThrow().stream().map(value -> property + " == " + stringLiteral(tagValue(column.getType(), value)))
						.toList()));
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
		if (column.getFieldType() != RediSearchFieldType.NUMERIC) {
			tagValues(domain.getValues()).orElseThrow().forEach(value -> singleValues.add(translateValue(value, column)));
			return singleValues(column, singleValues);
		}
		for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
			if (range.isSingleValue()) {
				singleValues.add(translateValue(range.getSingleValue(), column));
			} else {
				List<String> rangeConjuncts = new ArrayList<>();
				if (!range.isLowUnbounded()) {
					Object translated = translateValue(range.getLowBoundedValue(), column);
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
					Object translated = translateValue(range.getHighBoundedValue(), column);
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
		if (column.getFieldType() == RediSearchFieldType.TAG) {
			// Takes care of IN: col IN ('value1', 'value2', ...)
			return Optional.of(field(column.getName(),
					tags(singleValues.stream().map(String.class::cast).map(RediSearchQueryBuilder::escapeTag).toList())));
		}
		return Optional.of(union(fields(column.getName(),
				singleValues.stream().map(v -> value(v, column)).collect(Collectors.toList()))));
	}

	// A value translateValue returned
	private String value(Object translated, RediSearchColumnHandle column) {
		requireNonNull(translated, "translated is null");
		if (translated instanceof Double number) {
			return numericEquals(number);
		}
		return tags(List.of(escapeTag((String) translated)));
	}

	/**
	 * @return the double a NUMERIC field's value is compared with, or the text of a TAG or TEXT field's value
	 */
	private Object translateValue(Object trinoNativeValue, RediSearchColumnHandle column) {
		requireNonNull(trinoNativeValue, "trinoNativeValue is null");
		Type type = column.getType();
		checkArgument(Primitives.wrap(type.getJavaType()).isInstance(trinoNativeValue),
				"%s (%s) is not a valid representation for %s", trinoNativeValue, trinoNativeValue.getClass(), type);
		if (column.getFieldType() == RediSearchFieldType.NUMERIC) {
			return numericValue(type, trinoNativeValue).orElseThrow(
					() -> new IllegalArgumentException("Not exact as a double: " + trinoNativeValue + " for " + type));
		}
		return tagValue(type, trinoNativeValue);
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
		log.debug("Group fields=%s reducers=%s", groupFields, reducers);
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
