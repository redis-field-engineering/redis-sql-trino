package com.redis.trino;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Range.equal;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.predicate.Range.lessThan;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.search.FieldValue;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;

public class TestQueryBuilder {

	private static final RediSearchColumnHandle COL1 = new RediSearchColumnHandle("col1", BIGINT, RediSearchFieldType.NUMERIC,
			false, true, Optional.empty());
	private static final RediSearchColumnHandle COL2 = tag("col2", Optional.of(','));
	private static final RediSearchColumnHandle TEXT_COL = new RediSearchColumnHandle("name",
			createUnboundedVarcharType(), RediSearchFieldType.TEXT, false, true, Optional.empty());

	private static RediSearchColumnHandle tag(String name, Optional<Character> separator) {
		return new RediSearchColumnHandle(name, createUnboundedVarcharType(), RediSearchFieldType.TAG, false, true,
				separator);
	}

	// A field of a hash index, whose values a FILTER compares as they're stored
	private static RediSearchColumnHandle filterable(String name, RediSearchFieldType fieldType) {
		return new RediSearchColumnHandle(name, createUnboundedVarcharType(), fieldType, false, true,
				fieldType == RediSearchFieldType.TAG ? Optional.of(',') : Optional.empty(), true);
	}

	private static Domain varchars(String... values) {
		return Domain.multipleValues(createUnboundedVarcharType(), Stream.of(values).map(value -> utf8Slice(value)).toList());
	}

	@Test
	public void testBuildQuery() {
		TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
				ImmutableMap.of(COL1, Domain.create(ValueSet.ofRanges(range(BIGINT, 100L, false, 200L, true)), false),
						COL2, Domain.singleValue(createUnboundedVarcharType(), utf8Slice("a value"))));

		String query = new RediSearchQueryBuilder().buildQuery(tupleDomain);
		String expected = "((@col1:[(100.0 inf] @col1:[-inf 200.0]) @col2:{a\\ value})";
		assertThat(query).isEqualTo(expected);
	}

	@Test
	public void testBuildQueryIn() {
		TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(COL2,
				Domain.create(ValueSet.ofRanges(equal(createUnboundedVarcharType(), utf8Slice("hello")),
						equal(createUnboundedVarcharType(), utf8Slice("world"))), false)));
		String query = new RediSearchQueryBuilder().buildQuery(tupleDomain);
		String expected = "@col2:{hello | world}";
		assertThat(query).isEqualTo(expected);

	}

	@Test
	public void testBuildQueryInEscapesTags() {
		TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(COL2,
				varchars("Brown Ale", "Witbier")));
		assertThat(new RediSearchQueryBuilder().buildQuery(tupleDomain)).isEqualTo("@col2:{Brown\\ Ale | Witbier}");
	}

	@Test
	public void testBuildQueryText() {
		// Documents containing the terms, without punctuation and stop words; Trino keeps the equal ones
		TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(TEXT_COL,
				varchars("Grimm's Witbier", "The Pocus")));
		assertThat(new RediSearchQueryBuilder().buildQuery(tupleDomain))
				.isEqualTo("(@name:(Grimm s Witbier)|@name:(Pocus))");
		assertThat(RediSearchQueryBuilder.isExact(TEXT_COL, varchars("Pocus"))).isFalse();
	}

	@Test
	public void testBuildQueryTagEscapesAsciiOnly() {
		// Redis matches nothing for a backslash before a non-ASCII character
		TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(COL2,
				varchars("Café", "日本 & co.", "a\\b")));
		assertThat(new RediSearchQueryBuilder().buildQuery(tupleDomain))
				.isEqualTo("@col2:{Café | a\\\\b | 日本\\ \\&\\ co\\.}");
		// Values of a JSON TAG field aren't split, so a separator character is escaped like any other
		TupleDomain<ColumnHandle> json = TupleDomain.withColumnDomains(ImmutableMap.of(tag("col2", Optional.empty()),
				varchars("a,b")));
		assertThat(new RediSearchQueryBuilder().buildQuery(json)).isEqualTo("@col2:{a\\,b}");
	}

	@Test
	public void testTagValues() {
		// Redis splits stored values into tags, trims them and folds their case, so Trino filters the rows
		assertThat(RediSearchQueryBuilder.isExact(COL2, varchars("Wheat"))).isFalse();
		assertThat(RediSearchQueryBuilder.isExact(COL1, Domain.singleValue(BIGINT, 1L))).isTrue();
		assertThat(RediSearchQueryBuilder.isSupported(COL2, varchars("Wheat", "brown ale", "a\tb", "Café"))).isTrue();
		// A tag query can't match a value containing the separator, surrounded by whitespace or with control characters
		assertThat(RediSearchQueryBuilder.isSupported(COL2, varchars("Wheat", "a,b"))).isFalse();
		assertThat(RediSearchQueryBuilder.isSupported(COL2, varchars(" a"))).isFalse();
		assertThat(RediSearchQueryBuilder.isSupported(COL2, varchars("a\n"))).isFalse();
		assertThat(RediSearchQueryBuilder.isSupported(COL2, varchars("a\u0001b"))).isFalse();
		assertThat(RediSearchQueryBuilder.isSupported(COL2, varchars("a\u007Fb"))).isFalse();
		// The separator is the field's own
		RediSearchColumnHandle semicolon = tag("col2", Optional.of(';'));
		assertThat(RediSearchQueryBuilder.isSupported(semicolon, varchars("a,b"))).isTrue();
		assertThat(RediSearchQueryBuilder.isSupported(semicolon, varchars("a;b"))).isFalse();
		assertThat(RediSearchQueryBuilder.isSupported(tag("col2", Optional.empty()), varchars("a,b;c"))).isTrue();
	}

	@Test
	public void testFilters() {
		RediSearchColumnHandle style = filterable("style", RediSearchFieldType.TAG);
		RediSearchColumnHandle name = filterable("name", RediSearchFieldType.TEXT);
		Map<String, String> filters = new RediSearchQueryBuilder().filters(TupleDomain.withColumnDomains(
				ImmutableMap.of(COL1, Domain.singleValue(BIGINT, 1L), style, varchars("Wheat"), name,
						varchars("Hocus Pocus", "Big"), COL2, varchars("Wheat"))));
		// NUMERIC queries are exact, and COL2 isn't filterable, so Trino filters it
		assertThat(filters).containsOnlyKeys("style", "name");
		assertThat(filters).containsEntry("style", "exists(@style) && @style == \"Wheat\"");
		assertThat(filters).containsEntry("name", "exists(@name) && (@name == \"Big\" || @name == \"Hocus Pocus\")");
		assertThat(RediSearchQueryBuilder.isExact(style, varchars("Wheat"))).isTrue();
		assertThat(RediSearchQueryBuilder.isExact(name, varchars("Hocus Pocus"))).isTrue();
	}

	@Test
	public void testFilterValues() {
		RediSearchColumnHandle style = filterable("style", RediSearchFieldType.TAG);
		// Quotes and backslashes are escaped; FILTER takes the other characters as they are
		assertThat(new RediSearchQueryBuilder().filters(TupleDomain.withColumnDomains(ImmutableMap.of(style,
				varchars("say \"hi\"", "back\\slash", "a$b@c'd|e", "Café")))))
				.containsEntry("style", "exists(@style) && ((@style == \"Café\" || @style == \"a$b@c'd|e\") "
						+ "|| (@style == \"back\\\\slash\" || @style == \"say \\\"hi\\\"\"))");
		// IN nests its comparisons in halves
		assertThat(new RediSearchQueryBuilder().filters(TupleDomain.withColumnDomains(ImmutableMap.of(style,
				varchars("a", "b", "c", "d", "e"))))).containsEntry("style", "exists(@style) && "
						+ "((@style == \"a\" || @style == \"b\") || (@style == \"c\" || (@style == \"d\" || @style == \"e\")))");
		// A control character, which a string literal may not hold, leaves the filter to Trino
		assertThat(RediSearchQueryBuilder.isExact(style, varchars("Wheat", "a\tb"))).isFalse();
		assertThat(new RediSearchQueryBuilder().filters(TupleDomain.withColumnDomains(ImmutableMap.of(style,
				varchars("Wheat", "a\tb"))))).isEmpty();
		// and so does a field name an expression can't refer to
		assertThat(RediSearchQueryBuilder.isExact(filterable("brewery-id", RediSearchFieldType.TAG), varchars("1")))
				.isFalse();
	}

	@Test
	public void testFilterBeforeGroupByAndLimit() {
		RediSearchColumnHandle style = filterable("style", RediSearchFieldType.TAG);
		RediSearchTableHandle table = new RediSearchTableHandle(new SchemaTableName("tpch", "beers"), "beers",
				TupleDomain.withColumnDomains(ImmutableMap.of(style, varchars("Wheat"))), OptionalLong.of(10), List.of(),
				List.of(new RediSearchAggregation(RediSearchAggregation.COUNT, BIGINT, Optional.empty(), "c")));
		RediSearchColumnHandle count = new RediSearchColumnHandle("c", BIGINT, RediSearchFieldType.NUMERIC, false, false,
				Optional.empty());
		RediSearchTranslator.Aggregation aggregation = new RediSearchTranslator(new RediSearchConfig()).aggregate(table,
				List.of(count, style), Optional.of(hashIndex()));
		assertThat(aggregation.getQuery()).isEqualTo("@style:{Wheat}");
		// The field is loaded once, and FILTER runs before GROUPBY and LIMIT
		assertThat(commandString(aggregation)).isEqualTo("LOAD 3 __key style c FILTER exists(@style) && @style == \"Wheat\" "
				+ "GROUPBY 0 REDUCE COUNT 0 AS c LIMIT 0 10 WITHCURSOR COUNT 1000 DIALECT 2");
	}

	@Test
	public void testScanLoadsNumericValuesAsStored() {
		RediSearchColumnHandle style = filterable("style", RediSearchFieldType.TAG);
		RediSearchColumnHandle abv = numeric("abv", DoubleType.DOUBLE);
		RediSearchColumnHandle ibu = numeric("ibu", IntegerType.INTEGER);
		RediSearchTableHandle table = new RediSearchTableHandle(new SchemaTableName("tpch", "beers"), "beers",
				TupleDomain.withColumnDomains(ImmutableMap.of(style, varchars("Wheat"))), OptionalLong.empty(), List.of(),
				List.of());
		RediSearchTranslator translator = new RediSearchTranslator(new RediSearchConfig());
		// A DOUBLE loaded by name comes back rounded to 12 significant digits, so the hash's fields are loaded as
		// stored, and the DOUBLE isn't loaded by name, which would round it again
		RediSearchTranslator.Aggregation aggregation = translator.aggregate(table, List.of(style, abv, ibu),
				Optional.of(hashIndex()));
		assertThat(commandString(aggregation)).isEqualTo("LOAD * LOAD 3 __key style ibu "
				+ "FILTER exists(@style) && @style == \"Wheat\" WITHCURSOR COUNT 1000 DIALECT 2");
		// LOAD * names a field indexed AS another name by its hash field
		assertThat(aggregation.getReader().read(Map.of("raw_abv", value("4.123456789012345"), "style", value("Wheat"),
				"name", value("Other field"))))
				.containsExactly("Wheat", "4.123456789012345", null);
		// INTEGER values are exact as doubles
		assertThat(commandString(translator.aggregate(table, List.of(style, ibu), Optional.of(hashIndex()))))
				.startsWith("LOAD 3 __key style ibu ");
		// Without FT.INFO, columns are loaded by name
		assertThat(commandString(translator.aggregate(table, List.of(style, abv), Optional.empty())))
				.startsWith("LOAD 3 __key style abv ");
	}

	@Test
	public void testJsonScanUsesDialect3() {
		RediSearchColumnHandle score = numeric("score", DoubleType.DOUBLE);
		RediSearchColumnHandle id = tag("id", Optional.empty());
		RediSearchTableHandle table = new RediSearchTableHandle(new SchemaTableName("tpch", "docs"), "docs");
		RediSearchIndexInfo json = new RediSearchIndexInfo(Optional.of(RediSearchIndexInfo.KeyType.JSON), List.of(),
				List.of(new RediSearchIndexInfo.Field("score", "$.score", RediSearchFieldType.NUMERIC, Optional.empty()),
						new RediSearchIndexInfo.Field("id", "$.id", RediSearchFieldType.TAG, Optional.empty())),
				false, 1, false);
		RediSearchTranslator.Aggregation aggregation = new RediSearchTranslator(new RediSearchConfig()).aggregate(table,
				List.of(RediSearchBuiltinField.KEY.getColumnHandle(), score, id), Optional.of(json));
		assertThat(commandString(aggregation)).isEqualTo("LOAD 3 __key score id WITHCURSOR COUNT 1000 DIALECT 3");
		// DIALECT 3 returns the values at each JSON path as an array, with numbers as stored
		assertThat(aggregation.getReader().read(Map.of("__key", value("doc:1"), "score", value("[9007199254740993]"),
				"id", value("[\"1\"]")))).containsExactly("doc:1", "9007199254740993", "1");
	}

	@Test
	public void testReadsReducerResults() {
		RediSearchAggregation count = new RediSearchAggregation(RediSearchAggregation.COUNT, BIGINT, Optional.empty(),
				"c");
		RediSearchAggregation sum = new RediSearchAggregation(RediSearchAggregation.SUM, DoubleType.DOUBLE,
				Optional.of(numeric("abv", DoubleType.DOUBLE)), "s");
		RediSearchRowReader reader = new RediSearchRowReader(List.of("style", "c", "s"), Map.of(), Set.of(),
				List.of(count, sum));
		// SUM over no values is nan, which SQL represents as null
		assertThat(reader.read(Map.of("style", value("Wheat"), "c", value("2"), "s", value("nan"))))
				.containsExactly("Wheat", "2", null);
		// A global aggregation over no documents counts 0
		assertThat(reader.emptyAggregation()).containsExactly(null, "0", null);
	}

	@Test
	public void testJsonFirstValue() {
		// As DIALECT 2 returns them, but with numbers as written
		assertThat(RediSearchRowReader.firstValue("[0.1234567890123456]")).contains("0.1234567890123456");
		assertThat(RediSearchRowReader.firstValue("[1234567890123456789012345.12345]"))
				.contains("1234567890123456789012345.12345");
		assertThat(RediSearchRowReader.firstValue("[\"He said \\\"hi\\\"\\\\n\", \"x\"]")).contains("He said \"hi\"\\n");
		assertThat(RediSearchRowReader.firstValue("[\"a\",\"b,c\"]")).contains("a");
		assertThat(RediSearchRowReader.firstValue("[true]")).contains("1");
		assertThat(RediSearchRowReader.firstValue("[false]")).contains("0");
		assertThat(RediSearchRowReader.firstValue("[null]")).isEmpty();
		assertThat(RediSearchRowReader.firstValue("[]")).isEmpty();
		assertThat(RediSearchRowReader.firstValue("[{\"id\": \"1\", \"n\": {\"x\": 0.1234567890123456}}, 2]"))
				.contains("{\"id\": \"1\", \"n\": {\"x\": 0.1234567890123456}}");
		assertThat(RediSearchRowReader.firstValue("[[1, 2], 3]")).contains("[1, 2]");
	}

	private static RediSearchColumnHandle numeric(String name, io.trino.spi.type.Type type) {
		return new RediSearchColumnHandle(name, type, RediSearchFieldType.NUMERIC, false, true, Optional.empty());
	}

	// A hash index whose abv field is the hash field raw_abv
	private static RediSearchIndexInfo hashIndex() {
		return new RediSearchIndexInfo(Optional.of(RediSearchIndexInfo.KeyType.HASH), List.of("beer:"),
				List.of(new RediSearchIndexInfo.Field("style", "style", RediSearchFieldType.TAG, Optional.of(',')),
						new RediSearchIndexInfo.Field("abv", "raw_abv", RediSearchFieldType.NUMERIC, Optional.empty()),
						new RediSearchIndexInfo.Field("ibu", "ibu", RediSearchFieldType.NUMERIC, Optional.empty())),
				false, 1, false);
	}

	private static FieldValue value(String value) {
		return FieldValue.of(value.getBytes(StandardCharsets.UTF_8));
	}

	private static String commandString(RediSearchTranslator.Aggregation aggregation) {
		CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8);
		aggregation.getArgs().build(args);
		return args.toCommandString();
	}

	@Test
	public void testTextTerms() {
		assertThat(RediSearchQueryBuilder.textTerms("Hocus Pocus")).contains(List.of("Hocus", "Pocus"));
		assertThat(RediSearchQueryBuilder.textTerms("foo@bar.com\t4.5")).contains(List.of("foo", "bar", "com", "4", "5"));
		assertThat(RediSearchQueryBuilder.textTerms("snake_case Café")).contains(List.of("snake_case", "Café"));
		assertThat(RediSearchQueryBuilder.textTerms("The Beer")).contains(List.of("Beer"));
		// Nothing left to match, or characters Redis doesn't index as they are
		assertThat(RediSearchQueryBuilder.textTerms("To be or not to be")).isEqualTo(Optional.empty());
		assertThat(RediSearchQueryBuilder.textTerms("--")).isEqualTo(Optional.empty());
		assertThat(RediSearchQueryBuilder.textTerms("")).isEqualTo(Optional.empty());
		assertThat(RediSearchQueryBuilder.textTerms("back\\slash")).isEqualTo(Optional.empty());
		assertThat(RediSearchQueryBuilder.textTerms("two\nlines")).isEqualTo(Optional.empty());
	}

	@Test
	public void testUnsupportedDomains() {
		// Redis can't match missing fields: IS NULL, IS NOT NULL, and anything OR IS NULL
		assertThat(RediSearchQueryBuilder.isSupported(COL1, Domain.onlyNull(BIGINT))).isFalse();
		assertThat(RediSearchQueryBuilder.isSupported(COL1, Domain.notNull(BIGINT))).isFalse();
		assertThat(RediSearchQueryBuilder.isSupported(COL1,
				Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 200L)), true))).isFalse();
		// BIGINT bounds that NUMERIC fields, which hold doubles, can't compare exactly
		assertThat(RediSearchQueryBuilder.isSupported(COL1, Domain.singleValue(BIGINT, (1L << 53) - 1))).isTrue();
		assertThat(RediSearchQueryBuilder.isSupported(COL1, Domain.singleValue(BIGINT, 1L << 53))).isFalse();
		assertThat(RediSearchQueryBuilder.isSupported(COL1,
				Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, -(1L << 53))), false))).isFalse();
		assertThat(RediSearchQueryBuilder.isSupported(COL1,
				Domain.create(ValueSet.ofRanges(lessThan(BIGINT, Long.MIN_VALUE + 1)), false))).isFalse();
		// VARCHAR ranges, such as <>
		assertThat(RediSearchQueryBuilder.isSupported(COL2, Domain.create(
				ValueSet.ofRanges(lessThan(createUnboundedVarcharType(), utf8Slice("1")),
						greaterThan(createUnboundedVarcharType(), utf8Slice("1"))),
				false))).isFalse();
		assertThat(RediSearchQueryBuilder.isSupported(TEXT_COL, Domain.create(
				ValueSet.ofRanges(greaterThan(createUnboundedVarcharType(), utf8Slice("B"))), false))).isFalse();
		// An empty tag, and a text value with no terms to match
		assertThat(RediSearchQueryBuilder.isSupported(COL2, varchars(""))).isFalse();
		assertThat(RediSearchQueryBuilder.isSupported(TEXT_COL, varchars("Pocus", "The"))).isFalse();

		assertThatThrownBy(() -> new RediSearchQueryBuilder().buildQuery(
				TupleDomain.withColumnDomains(ImmutableMap.of(COL1, Domain.onlyNull(BIGINT)))))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testBuildQueryOr() {
		TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(COL1,
				Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 100L), greaterThan(BIGINT, 200L)), false)));

		String query = new RediSearchQueryBuilder().buildQuery(tupleDomain);
		String expected = "(@col1:[-inf (100.0]|@col1:[(200.0 inf])";
		assertThat(query).isEqualTo(expected);
	}

	@Test
	public void testBuildQueryInDouble() {
		RediSearchColumnHandle orderkey = new RediSearchColumnHandle("orderkey", DoubleType.DOUBLE, RediSearchFieldType.NUMERIC,
				false, true, Optional.empty());
		ValueSet values = ValueSet.ofRanges(equal(DoubleType.DOUBLE, 1.0), equal(DoubleType.DOUBLE, 2.0),
				equal(DoubleType.DOUBLE, 3.0));
		TupleDomain<ColumnHandle> tupleDomain = TupleDomain
				.withColumnDomains(ImmutableMap.of(orderkey, Domain.create(values, false)));
		String query = new RediSearchQueryBuilder().buildQuery(tupleDomain);
		String expected = "(@orderkey:[1.0 1.0]|@orderkey:[2.0 2.0]|@orderkey:[3.0 3.0])";
		assertThat(query).isEqualTo(expected);
	}

}
