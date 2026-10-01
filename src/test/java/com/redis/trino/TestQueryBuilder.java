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

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DoubleType;

public class TestQueryBuilder {

	private static final RediSearchColumnHandle COL1 = new RediSearchColumnHandle("col1", BIGINT, RediSearchFieldType.NUMERIC,
			false, true);
	private static final RediSearchColumnHandle COL2 = new RediSearchColumnHandle("col2", createUnboundedVarcharType(),
			RediSearchFieldType.TAG, false, true);
	private static final RediSearchColumnHandle TEXT_COL = new RediSearchColumnHandle("name",
			createUnboundedVarcharType(), RediSearchFieldType.TEXT, false, true);

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
		assertThat(RediSearchQueryBuilder.isExact(TEXT_COL)).isFalse();
		assertThat(RediSearchQueryBuilder.isExact(COL2)).isTrue();
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
				false, true);
		ValueSet values = ValueSet.ofRanges(equal(DoubleType.DOUBLE, 1.0), equal(DoubleType.DOUBLE, 2.0),
				equal(DoubleType.DOUBLE, 3.0));
		TupleDomain<ColumnHandle> tupleDomain = TupleDomain
				.withColumnDomains(ImmutableMap.of(orderkey, Domain.create(values, false)));
		String query = new RediSearchQueryBuilder().buildQuery(tupleDomain);
		String expected = "(@orderkey:[1.0 1.0]|@orderkey:[2.0 2.0]|@orderkey:[3.0 3.0])";
		assertThat(query).isEqualTo(expected);
	}

}
