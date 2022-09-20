package com.redis.trino;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Range.equal;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.predicate.Range.lessThan;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.redis.lettucemod.search.Field;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DoubleType;

public class TestRediSearchQueryBuilder {

	private static final RediSearchColumnHandle COL1 = new RediSearchColumnHandle("col1", BIGINT, Field.Type.NUMERIC,
			false, true);
	private static final RediSearchColumnHandle COL2 = new RediSearchColumnHandle("col2", createUnboundedVarcharType(),
			Field.Type.TAG, false, true);

	@Test
	public void testBuildQuery() {
		TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
				ImmutableMap.of(COL1, Domain.create(ValueSet.ofRanges(range(BIGINT, 100L, false, 200L, true)), false),
						COL2, Domain.singleValue(createUnboundedVarcharType(), utf8Slice("a value"))));

		String query = new RediSearchQueryBuilder().buildQuery(tupleDomain);
		String expected = "((@col1:[(100.0 inf] @col1:[-inf 200.0]) @col2:{a\\ value})";
		assertEquals(query, expected);
	}

	@Test
	public void testBuildQueryIn() {
		TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(COL2,
				Domain.create(ValueSet.ofRanges(equal(createUnboundedVarcharType(), utf8Slice("hello")),
						equal(createUnboundedVarcharType(), utf8Slice("world"))), false)));
		String query = new RediSearchQueryBuilder().buildQuery(tupleDomain);
		String expected = "(@col2:{world}|@col2:{hello})";
		assertEquals(query, expected);

	}

	@Test
	public void testBuildQueryOr() {
		TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(COL1,
				Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 100L), greaterThan(BIGINT, 200L)), false)));

		String query = new RediSearchQueryBuilder().buildQuery(tupleDomain);
		String expected = "(@col1:[-inf (100.0]|@col1:[(200.0 inf])";
		assertEquals(query, expected);
	}

	@Test
	public void testBuildQueryNull() {
		TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
				ImmutableMap.of(COL1, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 200L)), true)));

		String query = new RediSearchQueryBuilder().buildQuery(tupleDomain);
		String expected = "@col1:[(200.0 inf]";
		assertEquals(query, expected);
	}

	@Test
	public void testBuildQueryInDouble() {
		RediSearchColumnHandle orderkey = new RediSearchColumnHandle("orderkey", DoubleType.DOUBLE, Field.Type.NUMERIC,
				false, true);
		ValueSet values = ValueSet.ofRanges(equal(DoubleType.DOUBLE, 1.0), equal(DoubleType.DOUBLE, 2.0),
				equal(DoubleType.DOUBLE, 3.0));
		TupleDomain<ColumnHandle> tupleDomain = TupleDomain
				.withColumnDomains(ImmutableMap.of(orderkey, Domain.create(values, false)));
		String query = new RediSearchQueryBuilder().buildQuery(tupleDomain);
		String expected = "(@orderkey:[1.0 1.0]|@orderkey:[2.0 2.0]|@orderkey:[3.0 3.0])";
		assertEquals(query, expected);
	}

}
