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

import static io.trino.spi.type.BigintType.BIGINT;

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.CharType;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.DateType.DATE;

public class RediSearchAggregationTerm {

	private final String term;
	private final Type type;

	@JsonCreator
	public RediSearchAggregationTerm(@JsonProperty("term") String term, @JsonProperty("type") Type type) {
		this.term = term;
		this.type = type;
	}

	@JsonProperty
	public String getTerm() {
		return term;
	}

	@JsonProperty
	public Type getType() {
		return type;
	}

	public static Optional<RediSearchAggregationTerm> fromColumnHandle(ColumnHandle columnHandle) {
		RediSearchColumnHandle column = (RediSearchColumnHandle) columnHandle;
		// Arithmetic isn't a field Redis can group by
		if (column.getExpression().isPresent()) {
			return Optional.empty();
		}
		// Encoded SQL types must group after decoding; their Redis strings need not have SQL equality semantics.
		// Unindexed VARCHAR is already its SQL representation and may still be loaded and grouped exactly.
		if (!column.isSupportsPredicates() && !(column.getType() instanceof VarcharType) && !column.getType().equals(BOOLEAN)
                && !(column.getType() instanceof CharType && RediSearchGroupingNormalizer.requiresNormalization(column.getType()))) {
			return Optional.empty();
		}
		// SQL decoding can collapse distinct stored spellings (TRUE/true, UUID case, CHAR padding,
		// and double-to-float rounding). Grouping raw Redis values would leave duplicate SQL groups.
		if (column.getType().equals(UUID) || column.getType().equals(REAL) || column.getType().equals(DATE)
				|| (column.getType() instanceof CharType && !RediSearchGroupingNormalizer.requiresNormalization(column.getType()))
                || column.getType().equals(TIMESTAMP_MILLIS)) {
			return Optional.empty();
		}
		// Redis groups a NUMERIC field by its double value and returns it formatted as one, which a DECIMAL column
		// may not be able to hold exactly
		if (column.getType() instanceof DecimalType) {
			return Optional.empty();
		}
		// NUMERIC groups use doubles: adjacent BIGINTs beyond 2^53 can collapse into one group. Reading the
		// stored hash value afterward cannot recover either the original groups or their counts.
		if (column.getType().equals(BIGINT) && column.getFieldType() == RediSearchFieldType.NUMERIC) {
			return Optional.empty();
		}
		// A floating-point key is also returned as a mantissa and exponent, by expressions that refer to it as @name
		if ((RediSearchExactNumbers.isFloatingPoint(column.getType()) || RediSearchGroupingNormalizer.requiresNormalization(column.getType()))
				&& !RediSearchQueryBuilder.isProperty(column.getName())) {
			return Optional.empty();
		}
		return Optional.of(new RediSearchAggregationTerm(column.getName(), column.getType()));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		RediSearchAggregationTerm that = (RediSearchAggregationTerm) o;
		return Objects.equals(term, that.term) && Objects.equals(type, that.type);
	}

	@Override
	public int hashCode() {
		return Objects.hash(term, type);
	}

	@Override
	public String toString() {
		return term;
	}
}
