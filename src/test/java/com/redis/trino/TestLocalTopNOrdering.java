package com.redis.trino;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.airlift.json.JsonCodec;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

class TestLocalTopNOrdering {
    @Test
    void testTypedMultiKeyNullOrderingAndPrecision() {
        var comparator = RediSearchLocalTopN.comparator(List.of(DecimalType.createDecimalType(30, 4), BIGINT, VARCHAR),
                List.of(sort(false, true), sort(true, false), sort(false, true)));
        assertThat(comparator.compare(row(null, "9223372036854775807", "x"), row("-1.0", "1", "z"))).isNegative();
        assertThat(comparator.compare(row("9007199254740993.0001", "1", "a"), row("9007199254740993.0000", "0", "z"))).isNegative();
        assertThat(comparator.compare(row("1.0", "9007199254740993", "a"), row("1.0000", "9007199254740992", "z"))).isPositive();
        assertThat(comparator.compare(row("1", "2", null), row("1.0000", "2", "z"))).isNegative();
        assertThat(comparator.compare(row("1", "2", "z"), row("1.0000", "2", "a"))).isNegative();
    }

    @Test
    void testTrinoScalarAndNestedOrdering() {
        assertLess(VARCHAR, "\uE000", "\uD800\uDC00"); // UTF-8/codepoint ordering differs from UTF-16 ordering.
        assertLess(DATE, "1969-12-31", "1970-01-01");
        assertLess(VARBINARY, "AA==", "/w==");
        assertLess(TimestampType.createTimestampType(12), "-1:999999", "0:0");
        assertLess(new ArrayType(BIGINT), "trino:v1:[\"9007199254740992\",null]", "trino:v1:[\"9007199254740993\"]");
        assertThat(RediSearchLocalTopN.comparator(CharType.createCharType(4), true)
                .compare(row("a"), row("a   "))).isZero();
        assertThat(RediSearchLocalTopN.comparator(DOUBLE, true).compare(row("NaN"), row("Infinity"))).isPositive();
        assertThat(RediSearchLocalTopN.comparator(DOUBLE, false).compare(row("NaN"), row("Infinity"))).isNegative();
        short zone = TimeZoneKey.getTimeZoneKey("America/Los_Angeles").getKey();
        assertThat(RediSearchLocalTopN.comparator(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(12), true)
                .compare(row("0:0:" + zone), row("0:0:0"))).isZero();
        assertThat(RediSearchLocalTopN.comparator(TimeWithTimeZoneType.createTimeWithTimeZoneType(12), true)
                .compare(row("3600000000000000:60"), row("0:0"))).isZero();
    }

    @Test
    void testMalformedValuesFailBeforeRanking() {
        var bigint = RediSearchLocalTopN.comparator(BIGINT, true);
        assertThat(bigint.compare(row("4.2e1"), row("43"))).isNegative();
        assertThatThrownBy(() -> bigint.compare(row("1.5"), row("43"))).hasMessageContaining("not a valid bigint");
        assertThatThrownBy(() -> bigint.compare(row("9223372036854775808"), row("43"))).hasMessageContaining("not a valid bigint");
        assertThatThrownBy(() -> RediSearchLocalTopN.comparator(new ArrayType(BIGINT), true)
                .compare(row("trino:v1:null"), row("trino:v1:[]"))).hasMessageContaining("Invalid encoded");
    }

    @Test
    void testSortItemSerializationAndLegacyDefaults() {
        JsonCodec<RediSearchSortItem> codec = JsonCodec.jsonCodec(RediSearchSortItem.class);
        RediSearchSortItem item = new RediSearchSortItem("amount", false, true, true);
        assertThat(codec.fromJson(codec.toJson(item))).isEqualTo(item);
        assertThat(codec.fromJson("{\"column\":\"id\",\"ascending\":true,\"local\":true}").isNullsFirst()).isFalse();
    }

    private static void assertLess(Type type, String left, String right) {
        assertThat(RediSearchLocalTopN.comparator(type, true).compare(row(left), row(right))).isNegative();
        assertThat(RediSearchLocalTopN.comparator(type, false).compare(row(left), row(right))).isPositive();
    }

    private static RediSearchSortItem sort(boolean ascending, boolean nullsFirst) {
        return new RediSearchSortItem("unused", ascending, true, nullsFirst);
    }

    private static String[] row(String... values) {
        String[] row = new String[values.length + 1];
        row[0] = "key";
        System.arraycopy(values, 0, row, 1, values.length);
        return row;
    }
}
