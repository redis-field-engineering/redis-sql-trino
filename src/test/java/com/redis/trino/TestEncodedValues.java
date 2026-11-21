package com.redis.trino;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.JsonType.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

class TestEncodedValues {
	@Test
	void testNestedCollectionsAndRows() {
		Type array = new ArrayType(BIGINT);
		String encoded = "trino:v1:[\"9007199254740993\",null,\"-9223372036854775808\"]";
		Block value = decode(array, encoded);
		assertThat(array.getObjectValue(value, 0)).isEqualTo(Arrays.asList(9007199254740993L, null, Long.MIN_VALUE));
		assertThat(RediSearchPageSink.value(array, value, 0)).isEqualTo(encoded);

		Type map = new MapType(INTEGER, new ArrayType(VARCHAR), new TypeOperators());
		String mapEncoded = "trino:v1:[[\"1\",[\"x\",null]],[\"2\",null]]";
		assertThat(RediSearchPageSink.value(map, decode(map, mapEncoded), 0)).isEqualTo(mapEncoded);
		Type row = RowType.rowType(RowType.field("numbers", array), RowType.field("dictionary", map));
		String rowEncoded = "trino:v1:[[\"9007199254740993\",null,\"-9223372036854775808\"],[[\"1\",[\"x\",null]],[\"2\",null]]]";
		assertThat(RediSearchPageSink.value(row, decode(row, rowEncoded), 0)).isEqualTo(rowEncoded);
		for (Type type : List.of(array, map, row)) {
			assertThatThrownBy(() -> decode(type, "[]")).hasMessageContaining("Invalid encoded");
			assertThatThrownBy(() -> decode(type, "trino:v1:[")).hasMessageContaining("Invalid encoded");
            assertThatThrownBy(() -> decode(type, "trino:v1:null")).hasMessageContaining("Invalid encoded");
		}
		assertThatThrownBy(() -> decode(map, "trino:v1:[[null,[]]]")).hasMessageContaining("Invalid encoded");
		assertThatThrownBy(() -> decode(row, "trino:v1:[]")).hasMessageContaining("Invalid encoded");
	}

	@Test
	void testBinaryJsonAndTime() {
		BlockBuilder binary = VARBINARY.createBlockBuilder(null, 1);
		VARBINARY.writeSlice(binary, wrappedBuffer(new byte[] {0, -1, -128, 65}));
		assertRoundTrip(VARBINARY, binary.build());
		Block json = decode(JSON, "{\"a\":[true,null,9007199254740993]}");
		assertThat(RediSearchPageSink.value(JSON, json, 0)).isEqualTo("{\"a\":[true,null,9007199254740993]}");
		for (int precision = 0; precision <= 12; precision++) {
			TimeType type = TimeType.createTimeType(precision);
			long step = (long) Math.pow(10, 12 - precision);
			BlockBuilder value = type.createBlockBuilder(null, 1);
			type.writeLong(value, 86_399_123_456_789_012L / step * step);
			assertRoundTrip(type, value.build());
		}
		assertThatThrownBy(() -> decode(TimeType.TIME_MILLIS, "86400000000000000"))
				.hasMessageContaining("not a valid time");
	}

	@Test
	void testEveryTemporalPrecision() {
		for (int precision = 0; precision <= 12; precision++) {
			TimestampType timestamp = TimestampType.createTimestampType(precision);
			BlockBuilder value = timestamp.createBlockBuilder(null, 1);
			if (timestamp.isShort()) {
				long step = (long) Math.pow(10, 6 - precision);
				timestamp.writeLong(value, -876_543_210L / step * step);
			} else {
				int step = (int) Math.pow(10, 12 - precision);
				timestamp.writeObject(value, new LongTimestamp(-876_543_210L, 123456 / step * step));
			}
			assertRoundTrip(timestamp, value.build());

			TimestampWithTimeZoneType zoned = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision);
			BlockBuilder zoneValue = zoned.createBlockBuilder(null, 1);
			if (zoned.isShort()) {
				long step = (long) Math.pow(10, 3 - precision);
				zoned.writeLong(zoneValue, DateTimeEncoding.packDateTimeWithZone(-876_543L / step * step, TimeZoneKey.UTC_KEY));
			} else {
				int step = (int) Math.pow(10, 12 - precision);
				zoned.writeObject(zoneValue, LongTimestampWithTimeZone.fromEpochMillisAndFraction(-876_543L,
						123456789 / step * step, TimeZoneKey.getTimeZoneKey("America/Los_Angeles")));
			}
			assertRoundTrip(zoned, zoneValue.build());

			TimeWithTimeZoneType time = TimeWithTimeZoneType.createTimeWithTimeZoneType(precision);
			BlockBuilder timeValue = time.createBlockBuilder(null, 1);
			long step = (long) Math.pow(10, 12 - precision);
			long picos = 12_345_123_456_789_012L / step * step;
			if (time.isShort()) {
				time.writeLong(timeValue, DateTimeEncoding.packTimeWithTimeZone(picos / 1000, -7 * 60));
			} else {
				time.writeObject(timeValue, new LongTimeWithTimeZone(picos, -7 * 60));
			}
			assertRoundTrip(time, timeValue.build());
		}
	}

	private static void assertRoundTrip(Type type, Block original) {
		String encoded = RediSearchPageSink.value(type, original, 0);
		Block decoded = decode(type, encoded);
		assertThat(type.getObjectValue(decoded, 0)).isEqualTo(type.getObjectValue(original, 0));
		assertThat(RediSearchSession.toFieldType(type)).isNotNull();
	}

	private static Block decode(Type type, String encoded) {
		BlockBuilder output = type.createBlockBuilder(null, 1);
		RediSearchPageSourceResultWriter.writer(type).write(output, encoded);
		return output.build();
	}
}
