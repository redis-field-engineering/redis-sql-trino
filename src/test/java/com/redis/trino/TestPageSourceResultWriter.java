package com.redis.trino;

import static io.trino.type.JsonType.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.trino.json.Json;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

class TestPageSourceResultWriter {

	@ParameterizedTest
	@ValueSource(strings = { "{\"a\":[true,null,\"hello\"],\"b\":9007199254740993}",
			"[0.1234567890123456,9223372036854775807]", "true", "null", "\"hello\"" })
	void testNativeJsonValues(String value) {
		BlockBuilder output = JSON.createBlockBuilder(null, 1);
		RediSearchPageSourceResultWriter.writer(JSON).write(output, value);
		Block block = output.build();

		assertThat(block.isNull(0)).isFalse();
		assertThat(JSON.getObject(block, 0)).isInstanceOf(Json.class);
		assertThat(JSON.getSlice(block, 0).toStringUtf8()).isEqualTo(value);
	}

	@ParameterizedTest
	@ValueSource(strings = { "{", "true false", "not json" })
	void testInvalidJsonIsRejected(String value) {
		BlockBuilder output = JSON.createBlockBuilder(null, 1);
		assertThatThrownBy(() -> RediSearchPageSourceResultWriter.writer(JSON).write(output, value))
				.isInstanceOf(TrinoException.class)
				.hasMessageContaining("Cannot convert value to JSON");
	}
}
