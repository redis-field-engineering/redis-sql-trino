package com.redis.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Objects;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RediSearchTransactionHandle implements ConnectorTransactionHandle {
	private final UUID uuid;

	public RediSearchTransactionHandle() {
		this(UUID.randomUUID());
	}

	@JsonCreator
	public RediSearchTransactionHandle(@JsonProperty("uuid") UUID uuid) {
		this.uuid = requireNonNull(uuid, "uuid is null");
	}

	@JsonProperty
	public UUID getUuid() {
		return uuid;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if ((obj == null) || (getClass() != obj.getClass())) {
			return false;
		}
		RediSearchTransactionHandle other = (RediSearchTransactionHandle) obj;
		return Objects.equals(uuid, other.uuid);
	}

	@Override
	public int hashCode() {
		return Objects.hash(uuid);
	}

	@Override
	public String toString() {
		return toStringHelper(this).add("uuid", uuid).toString();
	}
}
