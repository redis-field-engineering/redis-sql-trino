package com.redis.trino;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.openjdk.jol.info.ClassLayout;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

public class RediSearchSplit implements ConnectorSplit {

	private static final int INSTANCE_SIZE = ClassLayout.parseClass(RediSearchSplit.class).instanceSize();

	private final List<HostAddress> addresses;

	@JsonCreator
	public RediSearchSplit(@JsonProperty("addresses") List<HostAddress> addresses) {
		this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
	}

	@Override
	public boolean isRemotelyAccessible() {
		return true;
	}

	@Override
	@JsonProperty
	public List<HostAddress> getAddresses() {
		return addresses;
	}

	@Override
	public Object getInfo() {
		return this;
	}

	@Override
	public long getRetainedSizeInBytes() {
		return INSTANCE_SIZE + SizeOf.estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
	}
}
