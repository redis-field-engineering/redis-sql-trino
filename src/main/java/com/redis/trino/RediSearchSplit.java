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
