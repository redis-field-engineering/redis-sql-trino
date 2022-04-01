package com.redis.trino;

import static com.google.common.base.Verify.verify;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.redis.lettucemod.search.Document;

import io.airlift.slice.SliceOutput;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;

public class RediSearchPageSource implements ConnectorPageSource {

	private static final int ROWS_PER_REQUEST = 1024;

	private final RediSearchPageSourceResultWriter writer = new RediSearchPageSourceResultWriter();
	private final Iterator<Document<String, String>> cursor;
	private final List<String> columnNames;
	private final List<Type> columnTypes;
	private Document<String, String> currentDoc;
	private long count;
	private boolean finished;

	private final PageBuilder pageBuilder;

	public RediSearchPageSource(RediSearchSession rediSearchSession, RediSearchTableHandle tableHandle,
			List<RediSearchColumnHandle> columns) {
		this.columnNames = columns.stream().map(RediSearchColumnHandle::getName).collect(toList());
		this.columnTypes = columns.stream().map(RediSearchColumnHandle::getType).collect(toList());
		this.cursor = rediSearchSession.search(tableHandle, columns).iterator();
		this.currentDoc = null;
		this.pageBuilder = new PageBuilder(columnTypes);
	}

	@Override
	public long getCompletedBytes() {
		return count;
	}

	@Override
	public long getReadTimeNanos() {
		return 0;
	}

	@Override
	public boolean isFinished() {
		return finished;
	}

	@Override
	public long getMemoryUsage() {
		return 0L;
	}

	@Override
	public Page getNextPage() {
		verify(pageBuilder.isEmpty());
		count = 0;
		for (int i = 0; i < ROWS_PER_REQUEST; i++) {
			if (!cursor.hasNext()) {
				finished = true;
				break;
			}
			currentDoc = cursor.next();
			count++;

			pageBuilder.declarePosition();
			for (int column = 0; column < columnTypes.size(); column++) {
				BlockBuilder output = pageBuilder.getBlockBuilder(column);
				String value = currentDoc.get(columnNames.get(column));
				if (value == null) {
					output.appendNull();
				} else {
					writer.appendTo(columnTypes.get(column), value, output);
				}
			}
		}

		Page page = pageBuilder.build();
		pageBuilder.reset();
		return page;
	}

	public static JsonGenerator createJsonGenerator(JsonFactory factory, SliceOutput output) throws IOException {
		return factory.createGenerator((OutputStream) output);
	}

	@Override
	public void close() {
		// nothing to do
	}
}
