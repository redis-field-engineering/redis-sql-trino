package com.redis.trino;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;

import io.airlift.slice.SizeOf;
import io.lettuce.core.search.AggregationReply.Cursor;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

/** Exact bounded TopN: retain typed sort keys, then fetch wide projections for only the winners. */
final class RediSearchLocalTopN {
    private RediSearchLocalTopN() {}

    static boolean supports(Type type) {
        if (!type.isOrderable()) { return false; }
        try {
            RediSearchSession.toFieldType(type);
            return true;
        } catch (io.trino.spi.TrinoException unsupported) {
            return false;
        }
    }

    static Comparator<String[]> comparator(boolean ascending) { return comparator(BIGINT, ascending); }

    static Comparator<String[]> comparator(Type type, boolean ascending) {
        return comparator(List.of(type), List.of(new RediSearchSortItem("sort", ascending, true)));
    }

    /** Raw-row adapter for focused ordering tests; production decodes each scanned row only once. */
    static Comparator<String[]> comparator(List<Type> types, List<RediSearchSortItem> sorts) {
        Ordering ordering = new Ordering(types, sorts);
        return (left, right) -> ordering.compare(ordering.decode(left), ordering.decode(right));
    }

    static RediSearchSession.AggregateResult read(RediSearchSession session, RediSearchSession.Connection scan,
            RediSearchTableHandle table, List<RediSearchColumnHandle> outputs,
            RediSearchSession.ExactHashReader exactReader, RediSearchReadStats stats) {
        return read(session, scan, table, outputs, exactReader, stats, MemoryContext.NO_LIMIT);
    }

    static RediSearchSession.AggregateResult read(RediSearchSession session, RediSearchSession.Connection scan,
            RediSearchTableHandle table, List<RediSearchColumnHandle> outputs,
            RediSearchSession.ExactHashReader exactReader, RediSearchReadStats stats, MemoryContext memoryContext) {
        try {
            return readTracked(session, scan, table, outputs, exactReader, stats, new Reservation(memoryContext));
        } finally {
            // The page source immediately takes ownership of and reserves the returned batch.
            memoryContext.setBytes(0);
        }
    }

    private static RediSearchSession.AggregateResult readTracked(RediSearchSession session, RediSearchSession.Connection scan,
            RediSearchTableHandle table, List<RediSearchColumnHandle> outputs,
            RediSearchSession.ExactHashReader exactReader, RediSearchReadStats stats, Reservation memory) {
        RediSearchTable schema = session.getTable(table.getSchemaTableName());
        Map<String, RediSearchColumnHandle> schemaColumns = new HashMap<>();
        schema.getColumns().forEach(column -> schemaColumns.put(column.getName(), column));
        Map<String, String> identifiers = new HashMap<>();
        schema.getIndexInfo().getFields().forEach(field -> identifiers.put(field.getAttribute(), field.getIdentifier()));
        List<RediSearchColumnHandle> orders = table.getSort().stream().map(sort -> schemaColumns.get(sort.getColumn())).toList();
        Ordering ordering = new Ordering(orders.stream().map(RediSearchColumnHandle::getType).toList(), table.getSort());

        // Reading DOUBLE/DECIMAL through LOAD * would transfer every wide HASH payload. Fetch only those exact sort
        // fields with pipelined HMGET. Other types keep the existing efficient narrow cursor path (including BIGINT
        // stored-value recovery), and repeated sort columns share the same source column.
        List<RediSearchColumnHandle> selection = new ArrayList<>();
        selection.add(RediSearchBuiltinField.KEY.getColumnHandle());
        Map<String, Integer> selectedPositions = new HashMap<>();
        selectedPositions.put(RediSearchBuiltinField.KEY.getName(), 0);
        Map<String, List<Integer>> exactPositions = new LinkedHashMap<>();
        int[] scanPositions = new int[orders.size()];
        for (int i = 0; i < orders.size(); i++) {
            RediSearchColumnHandle column = orders.get(i);
            if (needsExactSortRead(column)) {
                exactPositions.computeIfAbsent(identifiers.getOrDefault(column.getName(), column.getName()), ignored -> new ArrayList<>()).add(i + 1);
                scanPositions[i] = -1;
            } else {
                Integer position = selectedPositions.get(column.getName());
                if (position == null) {
                    position = selection.size();
                    selectedPositions.put(column.getName(), position);
                    selection.add(column);
                }
                scanPositions[i] = position;
            }
        }
        List<String> exactFields = List.copyOf(exactPositions.keySet());
        RediSearchTableHandle narrow = table.withoutTopN();
        int limit = Math.toIntExact(table.getLimit().orElseThrow());
        PriorityQueue<Candidate> winners = new PriorityQueue<>(limit, ordering.reversed());
        long heapBytes = SizeOf.sizeOfObjectArray(limit);
        long started = System.nanoTime();
        long sortReadsBefore = stats.exactHashReads.sum();
        Optional<Cursor> cursor = Optional.empty();
        try {
            RediSearchSession.AggregateResult batch = session.aggregate(scan, narrow, selection, exactReader, stats);
            cursor = batch.getCursor();
            while (true) {
                checkActive(session, started);
                List<String[]> inputs = new ArrayList<>(batch.getRows().size());
                List<CompletableFuture<?>> reads = new ArrayList<>();
                memory.setBase(heapBytes + retainedRows(batch.getRows()));
                for (String[] source : batch.getRows()) {
                    String[] input = new String[orders.size() + 1];
                    input[0] = source[0];
                    for (int i = 0; i < orders.size(); i++) {
                        if (scanPositions[i] >= 0) { input[i + 1] = source[scanPositions[i]]; }
                    }
                    inputs.add(input);
                    memory.addHydrated(SizeOf.sizeOf(input));
                    if (!exactFields.isEmpty()) {
                        stats.exactHashReads.add(exactFields.size());
                        stats.exactHashCommands.increment();
                        reads.add(exactReader.readFields(input[0], exactFields).thenAccept(values -> {
                            long bytes = 0;
                            for (int i = 0; i < values.size(); i++) {
                                String value = values.get(i).hasValue() ? values.get(i).getValue() : null;
                                for (int position : exactPositions.get(exactFields.get(i))) { input[position] = value; }
                                bytes += SizeOf.estimatedSizeOf(value);
                            }
                            memory.addHydrated(bytes);
                        }));
                    }
                }
                awaitReads(exactReader, reads, stats);
                checkActive(session, started);
                long batchBytes = retainedRows(batch.getRows()) + retainedRows(inputs);
                memory.resetHydrated(batchBytes + heapBytes);
                long conversionStarted = System.nanoTime();
                try {
                    int checked = 0;
                    for (String[] row : inputs) {
                        if ((checked++ & 127) == 0) { checkActive(session, started); }
                        // Decode every row before deciding whether it wins. Invalid loser rows must fail exactly as
                        // they would in an ordinary Trino sort, including overflow and malformed encoded values.
                        Candidate candidate = ordering.decode(row);
                        memory.setBase(batchBytes + heapBytes + candidate.retainedBytes());
                        if (winners.size() < limit) {
                            winners.add(candidate);
                            heapBytes += candidate.retainedBytes();
                        } else if (ordering.compare(candidate, winners.peek()) < 0) {
                            heapBytes -= winners.remove().retainedBytes();
                            winners.add(candidate);
                            heapBytes += candidate.retainedBytes();
                        }
                        memory.setBase(batchBytes + heapBytes);
                    }
                } finally {
                    stats.conversionNanos.add(System.nanoTime() - conversionStarted);
                }
                if (cursor.isEmpty()) { break; }
                var reply = session.cursorReadAsync(scan, narrow, cursor.orElseThrow(), stats, batch.getReader().getCursorCount()).join();
                Optional<Cursor> next = RediSearchSession.nextCursor(reply, cursor);
                try { batch = session.result(exactReader, batch.getReader(), cursor, reply, stats); }
                finally { cursor = next; }
            }
        } finally {
            cursor.ifPresent(current -> session.cursorDeleteAsync(scan, narrow, current));
        }
        stats.localTopNSortHashReads.add(stats.exactHashReads.sum() - sortReadsBefore);
        List<Candidate> candidates = new ArrayList<>(winners);
        candidates.sort(ordering);
        stats.localTopNCandidates.add(candidates.size());
        memory.setBase(heapBytes + SizeOf.sizeOfObjectArray(candidates.size()));
        List<RediSearchColumnHandle> columns = RediSearchTranslator.readColumns(outputs);
        List<String[]> rows = new ArrayList<>();
        List<CompletableFuture<?>> reads = new ArrayList<>();
        for (Candidate candidate : candidates) {
            checkActive(session, started);
            String[] row = new String[columns.size()];
            memory.addHydrated(SizeOf.sizeOf(row));
            List<String> fields = new ArrayList<>();
            List<Integer> positions = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                String name = columns.get(i).getName();
                if (RediSearchBuiltinField.isKeyColumn(name)) { row[i] = candidate.raw[0]; continue; }
                // Keep winner hydration unchanged: it can include the ordering column and is counted separately.
                fields.add(identifiers.getOrDefault(name, name));
                positions.add(i);
            }
            if (!fields.isEmpty()) {
                stats.exactHashReads.add(fields.size());
                stats.exactHashCommands.increment();
                stats.localTopNProjectedHashReads.add(fields.size());
                reads.add(exactReader.readFields(candidate.raw[0], fields).thenAccept(values -> {
                    for (int i = 0; i < values.size(); i++) {
                        row[positions.get(i)] = values.get(i).hasValue() ? values.get(i).getValue() : null;
                    }
                    long bytes = 0;
                    for (String value : row) { bytes += SizeOf.estimatedSizeOf(value); }
                    memory.addHydrated(bytes);
                }));
            }
            rows.add(row);
            // A raised candidate budget must not queue an equally large wave of client futures/commands.
            if (reads.size() >= 1000) {
                awaitReads(exactReader, reads, stats);
                reads.clear();
                checkActive(session, started);
            }
        }
        awaitReads(exactReader, reads, stats);
        checkActive(session, started);
        RediSearchRowReader reader = new RediSearchRowReader(columns.stream().map(RediSearchColumnHandle::getName).toList(),
                Map.of(), Map.of(), java.util.Set.of(), List.of(), Map.of(), Map.of(), Optional.of(outputs));
        rows.replaceAll(reader::project);
        memory.setBase(heapBytes + retainedRows(rows));
        return new RediSearchSession.AggregateResult(rows, Optional.empty(), reader);
    }

    private static boolean needsExactSortRead(RediSearchColumnHandle column) {
        return column.getFieldType() == RediSearchFieldType.NUMERIC
                && (column.getType() instanceof DecimalType || column.getType().equals(DoubleType.DOUBLE) || column.getType().equals(RealType.REAL));
    }

    private static void awaitReads(RediSearchSession.ExactHashReader reader, List<CompletableFuture<?>> reads,
            RediSearchReadStats stats) {
        if (reads.isEmpty()) { return; }
        stats.exactHashReadBatches.increment();
        long start = System.nanoTime();
        try { reader.flush(); CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new)).join(); }
        finally { stats.exactWaitNanos.add(System.nanoTime() - start); }
    }

    private static void checkActive(RediSearchSession session, long started) {
        if (Thread.currentThread().isInterrupted()) { throw new java.util.concurrent.CancellationException(); }
        long timeout = session.getConfig().getQueryTimeoutMillis();
        if (timeout > 0 && java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) >= timeout) {
            throw new io.trino.spi.TrinoException(RediSearchErrorCode.REDISEARCH_INCOMPLETE_RESULT,
                    "Exact TopN selection exceeded query-timeout-ms; no candidate rows were returned");
        }
    }

    private static final class Candidate {
        private static final int INSTANCE_SIZE = SizeOf.instanceSize(Candidate.class);
        private final String[] raw;
        private final Block[] keys;

        private Candidate(String[] raw, Block[] keys) { this.raw = raw; this.keys = keys; }

        long retainedBytes() {
            long bytes = INSTANCE_SIZE + rowBytes(raw) + SizeOf.sizeOf(keys);
            for (Block key : keys) { bytes += key.getRetainedSizeInBytes(); }
            return bytes;
        }
    }

    private static final class Ordering implements Comparator<Candidate> {
        private final List<Type> types;
        private final RediSearchPageSourceResultWriter.ValueWriter[] writers;
        private final MethodHandle[] comparisons;

        private Ordering(List<Type> types, List<RediSearchSortItem> sorts) {
            this.types = List.copyOf(types);
            writers = types.stream().map(RediSearchPageSourceResultWriter::writer).toArray(RediSearchPageSourceResultWriter.ValueWriter[]::new);
            comparisons = new MethodHandle[types.size()];
            TypeOperators operators = new TypeOperators();
            for (int i = 0; i < types.size(); i++) {
                comparisons[i] = operators.getOrderingOperator(types.get(i), sorts.get(i).sortOrder(),
                        simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
            }
        }

        private Candidate decode(String[] raw) {
            Block[] keys = new Block[types.size()];
            for (int i = 0; i < types.size(); i++) {
                BlockBuilder builder = types.get(i).createBlockBuilder(null, 1);
                if (raw[i + 1] == null) { builder.appendNull(); }
                else { writers[i].write(builder, raw[i + 1]); }
                keys[i] = builder.build();
            }
            return new Candidate(raw, keys);
        }

        @Override
        public int compare(Candidate left, Candidate right) {
            try {
                for (int i = 0; i < comparisons.length; i++) {
                    int comparison = (int) comparisons[i].invokeExact(left.keys[i], 0, right.keys[i], 0);
                    if (comparison != 0) { return comparison; }
                }
                return 0;
            } catch (RuntimeException | Error failure) {
                throw failure;
            } catch (Throwable failure) {
                throw new RuntimeException("Cannot compare exact TopN keys", failure);
            }
        }
    }

    private static long rowBytes(String[] row) {
        long bytes = SizeOf.sizeOf(row);
        for (String value : row) { bytes += SizeOf.estimatedSizeOf(value); }
        return bytes;
    }

    private static long retainedRows(List<String[]> rows) { return SizeOf.estimatedSizeOf(rows, RediSearchLocalTopN::rowBytes); }

    /** Hydration callbacks may finish on different Lettuce threads; serialize reservation updates. */
    private static final class Reservation {
        private final MemoryContext context;
        private long base;
        private long hydrated;

        private Reservation(MemoryContext context) { this.context = context; }
        synchronized void setBase(long bytes) { base = bytes; context.setBytes(base + hydrated); }
        synchronized void addHydrated(long bytes) { hydrated += bytes; context.setBytes(base + hydrated); }
        synchronized void resetHydrated(long bytes) { hydrated = 0; base = bytes; context.setBytes(base); }
    }
}
