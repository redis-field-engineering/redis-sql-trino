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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Differences of nested sets S(b) = documents matching @field:[b +inf].
 * Unlike adjacent ranges, S(a) minus S(b) cannot overlap on multi-valued JSON fields.
 * The complement of the first set also covers missing and non-numeric values.
 */
public record RediSearchScanPartition(@JsonProperty("field") String field,
        @JsonProperty("lower") Optional<Double> lower, @JsonProperty("upper") Optional<Double> upper,
        @JsonProperty("connectionSlot") int connectionSlot) {
    public RediSearchScanPartition(String field, Optional<Double> lower, Optional<Double> upper) {
        this(field, lower, upper, 0);
    }

    public RediSearchScanPartition {
        com.google.common.base.Preconditions.checkArgument(connectionSlot >= 0 && connectionSlot < 64, "Invalid connection slot");
        java.util.Objects.requireNonNull(field, "field is null");
        java.util.Objects.requireNonNull(lower, "lower is null");
        java.util.Objects.requireNonNull(upper, "upper is null");
        com.google.common.base.Preconditions.checkArgument(RediSearchQueryBuilder.isProperty(field), "Invalid partition field");
        com.google.common.base.Preconditions.checkArgument(lower.isPresent() || upper.isPresent(), "Unbounded partition");
        com.google.common.base.Preconditions.checkArgument(lower.stream().allMatch(Double::isFinite)
                && upper.stream().allMatch(Double::isFinite)
                && (lower.isEmpty() || upper.isEmpty() || lower.get() < upper.get()), "Invalid partition bounds");
    }

    public String query() {
        return lower.map(this::atLeast).orElse("") + (lower.isPresent() && upper.isPresent() ? " " : "")
                + upper.map(value -> "-" + atLeast(value)).orElse("");
    }

    private String atLeast(double value) { return "@" + field + ":[" + Double.toString(value) + " +inf]"; }

    static boolean supported(RediSearchIndexInfo info, String field) {
        return info.getKeyType().isPresent() && info.getFields().stream().anyMatch(candidate ->
                candidate.getAttribute().equals(field) && candidate.getType() == RediSearchFieldType.NUMERIC
                        && candidate.isIndexed());
    }

    static List<RediSearchScanPartition> plan(RediSearchConfig config, RediSearchTableHandle table,
            RediSearchIndexInfo info) {
        int count = (int) Math.min(config.getScanSplits(), config.getScanConnections());
        String field = config.getScanPartitionField();
        List<Double> cuts = config.getScanPartitionBoundaries();
        if (count <= 1 || cuts.isEmpty() || field == null || !RediSearchQueryBuilder.isProperty(field)
                || !supported(info, field) || !isDocumentScan(table)) {
            return List.of();
        }
        count = Math.min(count, cuts.size() + 1);
        List<Double> selected = new ArrayList<>();
        for (int i = 1; i < count; i++) {
            selected.add(cuts.get((int) ((long) i * (cuts.size() + 1) / count) - 1));
        }
        List<RediSearchScanPartition> partitions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            partitions.add(new RediSearchScanPartition(field,
                    i == 0 ? Optional.empty() : Optional.of(selected.get(i - 1)),
                    i == count - 1 ? Optional.empty() : Optional.of(selected.get(i)), i));
        }
        return List.copyOf(partitions);
    }

    static boolean isDocumentScan(RediSearchTableHandle table) {
        return table.getLimit().isEmpty() && table.getSort().isEmpty()
                && table.getTermAggregations().isEmpty() && table.getMetricAggregations().isEmpty();
    }
}
