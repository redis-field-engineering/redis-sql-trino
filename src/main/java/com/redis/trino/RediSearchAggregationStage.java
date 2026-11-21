package com.redis.trino;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** One ordered GROUPBY/REDUCE step; later stages consume its keys and reducer outputs, not source documents. */
public record RediSearchAggregationStage(
        @JsonProperty("terms") List<RediSearchAggregationTerm> terms,
        @JsonProperty("metrics") List<RediSearchAggregation> metrics,
        @JsonProperty("groupingOnly") boolean groupingOnly) {
    @JsonCreator
    public RediSearchAggregationStage {
        terms = List.copyOf(terms);
        metrics = List.copyOf(metrics);
        if (groupingOnly && (terms.isEmpty() || metrics.isEmpty())) {
            throw new IllegalArgumentException("Grouping-only stages require keys and an internal reducer");
        }
    }
}
