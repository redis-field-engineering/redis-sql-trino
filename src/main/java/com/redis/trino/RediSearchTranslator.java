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
import java.util.Optional;

import io.lettuce.core.search.arguments.AggregateArgs;
import io.lettuce.core.search.arguments.AggregateArgs.GroupBy;
import io.lettuce.core.search.arguments.AggregateArgs.WithCursor;
import io.lettuce.core.search.arguments.QueryDialects;

public class RediSearchTranslator {

	private static final QueryDialects DIALECT = QueryDialects.DIALECT2;

	private final RediSearchQueryBuilder queryBuilder = new RediSearchQueryBuilder();

	private final RediSearchConfig config;

	public RediSearchTranslator(RediSearchConfig config) {
		this.config = requireNonNull(config, "config is null");
	}

	public static class Aggregation {
		private final String index;
		private final String query;
		private final AggregateArgs args;
		private final boolean global;

		public Aggregation(String index, String query, AggregateArgs args, boolean global) {
			this.index = index;
			this.query = query;
			this.args = args;
			this.global = global;
		}

		public String getIndex() {
			return index;
		}

		public String getQuery() {
			return query;
		}

		public AggregateArgs getArgs() {
			return args;
		}

		/**
		 * Whether this aggregates all matching documents into a single row: reducers without GROUP BY terms, e.g.
		 * count(*).
		 */
		public boolean isGlobal() {
			return global;
		}

		@Override
		public String toString() {
			return "Aggregation [index=" + index + ", query=" + query + ", global=" + global + "]";
		}
	}

	public Aggregation aggregate(RediSearchTableHandle table, String[] columnNames) {
		String query = queryBuilder.buildQuery(table.getConstraint(), table.getWildcards());
		AggregateArgs.Builder args = AggregateArgs.builder().dialect(DIALECT);
		args.load(RediSearchBuiltinField.KEY.getName());
		for (String columnName : columnNames) {
			args.load(columnName);
		}
		Optional<GroupBy> groupBy = queryBuilder.group(table);
		groupBy.ifPresent(args::groupBy);
		// Only a pushed-down SQL LIMIT caps the results; otherwise the cursor streams every matching document
		table.getLimit().ifPresent(limit -> args.limit(0, limit));
		args.withCursor(WithCursor.of(config.getCursorCount() > 0 ? config.getCursorCount() : null));
		List<RediSearchAggregationTerm> terms = table.getTermAggregations();
		boolean global = groupBy.isPresent() && (terms == null || terms.isEmpty());
		return new Aggregation(table.getIndex(), query, args.build(), global);
	}

}
