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

import java.util.Optional;

import io.lettuce.core.search.arguments.AggregateArgs;
import io.lettuce.core.search.arguments.AggregateArgs.GroupBy;
import io.lettuce.core.search.arguments.AggregateArgs.WithCursor;
import io.lettuce.core.search.arguments.QueryDialects;
import io.lettuce.core.search.arguments.SearchArgs;

public class RediSearchTranslator {

	private static final QueryDialects DIALECT = QueryDialects.DIALECT2;

	private final RediSearchQueryBuilder queryBuilder = new RediSearchQueryBuilder();

	private final RediSearchConfig config;

	public RediSearchTranslator(RediSearchConfig config) {
		this.config = requireNonNull(config, "config is null");
	}

	public RediSearchConfig getConfig() {
		return config;
	}

	public static class Aggregation {
		private final String index;
		private final String query;
		private final AggregateArgs args;
		private final boolean grouped;

		public Aggregation(String index, String query, AggregateArgs args, boolean grouped) {
			this.index = index;
			this.query = query;
			this.args = args;
			this.grouped = grouped;
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

		public boolean isGrouped() {
			return grouped;
		}

		@Override
		public String toString() {
			return "Aggregation [index=" + index + ", query=" + query + ", grouped=" + grouped + "]";
		}
	}

	public static class Search {
		private final String index;
		private final String query;
		private final SearchArgs<String> args;

		public Search(String index, String query, SearchArgs<String> args) {
			this.index = index;
			this.query = query;
			this.args = args;
		}

		public String getIndex() {
			return index;
		}

		public String getQuery() {
			return query;
		}

		public SearchArgs<String> getArgs() {
			return args;
		}

		@Override
		public String toString() {
			return "Search [index=" + index + ", query=" + query + "]";
		}
	}

	public Search search(RediSearchTableHandle table, String[] columnNames) {
		String query = queryBuilder.buildQuery(table.getConstraint(), table.getWildcards());
		SearchArgs.Builder<String> args = SearchArgs.<String>builder().withScores().limit(0, limit(table))
				.dialect(DIALECT);
		for (String columnName : columnNames) {
			args.returnField(columnName);
		}
		return new Search(table.getIndex(), query, args.build());
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
		args.limit(0, limit(table));
		args.withCursor(WithCursor.of(config.getCursorCount() > 0 ? config.getCursorCount() : null));
		return new Aggregation(table.getIndex(), query, args.build(), groupBy.isPresent());
	}

	private long limit(RediSearchTableHandle tableHandle) {
		if (tableHandle.getLimit().isPresent()) {
			return tableHandle.getLimit().getAsLong();
		}
		return config.getDefaultLimit();
	}

}
