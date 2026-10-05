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

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.search.arguments.AggregateArgs;
import io.lettuce.core.search.arguments.AggregateArgs.GroupBy;
import io.lettuce.core.search.arguments.AggregateArgs.SortBy;
import io.lettuce.core.search.arguments.AggregateArgs.SortDirection;
import io.lettuce.core.search.arguments.AggregateArgs.SortProperty;
import io.lettuce.core.search.arguments.AggregateArgs.WithCursor;
import io.lettuce.core.search.arguments.QueryDialects;
import io.trino.spi.type.DecimalType;

public class RediSearchTranslator {

	private static final QueryDialects DIALECT = QueryDialects.DIALECT2;

	// Has the query syntax of DIALECT 2, and returns the values at JSON paths as JSON arrays, with numbers as stored
	private static final QueryDialects JSON_DIALECT = QueryDialects.DIALECT3;

	private final RediSearchQueryBuilder queryBuilder = new RediSearchQueryBuilder();

	private final RediSearchConfig config;

	public RediSearchTranslator(RediSearchConfig config) {
		this.config = requireNonNull(config, "config is null");
	}

	public static class Aggregation {
		private final String index;
		private final String query;
		private final Collection<String> filters;
		private final AggregateArgs args;
		private final boolean global;
		private final RediSearchRowReader reader;

		public Aggregation(String index, String query, Collection<String> filters, AggregateArgs args,
				boolean global, RediSearchRowReader reader) {
			this.index = index;
			this.query = query;
			this.filters = filters;
			this.args = args;
			this.global = global;
			this.reader = reader;
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

		/**
		 * @return how to read the columns from the rows of this aggregation, and of its cursor's later batches
		 */
		public RediSearchRowReader getReader() {
			return reader;
		}

		@Override
		public String toString() {
			return "Aggregation [index=" + index + ", query=" + query + ", filters=" + filters + ", global=" + global
					+ "]";
		}
	}

	/**
	 * @param columns   the columns to read
	 * @param indexInfo the index's FT.INFO, which tells how to read the values exactly; without it, they're loaded
	 *                  by name
	 */
	public Aggregation aggregate(RediSearchTableHandle table, List<RediSearchColumnHandle> columns,
			Optional<RediSearchIndexInfo> indexInfo) {
		String query = queryBuilder.buildQuery(table.getConstraint());
		Map<String, String> filters = queryBuilder.filters(table.getConstraint());
		Optional<GroupBy> groupBy = queryBuilder.group(table);
		// A scan reads the documents' values. Redis returns a NUMERIC field loaded by name formatted as a double, and
		// rounded to 12 significant digits unless it's an integer; LOAD * on a hash and DIALECT 3 on JSON return the
		// values as stored. GROUPBY and REDUCE results have no such format.
		Optional<RediSearchIndexInfo.KeyType> keyType = indexInfo.flatMap(RediSearchIndexInfo::getKeyType);
		boolean scan = groupBy.isEmpty();
		boolean json = scan && keyType.filter(RediSearchIndexInfo.KeyType.JSON::equals).isPresent();
		boolean hashScan = scan && keyType.filter(RediSearchIndexInfo.KeyType.HASH::equals).isPresent();
		// LOAD * returns every field of each hash, so only scans that can't tell a rounded value from an exact one use it
		boolean loadAll = hashScan && columns.stream().anyMatch(RediSearchTranslator::isRoundedUndetectably);
		AggregateArgs.Builder args = AggregateArgs.builder().dialect(json ? JSON_DIALECT : DIALECT);
		// Lettuce writes LOAD before the other steps, so FILTER compares the loaded values
		Set<String> loads = new LinkedHashSet<>();
		loads.add(RediSearchBuiltinField.KEY.getName());
		loads.addAll(filters.keySet());
		Map<String, String> sources = new HashMap<>();
		Map<String, String> exactSources = new HashMap<>();
		Map<String, RediSearchIndexInfo.Field> fields = indexInfo.map(RediSearchIndexInfo::getFields).orElse(List.of())
				.stream().collect(toMap(RediSearchIndexInfo.Field::getAttribute, identity(), (first, second) -> first));
		for (RediSearchColumnHandle column : columns) {
			Optional<String> field = Optional.ofNullable(fields.get(column.getName()))
					.map(RediSearchIndexInfo.Field::getIdentifier);
			if (loadAll && isRoundedByRedis(column)) {
				// Loaded by name, it would replace the value LOAD * returns under the hash field's name
				field.filter(identifier -> !identifier.equals(column.getName()))
						.ifPresent(identifier -> sources.put(column.getName(), identifier));
			} else {
				loads.add(column.getName());
				if (hashScan && isRoundedByRedis(column)) {
					// A BIGINT: Redis returns an integer in full, so a value it could have rounded is 2^53 or more,
					// and is read again from the hash
					exactSources.put(column.getName(), field.orElse(column.getName()));
				}
			}
		}
		loads.forEach(args::load);
		// SORTBY sorts a copy of each column, loaded AS another name: next to LOAD *, a NUMERIC field's own name holds
		// the hash's text, which Redis would sort as a string
		List<RediSearchSortItem> sort = table.getSort();
		for (int i = 0; i < sort.size(); i++) {
			args.load(sort.get(i).getColumn(), sortAlias(i));
		}
		// Steps run in the order they're added: GROUPBY leaves only the groups, SORTBY keeps the first LIMIT of the
		// filtered rows, and LIMIT counts the filtered rows
		filters.values().forEach(args::filter);
		groupBy.ifPresent(args::groupBy);
		if (!sort.isEmpty()) {
			SortProperty[] properties = new SortProperty[sort.size()];
			for (int i = 0; i < sort.size(); i++) {
				properties[i] = new SortProperty("@" + sortAlias(i),
						sort.get(i).isAscending() ? SortDirection.ASC : SortDirection.DESC);
			}
			SortBy sortBy = SortBy.of(properties);
			table.getLimit().ifPresent(sortBy::max);
			args.sortBy(sortBy);
		}
		// Only a pushed-down SQL LIMIT caps the results; otherwise the cursor streams every matching document
		table.getLimit().ifPresent(limit -> args.limit(0, limit));
		args.withCursor(WithCursor.of(config.getCursorCount() > 0 ? config.getCursorCount() : null));
		List<RediSearchAggregationTerm> terms = table.getTermAggregations();
		boolean global = groupBy.isPresent() && (terms == null || terms.isEmpty());
		Set<String> jsonArrays = new LinkedHashSet<>();
		if (json) {
			loads.stream().filter(load -> !RediSearchBuiltinField.isKeyColumn(load)).forEach(jsonArrays::add);
		}
		AggregateArgs aggregateArgs = loadAll ? new LoadAllArgs(args.build()) : args.build();
		return new Aggregation(table.getIndex(), query, filters.values(), aggregateArgs, global,
				new RediSearchRowReader(columns.stream().map(RediSearchColumnHandle::getName).toList(), sources,
						exactSources, jsonArrays, table.getMetricAggregations()));
	}

	private static String sortAlias(int position) {
		return "__sort_" + position;
	}

	// Values of these types can lose digits formatted as doubles. Integers of the other types are exact as doubles,
	// and REAL values have fewer than 12 significant digits.
	private static boolean isRoundedByRedis(RediSearchColumnHandle column) {
		return isRoundedUndetectably(column) || (isNumericField(column) && column.getType() == BIGINT);
	}

	// A rounded DOUBLE or DECIMAL can't be told from an exact value, unlike a BIGINT
	private static boolean isRoundedUndetectably(RediSearchColumnHandle column) {
		return isNumericField(column) && (column.getType() == DOUBLE || column.getType() instanceof DecimalType);
	}

	private static boolean isNumericField(RediSearchColumnHandle column) {
		return column.getFieldType() == RediSearchFieldType.NUMERIC && column.isSupportsPredicates();
	}

	/**
	 * Writes {@code LOAD *} before the arguments Lettuce writes, including the fields they load by name: Lettuce's
	 * {@code loadAll()} can't be combined with those.
	 */
	private static class LoadAllArgs extends AggregateArgs {
		private final AggregateArgs args;

		LoadAllArgs(AggregateArgs args) {
			this.args = args;
		}

		@Override
		public void build(CommandArgs<?, ?> commandArgs) {
			commandArgs.add("LOAD").add("*");
			args.build(commandArgs);
		}

		@Override
		public Optional<WithCursor> getWithCursor() {
			return args.getWithCursor();
		}
	}

}
