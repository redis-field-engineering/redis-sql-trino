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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.search.arguments.AggregateArgs;
import io.lettuce.core.search.arguments.AggregateArgs.SortBy;
import io.lettuce.core.search.arguments.AggregateArgs.SortDirection;
import io.lettuce.core.search.arguments.AggregateArgs.SortProperty;
import io.lettuce.core.search.arguments.AggregateArgs.WithCursor;
import io.lettuce.core.search.arguments.QueryDialects;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;

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
		 * The ordered command tokens for diagnostics and direct reproduction. No connection URI or authentication
		 * is included; query literals are included, so only capture these at explicitly enabled DEBUG level.
		 */
		public List<String> getCommandArguments() {
			ArgumentCollector command = new ArgumentCollector();
			command.add("FT.AGGREGATE").add(index).add(query);
			args.build(command);
			return List.copyOf(command.arguments);
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
	public Aggregation aggregate(RediSearchTableHandle table, List<RediSearchColumnHandle> outputs,
			Optional<RediSearchIndexInfo> indexInfo) {
		return aggregate(table, outputs, indexInfo, Optional.empty());
	}

	public Aggregation aggregate(RediSearchTableHandle table, List<RediSearchColumnHandle> outputs,
			Optional<RediSearchIndexInfo> indexInfo, Optional<RediSearchScanPartition> partition) {
		return aggregate(table, outputs, indexInfo, partition, Optional.empty(), 0);
	}

	/** A raw document page, ordered by unique key. No SQL operation may discard or reorder its rows. */
	public Aggregation aggregatePage(RediSearchTableHandle table, List<RediSearchColumnHandle> outputs,
			Optional<RediSearchIndexInfo> indexInfo, Optional<String> afterKey, int count) {
		com.google.common.base.Preconditions.checkArgument(count > 0, "Page size must be positive");
		com.google.common.base.Preconditions.checkArgument(table.getTermAggregations().isEmpty()
				&& table.getMetricAggregations().isEmpty() && table.getSort().isEmpty()
				&& table.getLimit().isEmpty() && table.getConstraint().isAll() && table.getFilters().isEmpty(),
				"Cursor-free scans require an unmodified document table");
		afterKey.ifPresent(RediSearchCursorFreePageSource::verifyKey);
		return aggregate(table, outputs, indexInfo, Optional.empty(), afterKey, count);
	}

	private Aggregation aggregate(RediSearchTableHandle table, List<RediSearchColumnHandle> outputs,
			Optional<RediSearchIndexInfo> indexInfo, Optional<RediSearchScanPartition> partition,
			Optional<String> afterKey, int pageSize) {
		partition.ifPresent(value -> {
			com.google.common.base.Preconditions.checkArgument(RediSearchScanPartition.isDocumentScan(table),
					"Cannot partition a global operation");
			com.google.common.base.Preconditions.checkState(indexInfo.filter(info ->
					RediSearchScanPartition.supported(info, value.field())).isPresent(),
					"Partition field is no longer indexed NUMERIC: %s", value.field());
		});
		// Arithmetic the scan returns is computed by the reader, from the columns it refers to
		List<RediSearchColumnHandle> columns = readColumns(outputs);
		boolean computes = columns.size() != outputs.size() || !columns.equals(outputs);
		String query = queryBuilder.buildQuery(table.getConstraint());
		if (table.getNativeQuery().isPresent()) {
			query = query.equals("*") ? table.getNativeQuery().orElseThrow()
					: "(" + table.getNativeQuery().orElseThrow() + ") (" + query + ")";
		}
		if (partition.isPresent()) {
			query = query.equals("*") ? partition.get().query()
					: "(" + query + ") (" + partition.get().query() + ")";
		}
		Map<String, List<String>> equalities = RediSearchQueryBuilder.equalities(table.getConstraint());
		List<RediSearchAggregationStage> stages = table.getAggregationStages();
		Set<String> usedNames = new LinkedHashSet<>();
		columns.forEach(column -> usedNames.add(column.getName()));
		indexInfo.ifPresent(info -> info.getFields().forEach(field -> {
			usedNames.add(field.getAttribute());
			usedNames.add(field.getIdentifier());
		}));
		for (RediSearchAggregationStage stage : stages) {
			stage.terms().forEach(term -> usedNames.add(term.getTerm()));
			stage.metrics().forEach(metric -> {
				usedNames.add(metric.getAlias());
				usedNames.add(metric.getValueField());
				usedNames.add(metric.getHasValueField());
				usedNames.add(metric.getCountAlias());
				usedNames.add(metric.integerSumCountAlias());
				usedNames.addAll(metric.getInputFields());
			});
		}
		List<RediSearchGroupingNormalizer.Plan> normalizations = new ArrayList<>();
		for (int i = 0; i < stages.size(); i++) {
			// Appended COUNT stages only regroup a subset of already canonical keys. A grouped
			// NULL slot exists, so reapplying source-field normalization could turn it into false/zero.
			RediSearchGroupingNormalizer.Plan plan = RediSearchGroupingNormalizer.plan(i == 0 ? stages.get(i).terms() : List.of(), usedNames);
			normalizations.add(plan);
			usedNames.addAll(plan.beforeExpressions().keySet());
		}
		List<Map<String, String>> preservedPresence = new ArrayList<>();
		for (int i = 0; i < stages.size(); i++) {
			Map<String, String> presence = new LinkedHashMap<>();
			if (stages.get(i).groupingOnly() && i + 1 < stages.size()) {
				for (RediSearchAggregation metric : stages.get(i + 1).metrics()) {
					if (metric.isCountColumn()) {
						String field = metric.getColumnHandle().orElseThrow().getName();
						if (!presence.containsKey(field)) {
							String alias = "__trino_present" + presence.size();
							while (!usedNames.add(alias)) { alias += "_"; }
							presence.put(field, alias);
						}
					}
				}
			}
			preservedPresence.add(presence);
		}
		boolean grouped = stages.stream().anyMatch(stage -> !stage.metrics().isEmpty());
		// A scan reads the documents' values. Redis returns a NUMERIC field loaded by name formatted as a double, and
		// rounded to 12 significant digits unless it's an integer; LOAD * on a hash and DIALECT 3 on JSON return the
		// values as stored. GROUPBY keys and REDUCE results are formatted the same way, so APPLY also returns them as
		// mantissas and exponents.
		Optional<RediSearchIndexInfo.KeyType> keyType = indexInfo.flatMap(RediSearchIndexInfo::getKeyType);
		boolean scan = !grouped;
		boolean json = scan && keyType.filter(RediSearchIndexInfo.KeyType.JSON::equals).isPresent();
		boolean hashScan = scan && keyType.filter(RediSearchIndexInfo.KeyType.HASH::equals).isPresent();
		// DIALECT 3 loads JSON values as arrays, which FILTER can't compare, so the reader keeps a scan's equal rows
		Map<String, String> filters = new LinkedHashMap<>(json ? Map.of() : queryBuilder.filters(equalities));
		table.getFilters().forEach((field, filter) -> filters.merge(field, filter, (left, right) -> "(" + left + ") && (" + right + ")"));
		// LOAD * returns every field of each hash, so only scans that can't tell a rounded value from an exact one use it
		boolean loadAll = hashScan && columns.stream().anyMatch(RediSearchTranslator::isRoundedUndetectably);
		AggregateArgs.Builder args = AggregateArgs.builder().dialect(json ? JSON_DIALECT : DIALECT);
		if (config.getQueryTimeoutMillis() > 0) {
			args.timeout(java.time.Duration.ofMillis(config.getQueryTimeoutMillis()));
		}
		// Lettuce writes LOAD before the other steps, so FILTER compares the loaded values
		Set<String> loads = new LinkedHashSet<>();
		loads.add(RediSearchBuiltinField.KEY.getName());
		loads.addAll(equalities.keySet());
		loads.addAll(filters.keySet());
		// Sums and averages counting values, whose APPLY steps refer to their columns
		List<RediSearchAggregation> inputMetrics = stages.isEmpty() ? List.of() : stages.getFirst().metrics();
		List<RediSearchAggregation> countingValues = inputMetrics.stream()
				.filter(RediSearchAggregation::isCountingValues).toList();
		countingValues.forEach(metric -> loads.addAll(metric.getInputFields()));
		List<RediSearchAggregation> nonNullCounts = inputMetrics.stream()
				.filter(RediSearchAggregation::isConditionalCount).toList();
		nonNullCounts.forEach(metric -> loads.addAll(metric.getInputFields()));
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
		// A grouping key is still needed when an outer COUNT prunes it from the scan's projected output.
		if (!stages.isEmpty()) { stages.getFirst().terms().forEach(term -> loads.add(term.getTerm())); }
		// Always mark LOAD operands as field references. A bare field named "params" can be parsed as the
		// command's PARAMS clause by a distributed coordinator; @params is unambiguous.
		loads.forEach(field -> args.load("@" + field));
		// SORTBY sorts a copy of each column, loaded AS another name: next to LOAD *, a NUMERIC field's own name holds
		// the hash's text, which Redis would sort as a string
		List<RediSearchSortItem> sort = table.getSort();
		for (int i = 0; i < sort.size(); i++) {
			args.load("@" + sort.get(i).getColumn(), sortAlias(i));
		}
		// Steps run in the order they're added: GROUPBY leaves only the groups, SORTBY keeps the first LIMIT of the
		// filtered rows, and LIMIT counts the filtered rows
		// HASH filters only need their own fields. Execute these before loading a wide projection.
		boolean earlyFilters = hashScan && !filters.isEmpty();
		if (!earlyFilters) { filters.values().forEach(args::filter); }
		for (int i = 0; i < stages.size(); i++) {
			RediSearchAggregationStage stage = stages.get(i);
			Map<String, String> inputPresence = i == 0 ? Map.of() : preservedPresence.get(i - 1);
			for (RediSearchAggregation metric : stage.metrics()) {
				if (metric.isCountingValues()) {
					args.apply(metric.valueExpression(), metric.getValueField());
					args.apply(metric.hasValueExpression(), metric.getHasValueField());
				} else if (metric.isConditionalCount()) {
					String presence = metric.getColumnHandle().map(RediSearchColumnHandle::getName)
							.map(inputPresence::get).map(alias -> "@" + alias).orElseGet(metric::hasValueExpression);
					args.apply(presence, metric.getHasValueField());
				}
			}
			// A NULL group slot exists in Redis's intermediate row. Preserve source HASH presence
			// before grouping instead of mistaking that slot for a non-null SQL value in a later COUNT.
			preservedPresence.get(i).forEach((field, alias) -> args.apply(inputPresence.containsKey(field)
					? "@" + inputPresence.get(field) : "exists(@" + field + ")", alias));
			RediSearchGroupingNormalizer.Plan plan = normalizations.get(i);
			plan.beforeExpressions().forEach((alias, expression) -> args.apply(expression, alias));
			queryBuilder.group(stage, plan.aliases(), List.copyOf(preservedPresence.get(i).values())).ifPresent(args::groupBy);
			plan.afterExpressions().forEach((field, expression) -> args.apply(expression, field));
		}
		// The keys and results that needn't be integers, as mantissas and exponents
		Map<String, Type> exactNumbers = new HashMap<>();
		if (grouped) {
			Map<String, RediSearchAggregation> metrics = table.getMetricAggregations().stream()
					.collect(toMap(RediSearchAggregation::getAlias, identity()));
			for (int i = 0; i < columns.size(); i++) {
				RediSearchColumnHandle column = columns.get(i);
				Type type = column.getType();
				RediSearchAggregation metric = metrics.get(column.getName());
				if (metric != null && metric.isCountingValues()
						&& RediSearchAggregation.AVG.equals(metric.getFunctionName())) {
					// The reader divides the sum, a double of any column's type, by the count
					type = DOUBLE;
				}
				if (RediSearchExactNumbers.isFloatingPoint(type)) {
					RediSearchExactNumbers.apply(args, column.getName(), i);
					exactNumbers.put(column.getName(), type);
				}
			}
		}
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
		long cursorCount = scan && table.getLimit().isEmpty() && columns.size() <= 2
				&& config.getNarrowScanCursorCount() > 0 ? config.getNarrowScanCursorCount() : config.getCursorCount();
		if (pageSize > 0) {
			afterKey.ifPresent(key -> args.filter("@__key > " + RediSearchQueryBuilder.stringLiteral(key)));
			args.sortBy(SortBy.of(new SortProperty("@__key", SortDirection.ASC)).max(pageSize));
			args.limit(0, pageSize);
		} else {
			args.withCursor(WithCursor.of(cursorCount > 0 ? cursorCount : null));
		}
		List<RediSearchAggregationTerm> terms = table.getTermAggregations();
		boolean global = grouped && terms.isEmpty();
		Set<String> jsonArrays = new LinkedHashSet<>();
		if (json) {
			loads.stream().filter(load -> !RediSearchBuiltinField.isKeyColumn(load)).forEach(jsonArrays::add);
		}
		AggregateArgs aggregateArgs = loadAll ? new LoadAllArgs(args.build()) : args.build();
		if (earlyFilters) {
			AggregateArgs.Builder prefix = AggregateArgs.builder();
			filters.keySet().forEach(field -> prefix.load("@" + field));
			filters.values().forEach(prefix::filter);
			aggregateArgs = new PrefixArgs(prefix.build(), aggregateArgs);
		}
		return new Aggregation(table.getIndex(), query, filters.values(), aggregateArgs, global,
				new RediSearchRowReader(columns.stream().map(RediSearchColumnHandle::getName).toList(), sources,
						exactSources, jsonArrays, table.getMetricAggregations(), exactNumbers,
						json ? equalities : Map.of(), computes ? Optional.of(outputs) : Optional.empty()).withCursorCount(cursorCount));
	}

	/**
	 * The columns a scan reads: those it returns, except arithmetic, and the columns the arithmetic refers to.
	 */
	static List<RediSearchColumnHandle> readColumns(List<RediSearchColumnHandle> outputs) {
		Map<String, RediSearchColumnHandle> columns = new LinkedHashMap<>();
		outputs.stream().filter(column -> column.getExpression().isEmpty())
				.forEach(column -> columns.putIfAbsent(column.getName(), column));
		outputs.stream().flatMap(column -> column.getExpression().stream())
				.forEach(expression -> expression.getColumns().forEach((name, type) -> columns.putIfAbsent(name,
						new RediSearchColumnHandle(name, type, RediSearchFieldType.NUMERIC, false, true, Optional.empty()))));
		return List.copyOf(columns.values());
	}

	private static String sortAlias(int position) {
		return "__sort_" + position;
	}

	// Values of these types can lose digits formatted as doubles. Integers of the other types are exact as doubles,
	// and REAL values have fewer than 12 significant digits.
	private static boolean isRoundedByRedis(RediSearchColumnHandle column) {
		return isRoundedUndetectably(column) || (isNumericField(column) && (column.getType() == BIGINT || column.getType() == io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS));
	}

	// A rounded DOUBLE or DECIMAL can't be told from an exact value, unlike a BIGINT
	private static boolean isRoundedUndetectably(RediSearchColumnHandle column) {
		return isNumericField(column) && (column.getType() == DOUBLE || column.getType() instanceof DecimalType);
	}

	private static boolean isNumericField(RediSearchColumnHandle column) {
		return column.getFieldType() == RediSearchFieldType.NUMERIC && column.isSupportsPredicates();
	}

	// AggregateArgs writes textual values, integer counts and keywords through these overloads. Keep token
	// boundaries rather than splitting toCommandString(), whose FILTER/APPLY expressions contain spaces.
	private static final class ArgumentCollector extends CommandArgs<String, String> {
		private final List<String> arguments = new ArrayList<>();

		ArgumentCollector() {
			super(StringCodec.UTF8);
		}

		@Override
		public CommandArgs<String, String> add(String value) {
			arguments.add(value);
			return super.add(value);
		}

		@Override
		public CommandArgs<String, String> add(long value) {
			arguments.add(Long.toString(value));
			return super.add(value);
		}

		@Override
		public CommandArgs<String, String> add(Object value) {
			arguments.add(value.toString());
			return super.add(value);
		}

		@Override
		public CommandArgs<String, String> add(double value) {
			arguments.add(Double.toString(value));
			return super.add(value);
		}

		@Override
		public CommandArgs<String, String> add(CommandKeyword value) {
			arguments.add(value.name());
			return super.add(value);
		}
	}

	private static class PrefixArgs extends AggregateArgs {
		private final AggregateArgs prefix;
		private final AggregateArgs args;
		PrefixArgs(AggregateArgs prefix, AggregateArgs args) { this.prefix = prefix; this.args = args; }
		@Override
		public void build(CommandArgs<?, ?> commandArgs) { prefix.build(commandArgs); args.build(commandArgs); }
		@Override
		public Optional<WithCursor> getWithCursor() { return args.getWithCursor(); }
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
