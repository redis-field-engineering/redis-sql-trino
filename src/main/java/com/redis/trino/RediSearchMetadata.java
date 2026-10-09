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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.airlift.log.Logger;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.airlift.slice.Slice;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.NotFoundException;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;

public class RediSearchMetadata implements ConnectorMetadata {

	private static final Logger log = Logger.get(RediSearchMetadata.class);

	private static final String SYNTHETIC_COLUMN_NAME_PREFIX = "syntheticColumn";

	private final RediSearchSession rediSearchSession;
	private final String schemaName;
	private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

	public RediSearchMetadata(RediSearchSession rediSearchSession) {
		this.rediSearchSession = requireNonNull(rediSearchSession, "rediSearchSession is null");
		this.schemaName = rediSearchSession.getConfig().getDefaultSchema();
	}

	@Override
	public List<String> listSchemaNames(ConnectorSession session) {
		return List.of(schemaName);
	}

	@Override
	public RediSearchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
			Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
		requireNonNull(tableName, "tableName is null");
		if (startVersion.isPresent() || endVersion.isPresent()) {
			throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "This connector does not support versioned tables");
		}

		if (tableName.getSchemaName().equals(schemaName)) {
			try {
				return rediSearchSession.getTable(tableName).getTableHandle();
			} catch (TableNotFoundException e) {
				// ignore and return null
			}
		}
		return null;
	}

	@Override
	public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
		requireNonNull(tableHandle, "tableHandle is null");
		SchemaTableName tableName = getTableName(tableHandle);
		return getTableMetadata(session, tableName);
	}

	@Override
	public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName) {
		ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
		for (String tableName : rediSearchSession.getAllTables()) {
			tableNames.add(new SchemaTableName(schemaName, tableName));
		}
		return tableNames.build();
	}

	@Override
	public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
		RediSearchTableHandle table = (RediSearchTableHandle) tableHandle;
		List<RediSearchColumnHandle> columns = rediSearchSession.getTable(table.getSchemaTableName()).getColumns();

		ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
		for (RediSearchColumnHandle columnHandle : columns) {
			columnHandles.put(columnHandle.getName(), columnHandle);
		}
		return columnHandles.buildOrThrow();
	}

	@Override
	public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session,
			Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter) {
		ImmutableList.Builder<RelationColumnsMetadata> relationColumns = ImmutableList.builder();
		// Filter before fetching metadata so only visible indexes are inspected
		for (SchemaTableName tableName : relationFilter.apply(ImmutableSet.copyOf(listTables(session, schemaName)))) {
			try {
				relationColumns.add(RelationColumnsMetadata.forTable(tableName,
						getTableMetadata(session, tableName).getColumns()));
			} catch (NotFoundException e) {
				// table disappeared during listing operation
			}
		}
		return relationColumns.build().iterator();
	}

	@Override
	public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
			ColumnHandle columnHandle) {
		return ((RediSearchColumnHandle) columnHandle).toColumnMetadata();
	}

	@Override
	public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode) {
		if (saveMode == SaveMode.REPLACE) {
			throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "This connector does not support replacing tables");
		}
		rediSearchSession.createTable(tableMetadata.getTable(), buildColumnHandles(tableMetadata));
	}

	@Override
	public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
		RediSearchTableHandle table = (RediSearchTableHandle) tableHandle;
		rediSearchSession.dropTable(table.getSchemaTableName());
	}

	@Override
	public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column,
			ColumnPosition position) {
		if (!(position instanceof ColumnPosition.Last)) {
			throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "This connector only supports adding columns at the end");
		}
		rediSearchSession.addColumn(((RediSearchTableHandle) tableHandle).getSchemaTableName(), column);
	}

	@Override
	public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
			Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace) {
		checkRetry(retryMode);
		if (replace) {
			throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "This connector does not support replacing tables");
		}
		List<RediSearchColumnHandle> columns = buildColumnHandles(tableMetadata);

		rediSearchSession.createTable(tableMetadata.getTable(), columns);

		setRollback(() -> rediSearchSession.dropTable(tableMetadata.getTable()));

		return new RediSearchOutputTableHandle(tableMetadata.getTable(),
				columns.stream().filter(c -> !c.isHidden()).collect(Collectors.toList()));
	}

	private void checkRetry(RetryMode retryMode) {
		if (retryMode != RetryMode.NO_RETRIES) {
			throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "This connector does not support retries");
		}
	}

	@Override
	public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session,
			ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments,
			Collection<ComputedStatistics> computedStatistics) {
		clearRollback();
		return Optional.empty();
	}

	@Override
	public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle,
			List<ColumnHandle> insertedColumns, RetryMode retryMode) {
		checkRetry(retryMode);
		RediSearchTableHandle table = (RediSearchTableHandle) tableHandle;
		rediSearchSession.verifyWritable(table.getSchemaTableName());
		List<RediSearchColumnHandle> columns = rediSearchSession.getTable(table.getSchemaTableName()).getColumns();

		return new RediSearchInsertTableHandle(table.getSchemaTableName(),
				columns.stream().filter(column -> !column.isHidden()).collect(Collectors.toList()));
	}

	@Override
	public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
			ConnectorInsertTableHandle insertHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments,
			Collection<ComputedStatistics> computedStatistics) {
		return Optional.empty();
	}

	@Override
	public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle) {
		return RowChangeParadigm.CHANGE_ONLY_UPDATED_COLUMNS;
	}

	@Override
	public RediSearchColumnHandle getMergeRowIdColumnHandle(ConnectorSession session,
			ConnectorTableHandle tableHandle) {
		return RediSearchBuiltinField.KEY.getColumnHandle();
	}

	@Override
	public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle,
			Map<Integer, Collection<ColumnHandle>> updateCaseColumns, RetryMode retryMode) {
		checkRetry(retryMode);
		RediSearchTableHandle table = (RediSearchTableHandle) tableHandle;
		// DELETE has no update cases and works on any index. Inserts from MERGE are checked by the merge sink, since
		// the insert cases aren't listed here.
		if (!updateCaseColumns.isEmpty()) {
			rediSearchSession.verifyWritable(table.getSchemaTableName());
		}
		List<RediSearchColumnHandle> dataColumns = rediSearchSession.getTable(table.getSchemaTableName()).getColumns()
				.stream().filter(column -> !column.isHidden()).collect(toImmutableList());
		ImmutableMap.Builder<Integer, List<Integer>> updateCaseChannels = ImmutableMap.builder();
		updateCaseColumns.forEach((caseNumber, columns) -> updateCaseChannels.put(caseNumber,
				columns.stream().map(dataColumns::indexOf).collect(toImmutableList())));
		return new RediSearchMergeTableHandle(table, dataColumns, updateCaseChannels.buildOrThrow());
	}

	@Override
	public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle,
			List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments,
			Collection<ComputedStatistics> computedStatistics) {
		// Do nothing
	}

	/**
	 * The number of documents in the index, from the cached FT.INFO, so that the cost-based optimizer can order joins
	 * and choose how to distribute them. Filters pushed down to Redis aren't estimated.
	 */
	@Override
	public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle) {
		RediSearchTableHandle handle = (RediSearchTableHandle) tableHandle;
		// FT.INFO doesn't tell how many groups an aggregation returns
		if (!handle.getTermAggregations().isEmpty() || !handle.getMetricAggregations().isEmpty()) {
			return TableStatistics.empty();
		}
		OptionalLong documents = rediSearchSession.getTable(handle.getSchemaTableName()).getIndexInfo().getNumDocs();
		if (documents.isEmpty()) {
			return TableStatistics.empty();
		}
		long rows = documents.getAsLong();
		if (handle.getLimit().isPresent()) {
			rows = Math.min(rows, handle.getLimit().getAsLong());
		}
		return TableStatistics.builder().setRowCount(Estimate.of(rows)).build();
	}

	@Override
	public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table) {
		RediSearchTableHandle handle = (RediSearchTableHandle) table;
		// A TAG or TEXT prefilter without a FILTER doesn't guarantee its domain: Redis also returns rows that Trino
		// then filters out
		TupleDomain<ColumnHandle> predicate = handle.getConstraint()
				.filter((column, domain) -> RediSearchQueryBuilder.isExact((RediSearchColumnHandle) column, domain));
		return new ConnectorTableProperties(predicate, Optional.empty(), Optional.empty(), List.of());
	}

	@Override
	public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session,
			ConnectorTableHandle table, long limit) {
		RediSearchTableHandle handle = (RediSearchTableHandle) table;

		if (limit == 0) {
			return Optional.empty();
		}

		if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
			return Optional.empty();
		}

		// A scan of JSON documents keeps the rows equal to TAG and TEXT values after Redis has limited them
		if (handle.getTermAggregations().isEmpty() && handle.getMetricAggregations().isEmpty()
				&& !RediSearchQueryBuilder.equalities(handle.getConstraint()).isEmpty()
				&& rediSearchSession.getTable(handle.getSchemaTableName()).getIndexInfo().getKeyType()
						.filter(RediSearchIndexInfo.KeyType.JSON::equals).isPresent()) {
			return Optional.empty();
		}

		return Optional.of(new LimitApplicationResult<>(handle.withLimit(limit), true, false));
	}

	// Types whose values sort as doubles in the same order. A BIGINT of 2^53 or more could sort with its neighbors,
	// and a DECIMAL with more digits than a double holds.
	private static final Set<Type> SORTABLE_TYPES = Set.of(DOUBLE, REAL, INTEGER, SMALLINT, TINYINT);

	/**
	 * Pushes ORDER BY ... LIMIT down to Redis as SORTBY ... MAX for scans of hash indexes, on NUMERIC columns.
	 * Redis sorts documents that have no value last in both directions, so only NULLS LAST is pushed down. Trino still
	 * sorts the rows Redis returns.
	 */
	@Override
	public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(ConnectorSession session,
			ConnectorTableHandle table, long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments) {
		RediSearchTableHandle handle = (RediSearchTableHandle) table;
		// Sorting the first rows, or the groups, would differ from sorting the documents
		if (handle.getLimit().isPresent() || !handle.getSort().isEmpty() || !handle.getTermAggregations().isEmpty()
				|| !handle.getMetricAggregations().isEmpty() || topNCount == 0) {
			return Optional.empty();
		}
		// With DIALECT 3, a JSON index returns its values as JSON text, which Redis would sort as strings
		if (rediSearchSession.getTable(handle.getSchemaTableName()).getIndexInfo().getKeyType()
				.filter(RediSearchIndexInfo.KeyType.HASH::equals).isEmpty()) {
			return Optional.empty();
		}
		boolean local = sortItems.size() == 1 && topNCount <= 1000
				&& assignments.get(sortItems.get(0).getName()) instanceof RediSearchColumnHandle candidate
				&& candidate.getType() == io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
		ImmutableList.Builder<RediSearchSortItem> sort = ImmutableList.builder();
		for (SortItem sortItem : sortItems) {
			RediSearchColumnHandle column = (RediSearchColumnHandle) assignments.get(sortItem.getName());
			if (column == null || column.getFieldType() != RediSearchFieldType.NUMERIC || !column.isSupportsPredicates()
					|| (!local && !SORTABLE_TYPES.contains(column.getType())) || !RediSearchQueryBuilder.isProperty(column.getName())) {
				return Optional.empty();
			}
			switch (sortItem.getSortOrder()) {
			case ASC_NULLS_LAST -> sort.add(new RediSearchSortItem(column.getName(), true, local));
			case DESC_NULLS_LAST -> sort.add(new RediSearchSortItem(column.getName(), false, local));
			default -> {
				return Optional.empty();
			}
			}
		}
		return Optional.of(new TopNApplicationResult<>(handle.withTopN(sort.build(), topNCount), false, false));
	}

	@Override
	public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
			ConnectorTableHandle table, Constraint constraint) {
		RediSearchTableHandle handle = (RediSearchTableHandle) table;
		// The first rows of a sort, filtered, aren't the first of the filtered rows
		if (!handle.getSort().isEmpty() || handle.getLimit().isPresent()) {
			return Optional.empty();
		}

		// Only exact substring LIKE expressions on HASH fields are consumed; other wildcard shapes stay in Trino
		Map<ColumnHandle, Domain> supported = new HashMap<>();
		Map<ColumnHandle, Domain> unsupported = new HashMap<>();
		Map<ColumnHandle, Domain> domains = constraint.getSummary().getDomains()
				.orElseThrow(() -> new IllegalArgumentException("constraint summary is NONE"));
		for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
			RediSearchColumnHandle column = (RediSearchColumnHandle) entry.getKey();
			Domain domain = entry.getValue();

			if (rediSearchSession.canQuery(handle, column, domain)) {
				supported.put(column, domain);
				if (!RediSearchQueryBuilder.isExact(column, domain)) {
					// Redis returns a superset of the matching rows, which Trino filters
					unsupported.put(column, domain);
				}
			} else {
				unsupported.put(column, domain);
			}
		}

		Map<String, String> filters = new LinkedHashMap<>(handle.getFilters());
		List<ConnectorExpression> remaining = new java.util.ArrayList<>();
		for (ConnectorExpression expression : ConnectorExpressions.extractConjuncts(constraint.getExpression())) {
			if (expression instanceof Call call
					&& call.getFunctionName().equals(io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME)
					&& call.getArguments().size() == 2 && call.getArguments().get(0) instanceof Variable variable
					&& call.getArguments().get(1) instanceof Constant constant && constant.getValue() instanceof Slice pattern
					&& constraint.getAssignments().get(variable.getName()) instanceof RediSearchColumnHandle column
					&& rediSearchSession.getTable(handle.getSchemaTableName()).getIndexInfo().getKeyType()
							.filter(RediSearchIndexInfo.KeyType.HASH::equals).isPresent()) {
				Optional<String> filter = RediSearchQueryBuilder.containsFilter(column, pattern.toStringUtf8());
				if (filter.isPresent()) {
					String previous = filters.get(column.getName());
					if (previous == null) { filters.put(column.getName(), filter.get()); }
					else if (!previous.equals(filter.get()) && !previous.contains("(" + filter.get() + ")")) {
						filters.put(column.getName(), "(" + previous + ") && (" + filter.get() + ")");
					}
					continue;
				}
			}
			remaining.add(expression);
		}

		TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
		TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(TupleDomain.withColumnDomains(supported));
		if (oldDomain.equals(newDomain) && handle.getFilters().equals(filters)) {
			return Optional.empty();
		}

		handle = handle.withConstraint(newDomain).withFilters(filters);

		return Optional.of(new ConstraintApplicationResult<>(handle, TupleDomain.withColumnDomains(unsupported),
				ConnectorExpressions.and(remaining), false));
	}

	/**
	 * Pushes arithmetic on DOUBLE NUMERIC columns of hash indexes, such as {@code quantity * extendedprice}, down as
	 * columns of their own ({@link RediSearchExpression}), so that a sum or average of it is pushed down too: an APPLY
	 * step computes it before GROUPBY. A scan computes it in the connector. Value-preserving integer widening also
	 * pushes down, retaining the original integer field. Other projections stay with Trino.
	 */
	@Override
	public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session,
			ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments) {
		RediSearchTableHandle table = (RediSearchTableHandle) handle;
		// After GROUPBY the columns are keys and reducers' results; DIALECT 3 reads JSON numbers, which APPLY would
		// compute on as DIALECT 2 loads them
		if (!table.getTermAggregations().isEmpty() || !table.getMetricAggregations().isEmpty()
				|| rediSearchSession.getTable(table.getSchemaTableName()).getIndexInfo().getKeyType()
						.filter(RediSearchIndexInfo.KeyType.HASH::equals).isEmpty()) {
			return Optional.empty();
		}
		ImmutableList.Builder<ConnectorExpression> newProjections = ImmutableList.builder();
		Map<String, Assignment> newAssignments = new LinkedHashMap<>();
		// The variable of each pushed-down column, which equal expressions share
		Map<RediSearchColumnHandle, String> variables = new HashMap<>();
		for (ConnectorExpression projection : projections) {
			Optional<RediSearchExpression> expression = projection instanceof Call
					? RediSearchExpression.translate(projection, assignments)
					: Optional.empty();
			Optional<RediSearchColumnHandle> projected = RediSearchExpression.integerWidening(projection, assignments)
					.or(() -> expression.map(RediSearchColumnHandle::expression));
			if (projected.isEmpty()) {
				newProjections.add(projection);
				for (Variable variable : ConnectorExpressions.extractVariables(projection)) {
					newAssignments.putIfAbsent(variable.getName(),
							new Assignment(variable.getName(), assignments.get(variable.getName()), variable.getType()));
				}
				continue;
			}
			RediSearchColumnHandle column = projected.get();
			String variable = variables.computeIfAbsent(column, unused -> {
				String name = "expr_" + variables.size();
				while (assignments.containsKey(name) || newAssignments.containsKey(name)) {
					name = name + "_";
				}
				return name;
			});
			newProjections.add(new Variable(variable, column.getType()));
			newAssignments.putIfAbsent(variable, new Assignment(variable, column, column.getType()));
		}
		if (variables.isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(new ProjectionApplicationResult<>(table, newProjections.build(),
				ImmutableList.copyOf(newAssignments.values()), false));
	}

	@Override
	public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(ConnectorSession session,
			ConnectorTableHandle handle, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments,
			List<List<ColumnHandle>> groupingSets) {
		log.debug("applyAggregation aggregates=%s groupingSets=%s", aggregates, groupingSets);
		if (!rediSearchSession.getConfig().isAggregationPushdownEnabled()) {
			return Optional.empty();
		}
		RediSearchTableHandle table = (RediSearchTableHandle) handle;
		// Global aggregation is represented by [[]]
		verify(!groupingSets.isEmpty(), "No grouping sets provided");
		// GROUPBY would run before a LIMIT or SORTBY, over every document rather than the first ones
		if (!table.getTermAggregations().isEmpty() || table.getLimit().isPresent() || !table.getSort().isEmpty()) {
			return Optional.empty();
		}
		if (groupingSets.size() != 1) {
			return Optional.empty();
		}
		if (!groupingSets.get(0).isEmpty()) {
			// No reliable distinct-cardinality statistic is available. Document count is an upper bound;
			// unknown or over-budget tables aggregate in Trino, without runtime replay or partial output.
			OptionalLong documents = rediSearchSession.getTable(table.getSchemaTableName()).getIndexInfo().getNumDocs();
			if (!isGroupPushdownSafe(documents, rediSearchSession.getConfig().getAggregationGroupLimit())) {
				log.debug("Rejecting GROUP BY pushdown: index %s document count %s exceeds group budget %s",
						table.getIndex(), documents, rediSearchSession.getConfig().getAggregationGroupLimit());
				return Optional.empty();
			}
		}
		// Sums and averages count their values where Redis can, which a sharded database needs to compute them
		boolean countValues = aggregates.stream().map(AggregateFunction::getFunctionName)
				.anyMatch(name -> RediSearchAggregation.SUM.equals(name) || RediSearchAggregation.AVG.equals(name))
				&& rediSearchSession.isCaseSupported(table.getIndex());
		ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
		ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();
		ImmutableList.Builder<RediSearchAggregation> aggregations = ImmutableList.builder();
		ImmutableList.Builder<RediSearchAggregationTerm> terms = ImmutableList.builder();
		for (int i = 0; i < aggregates.size(); i++) {
			AggregateFunction function = aggregates.get(i);
			String colName = SYNTHETIC_COLUMN_NAME_PREFIX + i;
			Optional<RediSearchAggregation> aggregation = RediSearchAggregation.handleAggregation(function, assignments,
					colName, countValues);
			if (aggregation.isEmpty()) {
				log.debug("Rejecting aggregation pushdown: unsupported aggregate %s", function);
				return Optional.empty();
			}
			if (aggregation.get().getIntegerSumRowLimit().isPresent()) {
				OptionalLong documents = rediSearchSession.getTable(table.getSchemaTableName()).getIndexInfo().getNumDocs();
				if (!aggregation.get().isIntegerSumSafe(documents)) {
					log.debug("Rejecting integer SUM pushdown: index %s has document count %s, safe row limit %s",
							table.getIndex(), documents, aggregation.get().getIntegerSumRowLimit());
					return Optional.empty();
				}
			}
			io.trino.spi.type.Type outputType = function.getOutputType();
			// Not a field Redis can filter on: the query runs before GROUPBY, so Trino evaluates HAVING
			RediSearchColumnHandle newColumn = new RediSearchColumnHandle(colName, outputType,
					RediSearchSession.toFieldType(outputType), false, false, Optional.empty());
			projections.add(new Variable(colName, function.getOutputType()));
			resultAssignments.add(new Assignment(colName, newColumn, function.getOutputType()));
			aggregations.add(aggregation.get());
		}
		for (ColumnHandle columnHandle : groupingSets.get(0)) {
			Optional<RediSearchAggregationTerm> termAggregation = RediSearchAggregationTerm
					.fromColumnHandle(columnHandle);
			if (termAggregation.isEmpty()) {
				return Optional.empty();
			}
			terms.add(termAggregation.get());
		}
		ImmutableList<RediSearchAggregation> aggregationList = aggregations.build();
		if (aggregationList.isEmpty()) {
			return Optional.empty();
		}
		ImmutableList<RediSearchAggregationTerm> termList = terms.build();
		// Over RESP2, the shards of a sharded database send its coordinator the doubles they compute rounded to 12
		// significant digits, which no mantissa and exponent computed after it merges them could restore
		if (!rediSearchSession.isResp3()
				&& (aggregationList.stream().map(RediSearchAggregation::getOutputType)
						.anyMatch(RediSearchExactNumbers::isFloatingPoint)
						|| termList.stream().map(RediSearchAggregationTerm::getType)
								.anyMatch(RediSearchExactNumbers::isFloatingPoint))) {
			log.debug("Rejecting aggregation pushdown: RESP2 cannot preserve floating-point results or grouping keys");
			return Optional.empty();
		}
		RediSearchTableHandle tableHandle = table.withAggregations(termList, aggregationList);
		return Optional.of(new AggregationApplicationResult<>(tableHandle, projections.build(),
				resultAssignments.build(), Map.of(), false));
	}

	static boolean isGroupPushdownSafe(OptionalLong documents, long limit) {
		return documents.isPresent() && documents.getAsLong() >= 0 && documents.getAsLong() <= limit;
	}

	private void setRollback(Runnable action) {
		checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
	}

	private void clearRollback() {
		rollbackAction.set(null);
	}

	public void rollback() {
		Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
	}

	private SchemaTableName getTableName(ConnectorTableHandle tableHandle) {
		return ((RediSearchTableHandle) tableHandle).getSchemaTableName();
	}

	private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName) {
		RediSearchTableHandle tableHandle = rediSearchSession.getTable(tableName).getTableHandle();

		List<ColumnMetadata> columns = ImmutableList
				.copyOf(getColumnHandles(session, tableHandle).values().stream().map(RediSearchColumnHandle.class::cast)
						.map(RediSearchColumnHandle::toColumnMetadata).collect(Collectors.toList()));

		return new ConnectorTableMetadata(tableName, columns);
	}

	private List<RediSearchColumnHandle> buildColumnHandles(ConnectorTableMetadata tableMetadata) {
		return tableMetadata.getColumns().stream().map(m -> new RediSearchColumnHandle(m.getName(), m.getType(),
				RediSearchSession.toFieldType(m.getType()), m.isHidden(), true, Optional.empty())).collect(Collectors.toList());
	}
}
