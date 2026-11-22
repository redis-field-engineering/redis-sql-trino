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
import static io.trino.spi.type.BigintType.BIGINT;
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
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.RelationCommentMetadata;
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
	private final RediSearchRelationMetadata relationMetadata;
	private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

	public RediSearchMetadata(RediSearchSession rediSearchSession) {
		this.rediSearchSession = requireNonNull(rediSearchSession, "rediSearchSession is null");
		this.schemaName = rediSearchSession.getConfig().getDefaultSchema();
		this.relationMetadata = new RediSearchRelationMetadata(rediSearchSession.sync());
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
				return RediSearchPlanDiagnostics.annotate(rediSearchSession.getTable(tableName), rediSearchSession);
			} catch (TableNotFoundException e) {
				// ignore and return null
			}
		}
		return null;
	}

    @Override
    public Optional<Object> getInfo(ConnectorSession session, ConnectorTableHandle table) {
        RediSearchTableHandle handle = (RediSearchTableHandle) table;
        return Optional.of(Map.of("planningPolicy", handle.getPushdownPolicy(),
                "appliedPushdowns", RediSearchPlanDiagnostics.applied(handle)));
    }

	@Override
	public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
		requireNonNull(tableHandle, "tableHandle is null");
		SchemaTableName tableName = getTableName(tableHandle);
		return getTableMetadata(session, tableName);
	}

	@Override
	public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName) {
		if (optionalSchemaName.isPresent() && !optionalSchemaName.orElseThrow().equals(schemaName)) {
			return List.of();
		}
		ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
		for (String tableName : rediSearchSession.getAllTables()) {
			tableNames.add(new SchemaTableName(schemaName, tableName));
		}
		tableNames.addAll(listViews(session, optionalSchemaName));
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
				Optional<ConnectorViewDefinition> view = getView(session, tableName);
				if (view.isPresent()) {
					relationColumns.add(RelationColumnsMetadata.forView(tableName, view.orElseThrow().getColumns()));
					continue;
				}
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
		RediSearchColumnHandle column = (RediSearchColumnHandle) columnHandle;
		String index = ((RediSearchTableHandle) tableHandle).getIndex();
		return ColumnMetadata.builderFrom(column.toColumnMetadata())
				.setComment(Optional.ofNullable(relationMetadata.comments(index).get("column:" + column.getName()))).build();
	}

	@Override
	public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment) {
		relationMetadata.setComment(((RediSearchTableHandle) tableHandle).getIndex(), Optional.empty(), comment);
	}

	@Override
	public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle,
			Optional<String> comment) {
		relationMetadata.setComment(((RediSearchTableHandle) tableHandle).getIndex(),
				Optional.of(((RediSearchColumnHandle) columnHandle).getName()), comment);
	}

	@Override
	public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> optionalSchemaName) {
		if (optionalSchemaName.isPresent() && !optionalSchemaName.orElseThrow().equals(schemaName)) {
			return List.of();
		}
		return relationMetadata.viewNames(schemaName).stream().map(name -> new SchemaTableName(schemaName, name)).toList();
	}

	@Override
	public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> optionalSchemaName) {
		if (optionalSchemaName.isPresent() && !optionalSchemaName.orElseThrow().equals(schemaName)) {
			return Map.of();
		}
		Map<SchemaTableName, ConnectorViewDefinition> views = new LinkedHashMap<>();
		relationMetadata.views(schemaName).forEach((name, definition) -> views.put(new SchemaTableName(schemaName, name), definition));
		return Map.copyOf(views);
	}

	@Override
	public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName name) {
		if (!name.getSchemaName().equals(schemaName)) {
			return Optional.empty();
		}
		return relationMetadata.view(schemaName, name.getTableName());
	}

	@Override
	public void createView(ConnectorSession session, SchemaTableName name, ConnectorViewDefinition definition,
			Map<String, Object> properties, SaveMode saveMode) {
		if (!name.getSchemaName().equals(schemaName)) {
			throw new TrinoException(StandardErrorCode.SCHEMA_NOT_FOUND, "Schema does not exist: " + name.getSchemaName());
		}
		if (!properties.isEmpty()) {
			throw new TrinoException(StandardErrorCode.INVALID_VIEW_PROPERTY, "View properties are not supported");
		}
		if (getTableHandle(session, name, Optional.empty(), Optional.empty()) != null) {
			throw new TrinoException(StandardErrorCode.ALREADY_EXISTS, "Table already exists: " + name);
		}
		relationMetadata.createView(schemaName, name.getTableName(), definition, saveMode == SaveMode.REPLACE, saveMode == SaveMode.IGNORE);
	}

	@Override
	public void dropView(ConnectorSession session, SchemaTableName name) {
		if (!name.getSchemaName().equals(schemaName)) {
			throw new io.trino.spi.connector.ViewNotFoundException(name);
		}
		relationMetadata.dropView(schemaName, name.getTableName());
	}

	@Override
	public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target) {
		if (!source.getSchemaName().equals(schemaName) || !target.getSchemaName().equals(schemaName)) {
			throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Views can only be renamed within the configured schema");
		}
		if (getTableHandle(session, target, Optional.empty(), Optional.empty()) != null) {
			throw new TrinoException(StandardErrorCode.ALREADY_EXISTS, "Table already exists: " + target);
		}
		relationMetadata.renameView(schemaName, source.getTableName(), target.getTableName());
	}

	@Override
	public void setViewComment(ConnectorSession session, SchemaTableName name, Optional<String> comment) {
		updateView(session, name, definition -> new ConnectorViewDefinition(definition.getOriginalSql(), definition.getCatalog(),
				definition.getSchema(), definition.getColumns(), comment, definition.getOwner(), definition.isRunAsInvoker(), definition.getPath()));
	}

	@Override
	public void setViewColumnComment(ConnectorSession session, SchemaTableName name, String columnName, Optional<String> comment) {
		updateView(session, name, definition -> {
			List<ConnectorViewDefinition.ViewColumn> columns = definition.getColumns().stream()
					.map(column -> column.getName().equals(columnName)
							? new ConnectorViewDefinition.ViewColumn(column.getName(), column.getType(), comment) : column).toList();
			return new ConnectorViewDefinition(definition.getOriginalSql(), definition.getCatalog(), definition.getSchema(), columns,
					definition.getComment(), definition.getOwner(), definition.isRunAsInvoker(), definition.getPath());
		});
	}

	@Override
	public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schema,
			UnaryOperator<Set<SchemaTableName>> relationFilter) {
		ImmutableList.Builder<RelationCommentMetadata> comments = ImmutableList.builder();
		for (SchemaTableName name : relationFilter.apply(Set.copyOf(listTables(session, schema)))) {
			try {
				Optional<ConnectorViewDefinition> view = getView(session, name);
				comments.add(RelationCommentMetadata.forRelation(name, view.isPresent() ? view.orElseThrow().getComment()
						: getTableMetadata(session, name).getComment()));
			}
			catch (NotFoundException e) {
				// A relation can disappear while information_schema/system metadata is being listed.
			}
		}
		return comments.build().iterator();
	}

	private void updateView(ConnectorSession session, SchemaTableName name,
			UnaryOperator<ConnectorViewDefinition> update) {
		if (!name.getSchemaName().equals(schemaName)) {
			throw new io.trino.spi.connector.ViewNotFoundException(name);
		}
		relationMetadata.updateView(schemaName, name.getTableName(), update);
	}

	private void checkViewCollision(ConnectorSession session, SchemaTableName name) {
		if (getView(session, name).isPresent()) {
			throw new TrinoException(StandardErrorCode.ALREADY_EXISTS, "View already exists: " + name);
		}
	}

	private void saveInitialComments(ConnectorSession session, ConnectorTableMetadata metadata) {
		if (metadata.getComment().isEmpty() && metadata.getColumns().stream().noneMatch(column -> column.getComment().isPresent())) {
			return;
		}
		String index = rediSearchSession.getTable(metadata.getTable()).getTableHandle().getIndex();
		Map<String, String> comments = new LinkedHashMap<>();
		metadata.getComment().ifPresent(comment -> comments.put("table", comment));
		metadata.getColumns().forEach(column -> column.getComment().ifPresent(comment -> comments.put("column:" + column.getName(), comment)));
		relationMetadata.initializeComments(index, comments);
	}

	@Override
	public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode) {
		if (saveMode == SaveMode.REPLACE) {
			throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "This connector does not support replacing tables");
		}
		if (saveMode == SaveMode.IGNORE && (getTableHandle(session, tableMetadata.getTable(), Optional.empty(), Optional.empty()) != null
				|| getView(session, tableMetadata.getTable()).isPresent())) {
			return;
		}
		checkViewCollision(session, tableMetadata.getTable());
		rediSearchSession.createTable(tableMetadata.getTable(), buildColumnHandles(tableMetadata));
		saveInitialComments(session, tableMetadata);
	}

	@Override
	public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
		RediSearchTableHandle table = (RediSearchTableHandle) tableHandle;
		RediSearchTruncate.truncate(rediSearchSession, new RediSearchTableHandle(table.getSchemaTableName(), table.getIndex()));
	}

	@Override
	public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
		RediSearchTableHandle table = (RediSearchTableHandle) tableHandle;
		rediSearchSession.dropTable(table.getSchemaTableName());
		relationMetadata.deleteComments(table.getIndex());
	}

	@Override
	public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column,
			ColumnPosition position) {
		validateColumnName(column.getName());
		RediSearchTableHandle table = (RediSearchTableHandle) tableHandle;
		Map<String, Type> ordered = new LinkedHashMap<>();
		if (position instanceof ColumnPosition.First) {
			ordered.put(column.getName(), column.getType());
		}
		for (RediSearchColumnHandle existing : rediSearchSession.getTable(table.getSchemaTableName()).getColumns()) {
			if (!existing.isHidden()) {
				ordered.put(existing.getName(), existing.getType());
				if (position instanceof ColumnPosition.After after && existing.getName().equals(after.columnName())) {
					ordered.put(column.getName(), column.getType());
				}
			}
		}
		if (position instanceof ColumnPosition.Last) {
			ordered.put(column.getName(), column.getType());
		}
		if (!ordered.containsKey(column.getName())) {
			throw new TrinoException(StandardErrorCode.COLUMN_NOT_FOUND, "Column position does not exist: " + position);
		}
		rediSearchSession.addColumn(table.getSchemaTableName(), column, ordered);
		if (column.getComment().isPresent()) {
			relationMetadata.setComment(((RediSearchTableHandle) tableHandle).getIndex(), Optional.of(column.getName()), column.getComment());
		}
	}

	@Override
	public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
			Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace) {
		checkRetry(retryMode);
		if (replace) {
			throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "This connector does not support replacing tables");
		}
		checkViewCollision(session, tableMetadata.getTable());
		List<RediSearchColumnHandle> columns = buildColumnHandles(tableMetadata);

		rediSearchSession.createTable(tableMetadata.getTable(), columns);

		setRollback(() -> {
			// CREATE uses the table name as its index. A canceled writer may have left a
			// failed schema-cache load in flight; rollback must not depend on that load.
			String index = tableMetadata.getTable().getTableName();
			rediSearchSession.dropTable(tableMetadata.getTable());
			relationMetadata.deleteComments(index);
		});
		saveInitialComments(session, tableMetadata);

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
		rediSearchSession.publishFlexStaging(fragments);
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
		rediSearchSession.publishFlexStaging(fragments);
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
	public Optional<ConnectorOutputMetadata> finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle,
			List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments,
			Collection<ComputedStatistics> computedStatistics) {
		rediSearchSession.publishFlexStaging(fragments);
		return Optional.empty();
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
		long rows = documents.orElseThrow();
		if (handle.getLimit().isPresent()) {
			rows = Math.min(rows, handle.getLimit().orElseThrow());
		}
		return TableStatistics.builder().setRowCount(Estimate.of(rows)).build();
	}

	@Override
	public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table) {
		RediSearchTableHandle handle = (RediSearchTableHandle) table;
		// Prefilters do not guarantee their domains: TAG/TEXT can overmatch, and JSON NUMERIC indexes
		// match ANY array member. Use the same exactness policy as applyFilter so optimizer rules cannot
		// discard a residual that the connector still requires.
		TupleDomain<ColumnHandle> predicate = handle.getConstraint()
				.filter((column, domain) -> rediSearchSession.isQueryExact(handle, (RediSearchColumnHandle) column, domain));
		return new ConnectorTableProperties(predicate, Optional.empty(), Optional.empty(), List.of());
	}

	@Override
	public Optional<io.trino.spi.connector.TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(
			ConnectorSession session, io.trino.spi.function.table.ConnectorTableFunctionHandle function) {
		if (function instanceof RediSearchNativeSearch.Handle handle) {
			return Optional.of(new io.trino.spi.connector.TableFunctionApplicationResult<>(handle.getTable(),
					handle.getColumns().stream().map(column -> (ColumnHandle) column).toList()));
		}
		return Optional.empty();
	}

	@Override
	public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session,
			ConnectorTableHandle table, long limit) {
		if (rediSearchSession.getConfig().isFlexEnabled()) {
			return Optional.empty();
		}

		RediSearchTableHandle handle = (RediSearchTableHandle) table;

		if (limit == 0) {
			return Optional.empty();
		}

		if (handle.getLimit().isPresent() && handle.getLimit().orElseThrow() <= limit) {
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

	/** Prefer Redis numeric SORTBY, otherwise retain an exact bounded heap before fetching wide HASH projections. */
	@Override
	public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(ConnectorSession session,
			ConnectorTableHandle table, long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments) {
		if (rediSearchSession.getConfig().isFlexEnabled()) {
			return rejectTopN((RediSearchTableHandle) table, new RediSearchPushdownDiagnostics.Rejection(
					RediSearchPushdownDiagnostics.Reason.FLEX_MODE, "redisearch.flex.enabled=true"));
		}
		RediSearchTableHandle handle = (RediSearchTableHandle) table;
		if (handle.getLimit().isPresent() || !handle.getSort().isEmpty() || !handle.getTermAggregations().isEmpty()
				|| !handle.getMetricAggregations().isEmpty() || topNCount <= 0 || sortItems.isEmpty()) {
			return rejectTopN(handle, new RediSearchPushdownDiagnostics.Rejection(
					RediSearchPushdownDiagnostics.Reason.TOPN_INPUT_SHAPE,
					"existingLimit=" + handle.getLimit().isPresent() + " sortKeyCount=" + sortItems.size()
							+ " requestedCandidates=" + topNCount));
		}
		RediSearchTable schema = rediSearchSession.getTable(handle.getSchemaTableName());
		boolean json = schema.getIndexInfo().getKeyType().filter(RediSearchIndexInfo.KeyType.JSON::equals).isPresent();
		if (json ? !RediSearchJsonValues.supportsSchema(schema)
                : schema.getIndexInfo().getKeyType().filter(RediSearchIndexInfo.KeyType.HASH::equals).isEmpty()) {
			return rejectTopN(handle, new RediSearchPushdownDiagnostics.Rejection(
					RediSearchPushdownDiagnostics.Reason.TOPN_SCHEMA_UNSUPPORTED, "supportedOrderingSchemaAvailable=false"));
		}
		List<RediSearchColumnHandle> columns = new java.util.ArrayList<>();
		boolean nativeSort = !json;
		for (SortItem item : sortItems) {
			if (!(assignments.get(item.getName()) instanceof RediSearchColumnHandle column)
					|| column.getExpression().isPresent()
					|| schema.getColumns().stream().noneMatch(candidate -> candidate.getName().equals(column.getName()))) {
				return rejectTopN(handle, new RediSearchPushdownDiagnostics.Rejection(
						RediSearchPushdownDiagnostics.Reason.TOPN_SORT_KEY_UNSUPPORTED, "storedSortKeyAvailable=false"));
			}
			columns.add(column);
			nativeSort &= column.getFieldType() == RediSearchFieldType.NUMERIC && column.isSupportsPredicates()
                    // Distinct external doubles can decode to the same REAL; Redis must not break SQL ties before
                    // later sort keys. The typed local comparator groups those spellings as the same float.
                    && !column.getType().equals(REAL)
                    // RESP2 does not guarantee lossless floating shard transport; read exact HASH sort values.
                    && (!column.getType().equals(DOUBLE) || rediSearchSession.isResp3())
					&& SORTABLE_TYPES.contains(column.getType()) && RediSearchQueryBuilder.isProperty(column.getName())
					&& !item.getSortOrder().isNullsFirst();
		}
		boolean local = !nativeSort;
		if (local) {
			Optional<RediSearchPushdownDiagnostics.Rejection> budget = RediSearchPushdownDiagnostics.localTopNBudget(
					topNCount, rediSearchSession.getConfig().getLocalTopNMaxCandidates());
			if (budget.isPresent()) {
				return rejectTopN(handle, budget.orElseThrow());
			}
			if (columns.stream().anyMatch(column -> !RediSearchLocalTopN.supports(column.getType()))) {
				return rejectTopN(handle, new RediSearchPushdownDiagnostics.Rejection(
						RediSearchPushdownDiagnostics.Reason.TOPN_TYPE_UNSUPPORTED, "exactSortComparatorAvailable=false"));
			}
		}
		ImmutableList.Builder<RediSearchSortItem> sort = ImmutableList.builder();
		for (int i = 0; i < sortItems.size(); i++) {
			var order = sortItems.get(i).getSortOrder();
			sort.add(new RediSearchSortItem(columns.get(i).getName(), order.isAscending(), local, order.isNullsFirst()));
		}
		return Optional.of(new TopNApplicationResult<>(handle.withTopN(sort.build(), topNCount), false, false));
	}

	@Override
	public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
			ConnectorTableHandle table, Constraint constraint) {
		if (rediSearchSession.getConfig().isFlexEnabled()) {
			return Optional.empty();
		}

		RediSearchTableHandle handle = (RediSearchTableHandle) table;
		// The first rows of a sort, filtered, aren't the first of the filtered rows
		if (handle.hasNativeParameters() || !handle.getSort().isEmpty() || handle.getLimit().isPresent() || !handle.getAggregationStages().isEmpty()) {
			// The filters stored on the handle run on source documents. HAVING must not be moved before GROUPBY.
			return Optional.empty();
		}

		// Pipeline filters run after HASH values load, before a limit/group/sort can discard rows.
		boolean hash = rediSearchSession.getTable(handle.getSchemaTableName()).getIndexInfo().getKeyType()
				.filter(RediSearchIndexInfo.KeyType.HASH::equals).isPresent();
		Map<String, String> filters = new LinkedHashMap<>(handle.getFilters());
		Map<ColumnHandle, Domain> supported = new HashMap<>();
		Map<ColumnHandle, Domain> unsupported = new HashMap<>();
		Map<ColumnHandle, Domain> domains = constraint.getSummary().getDomains()
				.orElseThrow(() -> new IllegalArgumentException("constraint summary is NONE"));
		for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
			RediSearchColumnHandle column = (RediSearchColumnHandle) entry.getKey();
			Domain domain = entry.getValue();
			Optional<String> booleanFilter = hash ? RediSearchQueryBuilder.booleanDomainFilter(column, domain) : Optional.empty();
			if (booleanFilter.isPresent()) {
				mergeFilter(filters, column.getName(), booleanFilter.orElseThrow());
				continue;
			}

			if (rediSearchSession.canQuery(handle, column, domain)) {
				supported.put(column, domain);
				if (!rediSearchSession.isQueryExact(handle, column, domain)) {
					// Redis returns a superset of the matching rows, which Trino filters
					unsupported.put(column, domain);
				}
			} else {
				Optional<String> presence = hash ? RediSearchQueryBuilder.presenceFilter(column, domain)
						.or(() -> RediSearchQueryBuilder.stringDomainFilter(column, domain)) : Optional.empty();
				if (presence.isPresent()) {
					mergeFilter(filters, column.getName(), presence.orElseThrow());
				} else {
					log.debug("Retaining predicate in Trino: index %s column %s type %s nullAllowed %s discreteDomain %s",
							handle.getIndex(), column.getName(), domain.getType(), domain.isNullAllowed(), domain.getValues().isDiscreteSet());
					unsupported.put(column, domain);
				}
			}
		}

		List<ConnectorExpression> remaining = new java.util.ArrayList<>();
		for (ConnectorExpression expression : ConnectorExpressions.extractConjuncts(constraint.getExpression())) {
			if (expression instanceof Call call
					&& call.getFunctionName().equals(io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME)
					&& (call.getArguments().size() == 2 || call.getArguments().size() == 3)
					&& call.getArguments().get(0) instanceof Variable variable
					&& call.getArguments().get(1) instanceof Constant constant && constant.getValue() instanceof Slice pattern
					&& constraint.getAssignments().get(variable.getName()) instanceof RediSearchColumnHandle column
					&& hash) {
				Optional<String> escape = Optional.empty();
				if (call.getArguments().size() == 3) {
					if (!(call.getArguments().get(2) instanceof Constant escapeConstant)
							|| !(escapeConstant.getValue() instanceof Slice escapeSlice)
							|| !io.airlift.slice.Slices.utf8Slice(escapeSlice.toStringUtf8()).equals(escapeSlice)) {
						remaining.add(expression);
						continue;
					}
					escape = Optional.of(escapeSlice.toStringUtf8());
				}
				Optional<RediSearchQueryBuilder.LikePredicate> filter = io.airlift.slice.Slices.utf8Slice(pattern.toStringUtf8()).equals(pattern)
						? RediSearchQueryBuilder.likeFilter(column, pattern.toStringUtf8(), escape) : Optional.empty();
				if (filter.isPresent()) {
					mergeFilter(filters, column.getName(), filter.orElseThrow().expression());
					if (filter.orElseThrow().exact()) { continue; }
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

	private static void mergeFilter(Map<String, String> filters, String field, String filter) {
		String previous = filters.get(field);
		if (previous == null) {
			filters.put(field, filter);
		} else if (!previous.equals(filter) && !previous.contains("(" + filter + ")")) {
			filters.put(field, "(" + previous + ") && (" + filter + ")");
		}
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
		return projectExpressions(table, projections, assignments);
	}

	static Optional<ProjectionApplicationResult<ConnectorTableHandle>> projectExpressions(RediSearchTableHandle table,
			List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments) {
		ImmutableList.Builder<ConnectorExpression> newProjections = ImmutableList.builder();
		Map<String, Assignment> newAssignments = new LinkedHashMap<>();
		// Repeated optimizer passes may already have materialized a requested expression. Reuse that variable,
		// including when it is also projected unchanged, so a TableScan never gets two equal column handles.
		Map<RediSearchColumnHandle, String> variables = new HashMap<>();
		assignments.forEach((name, column) -> variables.putIfAbsent((RediSearchColumnHandle) column, name));
		boolean translated = false;
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
			translated = true;
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
		if (!translated) {
			return Optional.empty();
		}
		return Optional.of(new ProjectionApplicationResult<>(table, newProjections.build(),
				ImmutableList.copyOf(newAssignments.values()), false));
	}

	@Override
	public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(ConnectorSession session,
			ConnectorTableHandle handle, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments,
			List<List<ColumnHandle>> groupingSets) {
		log.debug("applyAggregation aggregateCount=%s groupingSetCount=%s", aggregates.size(), groupingSets.size());
		Optional<RediSearchPushdownDiagnostics.Rejection> configuration = RediSearchPushdownDiagnostics.configuration(
				rediSearchSession.getConfig().isAggregationPushdownEnabled(), rediSearchSession.getConfig().isFlexEnabled());
		if (configuration.isPresent()) {
			return rejectAggregation((RediSearchTableHandle) handle, configuration.orElseThrow());
		}
		RediSearchTableHandle table = (RediSearchTableHandle) handle;
		// Global aggregation is represented by [[]]
		verify(!groupingSets.isEmpty(), "No grouping sets provided");
		// GROUPBY would run before a LIMIT or SORTBY, over every document rather than the first ones
		if (table.getLimit().isPresent() || !table.getSort().isEmpty()) {
			return rejectAggregation(table, RediSearchPushdownDiagnostics.Reason.INPUT_ORDERING, "inputAlreadyLimitedOrOrdered=true");
		}
		if (groupingSets.size() != 1) {
			return rejectAggregation(table, RediSearchPushdownDiagnostics.Reason.MULTIPLE_GROUPING_SETS,
					"groupingSetCount=" + groupingSets.size());
		}
		if (!table.getAggregationStages().isEmpty()) {
			return applyCountOverDistinct(table, aggregates, assignments, groupingSets.getFirst());
		}
		if (!groupingSets.get(0).isEmpty()) {
			OptionalLong documents = rediSearchSession.getTable(table.getSchemaTableName()).getIndexInfo().getNumDocs();
			long groupBudget = rediSearchSession.getConfig().getAggregationGroupLimit();
			// A normalized finite key domain can bound groups independently of the number of input rows.
			// This relaxes only the group-memory budget; all type, precision and capability guards below remain.
			// JSON index predicates can match a different array element from the first scalar loaded as a key.
			TupleDomain<ColumnHandle> exactSourceDomains = rediSearchSession.getTable(table.getSchemaTableName()).getIndexInfo()
					.getKeyType().filter(RediSearchIndexInfo.KeyType.HASH::equals).isPresent()
					? table.getConstraint().filter((column, domain) -> RediSearchQueryBuilder.isExact((RediSearchColumnHandle) column, domain))
					: TupleDomain.all();
			OptionalLong provenGroups = RediSearchGroupingBounds.upperBoundWithin(documents, groupingSets.getFirst(), exactSourceDomains, groupBudget);
			Optional<RediSearchPushdownDiagnostics.Rejection> groupRejection = RediSearchPushdownDiagnostics.groupBudget(
					documents, groupingSets.getFirst(), exactSourceDomains, groupBudget);
			if (groupRejection.isPresent()) {
				return rejectAggregation(table, groupRejection.orElseThrow());
			}
			if (!isGroupPushdownSafe(documents, groupBudget)) {
				log.debug("GROUP BY finite key bound %s fits group budget %s for index %s with cached document count %s",
						provenGroups.orElseThrow(), groupBudget, table.getIndex(), documents);
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
		Set<String> aggregateNames = rediSearchSession.getTable(table.getSchemaTableName()).getColumns().stream()
				.map(RediSearchColumnHandle::getName).collect(Collectors.toSet());
		assignments.values().stream().map(RediSearchColumnHandle.class::cast)
				.forEach(column -> aggregateNames.add(column.getName()));
		for (int i = 0; i < aggregates.size(); i++) {
			AggregateFunction function = aggregates.get(i);
			String colName = aggregateAlias(SYNTHETIC_COLUMN_NAME_PREFIX + i, aggregateNames);
			Optional<RediSearchAggregation> aggregation = RediSearchAggregation.handleAggregation(function, assignments,
					colName, countValues);
			if (aggregation.isEmpty()) {
				return rejectAggregation(table, RediSearchPushdownDiagnostics.Reason.UNSUPPORTED_AGGREGATE,
						"function=" + function.getFunctionName() + " outputType=" + function.getOutputType());
			}
			if ((aggregation.get().isConditionalCount() || aggregation.get().getFilterColumn().isPresent())
					&& rediSearchSession.getTable(table.getSchemaTableName()).getIndexInfo().getKeyType()
							.filter(RediSearchIndexInfo.KeyType.HASH::equals).isEmpty()) {
				// JSON arrays/nulls have different existence and scalar extraction semantics.
				return rejectAggregation(table, RediSearchPushdownDiagnostics.Reason.CONDITIONAL_JSON, "sourceRequiresHashScalarSemantics=true");
			}
			if (aggregation.get().getIntegerSumRowLimit().isPresent()) {
				OptionalLong documents = rediSearchSession.getTable(table.getSchemaTableName()).getIndexInfo().getNumDocs();
				Optional<RediSearchPushdownDiagnostics.Rejection> precision = RediSearchPushdownDiagnostics.integerPrecision(aggregation.get(), documents);
				if (precision.isPresent()) {
					return rejectAggregation(table, precision.orElseThrow());
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
			Type keyType = ((RediSearchColumnHandle) columnHandle).getType();
			if (RediSearchGroupingNormalizer.requiresNormalization(keyType)
					&& !rediSearchSession.isCaseSupported(table.getIndex())) {
				return rejectAggregation(table, RediSearchPushdownDiagnostics.Reason.CASE_EXPRESSION_UNAVAILABLE,
						"keyType=" + keyType);
			}
			if ((keyType.equals(io.trino.spi.type.BooleanType.BOOLEAN) || keyType instanceof io.trino.spi.type.CharType)
					&& rediSearchSession.getTable(table.getSchemaTableName()).getIndexInfo().getKeyType()
							.filter(RediSearchIndexInfo.KeyType.HASH::equals).isEmpty()) {
				return rejectAggregation(table, RediSearchPushdownDiagnostics.Reason.JSON_GROUP_KEY, "keyType=" + keyType);
			}
			Optional<RediSearchAggregationTerm> termAggregation = RediSearchAggregationTerm
					.fromColumnHandle(columnHandle);
			if (termAggregation.isEmpty()) {
				return rejectAggregation(table, RediSearchPushdownDiagnostics.Reason.UNSAFE_GROUP_KEY, "keyType=" + keyType);
			}
			terms.add(termAggregation.get());
		}
		ImmutableList<RediSearchAggregation> aggregationList = aggregations.build();
		ImmutableList<RediSearchAggregationTerm> termList = terms.build();
		boolean groupingOnly = aggregationList.isEmpty();
		if (aggregationList.isEmpty()) {
			if (termList.isEmpty() || rediSearchSession.getTable(table.getSchemaTableName()).getIndexInfo().getKeyType()
					.filter(RediSearchIndexInfo.KeyType.HASH::equals).isEmpty()) {
				return rejectAggregation(table, RediSearchPushdownDiagnostics.Reason.GROUPING_ONLY_UNSUPPORTED,
						"groupingKeyCount=" + termList.size());
			}
			// Redis requires a reducer, although SQL DISTINCT only needs group keys. Keep this internal;
			// Keys include NULL; an eligible following COUNT can append a second Redis aggregation stage.
			Set<String> inputNames = assignments.values().stream().map(RediSearchColumnHandle.class::cast)
					.map(RediSearchColumnHandle::getName).collect(Collectors.toSet());
			String alias = "__trino_distinct_rows";
			while (inputNames.contains(alias)) { alias += "_"; }
			aggregationList = ImmutableList.of(new RediSearchAggregation(RediSearchAggregation.COUNT, BIGINT,
					Optional.empty(), alias));
		}
		// Over RESP2, the shards of a sharded database send its coordinator the doubles they compute rounded to 12
		// significant digits, which no mantissa and exponent computed after it merges them could restore
		Optional<RediSearchPushdownDiagnostics.Rejection> transport = RediSearchPushdownDiagnostics.floatingTransport(
				rediSearchSession.isResp3(), aggregationList.stream().map(RediSearchAggregation::getOutputType)
						.anyMatch(RediSearchExactNumbers::isFloatingPoint),
				termList.stream().map(RediSearchAggregationTerm::getType).anyMatch(RediSearchExactNumbers::isFloatingPoint));
		if (transport.isPresent()) {
			return rejectAggregation(table, transport.orElseThrow());
		}
		RediSearchTableHandle tableHandle = table.withAggregationStages(List.of(
				new RediSearchAggregationStage(termList, aggregationList, groupingOnly)));
		return Optional.of(new AggregationApplicationResult<>(tableHandle, projections.build(),
				resultAssignments.build(), Map.of(), false));
	}

	private Optional<TopNApplicationResult<ConnectorTableHandle>> rejectTopN(RediSearchTableHandle table,
			RediSearchPushdownDiagnostics.Rejection rejection) {
		log.debug("%s", rejection.message("TopN", table.getIndex()));
		return Optional.empty();
	}

	private Optional<AggregationApplicationResult<ConnectorTableHandle>> rejectAggregation(RediSearchTableHandle table,
			RediSearchPushdownDiagnostics.Reason reason, String detail) {
		return rejectAggregation(table, new RediSearchPushdownDiagnostics.Rejection(reason, detail));
	}

	private Optional<AggregationApplicationResult<ConnectorTableHandle>> rejectAggregation(RediSearchTableHandle table,
			RediSearchPushdownDiagnostics.Rejection rejection) {
		log.debug("%s", rejection.message("aggregation", table.getIndex()));
		return Optional.empty();
	}

	/** Completes Trino's single-DISTINCT rewrite without sending the distinct keys back to Trino. */
	private Optional<AggregationApplicationResult<ConnectorTableHandle>> applyCountOverDistinct(
			RediSearchTableHandle table, List<AggregateFunction> functions, Map<String, ColumnHandle> assignments,
			List<ColumnHandle> groupingKeys) {
		if (table.getAggregationStages().size() != 1 || !table.getAggregationStages().getFirst().groupingOnly()
				|| functions.isEmpty()) {
			return rejectAggregation(table, RediSearchPushdownDiagnostics.Reason.DISTINCT_REWRITE_SHAPE,
					"inputStageCount=" + table.getAggregationStages().size() + " aggregateCount=" + functions.size());
		}
		RediSearchAggregationStage input = table.getAggregationStages().getFirst();
		Map<String, RediSearchAggregationTerm> keys = input.terms().stream()
				.collect(Collectors.toMap(RediSearchAggregationTerm::getTerm, term -> term));
		ImmutableList.Builder<RediSearchAggregationTerm> terms = ImmutableList.builder();
		for (ColumnHandle handle : groupingKeys) {
			if (!(handle instanceof RediSearchColumnHandle column) || column.getExpression().isPresent()
					|| !keys.containsKey(column.getName()) || !keys.get(column.getName()).getType().equals(column.getType())) {
				return rejectAggregation(table, RediSearchPushdownDiagnostics.Reason.DISTINCT_KEY_MISMATCH,
						"secondStageGroupingKeyNotPreserved=true");
			}
			terms.add(keys.get(column.getName()));
		}
		Set<String> reserved = assignments.values().stream().map(RediSearchColumnHandle.class::cast)
				.map(RediSearchColumnHandle::getName).collect(Collectors.toSet());
		input.metrics().forEach(metric -> reserved.add(metric.getAlias()));
		ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
		ImmutableList.Builder<Assignment> results = ImmutableList.builder();
		ImmutableList.Builder<RediSearchAggregation> metrics = ImmutableList.builder();
		OptionalLong documents = rediSearchSession.getTable(table.getSchemaTableName()).getIndexInfo().getNumDocs();
		for (int i = 0; i < functions.size(); i++) {
			AggregateFunction function = functions.get(i);
			if (!function.getFunctionName().equals(RediSearchAggregation.COUNT) || function.isDistinct()
					|| function.getFilter().isPresent() || !function.getSortItems().isEmpty() || function.getArguments().size() > 1) {
				return rejectAggregation(table, RediSearchPushdownDiagnostics.Reason.DISTINCT_REWRITE_SHAPE,
						"secondStageFunction=" + function.getFunctionName());
			}
			if (!function.getArguments().isEmpty()) {
				if (!(function.getArguments().getFirst() instanceof Variable variable)
						|| !(assignments.get(variable.getName()) instanceof RediSearchColumnHandle column)
						|| column.getExpression().isPresent() || !keys.containsKey(column.getName())
						|| !keys.get(column.getName()).getType().equals(column.getType())) {
					return rejectAggregation(table, RediSearchPushdownDiagnostics.Reason.DISTINCT_KEY_MISMATCH,
							"secondStageCountArgumentNotPreserved=true");
				}
			}
			String alias = aggregateAlias(SYNTHETIC_COLUMN_NAME_PREFIX + "_distinct_" + i, reserved);
			Optional<RediSearchAggregation> translated = RediSearchAggregation.handleAggregation(function, assignments, alias, false);
			if (translated.isEmpty()) {
				return rejectAggregation(table, RediSearchPushdownDiagnostics.Reason.UNSUPPORTED_AGGREGATE,
						"secondStageCountTranslationUnavailable=true");
			}
			Optional<RediSearchPushdownDiagnostics.Rejection> precision = RediSearchPushdownDiagnostics.integerPrecision(translated.orElseThrow(), documents);
			if (precision.isPresent()) {
				return rejectAggregation(table, precision.orElseThrow());
			}
			RediSearchColumnHandle output = new RediSearchColumnHandle(alias, function.getOutputType(),
					RediSearchFieldType.NUMERIC, false, false, Optional.empty());
			projections.add(new Variable(alias, function.getOutputType()));
			results.add(new Assignment(alias, output, function.getOutputType()));
			metrics.add(translated.orElseThrow());
		}
		RediSearchTableHandle result = table.appendAggregation(terms.build(), metrics.build());
		return Optional.of(new AggregationApplicationResult<>(result, projections.build(), results.build(), Map.of(), false));
	}

	private static String aggregateAlias(String base, Set<String> reserved) {
		String alias = base;
		while (true) {
			List<String> names = List.of(alias, "__has_" + alias, "__value_" + alias, "__count_" + alias,
					"__precision_count_" + alias);
			if (names.stream().noneMatch(reserved::contains)) {
				reserved.addAll(names);
				return alias;
			}
			alias += "_";
		}
	}

	static boolean isGroupPushdownSafe(OptionalLong documents, long limit) {
		return documents.isPresent() && documents.orElseThrow() >= 0 && documents.orElseThrow() <= limit;
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

		String index = tableHandle.getIndex();
		Map<String, String> comments = relationMetadata.comments(index);
		columns = columns.stream().map(column -> ColumnMetadata.builderFrom(column)
				.setComment(Optional.ofNullable(comments.get("column:" + column.getName()))).build()).toList();
		return new ConnectorTableMetadata(tableName, columns, Map.of(), Optional.ofNullable(comments.get("table")));
	}

	private List<RediSearchColumnHandle> buildColumnHandles(ConnectorTableMetadata tableMetadata) {
		tableMetadata.getColumns().forEach(column -> validateColumnName(column.getName()));
		return tableMetadata.getColumns().stream().map(m -> new RediSearchColumnHandle(m.getName(), m.getType(),
				RediSearchSession.toFieldType(m.getType()), m.isHidden(), !RediSearchSession.isEncodedScalar(m.getType()), Optional.empty())).collect(Collectors.toList());
	}
	private void validateColumnName(String name) {
		boolean reserved = rediSearchSession.getConfig().isCaseInsensitiveNames()
				? name.equalsIgnoreCase(RediSearchSession.ROW_MARKER) : name.equals(RediSearchSession.ROW_MARKER);
		if (reserved) {
			throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Column name is reserved for connector row storage: " + name);
		}
	}

}
