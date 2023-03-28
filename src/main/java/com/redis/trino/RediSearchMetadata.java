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
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.querybuilder.Values;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.NotFoundException;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;

public class RediSearchMetadata implements ConnectorMetadata {

	private static final Logger log = Logger.get(RediSearchMetadata.class);

	private static final String SYNTHETIC_COLUMN_NAME_PREFIX = "syntheticColumn";
	private static final Set<Integer> REDISEARCH_RESERVED_CHARACTERS = IntStream
			.of('?', '*', '|', '{', '}', '[', ']', '(', ')', '"', '#', '@', '&', '<', '>', '~').boxed()
			.collect(toImmutableSet());

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
	public RediSearchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
		requireNonNull(tableName, "tableName is null");

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
		return columnHandles.build();
	}

	@Override
	public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
			SchemaTablePrefix prefix) {
		requireNonNull(prefix, "prefix is null");
		ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
		for (SchemaTableName tableName : listTables(session, prefix)) {
			try {
				columns.put(tableName, getTableMetadata(session, tableName).getColumns());
			} catch (NotFoundException e) {
				// table disappeared during listing operation
			}
		}
		return columns.buildOrThrow();
	}

	private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
		if (prefix.getTable().isEmpty()) {
			return listTables(session, prefix.getSchema());
		}
		return List.of(prefix.toSchemaTableName());
	}

	@Override
	public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
			ColumnHandle columnHandle) {
		return ((RediSearchColumnHandle) columnHandle).toColumnMetadata();
	}

	@Override
	public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting) {
		rediSearchSession.createTable(tableMetadata.getTable(), buildColumnHandles(tableMetadata));
	}

	@Override
	public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
		RediSearchTableHandle table = (RediSearchTableHandle) tableHandle;
		rediSearchSession.dropTable(table.getSchemaTableName());
	}

	@Override
	public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column) {
		rediSearchSession.addColumn(((RediSearchTableHandle) tableHandle).getSchemaTableName(), column);
	}

	@Override
	public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column) {
		rediSearchSession.dropColumn(((RediSearchTableHandle) tableHandle).getSchemaTableName(),
				((RediSearchColumnHandle) column).getName());
	}

	@Override
	public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
			Optional<ConnectorTableLayout> layout, RetryMode retryMode) {
		checkRetry(retryMode);
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
		List<RediSearchColumnHandle> columns = rediSearchSession.getTable(table.getSchemaTableName()).getColumns();

		return new RediSearchInsertTableHandle(table.getSchemaTableName(),
				columns.stream().filter(column -> !column.isHidden()).collect(Collectors.toList()));
	}

	@Override
	public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
			ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments,
			Collection<ComputedStatistics> computedStatistics) {
		return Optional.empty();
	}

	@Override
	public RediSearchColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session,
			ConnectorTableHandle tableHandle) {
		return RediSearchBuiltinField.KEY.getColumnHandle();
	}

	@Override
	public RediSearchTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle,
			RetryMode retryMode) {
		checkRetry(retryMode);
		return (RediSearchTableHandle) tableHandle;
	}

	@Override
	public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments) {
		// Do nothing
	}

	@Override
	public RediSearchColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle,
			List<ColumnHandle> updatedColumns) {
		return RediSearchBuiltinField.KEY.getColumnHandle();
	}

	@Override
	public RediSearchTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle,
			List<ColumnHandle> updatedColumns, RetryMode retryMode) {
		checkRetry(retryMode);
		RediSearchTableHandle table = (RediSearchTableHandle) tableHandle;
		return new RediSearchTableHandle(table.getSchemaTableName(), table.getConstraint(), table.getLimit(),
				table.getTermAggregations(), table.getMetricAggregations(), table.getWildcards(),
				updatedColumns.stream().map(RediSearchColumnHandle.class::cast).collect(toImmutableList()));
	}

	@Override
	public void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments) {
		// Do nothing
	}

	@Override
	public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table) {
		RediSearchTableHandle handle = (RediSearchTableHandle) table;
		return new ConnectorTableProperties(handle.getConstraint(), Optional.empty(), Optional.empty(),
				Optional.empty(), List.of());
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

		return Optional.of(new LimitApplicationResult<>(new RediSearchTableHandle(handle.getSchemaTableName(),
				handle.getConstraint(), OptionalLong.of(limit), handle.getTermAggregations(),
				handle.getMetricAggregations(), handle.getWildcards(), handle.getUpdatedColumns()), true, false));
	}

	@Override
	public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
			ConnectorTableHandle table, Constraint constraint) {
		RediSearchTableHandle handle = (RediSearchTableHandle) table;

		ConnectorExpression oldExpression = constraint.getExpression();
		Map<String, String> newWildcards = new HashMap<>(handle.getWildcards());
		List<ConnectorExpression> expressions = ConnectorExpressions.extractConjuncts(constraint.getExpression());
		List<ConnectorExpression> notHandledExpressions = new ArrayList<>();
		for (ConnectorExpression expression : expressions) {
			if (expression instanceof Call) {
				Call call = (Call) expression;
				if (isSupportedLikeCall(call)) {
					List<ConnectorExpression> arguments = call.getArguments();
					String variableName = ((Variable) arguments.get(0)).getName();
					RediSearchColumnHandle column = (RediSearchColumnHandle) constraint.getAssignments()
							.get(variableName);
					verifyNotNull(column, "No assignment for %s", variableName);
					String columnName = column.getName();
					Object pattern = ((Constant) arguments.get(1)).getValue();
					Optional<Slice> escape = Optional.empty();
					if (arguments.size() == 3) {
						escape = Optional.of((Slice) (((Constant) arguments.get(2)).getValue()));
					}

					if (!newWildcards.containsKey(columnName) && pattern instanceof Slice) {
						String wildcard = likeToWildcard((Slice) pattern, escape);
						if (column.getFieldType() == Field.Type.TAG) {
							wildcard = Values.tags(wildcard).toString();
						}
						newWildcards.put(columnName, wildcard);
						continue;
					}
				}
			}
			notHandledExpressions.add(expression);
		}

		Map<ColumnHandle, Domain> supported = new HashMap<>();
		Map<ColumnHandle, Domain> unsupported = new HashMap<>();
		Map<ColumnHandle, Domain> domains = constraint.getSummary().getDomains()
				.orElseThrow(() -> new IllegalArgumentException("constraint summary is NONE"));
		for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
			RediSearchColumnHandle column = (RediSearchColumnHandle) entry.getKey();

			if (column.isSupportsPredicates() && !newWildcards.containsKey(column.getName())) {
				supported.put(column, entry.getValue());
			} else {
				unsupported.put(column, entry.getValue());
			}
		}

		TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
		TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(TupleDomain.withColumnDomains(supported));
		ConnectorExpression newExpression = ConnectorExpressions.and(notHandledExpressions);
		if (oldDomain.equals(newDomain) && oldExpression.equals(newExpression)) {
			return Optional.empty();
		}

		handle = new RediSearchTableHandle(handle.getSchemaTableName(), newDomain, handle.getLimit(),
				handle.getTermAggregations(), handle.getMetricAggregations(), newWildcards, handle.getUpdatedColumns());

		return Optional.of(new ConstraintApplicationResult<>(handle, TupleDomain.withColumnDomains(unsupported),
				newExpression, false));

	}

	protected static boolean isSupportedLikeCall(Call call) {
		if (!LIKE_FUNCTION_NAME.equals(call.getFunctionName())) {
			return false;
		}

		List<ConnectorExpression> arguments = call.getArguments();
		if (arguments.size() < 2 || arguments.size() > 3) {
			return false;
		}

		if (!(arguments.get(0) instanceof Variable) || !(arguments.get(1) instanceof Constant)) {
			return false;
		}

		if (arguments.size() == 3) {
			return arguments.get(2) instanceof Constant;
		}

		return true;
	}

	private static char getEscapeChar(Slice escape) {
		String escapeString = escape.toStringUtf8();
		if (escapeString.length() == 1) {
			return escapeString.charAt(0);
		}
		throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Escape string must be a single character");
	}

	protected static String likeToWildcard(Slice pattern, Optional<Slice> escape) {
		Optional<Character> escapeChar = escape.map(RediSearchMetadata::getEscapeChar);
		StringBuilder wildcard = new StringBuilder();
		boolean escaped = false;
		int position = 0;
		while (position < pattern.length()) {
			int currentChar = getCodePointAt(pattern, position);
			position += 1;
			checkEscape(!escaped || currentChar == '%' || currentChar == '_' || currentChar == escapeChar.get());
			if (!escaped && escapeChar.isPresent() && currentChar == escapeChar.get()) {
				escaped = true;
			} else {
				switch (currentChar) {
				case '%':
					wildcard.append(escaped ? "%" : "*");
					escaped = false;
					break;
				case '_':
					wildcard.append(escaped ? "_" : "?");
					escaped = false;
					break;
				case '\\':
					wildcard.append("\\\\");
					break;
				default:
					// escape special RediSearch characters
					if (REDISEARCH_RESERVED_CHARACTERS.contains(currentChar)) {
						wildcard.append('\\');
					}

					wildcard.appendCodePoint(currentChar);
					escaped = false;
				}
			}
		}

		checkEscape(!escaped);
		return wildcard.toString();
	}

	private static void checkEscape(boolean condition) {
		if (!condition) {
			throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
					"Escape character must be followed by '%', '_' or the escape character itself");
		}
	}

	@Override
	public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(ConnectorSession session,
			ConnectorTableHandle handle, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments,
			List<List<ColumnHandle>> groupingSets) {
		log.info("applyAggregation aggregates=%s groupingSets=%s", aggregates, groupingSets);
		RediSearchTableHandle table = (RediSearchTableHandle) handle;
		// Global aggregation is represented by [[]]
		verify(!groupingSets.isEmpty(), "No grouping sets provided");
		if (!table.getTermAggregations().isEmpty()) {
			return Optional.empty();
		}
		ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
		ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();
		ImmutableList.Builder<RediSearchAggregation> aggregations = ImmutableList.builder();
		ImmutableList.Builder<RediSearchAggregationTerm> terms = ImmutableList.builder();
		for (int i = 0; i < aggregates.size(); i++) {
			AggregateFunction function = aggregates.get(i);
			String colName = SYNTHETIC_COLUMN_NAME_PREFIX + i;
			Optional<RediSearchAggregation> aggregation = RediSearchAggregation.handleAggregation(function, assignments,
					colName);
			if (aggregation.isEmpty()) {
				return Optional.empty();
			}
			io.trino.spi.type.Type outputType = function.getOutputType();
			RediSearchColumnHandle newColumn = new RediSearchColumnHandle(colName, outputType,
					RediSearchSession.toFieldType(outputType), false, true);
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
		RediSearchTableHandle tableHandle = new RediSearchTableHandle(table.getSchemaTableName(), table.getConstraint(),
				table.getLimit(), terms.build(), aggregationList, table.getWildcards(), table.getUpdatedColumns());
		return Optional.of(new AggregationApplicationResult<>(tableHandle, projections.build(),
				resultAssignments.build(), Map.of(), false));
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
				RediSearchSession.toFieldType(m.getType()), m.isHidden(), true)).collect(Collectors.toList());
	}
}
