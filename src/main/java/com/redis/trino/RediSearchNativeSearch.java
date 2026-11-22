package com.redis.trino;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

import java.util.List;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.SqlMap;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

/** Explicit native Search predicates, using the normal typed cursor scan and SQL finish operators. */
public final class RediSearchNativeSearch extends AbstractConnectorTableFunction {
    private static final MapType PARAMETERS = new MapType(VARCHAR, VARBINARY, new TypeOperators());
    private final RediSearchSession redis;

    private static SqlMap emptyParameters() {
        var builder = PARAMETERS.createBlockBuilder(null, 1);
        builder.buildEntry((keys, values) -> {});
        return PARAMETERS.getObject(builder.build(), 0);
    }

    public RediSearchNativeSearch(RediSearchSession redis) {
        super("system", "search", List.of(
                ScalarArgumentSpecification.builder().name("TABLE_NAME").type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name("QUERY").type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name("SCHEMA_NAME").type(VARCHAR)
                        .defaultValue(Slices.utf8Slice(redis.getConfig().getDefaultSchema())).build(),
                ScalarArgumentSpecification.builder().name("PARAMETERS").type(PARAMETERS).defaultValue(emptyParameters()).build()), GENERIC_TABLE,
                "Execute a native Redis Search predicate over a catalog table");
        this.redis = redis;
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments, ConnectorAccessControl accessControl) {
        String schema = argument(arguments, "SCHEMA_NAME");
        String name = argument(arguments, "TABLE_NAME");
        String query = argument(arguments, "QUERY");
        Map<String, String> parameters = parseParameters(((ScalarArgument) arguments.get("PARAMETERS")).getValue());
        if (!parameters.isEmpty() && redis.getConfig().isFlexEnabled()) {
            throw new TrinoException(NOT_SUPPORTED, "Parameterized native search requires cursor-based reads; Flex keyset paging cannot preserve ranked query membership");
        }
        SchemaTableName tableName = new SchemaTableName(schema, name);
        if (!schema.equals(redis.getConfig().getDefaultSchema())) {
            throw new TableNotFoundException(tableName);
        }
        RediSearchTable table = redis.getTable(tableName);
        List<RediSearchColumnHandle> columns = List.copyOf(table.getColumns());
        Set<String> names = columns.stream().map(RediSearchColumnHandle::getName).collect(Collectors.toSet());
        // Table functions receive an injected access control bound to the calling security context.
        accessControl.checkCanSelectFromColumns(null, tableName, Optional.empty(), names);
        if (!accessControl.getRowFilters(null, tableName).isEmpty()
                || !accessControl.getColumnMasks(null, tableName, columns.stream()
                        .map(column -> ColumnSchema.builder(column.toColumnMetadata()).build()).toList()).isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Native search cannot bypass row filters or column masks");
        }
        Descriptor descriptor = Descriptor.descriptor(columns.stream().map(RediSearchColumnHandle::getName).toList(),
                columns.stream().map(RediSearchColumnHandle::getType).toList());
        return TableFunctionAnalysis.builder().returnedType(descriptor)
                .handle(new Handle(RediSearchPlanDiagnostics.annotate(table, redis).withNativeQuery(query, parameters), columns)).build();
    }

    static Map<String, String> parseParameters(Object value) {
        if (!(value instanceof SqlMap map) || !PARAMETERS.equals(map.getMapType())) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "PARAMETERS must be a non-null MAP(VARCHAR, VARBINARY)");
        }
        Map<String, String> parameters = new LinkedHashMap<>();
        for (int i = 0; i < map.getSize(); i++) {
            int position = map.getRawOffset() + i;
            if (map.getRawKeyBlock().isNull(position) || map.getRawValueBlock().isNull(position)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "PARAMETERS keys and values must not be null");
            }
            Slice key = VARCHAR.getSlice(map.getRawKeyBlock(), position);
            String name = key.toStringUtf8();
            if (name.isEmpty() || name.indexOf('\0') >= 0 || !Slices.utf8Slice(name).equals(key)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "PARAMETERS keys must be non-empty UTF-8 names without NUL");
            }
            String encoded = Base64.getEncoder().encodeToString(VARBINARY.getSlice(map.getRawValueBlock(), position).getBytes());
            if (parameters.putIfAbsent(name, encoded) != null) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "PARAMETERS keys must be unique");
            }
        }
        return Map.copyOf(parameters);
    }

    private static String argument(Map<String, Argument> arguments, String name) {
        Object value = ((ScalarArgument) arguments.get(name)).getValue();
        if (!(value instanceof Slice text) || text.toStringUtf8().isBlank()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, name + " must be a non-empty string");
        }
        return text.toStringUtf8();
    }

    public static final class Handle implements ConnectorTableFunctionHandle {
        private final RediSearchTableHandle table;
        private final List<RediSearchColumnHandle> columns;

        @JsonCreator
        public Handle(@JsonProperty("table") RediSearchTableHandle table,
                @JsonProperty("columns") List<RediSearchColumnHandle> columns) {
            this.table = table;
            this.columns = List.copyOf(columns);
        }

        @JsonProperty
        public RediSearchTableHandle getTable() { return table; }

        @JsonProperty
        public List<RediSearchColumnHandle> getColumns() { return columns; }
    }
}
