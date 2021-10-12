package org.apache.calcite.adapter.redisearch;

import com.redis.lettucemod.api.search.Document;
import com.redis.lettucemod.api.search.SearchResults;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Enumerator that reads from a RediSearch Regions.
 */
@Slf4j
class RediSearchEnumerator implements Enumerator<Object> {

    private final Iterator<Document<String, String>> iterator;
    private final List<RelDataTypeField> fields;
    private Document<String, String> current;

    /**
     * Creates a RediSearchEnumerator.
     *
     * @param results      RediSearch search results ({@link SearchResults})
     * @param protoRowType The type of resulting rows
     */
    RediSearchEnumerator(SearchResults<String, String> results, RelProtoDataType protoRowType) {
        if (results == null) {
            log.warn("Null RediSearch results!");
        }
        this.iterator = (results == null) ? Collections.emptyIterator() : results.iterator();
        this.current = null;
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        this.fields = protoRowType.apply(typeFactory).getFieldList();
    }

    /**
     * Produces the next row from the results.
     *
     * @return A rel row from the results
     */
    @Override
    public Object current() {
        if (fields.size() == 1) {
            // If we just have one field, produce it directly
            RelDataTypeField field = fields.get(0);
            return currentRowField(field.getName(), field.getType().getSqlTypeName());
        } else {
            // Build an array with all fields in this row
            Object[] row = new Object[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                RelDataTypeField field = fields.get(i);
                row[i] = currentRowField(field.getName(), field.getType().getSqlTypeName());
            }
            return row;
        }
    }

    /** Get a field for the current row from the underlying object.
     *
     * @param name Name of the field within the Row object
     * @param type Type of the field in this row
     */
    private Object currentRowField(String name, SqlTypeName type) {
        String fieldValue = current.get(name);
        if (fieldValue == null) {
            return null;
        }
        if (type.equals(SqlTypeName.DOUBLE)) {
            return Double.parseDouble(fieldValue);
        }
        return fieldValue;
    }

    @Override
    public boolean moveNext() {
        if (iterator.hasNext()) {
            current = iterator.next();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        // Nothing to do here
    }
}
