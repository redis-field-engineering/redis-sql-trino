package com.redis.calcite;

import com.redis.lettucemod.api.search.Document;
import com.redis.lettucemod.api.search.Field;
import com.redis.lettucemod.api.search.SearchResults;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Enumerator that reads from a RediSearch Regions.
 */
@Slf4j
class RediSearchEnumerator implements Enumerator<Object> {

    private final Map<String, Field.Type> indexFields;
    private final Iterator<Document<String, String>> iterator;
    private final List<RelDataTypeField> fieldTypes;
    private Document<String, String> current;

    /**
     * Creates a RediSearchEnumerator.
     *
     * @param results      RediSearch search results ({@link SearchResults})
     * @param protoRowType The type of resulting rows
     */
    RediSearchEnumerator(Map<String, Field.Type> indexFields, SearchResults<String, String> results, RelProtoDataType protoRowType) {
        if (results == null) {
            log.warn("Null RediSearch results!");
        }
        this.indexFields = indexFields;
        this.iterator = (results == null) ? Collections.emptyIterator() : results.iterator();
        this.current = null;
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        this.fieldTypes = protoRowType.apply(typeFactory).getFieldList();
    }

    /**
     * Produces the next row from the results.
     *
     * @return A rel row from the results
     */
    @Override
    public Object current() {
        return RediSearchUtils.convertToRowValues(fieldTypes, indexFields, current);
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
