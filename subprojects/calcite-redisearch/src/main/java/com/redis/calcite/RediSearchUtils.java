package com.redis.calcite;

import com.redis.lettucemod.api.search.Document;
import com.redis.lettucemod.api.search.Field;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Utilities for the RediSearch adapter.
 */
public class RediSearchUtils {

    private static final JavaTypeFactoryExtImpl JAVA_TYPE_FACTORY = new JavaTypeFactoryExtImpl();

    private RediSearchUtils() {
    }

    /**
     * Converts a RediSearch object into a Row tuple.
     *
     * @param relDataTypeFields Table relation types
     * @return List of objects values corresponding to the relDataTypeFields
     */
    public static Object convertToRowValues(List<RelDataTypeField> relDataTypeFields, Map<String, Field.Type> fieldTypes, Document<String, String> doc) {
        Object[] values = new Object[relDataTypeFields.size()];
        int index = 0;
        for (RelDataTypeField relDataTypeField : relDataTypeFields) {
            Type javaType = JAVA_TYPE_FACTORY.getJavaClass(relDataTypeField.getType());
            String rawValue = doc.get(relDataTypeField.getName());
            values[index++] = convert(rawValue, fieldTypes.get(relDataTypeField.getName()), (Class<?>) javaType);
        }

        if (values.length == 1) {
            return values[0];
        }

        return values;
    }

    @SuppressWarnings("JavaUtilDate")
    private static Object convert(String s, Field.Type fieldType, Class<?> clazz) {
        if (s == null) {
            return null;
        }
        Primitive primitive = Primitive.of(clazz);
        if (primitive != null) {
            clazz = primitive.boxClass;
        } else {
            primitive = Primitive.ofBox(clazz);
        }
        if (clazz == null) {
            return s;
        }
        if (clazz.isInstance(s)) {
            return s;
        }
        if (fieldType == Field.Type.NUMERIC && primitive != null) {
            return primitive.number(Double.parseDouble(s));
        }
        return s;
    }


}
