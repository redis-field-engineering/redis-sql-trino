package com.redis.calcite;

import com.redis.lettucemod.api.search.IndexInfo;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaRecordType;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link JavaTypeFactory}.
 */
public class JavaTypeFactoryExtImpl extends JavaTypeFactoryImpl {

    /**
     * See <a href="http://stackoverflow.com/questions/16966629/what-is-the-difference-between-getfields-and-getdeclaredfields-in-java-reflectio">
     * the difference between fields and declared fields</a>.
     */
    @Override
    public RelDataType createStructType(Class type) {
        final List<RelDataTypeField> list = new ArrayList<>();
        for (Field field : type.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                // FIXME: watch out for recursion
                final Type fieldType = field.getType();
                list.add(new RelDataTypeFieldImpl(field.getName(), list.size(), createType(fieldType)));
            }
        }
        return canonize(new JavaRecordType(list, type));
    }

    public RelDataType createIndexType(IndexInfo indexInfo) {
        final List<RelDataTypeField> list = new ArrayList<>();
        for (com.redis.lettucemod.api.search.Field field : indexInfo.getFields()) {
            Type fieldType = fieldType(field);
            list.add(new RelDataTypeFieldImpl(field.getName(), list.size(), createType(fieldType)));
        }
        return canonize(new RelRecordType(list));
    }

    private Type fieldType(com.redis.lettucemod.api.search.Field field) {
        if (field.getType() == com.redis.lettucemod.api.search.Field.Type.NUMERIC) {
            return Double.class;
        }
        return String.class;
    }

}
