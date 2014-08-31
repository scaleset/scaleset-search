package com.scaleset.search.mongo;

public class SimpleSchemaMapper implements SchemaMapper {

    private String defaultField;

    public SimpleSchemaMapper(String defaultField) {
        this.defaultField = defaultField;
    }

    @Override
    public String mapField(String field) {
        return field;
    }

    @Override
    public Object mapValue(String field, Object value) {
        return value;
    }

    @Override
    public String defaultField() {
        return defaultField;
    }
}
