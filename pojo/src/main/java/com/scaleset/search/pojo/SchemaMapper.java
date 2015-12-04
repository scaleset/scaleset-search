package com.scaleset.search.pojo;

import java.util.List;

public interface SchemaMapper {

    List<String> mapField(String field);

    Object mapValue(String field, Object value);

    String defaultField();
}
