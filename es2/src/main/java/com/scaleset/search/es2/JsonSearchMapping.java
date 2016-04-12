package com.scaleset.search.es2;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonSearchMapping extends AbstractSearchMapping<ObjectNode, String> {

    private String idField = "id";

    public JsonSearchMapping(String defaultIndex, String defaultType) {
        super(ObjectNode.class, defaultIndex, defaultType);
    }

    @Override
    public String id(ObjectNode obj) throws Exception {
        String result = obj.get(idField).asText();
        return result;
    }

    @Override
    public String idForKey(String key) throws Exception {
        return key;
    }
}
