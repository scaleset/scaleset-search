package com.scaleset.search.pojo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.mvel2.integration.PropertyHandler;
import org.mvel2.integration.VariableResolverFactory;

import java.util.List;

public class JsonNodePropertyHandler implements PropertyHandler {

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Object getProperty(String s, Object o, VariableResolverFactory variableResolverFactory) {
        if (o instanceof JsonNode) {
            JsonNode result = ((ObjectNode) o).get(s);
            if (result == null) {
                return null;
            } else if (result.isTextual()) {
                return result.textValue();
            } else if (result.isInt()) {
                return result.asInt();
            } else if (result.isDouble()) {
                return result.doubleValue();
            } else if (result.isFloat()) {
                return result.floatValue();
            } else if (result.isLong()) {
                return result.longValue();
            } else if (result.isArray()) {
                try {
                    return objectMapper.treeToValue(result, List.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            } else {
                return result;
            }
        }
        return null;
    }

    @Override
    public Object setProperty(String s, Object o, VariableResolverFactory variableResolverFactory, Object o1) {
        return null;
    }
}
