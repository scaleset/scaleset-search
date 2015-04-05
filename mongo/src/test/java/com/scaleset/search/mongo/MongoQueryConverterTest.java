package com.scaleset.search.mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DBObject;
import org.junit.Assert;
import org.junit.Test;

public class MongoQueryConverterTest extends Assert {

    @Test
    public void testTermQuery() {
        MongoQueryConverter mapper = new MongoQueryConverter(new SimpleSchemaMapper("text"));
        DBObject result = mapper.convertQ("field:value");
        assertNotNull(result);
        assertEquals("value", result.get("field"));
    }

    @Test
    public void testTermQueryWithReplacement() {
        MongoQueryConverter mapper = new MongoQueryConverter(new SimpleSchemaMapper("text"), "value");
        DBObject result = mapper.convertQ("field:#");
        assertNotNull(result);
        assertEquals("value", result.get("field"));

        mapper = new MongoQueryConverter(new SimpleSchemaMapper("text").withMapping("field", Integer.class), 1);
        result = mapper.convertQ("field:#");
        assertEquals(1, result.get("field"));
    }

    @Test
    public void testBooleanQueryAnd() {
        MongoQueryConverter mapper = new MongoQueryConverter(new SimpleSchemaMapper("text"));
        DBObject result = mapper.convertQ("fieldA:a AND fieldB:b");
        assertNotNull(result);
        // could you do this w/o "$and" at the top-level?
        assertNotNull(result.get("$and"));
    }

    @Test
    public void testProhibitBoolean() throws Exception {
        MongoQueryConverter mapper = new MongoQueryConverter(new SimpleSchemaMapper("text"));
        DBObject result = mapper.convertQ("-(Fred Flinstone)");
        assertNotNull(result);
        String q = new ObjectMapper().writeValueAsString(result);
        assertEquals("{\"$and\":[{\"text\":{\"$ne\":\"Fred\"}},{\"text\":{\"$ne\":\"Flinstone\"}}]}", q);
    }
}
