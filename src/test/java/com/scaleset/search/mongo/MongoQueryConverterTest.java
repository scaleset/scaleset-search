package com.scaleset.search.mongo;

import com.scaleset.search.Query;
import com.scaleset.search.QueryBuilder;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.jongo.Jongo;
import org.jongo.Mapper;
import org.jongo.MongoCollection;
import org.jongo.marshall.jackson.JacksonMapper;
import org.jongo.query.QueryFactory;
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

        mapper = new MongoQueryConverter(new SimpleSchemaMapper("text"), 1);
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
    public void testComplex() throws Exception {
        Query query = new QueryBuilder().offset(0).limit(10).build();
        MongoClient mongoClient = new MongoClient("localhost");
        DB db = new MongoClient().getDB("repo");
        QueryFactory qf = new LuceneJongoQueryFactory();
        Mapper mapper = new JacksonMapper.Builder().withQueryFactory(qf).build();
        Jongo jongo = new Jongo(db, mapper);
        MongoCollection entities = jongo.getCollection("entities");
        assertEquals(1764, entities.count("organization: 53610e433004a0294b93ce96"));
        assertEquals(1, entities.count("deviceId: 8011159"));
    }

}
