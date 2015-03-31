package com.scaleset.search.mongo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import org.junit.Assert;
import org.junit.Test;

public class JacksonMongoTest extends Assert {

    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testToBean() {
        DBObject object = new BasicDBObject("_id", "value");
        Bean bean = mapper.convertValue(object, Bean.class);
        assertNotNull(bean);
    }

    @Test
    public void testToDBObject() {
        Bean bean = new Bean();
        bean.setId("value");
        DBObject object = mapper.convertValue(bean, BasicDBObject.class);
        assertNotNull(object);
    }

    @Test
    public void testBeanInsideBean() throws Exception {
        Bean outer = new Bean();
        Bean inner = new Bean();
        outer.setBean(inner);
        outer.setId("out");
        inner.setId("inner");
        DBObject object = mapper.convertValue(outer, BasicDBObject.class);
        assertNotNull(object);

    }

    @Test
    public void testWriteToMongo() throws Exception {
        DB db = new MongoClient().getDB("test");
        DBCollection collection = db.getCollection("beans");
        Bean outer = new Bean();
        Bean inner = new Bean();
        outer.setBean(inner);
        outer.setId("outer");
        inner.setId("inner");
        DBObject object = mapper.convertValue(outer, BasicDBObject.class);
        collection.save(object);

        object = collection.findOne(new BasicDBObject("_id", "outer"));
        Bean bean = mapper.convertValue(object, Bean.class);
        assertNotNull(bean);
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Bean {

        @JsonProperty("_id")
        private String id;
        private Bean bean;

        public Bean() {
        }

        public Bean getBean() {
            return bean;
        }

        public String getId() {
            return id;
        }

        public void setBean(Bean bean) {
            this.bean = bean;
        }

        public void setId(String id) {
            this.id = id;
        }

    }

}
