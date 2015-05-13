package com.scaleset.search.mongo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.scaleset.search.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MongoSearchDaoTest extends Assert {

    private MongoSearchDao<Item, String> mongoDao;

    @Before
    public void before() throws Exception {
        DB db = new MongoClient().getDB("test");
        SearchMapping<Item, String> mapping = new ItemSearchMapping();
        mongoDao = new MongoSearchDao<>(db, mapping);
    }

    @After
    public void after() throws Exception {
        mongoDao.close();
    }

    @Test
    public void testSave() throws Exception {
        Item item1 = new Item("1", "value", 1);
        mongoDao.save(item1);
        Item item2 = mongoDao.findById("1");
        assertNotNull(item2);
    }

    @Test
    public void testSearch() throws Exception {
        for (int i = 0; i < 10; ++i) {
            mongoDao.save(new Item("" + i, "_" + i, i));
        }
        Query query = new QueryBuilder().q("number:[1 TO 7]").limit(5).offset(2).sort(new Sort("number", Sort.Direction.Descending)).build();
        Results<Item> results = mongoDao.search(query);
        assertNotNull(results);

        // 7 items match (7,6,5,4,3,2,1)
        assertEquals(7, (int) results.getTotalItems());
        // 5 items returned (5,4,3,2,1)
        assertEquals(5, results.getItems().size());
        // 5 is first since offset = 2
        assertEquals(5, results.getItems().get(0).getNumber());
    }

    @Test
    public void testFilters() throws Exception {
        for (int i = 0; i < 10; ++i) {
            mongoDao.save(new Item("" + i, "_" + i, i));
        }
        QueryBuilder qb = new QueryBuilder("_id: (5 OR 6)");
        Filter f1 = new Filter("5to10", "query");
        f1.put("query", "number:[5 TO 9]");
        Filter f2 = new Filter("_5to_9", "query");
        f2.put("query", "value:[_5 TO _9]");
        qb.filter(f1);
        qb.filter(f2);
        Results<Item> results = mongoDao.search(qb.build());
        assertNotNull(results);
        assertEquals(2, (int) results.getTotalItems());
    }

    static class ItemSearchMapping extends AbstractSearchMapping<Item, String> {
        public ItemSearchMapping() {
            super("items");
            objectMapper(new ObjectMapper().registerModule(new ItemIdModule()));
            schemaMapper(new SimpleSchemaMapper("text").withMapping("number", Integer.class));
            idMapper(obj -> obj.getId());
        }
    }

    static class ItemIdModule extends SimpleModule {

        public ItemIdModule() {
            super("ItemIdModule");
        }

        @Override
        public void setupModule(SetupContext context) {
            context.setMixInAnnotations(Item.class, ItemMixIn.class);
        }
    }

    static class Item {

        private String id;
        private String value;
        private int number;

        public Item() {
        }

        public Item(String id, String value, int number) {
            this.id = id;
            this.value = value;
            this.number = number;
        }

        public String getId() {
            return id;
        }

        public int getNumber() {
            return number;
        }

        public String getValue() {
            return value;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setNumber(int number) {
            this.number = number;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    static class ItemMixIn {
        @JsonProperty("_id")
        private String id;
    }
}
