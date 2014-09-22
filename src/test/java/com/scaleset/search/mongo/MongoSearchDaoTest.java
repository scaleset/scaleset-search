package com.scaleset.search.mongo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.jongo.marshall.jackson.oid.Id;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

public class MongoSearchDaoTest extends Assert {

    private MongoSearchDao<Item, String> mongoDao;

    @Before
    public void before() throws Exception {
        DB db = new MongoClient().getDB("test");
        mongoDao = new MongoSearchDao<>(db, "test", Item.class, new ItemIdModule());
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
