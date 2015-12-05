package com.scaleset.search.pojo;

import org.junit.Test;

import java.util.function.Predicate;

import static org.junit.Assert.*;

public class QueryConverterTest {

    private SchemaMapper schema = new SimpleSchemaMapper("text");
    private Person fred = new Person("Fred", "Flinstone", 35);
    private Person barn = new Person("Barney", "Rubble", 27);

    @Test
    public void testTermQuery() {
        LuceneExpressionConverter mapper = new LuceneExpressionConverter(schema);
        Predicate result = mapper.convertQ("firstName: Fred");
        assertNotNull(result);
        assertTrue(result.test(fred));
        assertFalse(result.test(barn));
    }

    @Test
    public void testBooleanAndQuery() {
        LuceneExpressionConverter mapper = new LuceneExpressionConverter(schema);
        Predicate result = mapper.convertQ("firstName:fred AND lastName:flinstone");
        assertNotNull(result);
        assertTrue(result.test(fred));
        assertFalse(result.test(barn));
    }

    @Test
    public void testBooleanOrQuery() {
        LuceneExpressionConverter mapper = new LuceneExpressionConverter(schema);
        Predicate result = mapper.convertQ("firstName:(fred barney)");
        assertNotNull(result);
        assertTrue(result.test(fred));
        assertTrue(result.test(barn));
    }

    static class Person {
        private String firstName;
        private String lastName;
        private int age;

        public Person() {
        }

        public Person(String firstName, String lastName, int age) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }

        public String getFirstName() {
            return firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }
    }
}
