package com.scaleset.search.pojo;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MovieQueryTest {

    private SchemaMapper schema = new SimpleSchemaMapper("text");

    private static Map<String, Movie> movies;

    @BeforeClass
    public static void loadTestData() throws IOException {
        movies = Movie.load();
    }

    @Test
    public void testTitleTerm() {
        List<Movie> results = find("fields.title:gatsby");
        assertEquals(2, results.size());
    }

    @Test
    public void testTitleWildcard() {
        List<Movie> results = find("fields.title:robocop*");
        assertEquals(4, results.size());
    }

    @Test
    public void testTitleProhibitedTerm() {
        List<Movie> results = find("fields.title:*zen* AND (-fields.title:Frozen)");
        assertEquals(7, results.size());
    }

    @Test
    public void testTitleProhibitedWildcard() {
        List<Movie> results = find("(fields.title:*zen*) AND -(fields.title:fro*)");
        // no tokenization on values, so "The Frozen Groun" doesn't match the prefix query
        assertEquals(8, results.size());
    }

    @Test
    public void testTitleRange() {
        List<Movie> results = find("fields.title:[1 TO 2}");
        assertEquals(21, results.size());
    }

    @Test
    public void testTitlePrefix() {
        List<Movie> results = find("fields.title:Frozen*");
        assertEquals(3, results.size());
    }

    @Test
    public void testRankTerm() {
        List<Movie> results = find("fields.rank:18");
        assertEquals(1, results.size());
        assertEquals("The Great Gatsby", results.get(0).getFields().get("title"));
    }

    @Test
    public void testRankRange() {
        assertEquals(100, find("fields.rank:[1 TO 100]").size());
        assertEquals(100, find("fields.rank:[* TO 100]").size());
        assertEquals(98, find("fields.rank:{1 TO 100}").size());
        assertEquals(99, find("fields.rank:{* TO 100}").size());
    }

    @Test
    public void testGenreTerm() {
        assertEquals(591, find("fields.genres:Sci-Fi").size());
    }

    @Test
    public void testGenrePrefix() {
        assertEquals(1595, find("fields.genres:A*").size());
    }

    protected List<Movie> find(String query) {
        LuceneExpressionConverter mapper = new LuceneExpressionConverter(schema);
        Predicate<Movie> predicate = mapper.convertQ(query);
        assertNotNull("Unable to parse query", predicate);
        return movies.values().stream().filter(predicate).collect(Collectors.toList());
    }

}
