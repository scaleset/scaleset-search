package com.scaleset.search.pojo;

import com.scaleset.search.Query;
import com.scaleset.search.QueryBuilder;
import com.scaleset.search.Results;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

public class MapBackedDaoTest {

    private static Map<String, Movie> movies;
    private MapBackedSearchDao<Movie, String> dao;

    @BeforeClass
    public static void loadTestData() throws IOException {
        movies = Movie.load();
    }

    @Before
    public void setup() {
        dao = new MapBackedSearchDao<Movie, String>((m) -> m.getId(), movies);
    }

    @Test
    public void testFindOne() throws Exception {
        Movie movie = dao.findOne("fields.title:Robocop");
        assertNotNull(movie);
        assertTrue(dao.exists(movie.getId()));
    }

    @Test
    public void testSearch() throws Exception {
        Query query = new QueryBuilder().q("fields.genres:Sci-Fi").limit(10).build();
        Results<Movie> results = dao.search(query);
        assertNotNull(results);
        assertNotNull(results.getItems());
        assertEquals(591, (int) results.getTotalItems());
        assertEquals(10, results.getItems().size());
    }
}
