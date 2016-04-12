package com.scaleset.search.es2;


import com.scaleset.geo.Feature;
import com.scaleset.geo.FeatureCollection;
import com.scaleset.geo.FeatureCollectionHandler;
import com.scaleset.geo.geojson.GeoJsonParser;
import com.scaleset.search.*;
import com.scaleset.utils.Coerce;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.*;

public class QueryTest extends Assert {

    private static FeatureCollection earthquakes;
    private static Node node;
    private Client client;
    private ElasticSearchDao<Feature, String> featureDao;

    @AfterClass
    public static void afterClass() {
        node.close();
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        Settings settings = Settings.builder()
                .put("node.http.enabled", true)
                //.put("index.store.type", "memory")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .put("path.logs", "target/elasticsearch/logs")
                .put("path.data", "target/elasticsearch/data")
                .put("path.home", "target/elasticsearch")
                .build();
        node = NodeBuilder.nodeBuilder().local(true).settings(settings).node();
        earthquakes = earthquakes();
    }

    private static FeatureCollection earthquakes() throws Exception {
        FeatureCollectionHandler handler = new FeatureCollectionHandler();
        GeoJsonParser parser = new GeoJsonParser();
        parser.handler(handler);
        parser.parse(QueryTest.class.getResourceAsStream("/earthquakes_2.5_day.geojson"));
        FeatureCollection result = handler.getCollection();
        assertEquals(46, result.getFeatures().size());
        return result;
    }

    @After
    public void after() throws Exception {
        featureDao.deleteIndex("features");
        client.close();
    }

    @Before
    public void before() throws Exception {
        client = node.client();
        SearchMapping mapping = new FeatureSearchMapping();
        featureDao = new ElasticSearchDao(client, mapping);
        featureDao.createIndex("features");
        for (Feature feature : earthquakes.getFeatures()) {
            featureDao.save(feature);
        }
        featureDao.flush();
    }

    @Test
    public void sanityTest() {
        assertNotNull(client);
    }

    @Test
    public void testQuery() throws Exception {
        QueryBuilder queryBuilder = new QueryBuilder("properties.mag:[4.0 TO *]");
        Query query = queryBuilder.build();
        Results<Feature> results = featureDao.search(query);
        assertEquals(16, (long) results.getTotalItems());
        assertNotNull(results.getHeaders().get("took"));
        assertNotNull(query);
    }

    @Test
    public void testProjection() throws Exception {
        QueryBuilder queryBuilder = new QueryBuilder().field("id", "properties.time");
        Query query = queryBuilder.build();
        Results<Feature> results = featureDao.search(query);
        assertEquals(46, (long) results.getTotalItems());
    }

    @Test
    public void testScrolling() throws Exception {
        QueryBuilder queryBuilder = new QueryBuilder().limit(1).header("keepAlive", "1m");
        Query query = queryBuilder.build();
        Results<Feature> results = featureDao.search(query);
        int retrieved = 0;
        while (results.getItems().size() > 0) {
            retrieved += results.getItems().size();
            String scrollId = Coerce.toString(results.getHeaders().get("scrollId"));
            results = featureDao.scroll(scrollId, "60s", 1);
            System.out.println("returned: " + results.getItems().size());
        }
        assertEquals(46, (long) results.getTotalItems());
        assertEquals(46, (long) retrieved);
    }

    @Test
    public void testTermAggregation() throws Exception {
        QueryBuilder queryBuilder = new QueryBuilder();
        queryBuilder.limit(0);
        Aggregation magAgg = new Aggregation("magnitudeType", "terms");
        magAgg.put("field", "properties.magnitudeType");
        queryBuilder.aggregation(magAgg);
        Aggregation statsAgg = new Aggregation("magStats", "stats");
        statsAgg.put("field", "properties.mag");
        queryBuilder.aggregation(statsAgg);
        Query query = queryBuilder.build();
        Results<Feature> results = featureDao.search(query);
        AggregationResults aggResults = results.getAgg("magnitudeType");
        assertNotNull(aggResults);
        AggregationResults statsResults = results.getAgg("magStats");
        Stats stats = statsResults.getStats();
        assertNotNull(stats);
        assertTrue(stats.getMax() > 0);
        assertNotNull(statsResults);
    }

}
