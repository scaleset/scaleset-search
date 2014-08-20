package com.scaleset.search.es;


import com.scaleset.geo.Feature;
import com.scaleset.geo.FeatureCollection;
import com.scaleset.geo.FeatureCollectionHandler;
import com.scaleset.geo.geojson.GeoJsonParser;
import com.scaleset.search.*;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.*;

import java.io.FileInputStream;

import static org.elasticsearch.client.Requests.createIndexRequest;

public class QueryTest extends Assert {

    private static FeatureCollection earthquakes;
    private static Node node;
    private Client client;
    private ElasticSearchDao<Feature, String> featureDao;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder().put("node.http.enabled", true)
                .put("gateway.type", "none")
                .put("index.store.type", "memory")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .put("path.logs", "target/elasticsearch/logs")
                .put("path.data", "target/elasticsearch/data")
                .build();
        node = NodeBuilder.nodeBuilder().local(true).settings(settings).node();
        earthquakes = earthquakes();
    }

    @AfterClass
    public static void afterClass() {
        node.close();
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
    public void testQuery() throws Exception {
        QueryBuilder queryBuilder = new QueryBuilder("properties.mag:[4.0 TO *]");
        Query query = queryBuilder.build();
        Results<Feature> results = featureDao.search(query);
        assertEquals(16, (long) results.getTotalItems());
        assertNotNull(query);
    }

    @Test
    public void testTermAggregation() throws Exception {
        QueryBuilder queryBuilder = new QueryBuilder();
        queryBuilder.limit(0);
        Aggregation magAgg = new Aggregation("magnitudeType", "term");
        magAgg.property("field", "properties.magnitudeType");
        queryBuilder.aggregation(magAgg);
        Query query = queryBuilder.build();
        Results<Feature> results = featureDao.search(query);
        AggregationResults aggResults = results.getAgg("magnitudeType");
        assertNotNull(aggResults);
    }

    @After
    public void after() throws Exception {
        featureDao.deleteByQuery(new QueryBuilder("*:*").build());
        client.close();
    }

    @Test
    public void sanityTest() {
        assertNotNull(client);
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

}
