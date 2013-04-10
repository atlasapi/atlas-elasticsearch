package org.atlasapi.media.content;

import static org.atlasapi.media.util.ElasticSearchHelper.refresh;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.media.EsSchema;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.util.ElasticSearchHelper;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResults;
import org.elasticsearch.node.Node;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.SystemClock;

@Ignore("Fix NPE in Strings.tokenise")
public class EsContentSearcherTest {

    private final Node esClient = ElasticSearchHelper.testNode();

    @BeforeClass
    public static void before() throws Exception {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
            new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
    }

    @After
    public void after() throws Exception {
        ElasticSearchHelper.clearIndices(esClient);
        esClient.close();
    }
    
    @Test
    public void testSearch() throws Exception {
        Broadcast broadcast1 = new Broadcast("MB", new DateTime(), new DateTime().plusHours(1));
        Version version1 = new Version();
        Broadcast broadcast2 = new Broadcast("MB", new DateTime().plusHours(3), new DateTime().plusHours(4));
        Version version2 = new Version();
        version1.addBroadcast(broadcast1);
        version2.addBroadcast(broadcast2);

        Item item1 = new Item("uri1", "curie1", Publisher.METABROADCAST);
        item1.setTitle("title1");
        item1.setId(Id.valueOf(1));
        item1.addVersion(version1);
        item1.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        Item item2 = new Item("uri2", "curie2", Publisher.METABROADCAST);
        item2.setTitle("title2");
        item2.setId(Id.valueOf(2));
        item2.addVersion(version1);
        item2.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        Item item3 = new Item("uri3", "curie3", Publisher.METABROADCAST);
        item3.setTitle("pippo");
        item3.setId(Id.valueOf(3));
        item3.addVersion(version2);
        item3.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        Item item4 = new Item("uri4", "curie4", Publisher.METABROADCAST);
        item4.setTitle("title4");
        item4.setId(Id.valueOf(4));
        item4.addVersion(version2);
        item4.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        Brand brand1 = new Brand("buri1", "buri1", Publisher.METABROADCAST);
        brand1.setTitle("title");
        brand1.setId(Id.valueOf(5));
        brand1.setChildRefs(Arrays.asList(item1.childRef(), item2.childRef()));
        Brand brand2 = new Brand("buri2", "buri2", Publisher.METABROADCAST);
        brand2.setTitle("b");
        brand2.setId(Id.valueOf(6));
        brand2.setChildRefs(Arrays.asList(item3.childRef()));

        item1.setParentRef(ParentRef.parentRefFrom(brand1));
        item2.setParentRef(ParentRef.parentRefFrom(brand1));
        item3.setParentRef(ParentRef.parentRefFrom(brand2));

        EsContentIndexer contentIndexer = new EsContentIndexer(esClient, EsSchema.CONTENT_INDEX, new SystemClock(), 60000);
        contentIndexer.startAndWait();

        EsContentSearcher contentSearcher = new EsContentSearcher(esClient);

        contentIndexer.index(brand1);
        contentIndexer.index(brand2);
        contentIndexer.index(item1);
        contentIndexer.index(item2);
        contentIndexer.index(item3);
        contentIndexer.index(item4);

        refresh(esClient);

        SearchQuery query = SearchQuery.builder("title")
            .withSelection(Selection.offsetBy(0))
            .withSpecializations(ImmutableSet.<Specialization>of())
            .withPublishers(ImmutableSet.<Publisher>of())
            .withTitleWeighting(1)
            .withBroadcastWeighting(0)
            .withCatchupWeighting(0)
            .withPriorityChannelWeighting(0)
            .build();
            
        ListenableFuture<SearchResults> future = contentSearcher.search(query);
        
        SearchResults results = future.get();
        assertEquals(2, results.getIds().size());
        assertEquals(brand1.getId(), results.getIds().get(0));
        assertEquals(item4.getId(), results.getIds().get(1));
    }
}
