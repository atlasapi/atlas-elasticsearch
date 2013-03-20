package org.atlasapi.media.topic;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.media.EsSchema;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.EsContentIndexer;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.TopicRef.Relationship;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.util.Resolved;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.SystemClock;

public class EsPopularTopicsIndexTest {

    private final Node esClient = NodeBuilder.nodeBuilder()
        .local(true).clusterName(UUID.randomUUID().toString())
        .build().start();
    private final EsContentIndexer indexer = new EsContentIndexer(esClient, new SystemClock(), 60000);

    @BeforeClass
    public static void before() throws Exception {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
            new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
    }

    @Before
    public void setup() {
        indexer.startAndWait();
    }
    
    @After
    public void after() throws Exception {
        esClient.client().admin().indices()
            .delete(Requests.deleteIndexRequest(EsSchema.INDEX_NAME)).get();
        esClient.close();
    }

    @Test
    public void testPopularTopics() throws Exception {
        Broadcast broadcast1 = new Broadcast("MB", new DateTime(), new DateTime().plusHours(1));
        Version version1 = new Version();
        Broadcast broadcast2 = new Broadcast("MB", new DateTime().plusHours(2), new DateTime().plusHours(3));
        Version version2 = new Version();
        version1.addBroadcast(broadcast1);
        version2.addBroadcast(broadcast2);
        
        TopicRef topic1 = new TopicRef(Id.valueOf(1), 1.0f, Boolean.TRUE, Relationship.ABOUT);
        TopicRef topic2 = new TopicRef(Id.valueOf(2), 1.0f, Boolean.TRUE, Relationship.ABOUT);
        
        Item item1 = new Item("uri1", "curie1", Publisher.METABROADCAST);
        item1.addVersion(version1);
        item1.setId(Id.valueOf(1));
        item1.addTopicRef(topic1);
        Item item2 = new Item("uri2", "curie2", Publisher.METABROADCAST);
        item2.addVersion(version1);
        item2.setId(Id.valueOf(2));
        item2.addTopicRef(topic1);
        Item item3 = new Item("uri3", "curie3", Publisher.METABROADCAST);
        item3.addVersion(version1);
        item3.setId(Id.valueOf(3));
        item3.addTopicRef(topic1);
        item3.addTopicRef(topic2);
        Item item4 = new Item("uri4", "curie4", Publisher.METABROADCAST);
        item4.addVersion(version2);
        item4.setId(Id.valueOf(4));
        item4.addTopicRef(topic2);
        Item item5 = new Item("uri5", "curie5", Publisher.METABROADCAST);
        item5.addVersion(version2);
        item5.setId(Id.valueOf(5));
        item5.addTopicRef(topic2);
        
        indexer.index(item1);
        indexer.index(item2);
        indexer.index(item3);
        indexer.index(item4);
        indexer.index(item5);
        
        while(count() < 5) {
            Thread.sleep(50);
        }
        
        TopicResolver resolver = mock(TopicResolver.class);
        when(resolver.resolveIds(argThat(hasItems(Id.valueOf(1),Id.valueOf(2)))))
            .thenReturn(Resolved.valueOf(ImmutableList.of(new Topic(Id.valueOf(1)),new Topic(Id.valueOf(2)))));
        
        PopularTopicIndex searcher = new EsPopularTopicIndex(esClient);
        
        Interval interval = new Interval(new DateTime().minusHours(1), new DateTime().plusHours(1));
        
        FluentIterable<Id> topicIds = queryIndex(searcher, interval, Selection.offsetBy(0).withLimit(Integer.MAX_VALUE));
        
        assertEquals(2, topicIds.size());
        assertEquals(1l, topicIds.get(0).longValue());
        assertEquals(2l, topicIds.get(1).longValue());
        
        topicIds = queryIndex(searcher, interval, Selection.offsetBy(0).withLimit(1));
        assertEquals(1, topicIds.size());
        assertEquals(1, topicIds.get(0).longValue());
        
        topicIds = queryIndex(searcher, interval, Selection.offsetBy(1).withLimit(1));
        assertEquals(1, topicIds.size());
        assertEquals(2, topicIds.get(0).longValue());
    }

    private FluentIterable<Id> queryIndex(PopularTopicIndex searcher, Interval interval,
                                          Selection selection) {
        ListenableFuture<FluentIterable<Id>> futureIds = searcher.popularTopics(interval, selection);
        FluentIterable<Id> topicIds = Futures.getUnchecked(futureIds);
        return topicIds;
    }

    private long count() throws Exception {
        return esClient.client().count(Requests.countRequest(EsSchema.INDEX_NAME)).get().getCount();
    }
}
