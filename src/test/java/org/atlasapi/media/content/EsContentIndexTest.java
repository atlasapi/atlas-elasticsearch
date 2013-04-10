package org.atlasapi.media.content;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.content.criteria.AttributeQuery;
import org.atlasapi.content.criteria.AttributeQuerySet;
import org.atlasapi.content.criteria.attribute.Attributes;
import org.atlasapi.content.criteria.operator.Operators;
import org.atlasapi.media.EsSchema;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.TopicRef.Relationship;
import org.atlasapi.media.util.ElasticSearchHelper;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.SystemClock;

public class EsContentIndexTest {

    private final Node esClient = ElasticSearchHelper.testNode();
    
    private final EsContentIndexer indexer = new EsContentIndexer(esClient, EsSchema.CONTENT_INDEX, new SystemClock(), 60000);
    private final EsContentIndex index = new EsContentIndex(esClient, EsSchema.CONTENT_INDEX);

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
        ElasticSearchHelper.clearIndices(esClient);
        esClient.close();
    }
    
    @Test
    public void testTopicQuery() throws Exception {
        Content content = new Episode();
        content.setId(1);
        content.setPublisher(Publisher.METABROADCAST);
        content.setTopicRefs(ImmutableList.of(new TopicRef(2L, 1.0f, true, Relationship.ABOUT)));
        indexer.index(content);
        
        ElasticSearchHelper.refresh(esClient);
        
        AttributeQuery<Id> query = Attributes.TOPIC_ID.createQuery(Operators.EQUALS, ImmutableList.of(Id.valueOf(2)));
        
        AttributeQuerySet querySet = new AttributeQuerySet(ImmutableList.of(query));
        ListenableFuture<FluentIterable<Id>> result = index.query(querySet, ImmutableList.of(Publisher.METABROADCAST), Selection.all());
        
        FluentIterable<Id> ids = result.get(1, TimeUnit.SECONDS);
        assertThat(ids.first().get(), is(Id.valueOf(1)));
    }

    @Test
    public void testTopicWeightingQuery() throws Exception {
        Content content = new Episode();
        content.setId(1);
        content.setPublisher(Publisher.METABROADCAST);
        content.setTopicRefs(ImmutableList.of(new TopicRef(2L, 1.0f, true, Relationship.ABOUT)));
        
        indexer.index(content);
        ElasticSearchHelper.refresh(esClient);
        
        AttributeQuery<Float> query = Attributes.TOPIC_WEIGHTING.createQuery(
                Operators.EQUALS, ImmutableList.of(1.0f));
        
        FluentIterable<Id> ids = index.query(
                new AttributeQuerySet(ImmutableList.of(query)), 
                ImmutableList.of(Publisher.METABROADCAST), Selection.all())
            .get(1, TimeUnit.SECONDS);
        assertThat(ids.first().get(), is(Id.valueOf(1)));
        
        query = Attributes.TOPIC_WEIGHTING.createQuery(
                Operators.LESS_THAN, ImmutableList.of(0.5f));

        ids = index.query(
                new AttributeQuerySet(ImmutableList.of(query)), 
                ImmutableList.of(Publisher.METABROADCAST), Selection.all())
                .get(1, TimeUnit.SECONDS);
        assertThat(ids.first().isPresent(), is(false));

        query = Attributes.TOPIC_WEIGHTING.createQuery(
                Operators.GREATER_THAN, ImmutableList.of(0.5f));
        
        ids = index.query(
                new AttributeQuerySet(ImmutableList.of(query)), 
                ImmutableList.of(Publisher.METABROADCAST), Selection.all())
                .get(1, TimeUnit.SECONDS);
        assertThat(ids.first().get(), is(Id.valueOf(1)));
        
    }
    
}
