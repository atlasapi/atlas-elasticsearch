package org.atlasapi.media.topic;

import static org.atlasapi.media.topic.EsTopic.ALIASES;
import static org.atlasapi.media.topic.EsTopic.DESCRIPTION;
import static org.atlasapi.media.topic.EsTopic.ID;
import static org.atlasapi.media.topic.EsTopic.SOURCE;
import static org.atlasapi.media.topic.EsTopic.TITLE;
import static org.atlasapi.media.util.ElasticSearchHelper.refresh;
import static org.atlasapi.media.util.EsAlias.VALUE;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.content.criteria.AttributeQuerySet;
import org.atlasapi.content.criteria.attribute.Attributes;
import org.atlasapi.content.criteria.operator.Operators;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.query.Selection;

public class EsTopicIndexTest {

    private final Node esClient = NodeBuilder.nodeBuilder()
        .local(true).clusterName(UUID.randomUUID().toString())
        .build().start();
    private final String indexName = "topics";
    private final EsTopicIndex index = new EsTopicIndex(esClient, indexName, 60, TimeUnit.SECONDS);
    
    @BeforeClass
    public static void before() throws Exception {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
            new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
    }
    
    @Before
    public void setup() throws Exception {
        index.startAndWait();
    }
    
    @After
    public void after() throws Exception {
        esClient.client().admin().indices()
            .delete(Requests.deleteIndexRequest(indexName)).get();
        esClient.close();
    }
    
    @Test
    public void testIndexesAndRetrievesTopic() throws Exception {
        Topic topic = topic(1234, Publisher.DBPEDIA, "title", "description", "an:Alias");
        
        index.index(topic);
        
        refresh(esClient);
        
        GetResponse got = esClient.client().get(
            Requests.getRequest(indexName).id("1234")
        ).actionGet(10000);
        
        Map<String, Object> source = got.getSource();
        assertEquals(1234, source.get(ID));
        assertEquals("dbpedia.org", source.get(SOURCE));
        assertEquals("title", source.get(TITLE));
        assertEquals("description", source.get(DESCRIPTION));
        
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> aliases = (List<Map<String, Object>>) source.get(ALIASES);
        assertEquals("an:Alias", Iterables.getOnlyElement(aliases).get(VALUE));
    }

    @Test
    public void testIdOnlyQuery() throws Exception {
        Topic topic = topic(1234, Publisher.DBPEDIA, "title", "description", "an:Alias");
        
        index.index(topic);
        refresh(esClient);
        
        AttributeQuerySet query = new AttributeQuerySet(ImmutableList.of(
            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(topic.getId()))
        ));
        Iterable<Id> ids = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), Selection.ALL));
        assertThat(Iterables.getOnlyElement(ids),is(topic.getId()));
    }

    @Test
    public void testConjunctiveQuery() throws Exception {
        Topic topic = topic(1234, Publisher.DBPEDIA, "title", "description", "an:Alias");
        
        index.index(topic);
        refresh(esClient);
        
        AttributeQuerySet query = new AttributeQuerySet(ImmutableList.of(
            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(topic.getId())),
            Attributes.ALIASES_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("an:Alias"))
        ));
        Iterable<Id> ids = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), Selection.ALL));
        assertThat(Iterables.getOnlyElement(ids),is(topic.getId()));
    }

    @Test
    public void testPublisherFilter() throws Exception {
        Topic topic = topic(1234, Publisher.METABROADCAST, "title", "description", "an:Alias");
        
        index.index(topic);
        refresh(esClient);
        
        AttributeQuerySet query = new AttributeQuerySet(ImmutableList.of(
            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(topic.getId()))
        ));
        Iterable<Id> ids = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), Selection.ALL));
        assertThat(Iterables.isEmpty(ids), is(true));
    }

    @Test
    public void testSelection() throws Exception {
        Topic topic1 = topic(1234, Publisher.DBPEDIA, "title1", "description1", "an:Alias");
        Topic topic2 = topic(1235, Publisher.DBPEDIA, "title2", "description2", "an:Alias");
        
        index.index(topic2);
        index.index(topic1);
        refresh(esClient);
        
        AttributeQuerySet query = new AttributeQuerySet(ImmutableList.of(
            Attributes.ALIASES_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("an:Alias"))
        ));
        Iterable<Id> ids = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), 
            Selection.ALL));
        assertThat(Iterables.size(ids), is(2));

        ids = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), 
            new Selection(0,1)));
        assertThat(Iterables.getOnlyElement(ids), is(topic1.getId()));

        ids = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), 
            new Selection(1,1)));
        assertThat(Iterables.getOnlyElement(ids), is(topic2.getId()));

        ids = Futures.getUnchecked(index.query(query, ImmutableList.of(Publisher.DBPEDIA), 
            new Selection(1,5)));
        assertThat(Iterables.getOnlyElement(ids), is(topic2.getId()));
    }

    private Topic topic(int id, Publisher source, String title, String description, String... aliases) {
        Topic topic = new Topic();
        topic.setId(id);
        topic.setPublisher(source);
        topic.addAliases(ImmutableList.copyOf(aliases));
        topic.setTitle(title);
        topic.setDescription(description);
        return topic;
    }
}
