package org.atlasapi.media.util;

import static org.atlasapi.media.util.ElasticSearchHelper.refresh;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Set;
import java.util.UUID;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.content.criteria.AttributeQuery;
import org.atlasapi.content.criteria.AttributeQuerySet;
import org.atlasapi.content.criteria.attribute.Attribute;
import org.atlasapi.content.criteria.attribute.BooleanValuedAttribute;
import org.atlasapi.content.criteria.attribute.DateTimeValuedAttribute;
import org.atlasapi.content.criteria.attribute.EnumValuedAttribute;
import org.atlasapi.content.criteria.attribute.FloatValuedAttribute;
import org.atlasapi.content.criteria.attribute.IdAttribute;
import org.atlasapi.content.criteria.attribute.IntegerValuedAttribute;
import org.atlasapi.content.criteria.attribute.StringValuedAttribute;
import org.atlasapi.content.criteria.operator.Operators;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Identified;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHits;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;

public class EsQueryBuilderTest {

    private static final String INDEX = "test";
    private static final String TYPE = "test";
    
    private static enum TestEnum {
        INSTANCE;
    }
    
    private final Attribute<Integer> MINUS_ONE = 
        new IntegerValuedAttribute("minusone", Identified.class, true);
    private final Attribute<String> ZERO = 
        new StringValuedAttribute("zero", Identified.class, true);
    private final Attribute<String> ONE_FIRST = 
        new StringValuedAttribute("one.first", Identified.class, true);
    private final Attribute<String> ONE_SECOND = 
        new StringValuedAttribute("one.second", Identified.class, true);
    private final Attribute<Float> ONE_TWO_FIRST = 
        new FloatValuedAttribute("one.two.first", Identified.class, true);
    private final Attribute<TestEnum> ONE_TWO_SECOND = 
        new EnumValuedAttribute<TestEnum>("one.two.second", TestEnum.class, Identified.class, true);
    private final Attribute<Boolean> ONE_TWO_THREE_FIRST = 
        new BooleanValuedAttribute("one.two.three.first", Identified.class, true);
    private final Attribute<DateTime> ONE_TWO_THREE_SECOND = 
        new DateTimeValuedAttribute("one.two.three.second", Identified.class);
    private final Attribute<Id> ONE_TWO_THREE_THIRD = 
        new IdAttribute("one.two.three.third", Identified.class, true);

    private final EsQueryBuilder builder = new EsQueryBuilder();

    private final Node esClient = NodeBuilder.nodeBuilder()
        .local(true).clusterName(UUID.randomUUID().toString())
        .build().start();

    @BeforeClass
    public static void before() throws Exception {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
            new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
    }

    @After
    public void tearDown() throws Exception {
        IndicesStatusRequest req = Requests.indicesStatusRequest((String[])
            null);
        IndicesStatusResponse statuses =
            esClient.client().admin().indices().status(req).actionGet();
        for (String index : statuses.getIndices().keySet()) {
            esClient.client()
                .admin()
                .indices()
                .delete(Requests.deleteIndexRequest(index))
                .actionGet();
        }
        esClient.close();
    }

    @Before
    public void setup() throws Exception {
        createIndex(esClient, INDEX).actionGet();

        putMapping(esClient, INDEX, TYPE,
            Resources.toString(Resources.getResource("es-query-builder-schema.json"), Charsets.UTF_8)).actionGet();

        index(esClient, INDEX, TYPE, "one", Resources.toString(Resources.getResource("es-query-builder-object.json"), Charsets.UTF_8)).actionGet();

        refresh(esClient);

    }

    @Test
    public void testSingleTopLevelQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
            createQuery(ZERO, "one")
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }
    
    @Test
    public void testTwoTopLevelQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
            createQuery(ZERO, "one"),
            createQuery(MINUS_ONE, -1)
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testSingleNestedQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
            createQuery(ONE_FIRST, "one-first-one")
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }
    
    @Test
    public void testTopAndNestedQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
            createQuery(ZERO, "one"),
            createQuery(ONE_FIRST, "one-first-one")
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testTwoNestedQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
            createQuery(ONE_FIRST, "one-first-one"),
            createQuery(ONE_SECOND, "one-second-one")
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testDoublyNestedQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
            createQuery(ONE_TWO_FIRST, 1.0f)
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testTriplyNestedQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
            createQuery(ONE_TWO_THREE_FIRST, true)
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testTriplyNestedWithTopQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
            createQuery(ZERO, "one"),
            createQuery(ONE_TWO_THREE_FIRST, true)
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }
    
    @Test
    public void testAllTheAttributes() throws Exception {
        ImmutableSet<AttributeQuery<?>> attrQueries = ImmutableSet.<AttributeQuery<?>>of(
            createQuery(ZERO, "one"),
            createQuery(MINUS_ONE, -1),
            createQuery(ONE_FIRST, "one-first-three"),
            createQuery(ONE_SECOND, "one-second-three"),
            createQuery(ONE_TWO_FIRST, 1.0f),
            createQuery(ONE_TWO_SECOND, TestEnum.INSTANCE),
            createQuery(ONE_TWO_THREE_FIRST, true),
            createQuery(ONE_TWO_THREE_SECOND, new DateTime("1987-02-02T14:30:00.000Z")),
            createQuery(ONE_TWO_THREE_THIRD, Id.valueOf(1234))
        );
        for (Set<AttributeQuery<?>> queries : Iterables.skip(Sets.powerSet(attrQueries),1)) {
            AttributeQuerySet set = new AttributeQuerySet(queries);
            SearchHits hits = queryHits(set);
            assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
        }
    }
    
    @Test
    public void testPrefixQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
            ZERO.createQuery(Operators.BEGINNING, ImmutableList.of("on"))
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }
    
    @Test
    public void testDateTimeAfterQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
            ONE_TWO_THREE_SECOND.createQuery(Operators.AFTER,
                ImmutableList.of(new DateTime("1986-02-02T14:30:00.000Z")))
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }
    
    @Test
    public void testDateTimeBeforeQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
                ONE_TWO_THREE_SECOND.createQuery(Operators.BEFORE,
                        ImmutableList.of(new DateTime("1988-02-02T14:30:00.000Z")))
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }
    
    @Test
    public void testIntegerGreaterThanQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
            MINUS_ONE.createQuery(Operators.GREATER_THAN, ImmutableList.of(-2))
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }
    
    @Test
    public void testIntegerLessThanQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
            MINUS_ONE.createQuery(Operators.LESS_THAN, ImmutableList.of(0))
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }
    
    private <T> AttributeQuery<?> createQuery(Attribute<T> attr, T... vals) {
        return attr.createQuery(Operators.EQUALS, ImmutableList.copyOf(vals));
    }
    
    private SearchHits queryHits(AttributeQuerySet query) throws Exception {
        return esClient.client().prepareSearch()
            .setQuery(builder.buildQuery(query))
            .execute().get().getHits();
    }

    private ActionFuture<CreateIndexResponse> createIndex(Node client, String index) {
        return client.client().admin().indices().create(
            Requests.createIndexRequest(index)
        );
    }
    
    private ActionFuture<PutMappingResponse> putMapping(Node client, 
            String index, String type, String mapping) {
        return esClient.client().admin().indices().putMapping(
            Requests.putMappingRequest(INDEX).type(TYPE).source(mapping)
        );
    }
    
    private ActionFuture<IndexResponse> index(Node esClient, String index,
        String type, String id, String object) {
        return esClient.client().index(
            Requests.indexRequest().index(INDEX).type(TYPE).id(id).source(object)
        );
    }

}
