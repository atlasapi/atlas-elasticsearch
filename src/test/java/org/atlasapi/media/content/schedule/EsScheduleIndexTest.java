package org.atlasapi.media.content.schedule;

import static com.metabroadcast.common.time.DateTimeZones.UTC;
import static org.atlasapi.media.entity.Publisher.METABROADCAST;
import static org.atlasapi.media.util.ElasticSearchHelper.refresh;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.media.EsSchema;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.EsContentIndexer;
import org.atlasapi.media.content.schedule.ScheduleRef.ScheduleRefEntry;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.util.ElasticSearchHelper;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.TimeMachine;

@RunWith(MockitoJUnitRunner.class)
public class EsScheduleIndexTest {

    private final Node esClient = ElasticSearchHelper.testNode();
    private final Clock clock = new TimeMachine(new DateTime(2012,11,19,10,10,10,10,DateTimeZones.UTC));
    private final EsScheduleIndex scheduleIndex = new EsScheduleIndex(esClient, clock);
    private final EsContentIndexer contentIndexer = new EsContentIndexer(esClient, EsSchema.CONTENT_INDEX, clock, 60000);

    private final Channel channel1 = Channel.builder().withUri("http://www.bbc.co.uk/services/bbcone").build();
    private final Channel channel2 = Channel.builder().withUri("http://www.bbc.co.uk/services/bbctwo").build();
    
    @BeforeClass
    public static void before() throws Exception {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
            new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
    }
    
    @Before
    public void setUp() throws Exception {
        contentIndexer.startAndWait();
    }
    
    @After
    public void tearDown() throws Exception {
        IndicesStatusRequest req = Requests.indicesStatusRequest((String[]) null);
        IndicesStatusResponse statuses = esClient.client().admin().indices().status(req).actionGet();
        for (String index : statuses.getIndices().keySet()) {
            esClient.client().admin().indices().delete(Requests.deleteIndexRequest(index)).actionGet();
        }
        esClient.close();
    }
    
    @Test
    public void testReturnsContentContainedInInterval() throws Exception {
        
        Item contained = itemWithBroadcast(1L, "contained", channel1.getCanonicalUri(), new DateTime(10, UTC), new DateTime(20, UTC));
        
        contentIndexer.index(contained);
        refresh(esClient);
        scheduleIndex.updateExistingIndices();
        
        Interval interval = new Interval(new DateTime(00, UTC), new DateTime(30, UTC));
        Future<ScheduleRef> futureEntries = scheduleIndex.resolveSchedule(METABROADCAST, channel1, interval);
        
        ScheduleRef scheduleRef  = futureEntries.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(1));
        assertThat(entries.get(0).getItemId(), is(contained.getId()));
        
    }


    
    @Test
    public void testReturnsContentOverlappingInterval() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);

        Item overlapStart = itemWithBroadcast(1L, "overlapStart", channel1.getCanonicalUri(), start, start.plusHours(1));
        Item overlapEnd = itemWithBroadcast(2L, "overlapEnd", channel1.getCanonicalUri(), start.plusHours(2), start.plusHours(3));
        
        contentIndexer.index(overlapEnd);
        contentIndexer.index(overlapStart);
        refresh(esClient);
        scheduleIndex.updateExistingIndices();

        ListenableFuture<ScheduleRef> futureEntries = scheduleIndex.resolveSchedule(METABROADCAST, channel1, new Interval(start.plusMinutes(30), start.plusMinutes(150)));
        
        ScheduleRef scheduleRef  = futureEntries.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(2));
        assertThat(entries.get(0).getItemId(), is(overlapStart.getId()));
        assertThat(entries.get(1).getItemId(), is(overlapEnd.getId()));
    }
    
    @Test
    public void testReturnsContentContainingInterval() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);

        Item containsInterval = itemWithBroadcast(1L, "contains", channel1.getCanonicalUri(), start, start.plusHours(3));
        
        contentIndexer.index(containsInterval);
        refresh(esClient);
        scheduleIndex.updateExistingIndices();
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, new Interval(start.plusMinutes(30), start.plusMinutes(150)));

        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(1));
        assertThat(entries.get(0).getItemId(), is(containsInterval.getId()));
    }

    @Test
    public void testDoesntReturnContentOnDifferentChannel() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);

        Item wrongChannel = itemWithBroadcast(1L, "wrong", "http://www.bbc.co.uk/services/bbctwo", start.plusHours(1), start.plusHours(2));
        
        contentIndexer.index(wrongChannel);
        refresh(esClient);
        scheduleIndex.updateExistingIndices();
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, new Interval(start.plusMinutes(30), start.plusMinutes(150)));
        
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(0));
    }
    
    @Test
    public void testDoesntReturnContentOutsideInterval() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);

        Item tooLate = itemWithBroadcast(1L, "late", channel1.getCanonicalUri(), start.plusHours(3), start.plusHours(4));
      
        contentIndexer.index(tooLate);
        refresh(esClient);
        scheduleIndex.updateExistingIndices();
          
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, new Interval(start.plusMinutes(30), start.plusMinutes(150)));
          
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
          
        assertThat(entries.size(), is(0));
    }

    @Test
    public void testReturnsContentContainingInstanceInterval() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);
        
        Item containsInstance = itemWithBroadcast(1L, "late", channel1.getCanonicalUri(), start.minusHours(1), start.plusHours(4));
        
        contentIndexer.index(containsInstance);
        refresh(esClient);
        scheduleIndex.updateExistingIndices();
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, new Interval(start, start));
        
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(1));
    }

    @Test
    public void testReturnsContentAbuttingInstanceIntervalEnd() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);
        
        Item abutting = itemWithBroadcast(1L, "late", channel1.getCanonicalUri(), start, start.plusHours(4));
        
        contentIndexer.index(abutting);
        refresh(esClient);
        scheduleIndex.updateExistingIndices();
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, new Interval(start, start));
        
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(1));
    }

    @Test
    public void testDoesntReturnContentAbuttingInstanceIntervalStart() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);
        
        Item abutting = itemWithBroadcast(1L, "late", channel1.getCanonicalUri(), start.minusHours(1), start);
        
        contentIndexer.index(abutting);
        refresh(esClient);
        scheduleIndex.updateExistingIndices();
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, new Interval(start, start));
        
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(0));
    }
    
    @Test
    public void testReturnsContentMatchingIntervalExactly() throws Exception {
        
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        
        Item exactMatch = itemWithBroadcast(Long.MAX_VALUE-1, "exact", channel1.getCanonicalUri(), interval.getStart(), interval.getEnd());
        
        contentIndexer.index(exactMatch);
        refresh(esClient);
        scheduleIndex.updateExistingIndices();
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, interval);
        
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(1));
        assertThat(entries.get(0).getItemId(), is(exactMatch.getId()));
        
    }
    
    @Test
    public void testOnlyReturnsExactlyMatchingScheduleRef() throws Exception {
        
        Interval interval1 = new Interval(0, 100, DateTimeZones.UTC);
        Interval interval2 = new Interval(150, 200, DateTimeZones.UTC);
        
        Item itemWith2Broadcasts = itemWithBroadcast(1L, "exact", channel1.getCanonicalUri(), interval1.getStart(), interval1.getEnd());
        Broadcast broadcast = new Broadcast(channel2.getCanonicalUri(), interval2.getStart(), interval2.getEnd());
        Iterables.getOnlyElement(itemWith2Broadcasts.getVersions()).addBroadcast(broadcast);
        
        contentIndexer.index(itemWith2Broadcasts);
        refresh(esClient);
        scheduleIndex.updateExistingIndices();
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, interval2);
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        
        assertThat(scheduleRef.getScheduleEntries().size(), is(0));

        futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel2, interval1);
        scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);

        assertThat(scheduleRef.getScheduleEntries().size(), is(0));

        futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, interval1);
        scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        
        assertThat(scheduleRef.getScheduleEntries().size(), is(1));
        
        futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel2, interval2);
        scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        
        assertThat(scheduleRef.getScheduleEntries().size(), is(1));
    }
    
    @Test
    public void testThatItemAppearingTwiceInScheduleGetsTwoEntries() throws Exception {
        
        Interval interval1 = new Interval(0, 100, DateTimeZones.UTC);
        Interval interval2 = new Interval(150, 200, DateTimeZones.UTC);
        
        Item itemWith2Broadcasts = itemWithBroadcast(1L, "exact", channel1.getCanonicalUri(), interval1.getStart(), interval1.getEnd());
        Broadcast broadcast = new Broadcast(channel1.getCanonicalUri(), interval2.getStart(), interval2.getEnd());
        Iterables.getOnlyElement(itemWith2Broadcasts.getVersions()).addBroadcast(broadcast);
        
        contentIndexer.index(itemWith2Broadcasts);
        refresh(esClient);
        scheduleIndex.updateExistingIndices();
        
        Interval queryInterval = new Interval(interval1.getStartMillis(), interval2.getEndMillis(), DateTimeZones.UTC);
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, queryInterval);
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        
        assertThat(scheduleRef.getScheduleEntries().size(), is(2));
        
    }
    
    @Test
    public void testFindsBothChildrenAndTopLevelItems() throws Exception {
        
        Interval interval1 = new Interval(0, 100, DateTimeZones.UTC);
        Interval interval2 = new Interval(150, 200, DateTimeZones.UTC);
        
        Item childItem = itemWithBroadcast(1L, "exactone", channel1.getCanonicalUri(), interval1.getStart(), interval1.getEnd());
        Brand container = new Brand("brandUri","brandCurie",METABROADCAST);
        container.setId(Id.valueOf(4L));
        childItem.setContainer(container);
        
        Item topItem = itemWithBroadcast(2L, "exacttwo", channel1.getCanonicalUri(), interval2.getStart(), interval2.getEnd());
        
        contentIndexer.index(childItem);
        contentIndexer.index(topItem);
        refresh(esClient);
        scheduleIndex.updateExistingIndices();
        
        Interval queryInterval = new Interval(interval1.getStartMillis(), interval2.getEndMillis(), DateTimeZones.UTC);
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, queryInterval);
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        
        assertThat(scheduleRef.getScheduleEntries().size(), is(2));
        
    }
    
    @Test
    public void testRecentlyBroadcastItems() throws Exception {
        
        Interval interval = new Interval(clock.now().minusMonths(2), clock.now().plusMonths(1));
        
        Item recentItem = itemWithBroadcast(1L, "recent", channel1.getCanonicalUri(), interval.getStart(), interval.getEnd());
        
        contentIndexer.index(recentItem);
        refresh(esClient);
        
        scheduleIndex.updateExistingIndices();
        
        Interval queryInterval = new Interval(clock.now().minusMonths(3).getMillis(), clock.now().plusMonths(2).getMillis(), DateTimeZones.UTC);
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, queryInterval);
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        
        assertThat(scheduleRef.getScheduleEntries().size(), is(1));
        
    }
     
    private Item itemWithBroadcast(Long id, String itemUri, String channelUri, DateTime start, DateTime end) {
        
        Broadcast broadcast = new Broadcast(channelUri, start, end);
        Version version = new Version();
        version.addBroadcast(broadcast);

        Item item = new Item(itemUri, itemUri, Publisher.METABROADCAST);
        item.setId(Id.valueOf(id));
        item.addVersion(version);
        
        return item;
    }
    
}
