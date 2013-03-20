package org.atlasapi.media.content.schedule;

import static org.atlasapi.media.content.EsBroadcast.CHANNEL;
import static org.atlasapi.media.content.EsBroadcast.ID;
import static org.atlasapi.media.content.EsBroadcast.TRANSMISSION_END_TIME;
import static org.atlasapi.media.content.EsBroadcast.TRANSMISSION_TIME;
import static org.atlasapi.media.content.EsContent.BROADCASTS;
import static org.atlasapi.media.content.EsContent.CHILD_TYPE;
import static org.atlasapi.media.content.EsContent.PUBLISHER;
import static org.atlasapi.media.content.EsContent.TOP_LEVEL_TYPE;
import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.nestedFilter;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.content.schedule.ScheduleRef.ScheduleRefEntry;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.util.FutureSettingActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metabroadcast.common.caching.BackgroundTask;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;

public class EsScheduleIndex implements ScheduleIndex {

    //defines the maximum number of entries per day.
    private static final int SIZE_MULTIPLIER = 100;

    public static final Logger log = LoggerFactory.getLogger(EsScheduleIndex.class);
    
    private static final String BROADCAST_ID = BROADCASTS+"."+ID;
    private static final String BROADCAST_CHANNEL = BROADCASTS+"."+CHANNEL;
    private static final String BROADCAST_TRANSMISSION_TIME = BROADCASTS+"."+TRANSMISSION_TIME;
    private static final String BROADCAST_TRANSMISSION_END_TIME = BROADCASTS+"."+TRANSMISSION_END_TIME;
    
    private static final String[] FIELDS = new String[]{
        ID,
        BROADCASTS,
        BROADCAST_ID,
        BROADCAST_CHANNEL,
        BROADCAST_TRANSMISSION_TIME,
        BROADCAST_TRANSMISSION_END_TIME,
    };
    
    private final Node esClient;
    private final EsScheduleIndexNames scheduleNames;
    private final AtomicReference<Set<String>> existingIndices;
    private final BackgroundTask updateTask;

    public EsScheduleIndex(Node esClient, Clock clock) {
        this.esClient = esClient;
        this.scheduleNames = new EsScheduleIndexNames(esClient, clock);
        this.existingIndices = new AtomicReference<Set<String>>();
        this.updateTask = new BackgroundTask(Duration.standardHours(1), new Runnable() {
            @Override
            public void run() {
                updateExistingIndices();
            }
        });
        updateTask.start(true);
    }
    
    public void updateExistingIndices() {
        this.existingIndices.set(scheduleNames.existingIndexNames());
    }
    
    @Override
    public ListenableFuture<ScheduleRef> resolveSchedule(Publisher publisher, Channel channel, Interval scheduleInterval) {
        String broadcastOn = channel.getCanonicalUri();
        String pub = publisher.key();
        
        SettableFuture<SearchResponse> result = SettableFuture.create();
        
        String[] queryIndices = indicesFor(scheduleInterval);
        if (queryIndices == null ) { // there are no existing indices for this request
            return Futures.immediateFuture(ScheduleRef.forChannel(broadcastOn).build());
        }
        esClient.client()
            .prepareSearch(queryIndices)
            .setTypes(TOP_LEVEL_TYPE, CHILD_TYPE)
            .setSearchType(SearchType.DEFAULT)
            .setQuery(scheduleQueryFor(pub, broadcastOn, scheduleInterval))
            .addFields(FIELDS)
            .setSize(SIZE_MULTIPLIER * daysIn(scheduleInterval))
            .execute(FutureSettingActionListener.setting(result));
        
        return Futures.transform(result, resultTransformer(broadcastOn, ScheduleBroadcastFilter.valueOf(scheduleInterval)));
    }
    
    /* Take the intersection here to avoid missing index problems.
     */
    private String[] indicesFor(Interval interval) {
        Set<String> indices = Sets.intersection(
            existingIndices.get(),
            scheduleNames.queryingNamesFor(interval.getStart(), interval.getEnd()) 
        );
        if (indices.isEmpty()) {
            return null;
        }
        return indices.toArray(new String[indices.size()]);
    }

    private int daysIn(Interval scheduleInterval) {
        return Math.max(1, Days.daysIn(scheduleInterval).getDays());
    }

    private QueryBuilder scheduleQueryFor(String publisher, String broadcastOn, Interval scheduleInterval) {
        DateTime looseFrom = scheduleInterval.getStart().minusHours(12);
        DateTime looseTo = scheduleInterval.getEnd().plusHours(12);

        return filteredQuery(
            boolQuery()
                .must(termQuery(PUBLISHER, publisher))
                .must(nestedQuery(BROADCASTS,
                    boolQuery()
                        .must(termQuery(CHANNEL, broadcastOn))
                        .must(rangeQuery(TRANSMISSION_TIME).gte(looseFrom.toDate()))
                        .must(rangeQuery(TRANSMISSION_END_TIME).lte(looseTo.toDate()))
                )),
            nestedFilter(BROADCASTS, andFilter(
                termFilter(CHANNEL, broadcastOn),
                filterForInterval(scheduleInterval)
            ))
        );
    }

    private FilterBuilder filterForInterval(Interval scheduleInterval) {
        Date fromDate = scheduleInterval.getStart().toDate();
        Date toDate = scheduleInterval.getEnd().toDate();
        
        if (Duration.ZERO.equals(scheduleInterval.toDuration())) {
            return andFilter(
                rangeFilter(TRANSMISSION_TIME).lte(fromDate),
                rangeFilter(TRANSMISSION_END_TIME).gt(toDate)
            );
        }
        // this is an adaptation of joda's Interval.overlaps which is fine when
        // the scheduleInterval is non-empty
        // (otherStart < thisEnd && otherEnd > thisStart);
        return andFilter(
            rangeFilter(TRANSMISSION_TIME).lt(toDate),
            rangeFilter(TRANSMISSION_END_TIME).gt(fromDate)
        );
    }

    private Function<SearchResponse, ScheduleRef> resultTransformer(final String channel, final ScheduleBroadcastFilter scheduleBroadcastFilter) {
        return new Function<SearchResponse, ScheduleRef>() {
            @Override
            public ScheduleRef apply(@Nullable SearchResponse input) {
                ScheduleRef.Builder refBuilder = ScheduleRef.forChannel(channel);
                int hits = 0;
                for (SearchHit hit : input.getHits()) {
                    hits++;
                    refBuilder.addEntries(validEntries(hit,channel, scheduleBroadcastFilter));
                }
                ScheduleRef ref = refBuilder.build();
                log.info("{}: {} hits => {} entries, ({} queries, {}ms)", new Object[]{Thread.currentThread().getId(), hits, ref.getScheduleEntries().size(), 1, input.getTookInMillis()});
                return ref;
            }
        };
    }
    
    private Iterable<ScheduleRefEntry> validEntries(SearchHit hit, String channel, ScheduleBroadcastFilter scheduleBroadcastFilter) {
        ImmutableList.Builder<ScheduleRefEntry> entries = ImmutableList.builder();
        
        SearchHitField broadcastsHitField = hit.field(BROADCASTS);
        Long id = hit.field(ID).<Number>value().longValue();
        List<Object> fieldValues = broadcastsHitField.getValues();
        for (List<?> fieldValue : Iterables.filter(fieldValues, List.class)) {
            for (Map<Object,Object> broadcast : Iterables.filter(fieldValue, Map.class)) {
                ScheduleRefEntry validRef = getValidRef(id, channel, scheduleBroadcastFilter, broadcast);
                if (validRef != null) {
                    entries.add(validRef);
                }
            }
        }        
        return entries.build();
    }

    private ScheduleRefEntry getValidRef(Long id, String channel, ScheduleBroadcastFilter scheduleBroadcastFilter, Map<Object, Object> broadcast) {
        String broadcastChannel = (String) broadcast.get(CHANNEL);
        if (channel.equals(broadcastChannel)) {
            DateTime start = new DateTime(broadcast.get(TRANSMISSION_TIME)).toDateTime(DateTimeZones.UTC);
            DateTime end = new DateTime(broadcast.get(TRANSMISSION_END_TIME)).toDateTime(DateTimeZones.UTC);
            if (valid(scheduleBroadcastFilter, start, end)) {
                String broadcastId = (String) broadcast.get(BROADCAST_ID);
                return new ScheduleRefEntry(id, channel, start, end, broadcastId);
            }
        }
        return null;
    }

    private boolean valid(ScheduleBroadcastFilter scheduleBroadcastFilter, DateTime start, DateTime end) {
        return scheduleBroadcastFilter.apply(new Interval(start, end));
    }

}
