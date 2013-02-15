package org.atlasapi.media.content.schedule;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.elasticsearch.client.Requests.indicesStatusRequest;

import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.node.Node;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;

public class EsScheduleIndexNames {

    private static final String prefix = "schedule";
    private static final String yearFormat = prefix+"-%04d";
    private static final String monthFormat = prefix+"-%04d-%02d";
    
    private final Node esClient;
    private final Clock clock;

    public EsScheduleIndexNames(Node esClient, Clock clock) {
        this.esClient = checkNotNull(esClient);
        this.clock = checkNotNull(clock);
    }
    
    public ImmutableSet<String> existingIndexNames() {
        IndicesStatusRequest req = indicesStatusRequest((String[]) null);
        IndicesStatusResponse statuses = esClient.client().admin()
                .indices().status(req).actionGet();
        Builder<String> names = ImmutableSet.builder();
        for (String index : statuses.getIndices().keySet()) {
            if (index.startsWith(prefix)) {
                names.add(index);
            }
        }
        return names.build();
    }
    
    public ImmutableSet<String> indexingNamesFor(DateTime start, DateTime end) {
        start = start.withZone(DateTimeZones.UTC);
        end = end.withZone(DateTimeZones.UTC);
        
        Builder<String> names = ImmutableSet.builder();
        names.add(yearIndex(start)).add(yearIndex(end));
        
        DateTime aYearAgo = clock.now().minusYears(1);
        if (start.isAfter(aYearAgo) || end.isAfter(aYearAgo)) {
            for (DateTime cur = start; cur.isBefore(end); cur = cur.plusMonths(1)) {
                names.add(monthIndex(cur));
            }
        }
        
        return names.build();
    }
    
    public ImmutableSet<String> queryingNamesFor(DateTime start, DateTime end) {
        start = start.withZone(DateTimeZones.UTC);
        end = end.withZone(DateTimeZones.UTC);
        
        Builder<String> names = ImmutableSet.builder();

        DateTime aYearAgo = clock.now().minusYears(1);
        if (start.isAfter(aYearAgo)) {
            for (DateTime cur = start; cur.isBefore(end) || cur.isEqual(end); cur = cur.plusMonths(1)) {
                names.add(monthIndex(cur));
            }
        } else {
            for (DateTime cur = start; cur.isBefore(end) || cur.isEqual(end); cur = cur.plusYears(1)) {
                names.add(yearIndex(cur));
            }
        }
        
        return names.build();
    }

    private String monthIndex(DateTime dateTime) {
        return String.format(monthFormat, 
                dateTime.getYear(), dateTime.getMonthOfYear());
    }

    private String yearIndex(DateTime dateTime) {
        return String.format(yearFormat, dateTime.getYear());
    }

}
