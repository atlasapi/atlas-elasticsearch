package org.atlasapi.media.content.schedule;

import static com.metabroadcast.common.time.DateTimeZones.UTC;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;

import org.atlasapi.media.util.ElasticSearchHelper;
import org.elasticsearch.node.Node;
import org.joda.time.DateTime;
import org.junit.Test;

import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.TimeMachine;

public class EsScheduleIndexNamesTest {

    private final Node esClient = ElasticSearchHelper.testNode();
    private final DateTime time = new DateTime(2012, 11, 17, 0, 0, 0, 0, UTC);
    private final Clock clock = new TimeMachine(time);
    private final EsScheduleIndexNames scheduleNames = new EsScheduleIndexNames(esClient, clock);
    
    @Test
    public void testGetsMonthAndYearIndexForDatesWithinYear() {
        DateTime start = new DateTime(2012, 10, 1, 12, 0, 0, 0, UTC);
        DateTime end = new DateTime(2012, 10, 1, 13, 0, 0, 0, UTC);
        assertThat(scheduleNames.indexingNamesFor(start, end), 
            hasItems("schedule-2012-10", "schedule-2012"));
    }

    @Test
    public void testGetsYearIndexOnlyForDatesOlderThanYear() {
        DateTime start = new DateTime(2010, 10, 1, 12, 0, 0, 0, UTC);
        DateTime end = new DateTime(2010, 10, 1, 13, 0, 0, 0, UTC);
        assertThat(scheduleNames.indexingNamesFor(start, end), 
            hasItems("schedule-2010"));
    }

    @Test
    public void testGetsMonthAndYearIndexWhenOnlyOneDateWithinYear() {
        DateTime start = new DateTime(2011, 11, 16, 0, 0, 0, 0, UTC);
        DateTime end = new DateTime(2011, 11, 17, 12, 0, 0, 0, UTC);
        assertThat(scheduleNames.indexingNamesFor(start, end), 
            hasItems("schedule-2011-11", "schedule-2011"));
    }

    @Test
    public void testGetsBothMonthAndYearIndexWhenStartEndInDifferentYears() {
        DateTime start = new DateTime(2011, 11, 16, 0, 0, 0, 0, UTC);
        DateTime end = new DateTime(2012, 11, 17, 12, 0, 0, 0, UTC);
        assertThat(scheduleNames.indexingNamesFor(start, end), 
            hasItems("schedule-2011-11", "schedule-2011", 
                "schedule-2012-11", "schedule-2012"));
    }
    
    @Test
    public void testGetIntermediateMonthsWhenSpansMoreThanOne() {
        DateTime start = new DateTime(2012, 9, 16, 0, 0, 0, 0, UTC);
        DateTime end = new DateTime(2012, 11, 17, 12, 0, 0, 0, UTC);
        assertThat(scheduleNames.indexingNamesFor(start, end), 
            hasItems("schedule-2012-11",   "schedule-2012-09",
                "schedule-2012-10", "schedule-2012"));
    }
    
    @Test
    public void testGetsIntermediateMonthsForQueryNamesWithinYear() {
        DateTime start = new DateTime(2012, 9, 16, 0, 0, 0, 0, UTC);
        DateTime end = new DateTime(2012, 11, 17, 12, 0, 0, 0, UTC);
        assertThat(scheduleNames.queryingNamesFor(start, end), 
            hasItems("schedule-2012-09", "schedule-2012-10", 
                "schedule-2012-11"));
    }

    @Test
    public void testGetsIntermediateYearsForQueryNamesOlderThanYear() {
        DateTime start = new DateTime(2009, 9, 16, 0, 0, 0, 0, UTC);
        DateTime end = new DateTime(2012, 11, 17, 12, 0, 0, 0, UTC);
        assertThat(scheduleNames.queryingNamesFor(start, end), 
            hasItems("schedule-2009", "schedule-2012", 
                    "schedule-2011", "schedule-2010"));
    }

}
