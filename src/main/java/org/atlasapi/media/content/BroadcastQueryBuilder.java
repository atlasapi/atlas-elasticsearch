package org.atlasapi.media.content;

import java.util.concurrent.TimeUnit;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.DateTime;

public class BroadcastQueryBuilder {

    public static QueryBuilder build(QueryBuilder childQuery, float timeBoost, float firstBroadcastBoost) {
        return QueryBuilders.customScoreQuery(QueryBuilders.filteredQuery(childQuery,
                FilterBuilders.nestedFilter(EsContent.BROADCASTS,
                FilterBuilders.rangeFilter(EsBroadcast.TRANSMISSION_TIME).from(new DateTime().minusDays(30)).to(new DateTime().plusDays(30))))).
                param("firstBroadcastBoost", firstBroadcastBoost).
                param("timeBoost", timeBoost).
                param("oneWeek", TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS)).
                script(""
                + "if (_source.broadcasts != null) {"
                + "  now = time();"
                + "  t = Long.MAX_VALUE;"
                + "  f = 1;"
                + "  foreach (b : _source.broadcasts) {"
                + "    candidate = abs(now - b.transmissionTimeInMillis);"
                + "    if (candidate < t) t = candidate;"
                + "    if (b.repeat = false) f = firstBroadcastBoost;"
                + "  }"
                + "  _score + (_score * f * timeBoost * (1 / (1 + (t / (t < oneWeek ? 50 : 1)))));"
                + "} else _score;");
    }
}
